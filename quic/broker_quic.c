/*
 * broker_quic.c — Broker del sistema publicación-suscripción (versión QUIC, bono)
 *
 * QUIC real (RFC 9000) exige TLS 1.3, IDs de conexión, streams, etc., y no
 * puede implementarse solo con la librería estándar de C. Este programa
 * implementa un protocolo de transporte confiable "estilo QUIC" a nivel de
 * aplicación, SOBRE UDP, que reproduce las ideas centrales de QUIC:
 *
 *   - Handshake de conexión antes de enviar datos (SUBSCRIBE / ACK:SUBSCRIBE).
 *   - Números de secuencia por conexión (cada suscriptor tiene su propio
 *     contador, como los packet numbers de una conexión QUIC).
 *   - ACKs explícitos y retransmisión por timeout (confiabilidad).
 *   - Detección de duplicados (retransmisiones del publisher).
 *   - Reordenamiento en el receptor (lo hace el subscriber).
 *   - Cierre de la conexión por inactividad si el par deja de responder.
 *
 * Protocolo (texto plano, un mensaje por datagrama):
 *   Publisher  -> Broker : PUB:<seq_pub>:<tema>:<mensaje>
 *   Broker     -> Pub    : ACK:PUB:<seq_pub>
 *   Subscriber -> Broker : SUBSCRIBE:<tema1>[,<tema2>,...]
 *   Broker     -> Sub    : ACK:SUBSCRIBE
 *   Broker     -> Sub    : DATA:<seq_sub>:<tema>:<mensaje>
 *   Subscriber -> Broker : ACK:DATA:<seq_sub>
 *
 * Uso: ./broker_quic [porcentaje_perdida]
 *   porcentaje_perdida (0-90, opcional): descarta al azar ese porcentaje de
 *   datagramas entrantes y salientes para poder observar en Wireshark las
 *   retransmisiones y el reordenamiento. Por defecto 0.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <arpa/inet.h>

#define PORT_BROKER     9004
#define MAX_SUBS        50
#define MAX_PUBS        50
#define MAX_PENDING     1024
#define BUF_SIZE        1024
#define TOPICS_SIZE     256
#define RTO_MS          500   /* tiempo de retransmisión */
#define MAX_RETRIES     5     /* tras esto se cierra la conexión del suscriptor */

typedef struct {
    struct sockaddr_in addr;
    char topics[TOPICS_SIZE];  /* lista de temas separados por coma */
    int  active;
    int  next_seq;             /* próximo número de secuencia para este suscriptor */
} Subscriber;

typedef struct {
    struct sockaddr_in addr;
    int last_seq;              /* último seq_pub aceptado (para descartar duplicados) */
    int used;
} Publisher;

typedef struct {
    int  used;
    int  sub;                  /* índice del suscriptor destino */
    int  seq;
    char data[BUF_SIZE];
    long last_sent_ms;
    int  retries;
} Pending;

static Subscriber subs[MAX_SUBS];
static int        sub_count = 0;
static Publisher  pubs[MAX_PUBS];
static Pending    pending[MAX_PENDING];
static int        loss_pct = 0;
static int        sock;

static long now_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000L + tv.tv_usec / 1000;
}

static int perdido(void) {
    return loss_pct > 0 && (rand() % 100) < loss_pct;
}

static int same_addr(const struct sockaddr_in *a, const struct sockaddr_in *b) {
    return a->sin_addr.s_addr == b->sin_addr.s_addr && a->sin_port == b->sin_port;
}

/* Envía un datagrama aplicando (si está activada) la pérdida simulada. */
static void enviar(const char *msg, const struct sockaddr_in *dst) {
    if (perdido()) {
        printf("[BROKER QUIC] (simulado) se pierde datagrama saliente: %s\n", msg);
        return;
    }
    /* sendto(): envía un datagrama UDP a la dirección dst. Al no haber
       conexión a nivel de UDP, cada envío indica explícitamente el destino. */
    sendto(sock, msg, strlen(msg), 0, (const struct sockaddr *)dst, sizeof(*dst));
}

/* ¿El suscriptor está suscrito a este tema? (lista separada por comas) */
static int tiene_tema(const char *lista, const char *tema) {
    char copia[TOPICS_SIZE];
    strncpy(copia, lista, sizeof(copia) - 1);
    copia[sizeof(copia) - 1] = '\0';
    for (char *t = strtok(copia, ","); t; t = strtok(NULL, ","))
        if (strcmp(t, tema) == 0) return 1;
    return 0;
}

static void manejar_subscribe(const char *temas, const struct sockaddr_in *cli) {
    int idx = -1;
    for (int i = 0; i < sub_count; i++)
        if (subs[i].active && same_addr(&subs[i].addr, cli)) { idx = i; break; }

    if (idx < 0) {
        /* Reutiliza un espacio libre (suscriptor desconectado) o crea uno nuevo */
        for (int i = 0; i < sub_count; i++)
            if (!subs[i].active) { idx = i; break; }
        if (idx < 0) {
            if (sub_count >= MAX_SUBS) {
                printf("[BROKER QUIC] Máximo de suscriptores alcanzado\n");
                return;
            }
            idx = sub_count++;
        }
        subs[idx].addr     = *cli;
        subs[idx].active   = 1;
        subs[idx].next_seq = 1;
        strncpy(subs[idx].topics, temas, TOPICS_SIZE - 1);
        subs[idx].topics[TOPICS_SIZE - 1] = '\0';
        printf("[BROKER QUIC] Conexión nueva: suscriptor %s:%d en temas '%s'\n",
               inet_ntoa(cli->sin_addr), ntohs(cli->sin_port), temas);
    } else {
        /* SUBSCRIBE repetido: el ACK anterior se perdió. Solo se re-confirma. */
        printf("[BROKER QUIC] SUBSCRIBE duplicado, se reenvía ACK\n");
    }
    enviar("ACK:SUBSCRIBE", cli);
}

static void manejar_ack_data(int seq, const struct sockaddr_in *cli) {
    for (int i = 0; i < MAX_PENDING; i++) {
        if (pending[i].used && pending[i].seq == seq &&
            same_addr(&subs[pending[i].sub].addr, cli)) {
            pending[i].used = 0;
            printf("[BROKER QUIC] ACK de DATA:%d recibido de %s:%d\n", seq,
                   inet_ntoa(cli->sin_addr), ntohs(cli->sin_port));
            return;
        }
    }
    /* ACK de algo ya confirmado (ACK duplicado): se ignora */
}

static void manejar_pub(char *resto, const struct sockaddr_in *cli) {
    /* resto = "<seq_pub>:<tema>:<mensaje>" */
    char *p1 = strchr(resto, ':');
    if (!p1) return;
    *p1 = '\0';
    int seq_pub = atoi(resto);
    char *tema = p1 + 1;
    char *p2 = strchr(tema, ':');
    if (!p2) return;
    *p2 = '\0';
    char *mensaje = p2 + 1;
    if (strlen(tema) == 0 || strlen(tema) >= 64) return;

    /* Busca (o registra) al publisher para detectar duplicados */
    int pi = -1, libre = -1;
    for (int i = 0; i < MAX_PUBS; i++) {
        if (pubs[i].used && same_addr(&pubs[i].addr, cli)) { pi = i; break; }
        if (!pubs[i].used && libre < 0) libre = i;
    }
    if (pi < 0) {
        if (libre < 0) return;
        pi = libre;
        pubs[pi].used = 1;
        pubs[pi].addr = *cli;
        pubs[pi].last_seq = 0;
    }

    char ack[64];
    snprintf(ack, sizeof(ack), "ACK:PUB:%d", seq_pub);

    if (seq_pub <= pubs[pi].last_seq) {
        /* Retransmisión de algo ya recibido: se confirma de nuevo pero NO
           se reenvía a los suscriptores (evita mensajes duplicados). */
        printf("[BROKER QUIC] PUB:%d duplicado (retransmisión), solo se re-confirma\n",
               seq_pub);
        enviar(ack, cli);
        return;
    }
    pubs[pi].last_seq = seq_pub;
    enviar(ack, cli);

    int enviados = 0;
    for (int i = 0; i < sub_count; i++) {
        if (!subs[i].active || !tiene_tema(subs[i].topics, tema)) continue;

        int slot = -1;
        for (int k = 0; k < MAX_PENDING; k++)
            if (!pending[k].used) { slot = k; break; }
        if (slot < 0) {
            printf("[BROKER QUIC] Cola de pendientes llena, se descarta envío\n");
            continue;
        }
        Pending *pd = &pending[slot];
        pd->used = 1;
        pd->sub  = i;
        pd->seq  = subs[i].next_seq++;
        pd->retries = 0;
        snprintf(pd->data, BUF_SIZE, "DATA:%d:%s:%s", pd->seq, tema, mensaje);
        pd->last_sent_ms = now_ms();
        enviar(pd->data, &subs[i].addr);
        enviados++;
    }
    printf("[BROKER QUIC] PUB:%d del tema '%s' reenviado a %d suscriptores\n",
           seq_pub, tema, enviados);
}

/* Retransmite los DATA que no han sido confirmados dentro de RTO_MS. */
static void retransmitir(void) {
    long t = now_ms();
    for (int i = 0; i < MAX_PENDING; i++) {
        Pending *pd = &pending[i];
        if (!pd->used || t - pd->last_sent_ms < RTO_MS) continue;

        if (pd->retries >= MAX_RETRIES) {
            Subscriber *s = &subs[pd->sub];
            if (s->active) {
                printf("[BROKER QUIC] Suscriptor %s:%d no responde, se cierra la conexión\n",
                       inet_ntoa(s->addr.sin_addr), ntohs(s->addr.sin_port));
                s->active = 0;
            }
            /* Se descartan todos los pendientes de ese suscriptor */
            for (int k = 0; k < MAX_PENDING; k++)
                if (pending[k].used && pending[k].sub == pd->sub) pending[k].used = 0;
            continue;
        }
        pd->retries++;
        pd->last_sent_ms = t;
        printf("[BROKER QUIC] Retransmisión %d de DATA:%d\n", pd->retries, pd->seq);
        enviar(pd->data, &subs[pd->sub].addr);
    }
}

int main(int argc, char *argv[]) {
    if (argc > 1) {
        loss_pct = atoi(argv[1]);
        if (loss_pct < 0) loss_pct = 0;
        if (loss_pct > 90) loss_pct = 90;
    }
    srand((unsigned)time(NULL));

    /* socket(): crea un socket UDP (SOCK_DGRAM) en IPv4 (AF_INET).
       Igual que QUIC, todo el transporte confiable va encima de UDP. */
    sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) { perror("socket"); exit(1); }

    int opt = 1;
    /* setsockopt(SO_REUSEADDR): permite reutilizar el puerto inmediatamente. */
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    /* setsockopt(SO_RCVTIMEO): recvfrom() deja de bloquear cada 100 ms para
       que el bucle principal pueda revisar qué mensajes hay que retransmitir. */
    struct timeval tv = { .tv_sec = 0, .tv_usec = 100000 };
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    struct sockaddr_in broker_addr = {
        .sin_family      = AF_INET,
        .sin_addr.s_addr = INADDR_ANY,
        .sin_port        = htons(PORT_BROKER)
    };

    /* bind(): asocia el socket al puerto del broker. */
    if (bind(sock, (struct sockaddr *)&broker_addr, sizeof(broker_addr)) < 0) {
        perror("bind");
        exit(1);
    }

    printf("[BROKER QUIC] Escuchando en puerto %d (pérdida simulada: %d%%)\n",
           PORT_BROKER, loss_pct);

    char buffer[BUF_SIZE];
    while (1) {
        struct sockaddr_in cli;
        socklen_t len = sizeof(cli);

        /* recvfrom(): recibe un datagrama y la dirección del remitente.
           Retorna -1 al vencer el timeout (no es error, solo no llegó nada). */
        int n = recvfrom(sock, buffer, BUF_SIZE - 1, 0, (struct sockaddr *)&cli, &len);
        if (n > 0) {
            buffer[n] = '\0';
            if (perdido()) {
                printf("[BROKER QUIC] (simulado) se pierde datagrama entrante: %s\n", buffer);
            } else if (strncmp(buffer, "SUBSCRIBE:", 10) == 0) {
                manejar_subscribe(buffer + 10, &cli);
            } else if (strncmp(buffer, "ACK:DATA:", 9) == 0) {
                manejar_ack_data(atoi(buffer + 9), &cli);
            } else if (strncmp(buffer, "PUB:", 4) == 0) {
                printf("[BROKER QUIC] Recibido: %s\n", buffer);
                manejar_pub(buffer + 4, &cli);
            } else {
                printf("[BROKER QUIC] Formato desconocido, ignorado: %s\n", buffer);
            }
        }
        retransmitir();
    }

    /* close(): cierra el socket (no se alcanza; el broker se detiene con Ctrl+C). */
    close(sock);
    return 0;
}
