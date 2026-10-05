/*
 * subscriber_quic.c — Subscriber (versión QUIC, bono)
 *
 * 1. Handshake: envía SUBSCRIBE:<temas> y reintenta hasta recibir ACK:SUBSCRIBE.
 * 2. Recibe DATA:<seq>:<tema>:<mensaje> y responde siempre ACK:DATA:<seq>.
 * 3. Entrega en orden: si llega un seq mayor al esperado lo guarda en un
 *    buffer de reordenamiento; si llega uno ya entregado (duplicado por
 *    retransmisión) lo descarta. Así la aplicación ve los mensajes
 *    completos, sin duplicados y en orden, como en un stream de QUIC.
 *
 * Se puede seguir uno o varios partidos: PartidoA  o  PartidoA,PartidoB
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define PORT_BROKER  9004
#define BUF_SIZE     1024
#define BROKER_IP    "127.0.0.1"
#define WINDOW       64        /* tamaño del buffer de reordenamiento */
#define SUB_RETRIES  5

typedef struct {
    int  valid;
    int  seq;
    char texto[BUF_SIZE];
} Slot;

static Slot reorder[WINDOW];

int main() {
    /* socket(): crea un socket UDP (SOCK_DGRAM) en IPv4 (AF_INET). */
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) { perror("socket"); exit(1); }

    struct sockaddr_in broker_addr = {
        .sin_family = AF_INET,
        .sin_port   = htons(PORT_BROKER)
    };
    inet_pton(AF_INET, BROKER_IP, &broker_addr.sin_addr);

    /* bind(): fija un puerto local (elegido por el SO con puerto 0) para que
       el broker siempre conteste a la misma dirección durante la sesión. */
    struct sockaddr_in local_addr = {
        .sin_family      = AF_INET,
        .sin_addr.s_addr = INADDR_ANY,
        .sin_port        = htons(0)
    };
    if (bind(sock, (struct sockaddr *)&local_addr, sizeof(local_addr)) < 0) {
        perror("bind");
        exit(1);
    }

    char topics[256];
    printf("[SUBSCRIBER QUIC] Tema(s) a seguir, separados por coma (ej: PartidoA,PartidoB): ");
    if (scanf("%255s", topics) != 1) exit(1);
    getchar();

    char buffer[BUF_SIZE];
    snprintf(buffer, BUF_SIZE, "SUBSCRIBE:%s", topics);

    /* setsockopt(SO_RCVTIMEO): durante el handshake se espera 1 s el ACK. */
    struct timeval tv = { .tv_sec = 1, .tv_usec = 0 };
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    int conectado = 0;
    for (int intento = 1; intento <= SUB_RETRIES && !conectado; intento++) {
        /* sendto(): envía la solicitud de suscripción (inicio de la conexión). */
        sendto(sock, buffer, strlen(buffer), 0,
               (struct sockaddr *)&broker_addr, sizeof(broker_addr));
        printf("[SUBSCRIBER QUIC] SUBSCRIBE enviado (intento %d)\n", intento);

        char resp[BUF_SIZE];
        /* recvfrom(): espera el ACK:SUBSCRIBE o vence el timeout. */
        int n = recvfrom(sock, resp, BUF_SIZE - 1, 0, NULL, NULL);
        if (n > 0) {
            resp[n] = '\0';
            if (strcmp(resp, "ACK:SUBSCRIBE") == 0) conectado = 1;
        }
    }
    if (!conectado) {
        printf("[SUBSCRIBER QUIC] El broker no respondió. ¿Está corriendo?\n");
        close(sock);
        return 1;
    }

    /* Ya conectado: recvfrom() vuelve a ser bloqueante (sin timeout). */
    tv.tv_sec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    printf("[SUBSCRIBER QUIC] Conexión establecida. Siguiendo '%s'. Esperando mensajes...\n",
           topics);

    int esperado = 1;   /* próximo seq que se debe entregar a la aplicación */

    while (1) {
        struct sockaddr_in from;
        socklen_t len = sizeof(from);

        /* recvfrom(): recibe un datagrama del broker. */
        int n = recvfrom(sock, buffer, BUF_SIZE - 1, 0, (struct sockaddr *)&from, &len);
        if (n < 0) { perror("recvfrom"); break; }
        buffer[n] = '\0';

        if (strncmp(buffer, "DATA:", 5) != 0) continue;   /* p. ej. ACK:SUBSCRIBE tardío */

        char *p = strchr(buffer + 5, ':');
        if (!p) continue;
        int seq = atoi(buffer + 5);
        char *texto = p + 1;               /* "<tema>:<mensaje>", con espacios */

        /* Se confirma SIEMPRE, incluso duplicados: si el ACK anterior se
           perdió, el broker necesita otro para dejar de retransmitir. */
        char ack[64];
        snprintf(ack, sizeof(ack), "ACK:DATA:%d", seq);
        /* sendto(): envía el ACK al broker. */
        sendto(sock, ack, strlen(ack), 0, (struct sockaddr *)&from, len);

        if (seq < esperado) {
            printf("[SUBSCRIBER QUIC] DATA:%d duplicado (retransmisión), descartado\n", seq);
            continue;
        }
        if (seq >= esperado + WINDOW) {
            printf("[SUBSCRIBER QUIC] DATA:%d fuera de la ventana, descartado\n", seq);
            continue;
        }

        Slot *s = &reorder[seq % WINDOW];
        if (!s->valid) {
            s->valid = 1;
            s->seq   = seq;
            strncpy(s->texto, texto, BUF_SIZE - 1);
            s->texto[BUF_SIZE - 1] = '\0';
        }
        if (seq > esperado)
            printf("[SUBSCRIBER QUIC] DATA:%d llegó antes que DATA:%d, se guarda en buffer\n",
                   seq, esperado);

        /* Entrega en orden todo lo que ya esté disponible */
        while (reorder[esperado % WINDOW].valid &&
               reorder[esperado % WINDOW].seq == esperado) {
            Slot *e = &reorder[esperado % WINDOW];
            printf("[SUBSCRIBER QUIC] #%d | %s\n", e->seq, e->texto);
            e->valid = 0;
            esperado++;
        }
    }

    /* close(): cierra el socket. */
    close(sock);
    return 0;
}
