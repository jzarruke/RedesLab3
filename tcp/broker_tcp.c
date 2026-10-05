/*
 * broker_tcp.c — Broker del sistema publicación-suscripción (versión TCP)
 *
 * - Escucha publishers en PORT_PUB y suscriptores en PORT_SUB.
 * - Un hilo por cada conexión (pthread).
 * - TCP es un FLUJO de bytes, no respeta límites de mensaje: un recv() puede
 *   traer medio mensaje o dos mensajes pegados. Por eso cada mensaje de la
 *   aplicación termina en '\n' y se reconstruye línea por línea.
 *
 * Protocolo:
 *   Publisher  -> Broker : <tema>:<mensaje>\n
 *   Subscriber -> Broker : SUBSCRIBE:<tema1>[,<tema2>,...]\n
 *   Broker     -> Sub    : <tema>:<mensaje>\n   (sin modificar el contenido)
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <pthread.h>
#include <arpa/inet.h>

#define PORT_PUB    9001
#define PORT_SUB    9002
#define MAX_CLIENTS 50
#define BUF_SIZE    1024
#define TOPICS_SIZE 256

typedef struct {
    int  socket;
    char topics[TOPICS_SIZE];   /* temas separados por coma */
    int  active;
} Subscriber;

Subscriber subscribers[MAX_CLIENTS];
int sub_count = 0;
pthread_mutex_t sub_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Envía todos los bytes: send() puede enviar menos de lo pedido. */
static int send_all(int sock, const char *data, int len) {
    int total = 0;
    while (total < len) {
        /* send(): envía datos por el socket TCP. MSG_NOSIGNAL evita que el
           proceso muera con SIGPIPE si el otro extremo ya cerró. */
        int n = send(sock, data + total, len - total, MSG_NOSIGNAL);
        if (n <= 0) return -1;
        total += n;
    }
    return total;
}

/*
 * Lee UNA línea (hasta '\n') del socket usando un buffer acumulado por
 * conexión. Retorna la longitud de la línea (sin '\n') o -1 si se cerró.
 */
typedef struct { char data[BUF_SIZE]; int len; } LineBuf;

static int leer_linea(int sock, LineBuf *lb, char *out, int out_size) {
    while (1) {
        char *nl = memchr(lb->data, '\n', lb->len);
        if (nl) {
            int linea = nl - lb->data;
            int copiar = linea < out_size - 1 ? linea : out_size - 1;
            memcpy(out, lb->data, copiar);
            out[copiar] = '\0';
            lb->len -= linea + 1;
            memmove(lb->data, nl + 1, lb->len);
            return copiar;
        }
        if (lb->len == BUF_SIZE) lb->len = 0;   /* línea demasiado larga: se descarta */

        /* recv(): recibe bytes del socket. Retorna 0 si el otro extremo cerró
           la conexión o -1 si hubo error. */
        int n = recv(sock, lb->data + lb->len, BUF_SIZE - lb->len, 0);
        if (n <= 0) return -1;
        lb->len += n;
    }
}

static int tiene_tema(const char *lista, const char *tema) {
    char copia[TOPICS_SIZE];
    strncpy(copia, lista, sizeof(copia) - 1);
    copia[sizeof(copia) - 1] = '\0';
    char *save;
    for (char *t = strtok_r(copia, ",", &save); t; t = strtok_r(NULL, ",", &save))
        if (strcmp(t, tema) == 0) return 1;
    return 0;
}

void *manejar_publisher(void *arg) {
    int pub_sock = *(int *)arg;
    free(arg);

    LineBuf lb = { .len = 0 };
    char linea[BUF_SIZE];
    char salida[BUF_SIZE + 1];

    printf("[BROKER] Publisher conectado (socket %d)\n", pub_sock);

    while (leer_linea(pub_sock, &lb, linea, sizeof(linea)) >= 0) {
        printf("[BROKER] Mensaje recibido: %s\n", linea);

        char *sep = strchr(linea, ':');
        int topic_len = sep ? (int)(sep - linea) : 0;
        if (sep == NULL || topic_len == 0 || topic_len >= 64) {
            printf("[BROKER] Formato inválido, ignorando mensaje\n");
            continue;
        }
        char topic[64];
        memcpy(topic, linea, topic_len);
        topic[topic_len] = '\0';

        int len = snprintf(salida, sizeof(salida), "%s\n", linea);

        pthread_mutex_lock(&sub_mutex);
        int enviados = 0;
        for (int i = 0; i < sub_count; i++) {
            if (subscribers[i].active && tiene_tema(subscribers[i].topics, topic)) {
                if (send_all(subscribers[i].socket, salida, len) < 0)
                    subscribers[i].active = 0;
                else
                    enviados++;
            }
        }
        pthread_mutex_unlock(&sub_mutex);

        printf("[BROKER] Reenviado a %d suscriptores del tema '%s'\n", enviados, topic);
    }

    printf("[BROKER] Publisher desconectado (socket %d)\n", pub_sock);
    /* close(): cierra el socket y libera el descriptor de archivo. */
    close(pub_sock);
    return NULL;
}

void *manejar_subscriber(void *arg) {
    int sub_sock = *(int *)arg;
    free(arg);

    LineBuf lb = { .len = 0 };
    char linea[BUF_SIZE];

    /* Primer mensaje esperado: "SUBSCRIBE:Tema1,Tema2\n" */
    if (leer_linea(sub_sock, &lb, linea, sizeof(linea)) < 0 ||
        strncmp(linea, "SUBSCRIBE:", 10) != 0 || strlen(linea) == 10) {
        printf("[BROKER] Suscriptor no envió un SUBSCRIBE válido\n");
        close(sub_sock);
        return NULL;
    }

    pthread_mutex_lock(&sub_mutex);
    int idx = -1;
    for (int i = 0; i < sub_count; i++)          /* reutiliza un espacio libre */
        if (!subscribers[i].active) { idx = i; break; }
    if (idx < 0 && sub_count < MAX_CLIENTS) idx = sub_count++;
    if (idx >= 0) {
        subscribers[idx].socket = sub_sock;
        strncpy(subscribers[idx].topics, linea + 10, TOPICS_SIZE - 1);
        subscribers[idx].topics[TOPICS_SIZE - 1] = '\0';
        subscribers[idx].active = 1;
    }
    pthread_mutex_unlock(&sub_mutex);

    if (idx < 0) {
        printf("[BROKER] Máximo de suscriptores alcanzado\n");
        close(sub_sock);
        return NULL;
    }

    printf("[BROKER] Suscriptor registrado en '%s' (socket %d)\n", linea + 10, sub_sock);

    /* El hilo queda leyendo solo para detectar cuándo el suscriptor cierra. */
    while (leer_linea(sub_sock, &lb, linea, sizeof(linea)) >= 0) { }

    printf("[BROKER] Suscriptor desconectado (socket %d)\n", sub_sock);
    pthread_mutex_lock(&sub_mutex);
    if (subscribers[idx].socket == sub_sock)   /* el espacio pudo ser reutilizado */
        subscribers[idx].active = 0;
    /* close(): se cierra con el mutex tomado para que ningún publisher
       envíe a un descriptor ya cerrado (o reutilizado por otra conexión). */
    close(sub_sock);
    pthread_mutex_unlock(&sub_mutex);
    return NULL;
}

/* Crea, configura y pone a escuchar un socket TCP en el puerto dado. */
static int crear_servidor(int puerto) {
    /* socket(): crea un socket TCP (SOCK_STREAM) en IPv4 (AF_INET). */
    int s = socket(AF_INET, SOCK_STREAM, 0);
    if (s < 0) { perror("socket"); exit(1); }

    int opt = 1;
    /* setsockopt(SO_REUSEADDR): permite reutilizar el puerto inmediatamente
       después de cerrar el broker, evitando "Address already in use". */
    setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr = {
        .sin_family      = AF_INET,
        .sin_addr.s_addr = INADDR_ANY,
        .sin_port        = htons(puerto)
    };

    /* bind(): asocia el socket a la IP y puerto definidos. */
    if (bind(s, (struct sockaddr *)&addr, sizeof(addr)) < 0) { perror("bind"); exit(1); }

    /* listen(): modo escucha, hasta 10 conexiones pendientes en cola. */
    if (listen(s, 10) < 0) { perror("listen"); exit(1); }
    return s;
}

/* Bucle de accept() para un socket servidor; lanza un hilo por conexión. */
static void aceptar(int server_sock, void *(*handler)(void *), const char *tipo) {
    while (1) {
        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);
        int *client_sock = malloc(sizeof(int));

        /* accept(): bloquea hasta que llega una conexión (después del
           three-way handshake) y retorna un socket nuevo dedicado a ella. */
        *client_sock = accept(server_sock, (struct sockaddr *)&client_addr, &addr_len);
        if (*client_sock < 0) {
            fprintf(stderr, "[BROKER] Error en accept (%s)\n", tipo);
            free(client_sock);
            continue;
        }

        /* pthread_create(): atiende la conexión en un hilo independiente. */
        pthread_t tid;
        pthread_create(&tid, NULL, handler, client_sock);
        pthread_detach(tid);
    }
}

void *escuchar_publishers(void *arg) {
    aceptar(*(int *)arg, manejar_publisher, "publisher");
    return NULL;
}

int main() {
    /* Si un suscriptor se cae, escribir en su socket genera SIGPIPE, que por
       defecto mataría al broker completo. Se ignora y se maneja el error. */
    signal(SIGPIPE, SIG_IGN);

    static int pub_server, sub_server;
    pub_server = crear_servidor(PORT_PUB);
    sub_server = crear_servidor(PORT_SUB);

    printf("[BROKER] Escuchando publishers en puerto %d\n", PORT_PUB);
    printf("[BROKER] Escuchando subscribers en puerto %d\n", PORT_SUB);

    pthread_t pub_thread;
    pthread_create(&pub_thread, NULL, escuchar_publishers, &pub_server);
    pthread_detach(pub_thread);

    aceptar(sub_server, manejar_subscriber, "subscriber");
    return 0;
}
