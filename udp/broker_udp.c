/*
 * broker_udp.c — Broker del sistema publicación-suscripción (versión UDP)
 *
 * Un solo socket UDP recibe todo: suscripciones y publicaciones. Como UDP
 * no tiene conexiones, el broker identifica a cada suscriptor por su
 * dirección (IP:puerto), que obtiene de recvfrom(). No se usan hilos: un
 * único socket atendido en un bucle es suficiente (cada datagrama es
 * independiente).
 *
 * Protocolo (un mensaje por datagrama):
 *   Subscriber -> Broker : SUBSCRIBE:<tema1>[,<tema2>,...]
 *   Subscriber -> Broker : UNSUBSCRIBE
 *   Publisher  -> Broker : <tema>:<mensaje>
 *   Broker     -> Sub    : <tema>:<mensaje>   (sin modificar el contenido)
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define PORT_BROKER  9003
#define MAX_CLIENTS  50
#define BUF_SIZE     1024
#define TOPICS_SIZE  256

typedef struct {
    struct sockaddr_in addr;
    char topics[TOPICS_SIZE];   /* temas separados por coma */
    int  active;
} Subscriber;

Subscriber subscribers[MAX_CLIENTS];
int sub_count = 0;

static int same_addr(const struct sockaddr_in *a, const struct sockaddr_in *b) {
    return a->sin_addr.s_addr == b->sin_addr.s_addr && a->sin_port == b->sin_port;
}

static int tiene_tema(const char *lista, const char *tema) {
    char copia[TOPICS_SIZE];
    strncpy(copia, lista, sizeof(copia) - 1);
    copia[sizeof(copia) - 1] = '\0';
    for (char *t = strtok(copia, ","); t; t = strtok(NULL, ","))
        if (strcmp(t, tema) == 0) return 1;
    return 0;
}

int main() {
    /* socket(): crea un socket UDP (SOCK_DGRAM) en IPv4 (AF_INET).
       A diferencia de TCP, UDP no orienta la conexión, cada mensaje
       es un datagrama independiente. Retorna descriptor o -1 si hubo error. */
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) { perror("socket"); exit(1); }

    int opt = 1;
    /* setsockopt(SO_REUSEADDR): permite reutilizar el puerto inmediatamente. */
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in broker_addr = {
        .sin_family      = AF_INET,
        .sin_addr.s_addr = INADDR_ANY,
        .sin_port        = htons(PORT_BROKER)
    };

    /* bind(): asocia el socket a la IP y puerto definidos para recibir
       todos los datagramas que lleguen a ese puerto. */
    if (bind(sock, (struct sockaddr *)&broker_addr, sizeof(broker_addr)) < 0) {
        perror("bind");
        exit(1);
    }

    printf("[BROKER UDP] Escuchando en puerto %d\n", PORT_BROKER);

    char buffer[BUF_SIZE];

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);

        /* recvfrom(): recibe un datagrama UDP y guarda la dirección del
           remitente en client_addr. A diferencia de recv(), funciona sin
           conexión establecida. Retorna bytes recibidos o -1 si hubo error. */
        int bytes = recvfrom(sock, buffer, BUF_SIZE - 1, 0,
                             (struct sockaddr *)&client_addr, &addr_len);
        if (bytes < 0) {
            perror("recvfrom");
            continue;
        }
        buffer[bytes] = '\0';

        printf("[BROKER UDP] Mensaje recibido: %s\n", buffer);

        if (strncmp(buffer, "SUBSCRIBE:", 10) == 0) {
            int idx = -1;
            for (int i = 0; i < sub_count; i++)
                if (subscribers[i].active && same_addr(&subscribers[i].addr, &client_addr)) {
                    idx = i; break;
                }
            if (idx < 0) {
                for (int i = 0; i < sub_count; i++)      /* reutiliza espacios libres */
                    if (!subscribers[i].active) { idx = i; break; }
                if (idx < 0 && sub_count < MAX_CLIENTS) idx = sub_count++;
            }
            if (idx < 0) {
                printf("[BROKER UDP] Máximo de suscriptores alcanzado\n");
                continue;
            }
            subscribers[idx].addr   = client_addr;
            subscribers[idx].active = 1;
            strncpy(subscribers[idx].topics, buffer + 10, TOPICS_SIZE - 1);
            subscribers[idx].topics[TOPICS_SIZE - 1] = '\0';
            printf("[BROKER UDP] Suscriptor %s:%d registrado en '%s'\n",
                   inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port),
                   subscribers[idx].topics);

        } else if (strcmp(buffer, "UNSUBSCRIBE") == 0) {
            for (int i = 0; i < sub_count; i++)
                if (subscribers[i].active && same_addr(&subscribers[i].addr, &client_addr)) {
                    subscribers[i].active = 0;
                    printf("[BROKER UDP] Suscriptor %s:%d dado de baja\n",
                           inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));
                }

        } else {
            char *sep = strchr(buffer, ':');
            int topic_len = sep ? (int)(sep - buffer) : 0;
            if (sep == NULL || topic_len == 0 || topic_len >= 64) {
                printf("[BROKER UDP] Formato inválido, ignorando mensaje\n");
                continue;
            }
            char topic[64];
            memcpy(topic, buffer, topic_len);
            topic[topic_len] = '\0';

            int enviados = 0;
            for (int i = 0; i < sub_count; i++) {
                if (subscribers[i].active && tiene_tema(subscribers[i].topics, topic)) {
                    /* sendto(): envía un datagrama al suscriptor usando su
                       dirección almacenada. No hay confirmación de entrega. */
                    sendto(sock, buffer, bytes, 0,
                           (struct sockaddr *)&subscribers[i].addr,
                           sizeof(subscribers[i].addr));
                    enviados++;
                }
            }
            printf("[BROKER UDP] Reenviado a %d suscriptores del tema '%s'\n",
                   enviados, topic);
        }
    }

    /* close(): cierra el socket (no se alcanza; el broker se detiene con Ctrl+C). */
    close(sock);
    return 0;
}
