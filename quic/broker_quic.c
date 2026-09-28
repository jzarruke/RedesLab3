#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>

#define PORT_BROKER  9004
#define MAX_CLIENTS  50
#define BUF_SIZE     800
#define MSG_SIZE     900

typedef struct {
    struct sockaddr_in addr;
    char topic[64];
    int  active;
    int  last_seq;
} Subscriber;

Subscriber subscribers[MAX_CLIENTS];
int sub_count = 0;
pthread_mutex_t sub_mutex = PTHREAD_MUTEX_INITIALIZER;
int seq_global = 0;

int main() {
    /* socket(): crea un socket UDP (SOCK_DGRAM) en el dominio IPv4 (AF_INET).
       QUIC opera sobre UDP añadiendo confiabilidad a nivel de aplicación.
       Retorna un descriptor de archivo o -1 si hubo error. */
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) { perror("socket"); exit(1); }

    int opt = 1;
    /* setsockopt(): configura el socket para reutilizar la dirección
       inmediatamente después de cerrar, evitando "Address already in use". */
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in broker_addr = {
        .sin_family      = AF_INET,
        .sin_addr.s_addr = INADDR_ANY,
        .sin_port        = htons(PORT_BROKER)
    };

    /* bind(): asocia el socket a la dirección IP y puerto definidos
       para recibir todos los mensajes entrantes en ese puerto. */
    if (bind(sock, (struct sockaddr *)&broker_addr, sizeof(broker_addr)) < 0) {
        perror("bind");
        exit(1);
    }

    printf("[BROKER QUIC] Escuchando en puerto %d\n", PORT_BROKER);

    char buffer[BUF_SIZE];
    struct sockaddr_in client_addr;
    socklen_t addr_len = sizeof(client_addr);

    while (1) {
        memset(buffer, 0, BUF_SIZE);

        /* recvfrom(): recibe un datagrama UDP y almacena la dirección
           del remitente en client_addr. QUIC usa esto como base
           para añadir confiabilidad a nivel de aplicación.
           Retorna bytes recibidos o -1 si hubo error. */
        int bytes = recvfrom(sock, buffer, BUF_SIZE - 1, 0,
                             (struct sockaddr *)&client_addr, &addr_len);
        if (bytes < 0) {
            perror("recvfrom");
            continue;
        }

        printf("[BROKER QUIC] Mensaje recibido: %s\n", buffer);

        if (strncmp(buffer, "SUBSCRIBE:", 10) == 0) {
            char topic[64] = "";
            strncpy(topic, buffer + 10, sizeof(topic) - 1);

            pthread_mutex_lock(&sub_mutex);
            int encontrado = 0;
            for (int i = 0; i < sub_count; i++) {
                if (memcmp(&subscribers[i].addr, &client_addr,
                           sizeof(client_addr)) == 0) {
                    encontrado = 1;
                    break;
                }
            }

            if (!encontrado && sub_count < MAX_CLIENTS) {
                subscribers[sub_count].addr     = client_addr;
                subscribers[sub_count].active   = 1;
                subscribers[sub_count].last_seq = 0;
                strncpy(subscribers[sub_count].topic, topic,
                        sizeof(subscribers[sub_count].topic) - 1);
                sub_count++;
                printf("[BROKER QUIC] Suscriptor registrado en tema '%s'\n", topic);
            }
            pthread_mutex_unlock(&sub_mutex);

            /* sendto(): envía ACK de confirmación de suscripción al cliente.
               Esto simula el mecanismo de confirmación de QUIC sobre UDP. */
            char ack[BUF_SIZE];
            snprintf(ack, BUF_SIZE, "ACK:SUBSCRIBE");
            sendto(sock, ack, strlen(ack), 0,
                   (struct sockaddr *)&client_addr, addr_len);

        } else if (strncmp(buffer, "ACK:", 4) == 0) {
            printf("[BROKER QUIC] ACK recibido: %s\n", buffer);

        } else {
            char topic[64];
            char *sep = strchr(buffer, ':');
            if (sep == NULL) continue;

            int topic_len = sep - buffer;
            strncpy(topic, buffer, topic_len);
            topic[topic_len] = '\0';

            seq_global++;

            char msg_con_seq[MSG_SIZE];
	    snprintf(msg_con_seq, MSG_SIZE, "SEQ:%d:%s", seq_global, buffer);

            pthread_mutex_lock(&sub_mutex);
            int enviados = 0;
            for (int i = 0; i < sub_count; i++) {
                if (subscribers[i].active &&
                    strcmp(subscribers[i].topic, topic) == 0) {

                    /* sendto(): envía el mensaje con número de secuencia
                       al suscriptor. El número de secuencia permite detectar
                       pérdidas y reordenar mensajes a nivel de aplicación,
                       simulando el comportamiento de QUIC sobre UDP. */
                    sendto(sock, msg_con_seq, strlen(msg_con_seq), 0,
                           (struct sockaddr *)&subscribers[i].addr,
                           sizeof(subscribers[i].addr));
                    enviados++;
                }
            }
            pthread_mutex_unlock(&sub_mutex);

            printf("[BROKER QUIC] Reenviado con SEQ:%d a %d suscriptores del tema '%s'\n",
                   seq_global, enviados, topic);
        }
    }

    /* close(): cierra el socket y libera el descriptor de archivo. */
    close(sock);
    return 0;
}
