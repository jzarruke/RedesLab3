/*
 * publisher_quic.c — Publisher (versión QUIC, bono)
 *
 * Envía cada mensaje como PUB:<seq>:<tema>:<mensaje> y espera ACK:PUB:<seq>.
 * Si el ACK no llega en TIMEOUT_MS, retransmite (hasta MAX_RETRIES veces).
 * El número de secuencia permite al broker descartar las retransmisiones
 * que ya había recibido, así un reintento nunca genera mensajes duplicados.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define PORT_BROKER  9004
#define BUF_SIZE     1024
#define MSG_SIZE     900
#define BROKER_IP    "127.0.0.1"
#define TIMEOUT_MS   500
#define MAX_RETRIES  5

int main() {
    /* socket(): crea un socket UDP (SOCK_DGRAM) en IPv4 (AF_INET). */
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) { perror("socket"); exit(1); }

    struct sockaddr_in broker_addr = {
        .sin_family = AF_INET,
        .sin_port   = htons(PORT_BROKER)
    };
    /* inet_pton(): convierte la IP en texto a formato binario de red. */
    inet_pton(AF_INET, BROKER_IP, &broker_addr.sin_addr);

    /* setsockopt(SO_RCVTIMEO): recvfrom() espera como máximo TIMEOUT_MS
       el ACK; si no llega, se retransmite el mensaje. */
    struct timeval timeout = { .tv_sec = 0, .tv_usec = TIMEOUT_MS * 1000 };
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    char topic[64];
    printf("[PUBLISHER QUIC] Ingresa el tema del partido (ej: PartidoA): ");
    if (scanf("%63s", topic) != 1) exit(1);
    getchar();

    char mensaje[MSG_SIZE];
    char buffer[BUF_SIZE];
    char resp[BUF_SIZE];
    char ack_esperado[64];
    int  seq = 0;

    printf("[PUBLISHER QUIC] Escribe mensajes (escribe 'salir' para terminar):\n");

    while (1) {
        printf("> ");
        fflush(stdout);
        if (fgets(mensaje, MSG_SIZE, stdin) == NULL) break;   /* EOF */
        mensaje[strcspn(mensaje, "\n")] = '\0';
        if (strcmp(mensaje, "salir") == 0) break;
        if (strlen(mensaje) == 0) continue;

        seq++;
        snprintf(buffer, BUF_SIZE, "PUB:%d:%s:%s", seq, topic, mensaje);
        snprintf(ack_esperado, sizeof(ack_esperado), "ACK:PUB:%d", seq);

        int confirmado = 0;
        for (int intento = 1; intento <= MAX_RETRIES && !confirmado; intento++) {
            /* sendto(): envía el datagrama al broker. */
            if (sendto(sock, buffer, strlen(buffer), 0,
                       (struct sockaddr *)&broker_addr, sizeof(broker_addr)) < 0) {
                perror("sendto");
                break;
            }
            printf("[PUBLISHER QUIC] Enviado (intento %d): %s\n", intento, buffer);

            /* Espera el ACK de ESTE seq. Un ACK atrasado de un mensaje anterior
               se ignora y se sigue esperando hasta que venza el timeout. */
            while (1) {
                struct sockaddr_in from;
                socklen_t len = sizeof(from);
                /* recvfrom(): retorna -1 si vence el timeout. */
                int n = recvfrom(sock, resp, BUF_SIZE - 1, 0,
                                 (struct sockaddr *)&from, &len);
                if (n < 0) break;            /* timeout: hay que retransmitir */
                resp[n] = '\0';
                if (strcmp(resp, ack_esperado) == 0) {
                    printf("[PUBLISHER QUIC] Confirmado por el broker: %s\n", resp);
                    confirmado = 1;
                    break;
                }
            }
            if (!confirmado && intento < MAX_RETRIES)
                printf("[PUBLISHER QUIC] Sin ACK en %d ms, retransmitiendo...\n", TIMEOUT_MS);
        }

        if (!confirmado)
            printf("[PUBLISHER QUIC] El broker no confirmó el mensaje tras %d intentos\n",
                   MAX_RETRIES);
    }

    /* close(): cierra el socket. */
    close(sock);
    printf("[PUBLISHER QUIC] Desconectado\n");
    return 0;
}
