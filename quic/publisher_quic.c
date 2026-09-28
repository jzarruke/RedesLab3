#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define PORT_BROKER  9004
#define BUF_SIZE     800
#define MSG_SIZE     600
#define BROKER_IP    "127.0.0.1"
#define TIMEOUT_SEC  2

int main() {
    /* socket(): crea un socket UDP (SOCK_DGRAM) en el dominio IPv4 (AF_INET).
       QUIC opera sobre UDP añadiendo confiabilidad a nivel de aplicación.
       Retorna un descriptor de archivo o -1 si hubo error. */
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) { perror("socket"); exit(1); }

    struct sockaddr_in broker_addr = {
        .sin_family = AF_INET,
        .sin_port   = htons(PORT_BROKER)
    };
    inet_pton(AF_INET, BROKER_IP, &broker_addr.sin_addr);

    /* setsockopt(): configura un tiempo máximo de espera (timeout) para
       recvfrom(). Si no llega un ACK en TIMEOUT_SEC segundos, el publisher
       reintenta el envío, simulando la confiabilidad de QUIC sobre UDP. */
    struct timeval timeout = { .tv_sec = TIMEOUT_SEC, .tv_usec = 0 };
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    printf("[PUBLISHER QUIC] Conectado al broker\n");

    char topic[64];
    printf("[PUBLISHER QUIC] Ingresa el tema del partido (ej: PartidoA): ");
    scanf("%63s", topic);
    getchar();

    char mensaje[MSG_SIZE];
    char buffer[BUF_SIZE];
    char ack[BUF_SIZE];
    struct sockaddr_in from_addr;
    socklen_t addr_len = sizeof(from_addr);
    int seq = 0;

    printf("[PUBLISHER QUIC] Escribe mensajes (escribe 'salir' para terminar):\n");

    while (1) {
        printf("> ");
        fgets(mensaje, MSG_SIZE, stdin);
        mensaje[strcspn(mensaje, "\n")] = '\0';

        if (strcmp(mensaje, "salir") == 0) break;

        seq++;
        snprintf(buffer, BUF_SIZE, "%s:%s", topic, mensaje);

        int confirmado = 0;
        int intentos   = 0;

        while (!confirmado && intentos < 3) {
            /* sendto(): envía el datagrama UDP al broker. QUIC añade
               un mecanismo de reintento si no llega confirmación,
               garantizando entrega confiable sobre UDP.
               Retorna bytes enviados o -1 si hubo error. */
            if (sendto(sock, buffer, strlen(buffer), 0,
                       (struct sockaddr *)&broker_addr,
                       sizeof(broker_addr)) < 0) {
                perror("sendto");
                break;
            }

            printf("[PUBLISHER QUIC] Enviado (intento %d): %s\n", intentos + 1, buffer);

            memset(ack, 0, BUF_SIZE);

            /* recvfrom(): espera el ACK del broker confirmando la recepción
               del mensaje. Si no llega en TIMEOUT_SEC segundos, reintenta
               el envío. Esto simula el control de confiabilidad de QUIC. */
            int bytes = recvfrom(sock, ack, BUF_SIZE - 1, 0,
                                 (struct sockaddr *)&from_addr, &addr_len);

            if (bytes > 0 && strncmp(ack, "ACK:", 4) == 0) {
                printf("[PUBLISHER QUIC] ACK recibido: %s\n", ack);
                confirmado = 1;
            } else {
                printf("[PUBLISHER QUIC] Sin ACK, reintentando...\n");
                intentos++;
            }
        }

        if (!confirmado) {
            printf("[PUBLISHER QUIC] Mensaje no confirmado tras 3 intentos: %s\n", buffer);
        }
    }

    /* close(): cierra el socket y libera el descriptor de archivo. */
    close(sock);
    printf("[PUBLISHER QUIC] Desconectado\n");
    return 0;
}
