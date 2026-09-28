#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define PORT_SUB    9002
#define BUF_SIZE    1024
#define BROKER_IP   "127.0.0.1"

int main() {
    /* socket(): crea un socket TCP (SOCK_STREAM) en el dominio IPv4 (AF_INET).
       Retorna un descriptor de archivo o -1 si hubo error. */
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) { perror("socket"); exit(1); }

    struct sockaddr_in broker_addr = {
        .sin_family = AF_INET,
        .sin_port   = htons(PORT_SUB)
    };
    inet_pton(AF_INET, BROKER_IP, &broker_addr.sin_addr);

    /* connect(): establece la conexión TCP con el broker en la IP
       y puerto definidos. Retorna 0 si exitoso o -1 si hubo error. */
    if (connect(sock, (struct sockaddr *)&broker_addr, sizeof(broker_addr)) < 0) {
        perror("connect");
        exit(1);
    }

    printf("[SUBSCRIBER] Conectado al broker\n");

    char topic[64];
    printf("[SUBSCRIBER] Ingresa el tema al que deseas suscribirte (ej: PartidoA): ");
    scanf("%63s", topic);
    getchar();

    char buffer[BUF_SIZE];
    snprintf(buffer, BUF_SIZE, "SUBSCRIBE:%s", topic);

    /* send(): envía el mensaje de suscripción al broker indicando
       el tema de interés. Retorna bytes enviados o -1 si hubo error. */
    if (send(sock, buffer, strlen(buffer), 0) < 0) {
        perror("send");
        exit(1);
    }

    printf("[SUBSCRIBER] Suscrito al tema '%s'. Esperando mensajes...\n", topic);

    while (1) {
        memset(buffer, 0, BUF_SIZE);

        /* recv(): recibe mensajes enviados por el broker.
           Bloquea hasta recibir datos. Retorna bytes recibidos,
           0 si se cerró la conexión, o -1 si hubo error. */
        int bytes = recv(sock, buffer, BUF_SIZE - 1, 0);

        if (bytes <= 0) {
            printf("[SUBSCRIBER] Broker desconectado\n");
            break;
        }

        printf("[SUBSCRIBER] Mensaje recibido: %s\n", buffer);
    }

    /* close(): cierra el socket y libera el descriptor de archivo. */
    close(sock);
    return 0;
}
