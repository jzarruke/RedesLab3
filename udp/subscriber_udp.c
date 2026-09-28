#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define PORT_BROKER  9003
#define BUF_SIZE     1024
#define BROKER_IP    "127.0.0.1"

int main() {
    /* socket(): crea un socket UDP (SOCK_DGRAM) en el dominio IPv4 (AF_INET).
       No establece conexión, cada mensaje es un datagrama independiente.
       Retorna un descriptor de archivo o -1 si hubo error. */
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) { perror("socket"); exit(1); }

    struct sockaddr_in broker_addr = {
        .sin_family = AF_INET,
        .sin_port   = htons(PORT_BROKER)
    };
    inet_pton(AF_INET, BROKER_IP, &broker_addr.sin_addr);

    /* bind(): asocia el socket a un puerto local para poder
       recibir datagramas enviados por el broker. Sin bind(),
       el broker no sabría a qué puerto enviar los mensajes. */
    struct sockaddr_in local_addr = {
        .sin_family      = AF_INET,
        .sin_addr.s_addr = INADDR_ANY,
        .sin_port        = htons(0)
    };
    if (bind(sock, (struct sockaddr *)&local_addr, sizeof(local_addr)) < 0) {
        perror("bind");
        exit(1);
    }

    char topic[64];
    printf("[SUBSCRIBER UDP] Ingresa el tema al que deseas suscribirte (ej: PartidoA): ");
    scanf("%63s", topic);
    getchar();

    char buffer[BUF_SIZE];
    snprintf(buffer, BUF_SIZE, "SUBSCRIBE:%s", topic);

    /* sendto(): envía el mensaje de suscripción al broker indicando
       el tema de interés y la dirección del subscriber para que
       el broker pueda enviarle mensajes. No requiere conexión previa. */
    if (sendto(sock, buffer, strlen(buffer), 0,
               (struct sockaddr *)&broker_addr,
               sizeof(broker_addr)) < 0) {
        perror("sendto");
        exit(1);
    }

    printf("[SUBSCRIBER UDP] Suscrito al tema '%s'. Esperando mensajes...\n", topic);

    struct sockaddr_in sender_addr;
    socklen_t addr_len = sizeof(sender_addr);

    while (1) {
        memset(buffer, 0, BUF_SIZE);

        /* recvfrom(): recibe un datagrama UDP del broker.
           No garantiza orden ni entrega. Bloquea hasta recibir
           un datagrama. Retorna bytes recibidos o -1 si hubo error. */
        int bytes = recvfrom(sock, buffer, BUF_SIZE - 1, 0,
                             (struct sockaddr *)&sender_addr, &addr_len);
        if (bytes < 0) {
            perror("recvfrom");
            break;
        }

        printf("[SUBSCRIBER UDP] Mensaje recibido: %s\n", buffer);
    }

    /* close(): cierra el socket y libera el descriptor de archivo. */
    close(sock);
    return 0;
}
