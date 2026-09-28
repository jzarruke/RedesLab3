#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define PORT_BROKER  9004
#define BUF_SIZE     800
#define BROKER_IP    "127.0.0.1"

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
    printf("[SUBSCRIBER QUIC] Ingresa el tema al que deseas suscribirte (ej: PartidoA): ");
    scanf("%63s", topic);
    getchar();

    char buffer[BUF_SIZE];
    snprintf(buffer, BUF_SIZE, "SUBSCRIBE:%s", topic);

    /* sendto(): envía el mensaje de suscripción al broker.
       El broker responderá con un ACK confirmando el registro,
       simulando el mecanismo de confiabilidad de QUIC sobre UDP. */
    if (sendto(sock, buffer, strlen(buffer), 0,
               (struct sockaddr *)&broker_addr,
               sizeof(broker_addr)) < 0) {
        perror("sendto");
        exit(1);
    }

    struct sockaddr_in from_addr;
    socklen_t addr_len = sizeof(from_addr);

    memset(buffer, 0, BUF_SIZE);

    /* recvfrom(): espera el ACK del broker confirmando la suscripción.
       Esto simula el establecimiento de sesión de QUIC, garantizando
       que el broker registró al suscriptor antes de recibir mensajes. */
    int bytes = recvfrom(sock, buffer, BUF_SIZE - 1, 0,
                         (struct sockaddr *)&from_addr, &addr_len);
    if (bytes > 0 && strncmp(buffer, "ACK:SUBSCRIBE", 13) == 0) {
        printf("[SUBSCRIBER QUIC] Suscripción confirmada por el broker\n");
    }

    printf("[SUBSCRIBER QUIC] Suscrito al tema '%s'. Esperando mensajes...\n", topic);

    int ultimo_seq = 0;

    while (1) {
        memset(buffer, 0, BUF_SIZE);

        /* recvfrom(): recibe mensajes del broker con número de secuencia.
           El número de secuencia permite detectar pérdidas y mensajes
           desordenados, simulando el control de flujo de QUIC sobre UDP.
           Retorna bytes recibidos o -1 si hubo error. */
        bytes = recvfrom(sock, buffer, BUF_SIZE - 1, 0,
                         (struct sockaddr *)&from_addr, &addr_len);
        if (bytes < 0) {
            perror("recvfrom");
            break;
        }

        int seq = 0;
        char contenido[BUF_SIZE];

        if (sscanf(buffer, "SEQ:%d:%799s", &seq, contenido) == 2) {
            if (seq != ultimo_seq + 1) {
                printf("[SUBSCRIBER QUIC] ADVERTENCIA: mensaje fuera de orden. "
                       "Esperaba SEQ:%d, llegó SEQ:%d\n", ultimo_seq + 1, seq);
            }
            ultimo_seq = seq;
            printf("[SUBSCRIBER QUIC] SEQ:%d | Mensaje: %s\n", seq, contenido);

            /* sendto(): envía ACK al broker confirmando la recepción
               del mensaje. Esto simula el mecanismo de confirmación
               de entrega de QUIC a nivel de aplicación. */
            char ack[BUF_SIZE];
            snprintf(ack, BUF_SIZE, "ACK:%d", seq);
            sendto(sock, ack, strlen(ack), 0,
                   (struct sockaddr *)&from_addr, addr_len);
        } else {
            printf("[SUBSCRIBER QUIC] Mensaje recibido: %s\n", buffer);
        }
    }

    /* close(): cierra el socket y libera el descriptor de archivo. */
    close(sock);
    return 0;
}
