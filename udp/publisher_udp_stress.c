#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define PORT_BROKER  9003
#define BUF_SIZE     800
#define MSG_SIZE     600
#define BROKER_IP    "127.0.0.1"
#define NUM_MENSAJES 50

int main(int argc, char *argv[]) {
    if (argc < 2) {
        printf("Uso: ./publisher_udp_stress <tema>\n");
        exit(1);
    }

    char *topic = argv[1];

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

    printf("[PUBLISHER STRESS] Enviando %d mensajes al tema '%s'...\n",
           NUM_MENSAJES, topic);

    char buffer[BUF_SIZE];

    for (int i = 1; i <= NUM_MENSAJES; i++) {
        snprintf(buffer, BUF_SIZE, "%s:Mensaje numero %d de publisher stress",
                 topic, i);

        /* sendto(): envía el datagrama UDP al broker sin establecer
           conexión previa ni esperar confirmación. Los mensajes se
           envían lo más rápido posible para congestionar la red.
           Retorna bytes enviados o -1 si hubo error. */
        if (sendto(sock, buffer, strlen(buffer), 0,
                   (struct sockaddr *)&broker_addr,
                   sizeof(broker_addr)) < 0) {
            perror("sendto");
            break;
        }

        printf("[PUBLISHER STRESS] Enviado: %s\n", buffer);
    }

    /* close(): cierra el socket y libera el descriptor de archivo. */
    close(sock);
    printf("[PUBLISHER STRESS] Terminado\n");
    return 0;
}
