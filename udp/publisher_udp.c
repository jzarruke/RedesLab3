#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define PORT_BROKER  9003
#define BUF_SIZE     1024
#define MSG_SIZE     900
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

    printf("[PUBLISHER UDP] Conectado al broker\n");

    char topic[64];
    printf("[PUBLISHER UDP] Ingresa el tema del partido (ej: PartidoA): ");
    scanf("%63s", topic);
    getchar();

    char mensaje[MSG_SIZE];
    char buffer[BUF_SIZE];

    printf("[PUBLISHER UDP] Escribe mensajes (escribe 'salir' para terminar):\n");

    while (1) {
        printf("> ");
        fgets(mensaje, MSG_SIZE, stdin);
        mensaje[strcspn(mensaje, "\n")] = '\0';

        if (strcmp(mensaje, "salir") == 0) break;

        snprintf(buffer, BUF_SIZE, "%s:%s", topic, mensaje);

        /* sendto(): envía el datagrama UDP al broker sin establecer
           conexión previa. Cada llamada es independiente y no garantiza
           entrega ni orden. Retorna bytes enviados o -1 si hubo error. */
        if (sendto(sock, buffer, strlen(buffer), 0,
                   (struct sockaddr *)&broker_addr,
                   sizeof(broker_addr)) < 0) {
            perror("sendto");
            break;
        }

        printf("[PUBLISHER UDP] Enviado: %s\n", buffer);
    }

    /* close(): cierra el socket y libera el descriptor de archivo. */
    close(sock);
    printf("[PUBLISHER UDP] Desconectado\n");
    return 0;
}
