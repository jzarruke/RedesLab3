/*
 * subscriber_tcp.c — Subscriber (versión TCP)
 * Se suscribe a uno o varios temas ("PartidoA" o "PartidoA,PartidoB") y
 * muestra cada mensaje recibido. Como TCP es un flujo de bytes, los datos
 * recibidos se acumulan y se separan por '\n' para mostrar mensajes completos.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define PORT_SUB    9002
#define BUF_SIZE    1024
#define BROKER_IP   "127.0.0.1"

int main() {
    /* socket(): crea un socket TCP (SOCK_STREAM) en IPv4 (AF_INET).
       Retorna un descriptor de archivo o -1 si hubo error. */
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) { perror("socket"); exit(1); }

    struct sockaddr_in broker_addr = {
        .sin_family = AF_INET,
        .sin_port   = htons(PORT_SUB)
    };
    inet_pton(AF_INET, BROKER_IP, &broker_addr.sin_addr);

    /* connect(): establece la conexión TCP con el broker.
       Retorna 0 si exitoso o -1 si hubo error. */
    if (connect(sock, (struct sockaddr *)&broker_addr, sizeof(broker_addr)) < 0) {
        perror("connect");
        exit(1);
    }

    printf("[SUBSCRIBER] Conectado al broker\n");

    char topics[256];
    printf("[SUBSCRIBER] Tema(s) a seguir, separados por coma (ej: PartidoA,PartidoB): ");
    if (scanf("%255s", topics) != 1) exit(1);
    getchar();

    char buffer[BUF_SIZE];
    int len = snprintf(buffer, BUF_SIZE, "SUBSCRIBE:%s\n", topics);

    /* send(): envía la suscripción indicando los temas de interés. */
    if (send(sock, buffer, len, 0) < 0) {
        perror("send");
        exit(1);
    }

    printf("[SUBSCRIBER] Suscrito a '%s'. Esperando mensajes...\n", topics);

    int acumulado = 0;
    while (1) {
        /* recv(): recibe bytes del broker. Puede traer un mensaje parcial o
           varios mensajes juntos. Retorna 0 si el broker cerró la conexión. */
        int bytes = recv(sock, buffer + acumulado, BUF_SIZE - 1 - acumulado, 0);
        if (bytes <= 0) {
            printf("[SUBSCRIBER] Broker desconectado\n");
            break;
        }
        acumulado += bytes;
        buffer[acumulado] = '\0';

        /* Imprime cada mensaje completo (terminado en '\n') */
        char *inicio = buffer, *nl;
        while ((nl = strchr(inicio, '\n')) != NULL) {
            *nl = '\0';
            printf("[SUBSCRIBER] Mensaje recibido: %s\n", inicio);
            inicio = nl + 1;
        }
        /* Lo que quede es un mensaje incompleto: se mueve al inicio */
        acumulado = strlen(inicio);
        memmove(buffer, inicio, acumulado);
        if (acumulado == BUF_SIZE - 1) acumulado = 0;   /* línea demasiado larga */
    }

    /* close(): cierra el socket y libera el descriptor de archivo. */
    close(sock);
    return 0;
}
