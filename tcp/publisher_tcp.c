/*
 * publisher_tcp.c — Publisher (versión TCP)
 * Envía cada mensaje como "<tema>:<mensaje>\n". El '\n' delimita el mensaje,
 * ya que TCP entrega un flujo de bytes sin límites entre mensajes.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define PORT_PUB    9001
#define BUF_SIZE    1024
#define MSG_SIZE    900
#define BROKER_IP   "127.0.0.1"

int main() {
    /* socket(): crea un socket TCP (SOCK_STREAM) en IPv4 (AF_INET).
       Retorna un descriptor de archivo o -1 si hubo error. */
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) { perror("socket"); exit(1); }

    struct sockaddr_in broker_addr = {
        .sin_family = AF_INET,
        .sin_port   = htons(PORT_PUB)
    };
    /* inet_pton(): convierte la IP en texto a formato binario de red. */
    inet_pton(AF_INET, BROKER_IP, &broker_addr.sin_addr);

    /* connect(): establece la conexión TCP (three-way handshake) con el
       broker. Retorna 0 si exitoso o -1 si hubo error. */
    if (connect(sock, (struct sockaddr *)&broker_addr, sizeof(broker_addr)) < 0) {
        perror("connect");
        exit(1);
    }

    printf("[PUBLISHER] Conectado al broker\n");

    char topic[64];
    printf("[PUBLISHER] Ingresa el tema del partido (ej: PartidoA): ");
    if (scanf("%63s", topic) != 1) exit(1);
    getchar();

    char mensaje[MSG_SIZE];
    char buffer[BUF_SIZE];

    printf("[PUBLISHER] Escribe mensajes (escribe 'salir' para terminar):\n");

    while (1) {
        printf("> ");
        fflush(stdout);
        if (fgets(mensaje, MSG_SIZE, stdin) == NULL) break;   /* EOF */
        mensaje[strcspn(mensaje, "\n")] = '\0';

        if (strcmp(mensaje, "salir") == 0) break;
        if (strlen(mensaje) == 0) continue;

        int len = snprintf(buffer, BUF_SIZE, "%s:%s\n", topic, mensaje);

        /* send(): envía el mensaje al broker. MSG_NOSIGNAL evita que el
           programa muera por SIGPIPE si el broker se cayó. */
        if (send(sock, buffer, len, MSG_NOSIGNAL) < 0) {
            perror("send");
            break;
        }

        printf("[PUBLISHER] Enviado: %s:%s\n", topic, mensaje);
    }

    /* close(): cierra la conexión (envía FIN) y libera el descriptor. */
    close(sock);
    printf("[PUBLISHER] Desconectado\n");
    return 0;
}
