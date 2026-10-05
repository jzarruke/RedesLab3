/*
 * subscriber_udp.c — Subscriber (versión UDP)
 * Se suscribe a uno o varios temas ("PartidoA" o "PartidoA,PartidoB").
 * Al salir con Ctrl+C envía UNSUBSCRIBE para que el broker deje de enviarle
 * datagramas (en UDP el broker no tiene otra forma de saber que se fue).
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <arpa/inet.h>

#define PORT_BROKER  9003
#define BUF_SIZE     1024
#define BROKER_IP    "127.0.0.1"

static volatile sig_atomic_t salir = 0;
static void on_sigint(int s) { (void)s; salir = 1; }

int main() {
    /* socket(): crea un socket UDP (SOCK_DGRAM) en IPv4 (AF_INET).
       No establece conexión, cada mensaje es un datagrama independiente. */
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) { perror("socket"); exit(1); }

    struct sockaddr_in broker_addr = {
        .sin_family = AF_INET,
        .sin_port   = htons(PORT_BROKER)
    };
    inet_pton(AF_INET, BROKER_IP, &broker_addr.sin_addr);

    /* bind(): fija un puerto local (el SO elige uno libre con puerto 0) para
       que el broker siempre envíe los mensajes a la misma dirección. */
    struct sockaddr_in local_addr = {
        .sin_family      = AF_INET,
        .sin_addr.s_addr = INADDR_ANY,
        .sin_port        = htons(0)
    };
    if (bind(sock, (struct sockaddr *)&local_addr, sizeof(local_addr)) < 0) {
        perror("bind");
        exit(1);
    }

    char topics[256];
    printf("[SUBSCRIBER UDP] Tema(s) a seguir, separados por coma (ej: PartidoA,PartidoB): ");
    if (scanf("%255s", topics) != 1) exit(1);
    getchar();

    char buffer[BUF_SIZE];
    snprintf(buffer, BUF_SIZE, "SUBSCRIBE:%s", topics);

    /* sendto(): envía la suscripción al broker. El broker toma la dirección
       de origen de este datagrama como la dirección del suscriptor.
       Ojo: si este datagrama se pierde, el suscriptor nunca se registra
       (UDP no confirma nada). */
    if (sendto(sock, buffer, strlen(buffer), 0,
               (struct sockaddr *)&broker_addr, sizeof(broker_addr)) < 0) {
        perror("sendto");
        exit(1);
    }

    printf("[SUBSCRIBER UDP] Suscrito a '%s'. Esperando mensajes...\n", topics);

    /* sigaction() sin SA_RESTART: Ctrl+C interrumpe recvfrom() para poder
       enviar UNSUBSCRIBE antes de terminar. */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_sigint;
    sigaction(SIGINT, &sa, NULL);

    while (!salir) {
        struct sockaddr_in sender_addr;
        socklen_t addr_len = sizeof(sender_addr);

        /* recvfrom(): recibe un datagrama del broker. No garantiza orden
           ni entrega. Bloquea hasta recibir algo. */
        int bytes = recvfrom(sock, buffer, BUF_SIZE - 1, 0,
                             (struct sockaddr *)&sender_addr, &addr_len);
        if (bytes < 0) {
            if (salir) break;
            perror("recvfrom");
            break;
        }
        buffer[bytes] = '\0';
        printf("[SUBSCRIBER UDP] Mensaje recibido: %s\n", buffer);
    }

    /* sendto(): avisa al broker que este suscriptor se va. */
    sendto(sock, "UNSUBSCRIBE", 11, 0,
           (struct sockaddr *)&broker_addr, sizeof(broker_addr));
    printf("\n[SUBSCRIBER UDP] Desuscrito\n");

    /* close(): cierra el socket y libera el descriptor de archivo. */
    close(sock);
    return 0;
}
