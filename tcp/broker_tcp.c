#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>

#define PORT_PUB   9001
#define PORT_SUB   9002
#define MAX_CLIENTS 50
#define BUF_SIZE   1024

typedef struct {
    int  socket;
    char topic[64];
    int  active;
} Subscriber;

Subscriber subscribers[MAX_CLIENTS];
int sub_count = 0;
pthread_mutex_t sub_mutex = PTHREAD_MUTEX_INITIALIZER;

void *manejar_publisher(void *arg) {
    int pub_sock = *(int *)arg;
    free(arg);

    char buffer[BUF_SIZE];

    printf("[BROKER] Publisher conectado (socket %d)\n", pub_sock);

    while (1) {
        memset(buffer, 0, BUF_SIZE);

        /* recv(): recibe datos desde el socket del publisher.
           Retorna el número de bytes recibidos, 0 si se cerró
           la conexión, o -1 si hubo error. */
        int bytes = recv(pub_sock, buffer, BUF_SIZE - 1, 0);

        if (bytes <= 0) {
            printf("[BROKER] Publisher desconectado (socket %d)\n", pub_sock);
            break;
        }

        printf("[BROKER] Mensaje recibido: %s\n", buffer);

        char topic[64], mensaje[BUF_SIZE];
        char *sep = strchr(buffer, ':');

        if (sep == NULL) {
            printf("[BROKER] Formato inválido, ignorando mensaje\n");
            continue;
        }

        int topic_len = sep - buffer;
        strncpy(topic, buffer, topic_len);
        topic[topic_len] = '\0';
        strcpy(mensaje, sep + 1);

        pthread_mutex_lock(&sub_mutex);
        int enviados = 0;
        for (int i = 0; i < sub_count; i++) {
            if (subscribers[i].active &&
                strcmp(subscribers[i].topic, topic) == 0) {

                /* send(): envía datos al socket del suscriptor.
                   Retorna el número de bytes enviados o -1 si hubo error. */
                send(subscribers[i].socket, buffer, strlen(buffer), 0);
                enviados++;
            }
        }
        pthread_mutex_unlock(&sub_mutex);

        printf("[BROKER] Reenviado a %d suscriptores del tema '%s'\n", enviados, topic);
    }

    /* close(): cierra el socket y libera el descriptor de archivo. */
    close(pub_sock);
    return NULL;
}

void *manejar_subscriber(void *arg) {
    int sub_sock = *(int *)arg;
    free(arg);

    char buffer[BUF_SIZE];
    memset(buffer, 0, BUF_SIZE);

    /* recv(): recibe el mensaje de suscripción del cliente.
       Formato esperado: "SUBSCRIBE:NombreTema" */
    int bytes = recv(sub_sock, buffer, BUF_SIZE - 1, 0);
    if (bytes <= 0) {
        close(sub_sock);
        return NULL;
    }

    char topic[64] = "";
    if (strncmp(buffer, "SUBSCRIBE:", 10) == 0) {
        strncpy(topic, buffer + 10, sizeof(topic) - 1);
    } else {
        printf("[BROKER] Suscriptor no envió topic válido\n");
        close(sub_sock);
        return NULL;
    }

    pthread_mutex_lock(&sub_mutex);
    int idx = sub_count++;
    subscribers[idx].socket = sub_sock;
    strncpy(subscribers[idx].topic, topic, sizeof(subscribers[idx].topic) - 1);
    subscribers[idx].active = 1;
    pthread_mutex_unlock(&sub_mutex);

    printf("[BROKER] Suscriptor registrado en tema '%s' (socket %d)\n", topic, sub_sock);

    while (1) {
        memset(buffer, 0, BUF_SIZE);
        int r = recv(sub_sock, buffer, BUF_SIZE - 1, 0);
        if (r <= 0) {
            printf("[BROKER] Suscriptor desconectado (socket %d)\n", sub_sock);
            pthread_mutex_lock(&sub_mutex);
            subscribers[idx].active = 0;
            pthread_mutex_unlock(&sub_mutex);
            break;
        }
    }

    /* close(): cierra el socket del suscriptor al desconectarse. */
    close(sub_sock);
    return NULL;
}

void *escuchar_publishers(void *arg) {
    int server_sock = *(int *)arg;

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);

        int *client_sock = malloc(sizeof(int));

        /* accept(): acepta una conexión entrante de un publisher.
           Bloquea hasta que llega una conexión y retorna un nuevo
           socket dedicado a esa comunicación. */
        *client_sock = accept(server_sock, (struct sockaddr *)&client_addr, &addr_len);

        if (*client_sock < 0) {
            perror("[BROKER] Error en accept (publisher)");
            free(client_sock);
            continue;
        }

        pthread_t tid;
        pthread_create(&tid, NULL, manejar_publisher, client_sock);
        pthread_detach(tid);
    }
    return NULL;
}

int main() {
    /* socket(): crea un socket TCP (SOCK_STREAM) en el dominio IPv4 (AF_INET).
       Retorna un descriptor de archivo o -1 si hubo error. */
    int pub_server = socket(AF_INET, SOCK_STREAM, 0);
    if (pub_server < 0) { perror("socket publishers"); exit(1); }

    int opt = 1;
    /* setsockopt(): configura el socket para reutilizar la dirección
       inmediatamente después de cerrar, evitando "Address already in use". */
    setsockopt(pub_server, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in pub_addr = {
        .sin_family      = AF_INET,
        .sin_addr.s_addr = INADDR_ANY,
        .sin_port        = htons(PORT_PUB)
    };

    /* bind(): asocia el socket a la dirección IP y puerto definidos. */
    bind(pub_server, (struct sockaddr *)&pub_addr, sizeof(pub_addr));

    /* listen(): pone el socket en modo escucha, aceptando hasta 10
       conexiones pendientes en la cola. */
    listen(pub_server, 10);

    /* socket(): crea un segundo socket TCP para los suscriptores. */
    int sub_server = socket(AF_INET, SOCK_STREAM, 0);
    if (sub_server < 0) { perror("socket subscribers"); exit(1); }

    setsockopt(sub_server, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in sub_addr = {
        .sin_family      = AF_INET,
        .sin_addr.s_addr = INADDR_ANY,
        .sin_port        = htons(PORT_SUB)
    };

    /* bind(): asocia el segundo socket al puerto de suscriptores. */
    bind(sub_server, (struct sockaddr *)&sub_addr, sizeof(sub_addr));

    /* listen(): pone el socket de suscriptores en modo escucha. */
    listen(sub_server, 10);

    printf("[BROKER] Escuchando publishers en puerto %d\n", PORT_PUB);
    printf("[BROKER] Escuchando subscribers en puerto %d\n", PORT_SUB);

    pthread_t pub_thread;
    pthread_create(&pub_thread, NULL, escuchar_publishers, &pub_server);
    pthread_detach(pub_thread);

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);

        int *client_sock = malloc(sizeof(int));

        /* accept(): acepta conexiones entrantes de suscriptores.
           Bloquea hasta recibir una conexión y retorna un nuevo socket. */
        *client_sock = accept(sub_server, (struct sockaddr *)&client_addr, &addr_len);

        if (*client_sock < 0) {
            perror("[BROKER] Error en accept (subscriber)");
            free(client_sock);
            continue;
        }

        pthread_t tid;
        pthread_create(&tid, NULL, manejar_subscriber, client_sock);
        pthread_detach(tid);
    }

    return 0;
}
