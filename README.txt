LABORATORIO 3 — ANÁLISIS CAPA DE TRANSPORTE Y SOCKETS
ISIS 2311 — Redes y Servicios de Comunicaciones
Universidad de los Andes

Integrantes:
  - Jacobo Zarruk Estrada       (202223913)
  - Jerónimo Vásquez Ponce      (202223824)
  - Kevin Arenas                (202110673)


REQUISITOS

- Sistema operativo: Linux (probado en Ubuntu 22.04 via WSL)
- Compilador: gcc
- Librería: pthread (incluida en Linux por defecto)


ESTRUCTURA DE ARCHIVOS

lab3/
├── tcp/
│   ├── broker_tcp.c
│   ├── publisher_tcp.c
│   └── subscriber_tcp.c
├── udp/
│   ├── broker_udp.c
│   ├── publisher_udp.c
│   ├── publisher_udp_stress.c
│   └── subscriber_udp.c
└── quic/
    ├── broker_quic.c
    ├── publisher_quic.c
    └── subscriber_quic.c


COMPILACIÓN

TCP:
  gcc broker_tcp.c     -o broker_tcp     -lpthread
  gcc publisher_tcp.c  -o publisher_tcp
  gcc subscriber_tcp.c -o subscriber_tcp

UDP:
  gcc broker_udp.c            -o broker_udp            -lpthread
  gcc publisher_udp.c         -o publisher_udp
  gcc publisher_udp_stress.c  -o publisher_udp_stress
  gcc subscriber_udp.c        -o subscriber_udp

QUIC (Bono):
  gcc broker_quic.c     -o broker_quic     -lpthread
  gcc publisher_quic.c  -o publisher_quic
  gcc subscriber_quic.c -o subscriber_quic


EJECUCIÓN — TCP

El broker escucha publishers en el puerto 9001
y subscribers en el puerto 9002.

1. Terminal 1 — Broker:
     ./broker_tcp

2. Terminal 2 — Subscriber 1:
     ./subscriber_tcp
     (ingresar tema, ej: PartidoA)

3. Terminal 3 — Subscriber 2:
     ./subscriber_tcp
     (ingresar tema, ej: PartidoA)

4. Terminal 4 — Publisher:
     ./publisher_tcp
     (ingresar tema, ej: PartidoA)
     (escribir mensajes y presionar Enter para enviar)
     (escribir 'salir' para terminar)


EJECUCIÓN — UDP

El broker escucha en el puerto 9003.

1. Terminal 1 — Broker:
     ./broker_udp

2. Terminal 2 — Subscriber 1:
     ./subscriber_udp
     (ingresar tema, ej: PartidoA)

3. Terminal 3 — Subscriber 2:
     ./subscriber_udp
     (ingresar tema, ej: PartidoA)

4. Terminal 4 — Publisher normal:
     ./publisher_udp
     (ingresar tema, ej: PartidoA)

   O publisher de stress (envía 50 mensajes automáticamente):
     ./publisher_udp_stress PartidoA

   Para congestión con 3 publishers simultáneos:
     ./publisher_udp_stress PartidoA & \
     ./publisher_udp_stress PartidoA & \
     ./publisher_udp_stress PartidoA


EJECUCIÓN — QUIC (Bono)

El broker escucha en el puerto 9004.

1. Terminal 1 — Broker:
     ./broker_quic

2. Terminal 2 — Subscriber:
     ./subscriber_quic
     (ingresar tema, ej: PartidoA)

3. Terminal 3 — Publisher:
     ./publisher_quic
     (ingresar tema, ej: PartidoA)
     (escribir mensajes y presionar Enter para enviar)
     (escribir 'salir' para terminar)


NOTAS

- Los archivos de captura de tráfico (.pcap) están disponibles en:
  https://drive.google.com/drive/folders/1WwrxEm3n56f_1Dbqh9SS07_BxG0ImiU9?usp=sharing

- Para pruebas entre dos equipos en la misma red, reemplazar
  la IP 127.0.0.1 en el archivo del publisher por la IP
  real del equipo que ejecuta el broker.

- El publisher_udp_stress.c es un archivo adicional usado
  para pruebas de congestión y no forma parte del sistema
  principal del laboratorio.