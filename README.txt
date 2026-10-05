LABORATORIO 3 — ANÁLISIS CAPA DE TRANSPORTE Y SOCKETS
ISIS 2311 — Redes y Servicios de Comunicaciones
Universidad de los Andes

Integrantes:
  - Jacobo Zarruk Estrada       (202223913)
  - 


============================================================
1. DESCRIPCIÓN GENERAL
============================================================

Sistema de noticias deportivas en tiempo real con el modelo
publicación–suscripción, implementado tres veces sobre distintos
transportes para comparar su comportamiento:

  - tcp/   Versión TCP  (obligatoria)
  - udp/   Versión UDP  (obligatoria)
  - quic/  Versión "estilo QUIC" sobre UDP (bono)

Componentes (iguales en las tres versiones):

  - Publisher (periodista): lee un tema (el partido, ej. PartidoA) y
    luego envía al broker cada línea que se escriba como un evento
    del partido ("Gol de Equipo A al minuto 32").
  - Broker (canal central): recibe los mensajes de todos los
    publishers y los reenvía, SIN modificar el contenido, solo a los
    suscriptores interesados en ese tema.
  - Subscriber (hincha): se suscribe a uno o varios partidos y
    muestra en pantalla las actualizaciones que le llegan.

Publishers y subscribers nunca se conocen entre sí: solo conocen la
IP y el puerto del broker (desacoplamiento).

Todo está escrito en C usando únicamente la librería estándar y la
API de sockets POSIX del sistema operativo. No se usan librerías
externas.


============================================================
2. REQUISITOS
============================================================

- Sistema operativo: Linux (probado en Ubuntu 22.04 vía WSL)
- Compilador: gcc
- Librería: pthread (solo para broker_tcp; incluida en Linux)


============================================================
3. ESTRUCTURA DE ARCHIVOS
============================================================

lab3/
├── tcp/
│   ├── broker_tcp.c
│   ├── publisher_tcp.c
│   └── subscriber_tcp.c
├── udp/
│   ├── broker_udp.c
│   ├── publisher_udp.c
│   ├── publisher_udp_stress.c   (auxiliar para pruebas de carga)
│   └── subscriber_udp.c
└── quic/
    ├── broker_quic.c
    ├── publisher_quic.c
    └── subscriber_quic.c


============================================================
4. COMPILACIÓN
============================================================

TCP:
  gcc broker_tcp.c     -o broker_tcp     -lpthread
  gcc publisher_tcp.c  -o publisher_tcp
  gcc subscriber_tcp.c -o subscriber_tcp

UDP:
  gcc broker_udp.c            -o broker_udp
  gcc publisher_udp.c         -o publisher_udp
  gcc publisher_udp_stress.c  -o publisher_udp_stress
  gcc subscriber_udp.c        -o subscriber_udp

QUIC (bono):
  gcc broker_quic.c     -o broker_quic
  gcc publisher_quic.c  -o publisher_quic
  gcc subscriber_quic.c -o subscriber_quic


============================================================
5. VERSIÓN TCP
============================================================

Puertos: 9001 (publishers) y 9002 (subscribers).

Diseño:
  - El broker abre dos sockets de escucha (uno por tipo de cliente).
    Un hilo hace accept() de publishers y el hilo principal hace
    accept() de subscribers.
  - Cada conexión aceptada se atiende en su propio hilo (pthread).
    Así un cliente lento no bloquea a los demás.
  - La lista de suscriptores es compartida entre hilos, por lo que se
    protege con un mutex.
  - Cada publisher y cada subscriber mantiene UNA conexión TCP
    persistente con el broker (three-way handshake al conectarse,
    FIN al cerrar).

Formato de los mensajes:
  Publisher  -> Broker : <tema>:<mensaje>\n
  Subscriber -> Broker : SUBSCRIBE:<tema1>[,<tema2>,...]\n
  Broker     -> Sub    : <tema>:<mensaje>\n

Por qué el '\n' al final: TCP entrega un FLUJO de bytes y no respeta
los límites entre mensajes. Un solo recv() puede traer medio mensaje
o dos mensajes pegados. Por eso cada mensaje termina en '\n' y tanto
el broker como el subscriber acumulan lo recibido y lo separan línea
por línea.

Otros detalles:
  - send_all() repite send() hasta enviar todos los bytes, porque
    send() puede enviar menos de lo pedido.
  - Se ignora SIGPIPE y se usa MSG_NOSIGNAL. Si un suscriptor se cae,
    escribir en su socket ya no mata al broker: solo se marca como
    inactivo.
  - Cuando un subscriber se desconecta, recv() retorna 0, el broker lo
    marca inactivo y su espacio en la tabla se reutiliza.


============================================================
6. VERSIÓN UDP
============================================================

Puerto: 9003 (un único socket para todo).

Diseño:
  - No hay conexiones: el broker usa un único socket y un bucle con
    recvfrom(). No necesita hilos porque cada datagrama es
    independiente.
  - El broker identifica a cada suscriptor por la dirección de origen
    (IP:puerto) que entrega recvfrom() al recibir su SUBSCRIBE, y
    después le envía los mensajes con sendto() a esa dirección.
  - El subscriber hace bind() a un puerto local (elegido por el SO)
    para que esa dirección no cambie durante la sesión.

Formato de los mensajes (un mensaje por datagrama):
  Subscriber -> Broker : SUBSCRIBE:<tema1>[,<tema2>,...]
  Subscriber -> Broker : UNSUBSCRIBE        (al salir con Ctrl+C)
  Publisher  -> Broker : <tema>:<mensaje>
  Broker     -> Sub    : <tema>:<mensaje>

Aquí no hace falta delimitador: UDP sí respeta los límites de mensaje,
así que un recvfrom() equivale exactamente a un datagrama.

Comportamiento esperado de UDP (no son errores del código):
  - No hay confirmaciones: si un datagrama se pierde, nadie lo
    reenvía. Esto incluye el SUBSCRIBE: si se pierde, el suscriptor
    nunca queda registrado.
  - No se garantiza el orden de llegada.
  - El broker solo sabe que un suscriptor se fue si este envía
    UNSUBSCRIBE. Si el proceso muere sin enviarlo, el broker sigue
    mandándole datagramas.

publisher_udp_stress.c es un programa auxiliar que envía 50 mensajes
lo más rápido posible. Sirve para provocar pérdidas y congestión en
las pruebas. No forma parte del sistema principal.


============================================================
7. VERSIÓN "ESTILO QUIC" SOBRE UDP (BONO)
============================================================

Puerto: 9004.

Aclaración: QUIC real (RFC 9000) exige cifrado TLS 1.3, IDs de
conexión, streams multiplexados, etc. No se puede implementar solo
con la librería estándar de C, y las librerías que lo hacen
(msquic, ngtcp2, quiche) no están permitidas sin documentarlas. Esta
versión implementa sobre UDP, a nivel de aplicación, los mecanismos
centrales que QUIC agrega encima de UDP. En Wireshark el tráfico
aparece como UDP en el puerto 9004, no como QUIC.

Mecanismos implementados y dónde están:

  Handshake de conexión       El subscriber envía SUBSCRIBE y reintenta
                              hasta recibir ACK:SUBSCRIBE.
  Números de secuencia        El publisher numera sus mensajes. El broker
                              lleva un contador propio por cada
                              suscriptor (como los packet numbers de una
                              conexión QUIC).
  ACKs y retransmisión        Publisher->broker: el publisher espera
                              ACK:PUB y reintenta cada 500 ms (máx. 5).
                              Broker->subscriber: el broker guarda cada
                              DATA pendiente y lo retransmite cada 500 ms
                              hasta recibir ACK:DATA.
  Descarte de duplicados      El broker ignora un PUB ya recibido (solo
                              re-confirma). El subscriber descarta un
                              DATA ya entregado.
  Entrega en orden            El subscriber guarda en un buffer los DATA
                              que llegan adelantados y los muestra solo
                              cuando llegan los que faltaban.
  Cierre por inactividad      Si un suscriptor no confirma tras 5
                              retransmisiones, el broker cierra su
                              "conexión".

Formato de los mensajes:
  Publisher  -> Broker : PUB:<seq>:<tema>:<mensaje>
  Broker     -> Pub    : ACK:PUB:<seq>
  Subscriber -> Broker : SUBSCRIBE:<tema1>[,<tema2>,...]
  Broker     -> Sub    : ACK:SUBSCRIBE
  Broker     -> Sub    : DATA:<seq>:<tema>:<mensaje>
  Subscriber -> Broker : ACK:DATA:<seq>

Pérdida simulada: en localhost casi nunca se pierden paquetes, así que
el broker acepta un porcentaje opcional de pérdida
(./broker_quic 20). Con él descarta al azar ese porcentaje de los
datagramas que recibe y envía. Así se pueden observar las
retransmisiones, los duplicados descartados y el reordenamiento.


============================================================
8. EJECUCIÓN
============================================================

Siempre se inicia primero el broker. Para la prueba pedida en la guía
(1 broker, 2 subscribers, 2 publishers, al menos 10 mensajes por
publisher) se usan 5 terminales. Ejemplo con TCP (para UDP o QUIC
basta con cambiar el sufijo del ejecutable):

  Terminal 1:  ./broker_tcp
  Terminal 2:  ./subscriber_tcp     -> tema: PartidoA
  Terminal 3:  ./subscriber_tcp     -> tema: PartidoA,PartidoB
  Terminal 4:  ./publisher_tcp      -> tema: PartidoA
  Terminal 5:  ./publisher_tcp      -> tema: PartidoB

En cada publisher se escribe un mensaje por línea (Enter para enviar)
y 'salir' para terminar. El subscriber 1 recibe solo PartidoA y el
subscriber 2 recibe ambos partidos.

QUIC con pérdida simulada:
  Terminal 1:  ./broker_quic 20

Prueba de carga en UDP (3 publishers simultáneos):
  ./publisher_udp_stress PartidoA & \
  ./publisher_udp_stress PartidoA & \
  ./publisher_udp_stress PartidoA


============================================================
9. CAPTURA CON WIRESHARK
============================================================

Como todo corre en 127.0.0.1, se captura en la interfaz de loopback
("lo" en Linux; "Adapter for loopback traffic capture" en Windows).
Si se usa WSL, se puede capturar dentro de WSL con
  sudo tcpdump -i lo -w tcp_pubsub.pcap port 9001 or port 9002
y abrir el archivo .pcap en Wireshark.

Filtros de visualización útiles:
  TCP:   tcp.port == 9001 || tcp.port == 9002
  UDP:   udp.port == 9003
  QUIC:  udp.port == 9004

Capturas (.pcap):
  https://drive.google.com/drive/folders/1WwrxEm3n56f_1Dbqh9SS07_BxG0ImiU9?usp=sharing


============================================================
10. FUNCIONES DE SOCKETS UTILIZADAS
============================================================

Todas pertenecen a la API estándar del sistema (sys/socket.h,
arpa/inet.h). Cada llamada está comentada en el código.

  socket()       Crea el socket: SOCK_STREAM (TCP) o SOCK_DGRAM (UDP).
  setsockopt()   SO_REUSEADDR: reutilizar el puerto al reiniciar el
                 broker. SO_RCVTIMEO: límite de espera de recvfrom()
                 (timeouts de retransmisión en QUIC).
  bind()         Asocia el socket a un puerto local.
  listen()       (TCP) Pone el socket del broker en modo escucha.
  accept()       (TCP) Acepta una conexión y crea un socket dedicado.
  connect()      (TCP) Establece la conexión con el broker.
  send()/recv()  (TCP) Envía y recibe bytes por una conexión.
  sendto()       (UDP) Envía un datagrama a una dirección dada.
  recvfrom()     (UDP) Recibe un datagrama y la dirección del remitente.
  inet_pton()    Convierte la IP de texto a binario.
  htons()        Convierte el puerto al orden de bytes de red.
  close()        Cierra el socket (en TCP envía FIN).


============================================================
11. NOTAS
============================================================

- Para probar entre dos equipos de la misma red, cambiar BROKER_IP
  ("127.0.0.1") en los publishers y subscribers por la IP del equipo
  que ejecuta el broker.
- Los temas no pueden contener ':' ni ',' (se usan como separadores)
  y deben tener menos de 64 caracteres.
