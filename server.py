from socket import socket, AF_INET, SOCK_DGRAM, SOCK_STREAM
import random

server = socket(AF_INET, SOCK_DGRAM)
server.bind(('localhost', 10000))
print(f'Server bound')

msgs = []

while 1:
    data, address = server.recvfrom(4096)

    # Cuando recibes algo:
    # Los 4 primeros bytes son el tamaño en chunks del mensaje
    len_msg = int.from_bytes(data[:4], 'little')
    # Los siguientes 4 bytes son el número de chunk que está llegando
    chunk_id = int.from_bytes(data[4:8], 'little')

    if len(msgs) == 0:
        msgs = [None for _ in range(len_msg)]

    # El resto son los datos del mensaje
    content = data[8:]
    print(f'Received chunk {chunk_id} of {len_msg}')
    msgs[chunk_id] = content
    #if [a for a in msgs if not a]:
    if None not in msgs:
        break

'''aux = msgs[5]
msgs[5] = msgs[27]
msgs[25] = aux
'''

#msgs[5] = b''

with open('rec_img.jpg', 'wb') as outfile:
    outfile.write(b''.join([msg for msg in msgs]))

'''
Poder distinguir entre paquetes confiables (que tenemos que hacer todo cuando esté
en nuestro poder para que lleguen, paquetes normales (si no llegan, no pasa nada) y
paquetes de confirmación ACK, que se encargarán de confirmar la recepción de los
paquetes confiables.

Verificar mediante algún algoritmo hash si los datos recibidos son los que nos han
enviado. Si el paquete es confiable y ha llegado bien, enviaremos en ACK del paquete
correspondiente al remitente. Si el paquete no es confiable, lo ignoraremos (como si no
hubiera llegado). Si el paquete es confiable y llega mal, no enviaremos el ACK
correspondiente, de manera que el remitente lo volverá a enviar.

Los enteros asignados a cada tipo de paquete son: ACK = 0, NORMAL = 1,
CONFIABLE = 2.

Si enviamos un paquete confiable y no nos llega su ACK en 0,5 segundos, lo
volveremos a enviar. Repetiremos este proceso hasta un máximo de 3 veces.

Los paquetes tendrán las siguientes cabeceras:
-Tipo de paquete (entero codificado en 4 bytes con el formato ‘little’)
-Identificador del paquete (entero codificado en 4 bytes con el formato ‘little’)
-Hash del subpaquete (hashlib.sha256().digest() de los datos subpaquete)
-Número total de subpaquetes (entero codificado en 4 bytes con el formato ‘little’)
-Identificador del subpaquete (entero codificado en 4 bytes con el formato ‘little’)

El tamaño de la parte de datos del paquete (son cabeceras) será de 2048 bytes. En
caso de querer enviar datos más grandes, tendremos que ser capaces de partirlos en
origen y recomponerlos en el destino.

La librería transferirá directamente bytes, pero proporcionará un método que nos
permita mandar y recibir JSON.
'''