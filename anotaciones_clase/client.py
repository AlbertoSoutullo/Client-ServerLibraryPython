from socket import socket, AF_INET, SOCK_DGRAM, SOCK_STREAM


def split(b, size):
    result = []
    while len(b) > size:
        result.append(b[:size])
        b = b[size:]
    result.append(b)
    return result


BUFFER_SIZE = 2048
client = socket(AF_INET, SOCK_DGRAM)


with open('logo-vector-u-tad.jpg', 'rb') as file:
    msg = file.read()
    print(len(msg))
    address = ('localhost', 10000)

    # Formato de los paquetes:
    # Número total de chunks
    # Id del chunk actual
    # Datos del chunk

    # Partir msg en trozos de tamaño máximo BUFFER_SIZE

    msgs = split(msg, BUFFER_SIZE)

    for i, chunk in enumerate(msgs):
        chunk = len(msgs).to_bytes(4, 'little') + i.to_bytes(4, 'little') + chunk
        client.sendto(chunk, address)
        print(f'Sent data: {chunk} to {address}')

''' Como cliente
tapnet = TapNet()
tapnet.start()

with open file as file:
    data.read
    tapnet.senddata(data, tipo_de_datagrama, (localhost, port))
'''