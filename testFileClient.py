from TapNet.netLibrary import Client

if __name__ == '__main__':
    client = Client(address='localhost', address_port=10000, local_port=11000)

    with open("test-image.jpg", mode='rb') as file:
        fileContent = file.read()

        client.send_data(fileContent, 2, ('localhost', 10000))
        client.send_data(fileContent, 1, ('localhost', 10000))
        client.send_data(fileContent, 2, ('localhost', 10000))

    # Todo: Cambiar que cuando se quedan esperando los acks, siga enviando más paquetes
