from netLibrary import Client

if __name__ == '__main__':
    client = Client(address='localhost', port=10001)

    with open("test-image.jpg", mode='rb') as file:
        fileContent = file.read()

        client.send_data(fileContent, 2)
