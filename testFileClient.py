from TapNet.tapnet import TapNet

if __name__ == '__main__':
    client = TapNet()

    with open("Test1.jpg", mode='rb') as file:
        fileContent = file.read()

    with open("Test2.jpg", mode='rb') as file:
        fileContent2 = file.read()

    client.send_data(fileContent2, 2, ('localhost', 10000))
    client.send_data(fileContent, 1, ('localhost', 10000))
    client.send_data(fileContent2, 2, ('localhost', 10000))
    client.send_data(fileContent2, 2, ('localhost', 10000))
    client.send_data(fileContent, 1, ('localhost', 10000))
    client.send_data(fileContent2, 2, ('localhost', 10000))
    client.send_data(fileContent2, 2, ('localhost', 10000))
    client.send_data(fileContent, 1, ('localhost', 10000))
    client.send_data(fileContent2, 2, ('localhost', 10000))
    client.send_data(fileContent2, 2, ('localhost', 10000))
    client.send_data(fileContent, 1, ('localhost', 10000))
    client.send_data(fileContent2, 2, ('localhost', 10000))
    client.send_data(fileContent2, 2, ('localhost', 10000))
    client.send_data(fileContent, 1, ('localhost', 10000))
    client.send_data(fileContent2, 2, ('localhost', 10000))
    client.send_data(fileContent2, 2, ('localhost', 10000))
    client.send_data(fileContent, 1, ('localhost', 10000))
    client.send_data(fileContent2, 2, ('localhost', 10000))
    client.send_data(fileContent, 1, ('localhost', 10000))
    client.send_data(fileContent2, 2, ('localhost', 10000))
    client.send_data(fileContent2, 2, ('localhost', 10000))
    client.send_data(fileContent, 1, ('localhost', 10000))
    client.send_data(fileContent2, 2, ('localhost', 10000))
    client.send_data(fileContent2, 2, ('localhost', 10000))
    #client.send_data(fileContent, 1, ('localhost', 10000))
    #client.send_data(fileContent2, 2, ('localhost', 10000))



