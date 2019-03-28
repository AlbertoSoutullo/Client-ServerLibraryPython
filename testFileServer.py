from TapNet.netLibrary import Server


def on_image_received(data):
    with open('rec_img.jpg', 'wb') as outfile:
        outfile.write(data)


if __name__ == "__main__":
    server = Server(address='localhost', port=10000, handler=on_image_received)

    server.start()
