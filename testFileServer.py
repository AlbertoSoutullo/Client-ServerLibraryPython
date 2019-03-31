from TapNet.tapnet import TapNet


def on_image_received(data, address):
    with open('rec_img.jpg', 'wb') as outfile:
        outfile.write(data)


if __name__ == "__main__":
    server = TapNet(('localhost', 10000))
    server.response_handler = on_image_received

    server.start()
