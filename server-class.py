from tapnet import TapNet

def on_image_received(data):
    with open('rec_img.jpg', 'wb') as outfile:
        outfile.write(data)

tapnet = TapNet(('localhost', 10000), on_image_received)
tapn.stat

