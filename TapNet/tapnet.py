
class TapNet:
    DATAGRAM_ACK = 0
    DATAGRAM_NORMAL = 1
    DATAGRAM_RELIABLE = 2

    CHUNK_SIZE = 2048  # Tamaño de los chunks, en bytes

    def __init__(self, address):
        self.address = address
        self.paqueteId = 0  # Id del próximo datagrama a enviar
        self.datagrams_awating_ack = {}  # Paquetes 'confiables' enviados a la espera de confirmación
        self.cache = {}
        self.sock = socket(AF_INET, SOCK_DGRAM)
        self.response_handler = None

    def start(self):
        print('Starting server on  {}'.format(self.address))
        self.sock = socket(AF_INET, SOCK_DGRAM)
        self.sock.bind(self.address)
        listen_loop = Thread(target=self.listen_loop)
        listen_loop.start()
        datagram_check = Thread(target=self.datagram_check)
        datagram_check.start()

    def send_ack(self, paquete_id, subpaquete_id, to):
        """
        Envía un ACK
        :param paquete_id: ID del paquete del cliente que estamos confirmando
        :param subpaquete_id: ID del datagrama del cliente que estamos confirmando
        :param to: Cliente al que vamos a enviar el ACK
        :return:
        """
        # TODO: Implementar
        return

    def send_bytes(self, bytes_to_send, data_type, to):
        """
        Envía datos a través del socket
        :param bytes_to_send: Datos a enviar
        :param data_type: Tipo de datos a enviar, ACK, NORMAL o RELIABLE
        :param to: Receptor al que vamos a enviar los datos
        """
        # TODO: Implementar
        return

    def send_json(self, json_to_send, data_type, to):
        """
        Envía un json a través del socket
        :param json_to_send: JSON a enviar
        :param data_type: Tipo de datos a enviar, ACK, NORMAL o RELIABLE
        :param to: Cliente al que vamos a enviar los datos
        """
        # TODO: Implementar
        #json_bytes = json.dumps(json_to_send).encode(encoding='utf-8')
        #self.send_bytes(json_bytes, data_type, to)
        return

    def listen_loop(self):
        """
        Escucha las peticiones entrantes
        """
        # TODO: Implementar
        return
        while 1:
            pass

    def datagram_check(self):
        """
        Comprueba que el estado de los envíos de los datagramas confiables. Realiza su trabajo en otro thread.
        """
        # TODO: Implementar
        return
        while 1:
            # Loop de comprobación
            sleep(.5)