from socket import socket, AF_INET, SOCK_DGRAM
from threading import Thread
from hashlib import sha256
import json
import time
import threading


class Server:
    __DATAGRAM_ACK = 0
    __DATAGRAM_NORMAL = 1
    __DATAGRAM_RELIABLE = 2

    __CHUNK_SIZE = 2048

    def __init__(self, address=None, port=None, handler=None):
        self.address = address
        self.port = port
        self._socket = socket(AF_INET, SOCK_DGRAM)
        self._response_handler = handler
        self._packages = {}

    def start(self):
        self._bound_server()
        self._listen_loop()

    def end(self):
        self._socket.close()
        print(f'Connection closed.')

    def _bound_server(self):
        self._socket.bind((self.address, self.port))
        print(f'Server bound on port {self.port}, host {self.address}')

    def _send_ack(self, package_id, subpackage_id, address):
        ack = self.__DATAGRAM_RELIABLE.to_bytes(4, 'little') + package_id.to_bytes(4, 'little') + \
              subpackage_id.to_bytes(4, 'little')
        self._socket.sendto(ack, address)

    def _create_package_register(self, package_id, number_of_subpackages):
        self._packages[package_id] = {}
        self._packages[package_id]['data'] = [False for _ in range(number_of_subpackages)]
        self._packages[package_id]['remaining_subpackages'] = number_of_subpackages

    def _check_if_package_already_registered(self, package_id, number_of_subpackages):
        if package_id not in self._packages.keys():
            self._create_package_register(package_id, number_of_subpackages)

    def _save_package_payload(self, package_id, subpackage_id, payload):
        # We do this so we can avoid residual data with lates ACK
        if self._packages[package_id]['remaining_subpackages'] == 0:
            self._packages[package_id]['data'][subpackage_id] = payload
            self._packages[package_id]['remaining_subpackages'] -= 1

    def _parse_content(self, datagram):
        package_id = int.from_bytes(datagram[4:8], 'little')
        number_of_subpackages = int.from_bytes(datagram[40:44], 'little')
        subpackage_id = int.from_bytes(datagram[44:48], 'little')
        payload = datagram[48:]

        return package_id, number_of_subpackages, subpackage_id, payload

    def _if_package_completed_handle_and_clean(self, package_id):
        if self._packages[package_id]['remaining_subpackages'] == 0:
            self._response_handler(b''.join((subdata for subdata in self._packages[package_id]['data'])))
            del self._packages[package_id]['data'][:]

    def _parse_datagram(self, datagram, address, reliable=False):
        # Datagram Header: datagram_type, packet_id, hash, number_of_subpackages, subpackage_id
        if self._hash_is_correct(datagram):
            package_id, number_of_subpackages, subpackage_id, payload = self._parse_content(datagram)

            self._check_if_package_already_registered(package_id, number_of_subpackages)
            self._save_package_payload(package_id, subpackage_id, payload)
            self._if_package_completed_handle_and_clean(package_id)
            if reliable:
                self._send_ack(package_id, subpackage_id, address)

    def _get_datagram_type(self, datagram):
        return int.from_bytes(datagram[:4], 'little')

    def _listen_loop(self):
        while 1:
            datagram, address = self._socket.recvfrom(4096)

            datagram_type = self._get_datagram_type(datagram)

            # Todo: crear log.txt con los paquetes recibidos

            if datagram_type == self.__DATAGRAM_RELIABLE:
                self._parse_datagram(datagram, address, reliable=True)
            elif datagram_type == self.__DATAGRAM_NORMAL:
                self._parse_datagram(datagram, address, reliable=False)

    def _hash_is_correct(self, datagram):
        hash_received = datagram[8:40]
        payload = datagram[48:]

        payload_hash = sha256(payload).digest()

        if hash_received != payload_hash:
            return False
        else:
            return True


class Client:
    DATAGRAM_ACK = 0
    DATAGRAM_NORMAL = 1
    DATAGRAM_RELIABLE = 2

    CHUNK_SIZE = 2048

    AWAIT_TIME = 0.5
    SEND_ATTEMPTS = 3

    def __init__(self, address=None, port=None):
        self.address = address
        self.port = port
        self._socket = socket(AF_INET, SOCK_DGRAM)
        self._packet_ID = 0
        self._datagrams_waiting_ack = {}
        self._completed = False

    def send_data(self, data, datagram_type, is_json=False):
        # Thread sending
        send_data_thread = threading.Thread(target=self._divide_and_send, args=(data, datagram_type, is_json))
        send_data_thread.start()

        # Thread listening acks
        listen_ack_thread = threading.Thread(target=self._ack_listener)
        listen_ack_thread.start()

        # Thread resending subpackages
        resend_packages_thread = threading.Thread(target=self._ack_resend_monitor)
        resend_packages_thread.start()

    def _divide_and_send(self, data, datagram_type, is_json=False):
        data_to_split = data
        if is_json:
            data_to_split = json.dumps(data_to_split)

        data_splitted = self._split(data_to_split)

        self._check_if_already_waiting_ack(self._packet_ID, len(data_splitted))

        for i, chunk in enumerate(data_splitted):
            hash = sha256(chunk).digest()
            chunk = datagram_type.to_bytes(4, 'little') + self._packet_ID.to_bytes(4, 'little') + hash \
                    + len(data_splitted).to_bytes(4, 'little') + i.to_bytes(4, 'little') + chunk
            self._socket.sendto(chunk, (self.address, self.port))
            self._datagrams_waiting_ack[self._packet_ID]['bakcup_data'] = chunk

        self._packet_ID += 1

    def _check_if_already_waiting_ack(self, packet_id, data_length):
        if packet_id not in self._datagrams_waiting_ack.keys():
            self._datagrams_waiting_ack[packet_id] = {}
            self._datagrams_waiting_ack[packet_id]['acks'] = [False for _ in range(data_length)]
            self._datagrams_waiting_ack[packet_id]['remaining_acks'] = data_length
            self._datagrams_waiting_ack[packet_id]['backup_data'] = [None for _ in range(data_length)]
            self._datagrams_waiting_ack[packet_id]['remaining_attempts'] = [self.SEND_ATTEMPTS for _ in range(data_length)]

    def _split(self, data):
        result = []
        while len(data) > self.CHUNK_SIZE:
            result.append(data[:self.CHUNK_SIZE])
            data = data[self.CHUNK_SIZE:]
        result.append(data)
        return result

    def _ack_listener(self):
        self._bound_listener()

        while not self._completed:
            datagram, address = self._socket.recvfrom(4096)

            datagram_type = int.from_bytes(datagram[:4], 'little')
            package_id = int.from_bytes(datagram[4:8], 'little')
            subpackage_id = int.from_bytes(datagram[8:12], 'little')

            if datagram_type == self.DATAGRAM_ACK:
                self._mark_subpackage(package_id, subpackage_id)
                self._clean_if_sended_correctly(package_id)

        self._unbound_listener()

    def _bound_listener(self):
        self._socket.bind((self.address, self.port))
        print(f'Listener bound on port {self.port}, host {self.address}')

    def _unbound_listener(self):
        self._socket.close()
        print(f'Listener closed on port {self.port}, host {self.address}')

    def _mark_subpackage(self, package_id, subpackage_id):
        if self._datagrams_waiting_ack[package_id]['remaining_acks'] != 0:
            self._datagrams_waiting_ack[package_id][subpackage_id] = True
            self._datagrams_waiting_ack[package_id]['remaining_acks'] -= 1

    def _clean_if_sended_correctly(self, package_id):
        if self._datagrams_waiting_ack[package_id]['remaining_acks'] != 0:
            del self._datagrams_waiting_ack[package_id]['backup_data']

    def _ack_resend_monitor(self):
        while not self._completed:
            for key in self._datagrams_waiting_ack.keys():
                if self._datagrams_waiting_ack[key]['remaining_acks'] != 0:
                    self._resend_data(key)

    def _resend_data(self, package_id):
        remaining_acks = [i for i, x in enumerate(self._datagrams_waiting_ack[package_id]['acks']) if not x]
        if not remaining_acks: #if list is empty
            self._completed = True
        else:
            for ack_location in remaining_acks:
                self._socket.sendto(self._datagrams_waiting_ack[package_id]['backup_data'][ack_location],
                                    (self.address, self.port))
                self._datagrams_waiting_ack['remaining_attempts'][ack_location] -= 1
                if self._datagrams_waiting_ack['remaining_attempts'][ack_location] == 0:
                    self._datagrams_waiting_ack['acks'][ack_location] = True


#2048 bytes sin cabezera(48)
'''
Tipo de paquete (entero codificado en 4 bytes con el formato ‘little’)
Identificador del paquete (entero codificado en 4 bytes con el formato ‘little’)
Hash del subpaquete (hashlib.sha256().digest() de los datos subpaquete) 32 bytes
Número total de subpaquetes (entero codificado en 4 bytes con el formato ‘little’)
Identificador del subpaquete (entero codificado en 4 bytes con el formato ‘little’)
'''

#al handler se llama cuando le llegue un paquete completo

'''
En el caso de los ACK, no tendrán parte de datos, únicamente la siguiente cabecera:
-Tipo de paquete (entero codificado en 4 bytes con el formato ‘little’)
-Identificador del paquete (entero codificado en 4 bytes con el formato ‘little’)
-Identificador del subpaquete (entero codificado en 4 bytes con el formato ‘little’)
'''
#ack: 4 tipode paquete, id de l paquete, id del subpaquete