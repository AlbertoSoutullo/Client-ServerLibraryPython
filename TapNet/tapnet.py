from socket import socket, AF_INET, SOCK_DGRAM
from threading import Thread, Lock
from hashlib import sha256

import json
import time


class TapNet:
    _DATAGRAM_ACK = 0
    _DATAGRAM_NORMAL = 1
    _DATAGRAM_RELIABLE = 2

    _CHUNK_SIZE = 2048  # Tamaño de los chunks, en bytes

    def __init__(self, address):
        self.address = address  # Server Address ('localhost', 10000)
        self._package_ID = 0  # Id del próximo datagrama a enviar
        self._datagrams_awating_ack = {}  # Paquetes 'confiables' enviados a la espera de confirmación
        self._cache = {}  # Cache donde guardamos los datos recibidos
        self._socket = socket(AF_INET, SOCK_DGRAM)
        self._response_handler = None

    def start(self):
        print('Starting server on  {}'.format(self.address))
        self._socket = socket(AF_INET, SOCK_DGRAM)
        self._socket.bind(self.address)

        listen_loop = Thread(target=self._listen_loop)
        listen_loop.start()

        datagram_check = Thread(target=self.datagram_check)
        datagram_check.start()

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

    def _get_datagram_type(self, datagram):
        return int.from_bytes(datagram[:4], 'little')

    def _hash_is_correct(self, datagram):
        hash_received = datagram[8:40]
        payload = datagram[48:]

        payload_hash = sha256(payload).digest()

        if hash_received != payload_hash:
            return False
        else:
            return True

    def _parse_content(self, datagram):
        package_id = int.from_bytes(datagram[4:8], 'little')
        number_of_subpackages = int.from_bytes(datagram[40:44], 'little')
        subpackage_id = int.from_bytes(datagram[44:48], 'little')
        payload = datagram[48:]

        return package_id, number_of_subpackages, subpackage_id, payload

    def _create_unique_identifier(self, address, package_id):
        unique_identifier = (address[0], address[1], package_id)
        return unique_identifier

    def _create_package_register(self, unique_identifier, number_of_subpackages):
        self._cache[unique_identifier] = {}
        self._cache[unique_identifier]['data'] = [False for _ in range(number_of_subpackages)]
        self._cache[unique_identifier]['remaining_subpackages'] = number_of_subpackages

    def _check_if_package_already_registered(self, unique_identifier, number_of_subpackages):
        if unique_identifier not in self._cache.keys():
            self._create_package_register(unique_identifier, number_of_subpackages)

    def _send_ack(self, package_id, subpackage_id, address):
        ack = self._DATAGRAM_ACK.to_bytes(4, 'little') + package_id.to_bytes(4, 'little') + \
              subpackage_id.to_bytes(4, 'little')
        print(f"ACK sended to {package_id}, {subpackage_id}")
        self._socket.sendto(ack, address)

    def _save_package_payload(self, unique_identifier, package_id, subpackage_id, payload, address, reliable):
        if reliable:
            self._send_ack(package_id, subpackage_id, address)
        # We do this so we can avoid residual data with lates ACK
        if self._cache[unique_identifier]['remaining_subpackages'] != 0:
            print(f"Saving payload from {unique_identifier}, packageid:{package_id}, subpackageid{subpackage_id}")
            self._cache[unique_identifier]['data'][subpackage_id] = payload
            self._cache[unique_identifier]['remaining_subpackages'] -= 1

    def _if_package_completed_handle_and_clean(self, unique_identifier):
        if self._cache[unique_identifier]['remaining_subpackages'] == 0:
            print(f"Package id {unique_identifier} completed!")
            self._response_handler(b''.join((subdata for subdata in self._cache[unique_identifier]['data'])))
            del self._cache[unique_identifier]['data'][:]

    def _parse_datagram(self, datagram, address, reliable=False):
        # Datagram Header: datagram_type, packet_id, hash, number_of_subpackages, subpackage_id
        if self._hash_is_correct(datagram):
            package_id, number_of_subpackages, subpackage_id, payload = self._parse_content(datagram)
            print(f"Received datagram from {address}, packageid:{package_id}, subpackageid{subpackage_id}")
            unique_identifier = self._create_unique_identifier(address, package_id)
            self._check_if_package_already_registered(unique_identifier, number_of_subpackages)
            self._save_package_payload(unique_identifier, package_id, subpackage_id, payload, address, reliable)
            self._if_package_completed_handle_and_clean(unique_identifier)

    def _listen_loop(self):
        """
        Escucha las peticiones entrantes
        """

        while 1:
            datagram, address = self._socket.recvfrom(4096)

            datagram_type = self._get_datagram_type(datagram)

            # Todo: crear log.txt con los paquetes recibidos

            if datagram_type == self._DATAGRAM_RELIABLE:
                self._parse_datagram(datagram, address, reliable=True)
            elif datagram_type == self._DATAGRAM_NORMAL:
                self._parse_datagram(datagram, address, reliable=False)

    def datagram_check(self):
        """
        Comprueba que el estado de los envíos de los datagramas confiables. Realiza su trabajo en otro thread.
        """
        # TODO: Implementar
        return
        while 1:
            # Loop de comprobación
            time.sleep(.5)
