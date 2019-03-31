from socket import socket, AF_INET, SOCK_DGRAM
from threading import Thread, Lock
from hashlib import sha256

import json
import time


class TapNet:
    DATAGRAM_ACK = 0
    DATAGRAM_NORMAL = 1
    DATAGRAM_RELIABLE = 2

    _CHUNK_SIZE = 2048  # Tamaño de los chunks, en bytes

    _SEND_ATTEMPTS = 3
    _AWAIT_TIME = 0.5

    def __init__(self, address=None):
        self.address = address  # Server Address ('localhost', 10000)
        self._package_ID = 0  # Id del próximo datagrama a enviar
        self._datagrams_awating_ack = {}  # Paquetes 'confiables' enviados a la espera de confirmación
        self._cache = {}  # Cache donde guardamos los datos recibidos
        self._socket = socket(AF_INET, SOCK_DGRAM)
        self._is_binded = False
        self.response_handler = None
        self._mutex = Lock()

    def start(self):
        print('Starting server on  {}'.format(self.address))
        self._socket.bind(self.address)
        self._is_binded = True

        listen_loop = Thread(target=self._listen_loop)
        listen_loop.start()

    def _split(self, data):
        result = []
        while len(data) > self._CHUNK_SIZE:
            result.append(data[:self._CHUNK_SIZE])
            data = data[self._CHUNK_SIZE:]
        result.append(data)
        return result

    def _initialize_structure_for_reliable(self, data_length, unique_package_id):
        if unique_package_id not in self._datagrams_awating_ack.keys():
            self._datagrams_awating_ack[unique_package_id] = {}
            self._datagrams_awating_ack[unique_package_id]['acks'] = [False for _ in range(data_length)]
            self._datagrams_awating_ack[unique_package_id]['remaining_acks'] = data_length
            self._datagrams_awating_ack[unique_package_id]['backup_data'] = [None for _ in range(data_length)]
            self._datagrams_awating_ack[unique_package_id]['remaining_attempts'] = [self._SEND_ATTEMPTS for _ in range(data_length)]
            self._datagrams_awating_ack[unique_package_id]['last_send_time'] = [None for _ in range(data_length)]
            self._datagrams_awating_ack[unique_package_id]['completed_or_expired'] = False

    def _is_completed_or_expired(self, package_id):
        completed_or_expired = self._datagrams_awating_ack[package_id].get('completed_or_expired', True)
        return completed_or_expired

    def _mark_subpackage(self, package_id, subpackage_id):
        self._mutex.acquire()
        if self._datagrams_awating_ack[package_id]['remaining_acks'] > 0:
            self._datagrams_awating_ack[package_id]['acks'][subpackage_id] = True
            self._datagrams_awating_ack[package_id]['remaining_acks'] -= 1
            self._datagrams_awating_ack[package_id]['backup_data'][subpackage_id] = None
            if self._datagrams_awating_ack[package_id]['remaining_acks'] == 0:
                self._datagrams_awating_ack[package_id]['completed_or_expired'] = True
        self._mutex.release()

    def _clean_package_register(self, package_id_listener):
        # self._datagrams_awating_ack[package_id_listener].pop('acks')
        # self._datagrams_awating_ack[package_id_listener].pop('remaining_acks')
        # self._datagrams_awating_ack[package_id_listener].pop('backup_data')
        # self._datagrams_awating_ack[package_id_listener].pop('remaining_attempts')
        # self._datagrams_awating_ack[package_id_listener].pop('last_send_time')
        self._datagrams_awating_ack[package_id_listener].clear()

    def _ack_listener(self, unique_package_id):
        if not self._is_binded:
            self._socket.bind(('localhost', 10001))
            self._is_binded = True

        while not self._is_completed_or_expired(unique_package_id):

            datagram, address = self._socket.recvfrom(4096)

            datagram_type = int.from_bytes(datagram[:4], 'little')
            package_id = int.from_bytes(datagram[4:8], 'little')
            subpackage_id = int.from_bytes(datagram[8:12], 'little')

            if datagram_type == self.DATAGRAM_ACK:
                print(f'ACK received, type: {datagram_type}, Package id:{package_id}, subpackageid:{subpackage_id}')
                self._mark_subpackage(package_id, subpackage_id)

        print("Closing listener")
        # Todo: Export data to txt
        self._clean_package_register(unique_package_id)

    def _send(self, data_splitted, datagram_type, destination, unique_package_id, is_reliable):
        for i, chunk in enumerate(data_splitted):
            hash = sha256(chunk).digest()
            chunk = datagram_type.to_bytes(4, 'little') + unique_package_id.to_bytes(4, 'little') + hash \
                    + len(data_splitted).to_bytes(4, 'little') + i.to_bytes(4, 'little') + chunk
            self._socket.sendto(chunk, destination)
            if is_reliable:
                self._datagrams_awating_ack[unique_package_id]['backup_data'][i] = chunk
                self._datagrams_awating_ack[unique_package_id]['last_send_time'][i] = time.time()

    def send_data(self, bytes_to_send, datagram_type, to):
        """
        Envía datos a través del socket
        :param bytes_to_send: Datos a enviar
        :param datagram_type: Tipo de datos a enviar, ACK, NORMAL o RELIABLE
        :param to: Receptor al que vamos a enviar los datos
        """
        self._mutex.acquire()
        unique_package_id = self._package_ID #Gurdamos el id por seguridad, ya que igual otros threads mas adelante lo modifican.
        self._package_ID += 1
        self._mutex.release()

        is_reliable = datagram_type == self.DATAGRAM_RELIABLE

        #Dividir los datos.
        data_splitted = self._split(bytes_to_send)

        #Realizar la estructura de datos
        if is_reliable:
            self._initialize_structure_for_reliable(len(data_splitted), unique_package_id)
            # Thread: Preparar el ack listener
            listen_ack_thread = Thread(target=self._ack_listener, args=[unique_package_id])
            listen_ack_thread.start()

        #Thread: Enviar datos

        self._send(data_splitted, datagram_type, to, unique_package_id, is_reliable)

        #Activar monitor

        if is_reliable:
            resend_monitor = Thread(target=self._ack_resend_monitor, args=[unique_package_id, to])
            resend_monitor.start()

    def send_json(self, json_to_send, data_type, to):
        """
        Envía un json a través del socket
        :param json_to_send: JSON a enviar
        :param data_type: Tipo de datos a enviar, ACK, NORMAL o RELIABLE
        :param to: Cliente al que vamos a enviar los datos
        """
        json_bytes = json.dumps(json_to_send).encode(encoding='utf-8')
        self.send_data(json_bytes, data_type, to)

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

    def _create_package_cache(self, unique_identifier, number_of_subpackages):
        self._cache[unique_identifier] = {}
        self._cache[unique_identifier]['data'] = [False for _ in range(number_of_subpackages)]
        self._cache[unique_identifier]['remaining_subpackages'] = number_of_subpackages

    def _check_if_package_already_registered(self, unique_identifier, number_of_subpackages):
        if unique_identifier not in self._cache.keys():
            self._create_package_cache(unique_identifier, number_of_subpackages)

    def _send_ack(self, package_id, subpackage_id, address):
        ack = self.DATAGRAM_ACK.to_bytes(4, 'little') + package_id.to_bytes(4, 'little') + \
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

    def _if_package_completed_handle_and_clean(self, unique_identifier, address):
        if self._cache[unique_identifier]['remaining_subpackages'] == 0:
            print(f"Package id {unique_identifier} completed!")
            data = b''.join((subdata for subdata in self._cache[unique_identifier]['data']))
            self.response_handler(data, address)
            #del self._cache[unique_identifier]['data'][:]
            self._cache.pop(unique_identifier)

    def _parse_datagram(self, datagram, address, reliable=False):
        # Datagram Header: datagram_type, packet_id, hash, number_of_subpackages, subpackage_id
        if self._hash_is_correct(datagram):
            package_id, number_of_subpackages, subpackage_id, payload = self._parse_content(datagram)
            print(f"Received datagram from {address}, packageid:{package_id}, subpackageid{subpackage_id}")
            unique_identifier = self._create_unique_identifier(address, package_id)
            self._check_if_package_already_registered(unique_identifier, number_of_subpackages)
            self._save_package_payload(unique_identifier, package_id, subpackage_id, payload, address, reliable)
            self._if_package_completed_handle_and_clean(unique_identifier, address)

    def _listen_loop(self):
        """
        Escucha las peticiones entrantes
        """

        while 1:
            datagram, address = self._socket.recvfrom(4096)

            datagram_type = self._get_datagram_type(datagram)

            # Todo: crear log.txt con los paquetes recibidos

            if datagram_type == self.DATAGRAM_RELIABLE:
                self._parse_datagram(datagram, address, reliable=True)
            elif datagram_type == self.DATAGRAM_NORMAL:
                self._parse_datagram(datagram, address, reliable=False)

    def _ack_resend_monitor(self, unique_package_id, destination):
        while not self._is_completed_or_expired(unique_package_id):
            time.sleep(0.5)
            self._resend_data(unique_package_id, destination)

        print("Closing monitor.")

    def _resend_data(self, package_id, destination):
        self._mutex.acquire()
        completed_or_expired = self._datagrams_awating_ack[package_id].get('completed_or_expired', True)

        if completed_or_expired:
            remaining_acks_list = None
        else:
            remaining_acks_list = [i for i, x in enumerate(self._datagrams_awating_ack[package_id]['acks']) if not x]
        self._mutex.release()

        if remaining_acks_list is not None:
            for ack_location in remaining_acks_list:
                remaining_attempts_list = self._datagrams_awating_ack[package_id].get('remaining_attempts', None)

                if (remaining_attempts_list is not None) and (remaining_attempts_list[ack_location] > 0):

                    _data_exists = self._datagrams_awating_ack[package_id].get('backup_data', None)
                    _elapsed_time_list = self._datagrams_awating_ack[package_id].get('last_send_time', None)
                    if _elapsed_time_list is not None:
                        _elapsed_time = _elapsed_time_list[ack_location]
                    else:
                        _elapsed_time = None
                    if _data_exists is not None:
                        if _elapsed_time is not None:
                            if time.time() - _elapsed_time > self._AWAIT_TIME:
                                print(f'Resending subpackage {ack_location} of package {package_id}')
                                self._socket.sendto(_data_exists[ack_location], destination)
                                self._datagrams_awating_ack[package_id]['remaining_attempts'][ack_location] -= 1
                                self._datagrams_awating_ack[package_id]['last_send_time'][ack_location] = time.time()
                                if self._datagrams_awating_ack[package_id]['remaining_attempts'][ack_location] == 0:
                                    self._datagrams_awating_ack[package_id]['acks'][ack_location] = True

