from socket import socket, AF_INET, SOCK_DGRAM
from hashlib import sha256
import json
import time
from threading import Thread, Lock


class Server:
    _DATAGRAM_ACK = 0
    _DATAGRAM_NORMAL = 1
    _DATAGRAM_RELIABLE = 2

    _CHUNK_SIZE = 2048

    def __init__(self, address=None, port=None, handler=None):
        self.address = address
        self.port = port
        self._socket = socket(AF_INET, SOCK_DGRAM)
        self._response_handler = handler
        self._packages = {}
        self._package_identifier = 0
        # Todo: 4 ids

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
        ack = self._DATAGRAM_ACK.to_bytes(4, 'little') + package_id.to_bytes(4, 'little') + \
              subpackage_id.to_bytes(4, 'little')
        self._socket.sendto(ack, address)

    def _create_package_register(self, unique_identifier, number_of_subpackages):
        self._packages[unique_identifier] = {}
        self._packages[unique_identifier]['data'] = [False for _ in range(number_of_subpackages)]
        self._packages[unique_identifier]['remaining_subpackages'] = number_of_subpackages

    def _check_if_package_already_registered(self, unique_identifier, number_of_subpackages):
        if unique_identifier not in self._packages.keys():
            self._create_package_register(unique_identifier, number_of_subpackages)

    def _save_package_payload(self, unique_identifier, package_id, subpackage_id, payload, address, reliable):
        if reliable:
            self._send_ack(package_id, subpackage_id, address)
        # We do this so we can avoid residual data with lates ACK
        if self._packages[unique_identifier]['remaining_subpackages'] != 0:
            print(f"Saving payload from {unique_identifier}, packageid:{package_id}, subpackageid{subpackage_id}")
            self._packages[unique_identifier]['data'][subpackage_id] = payload
            self._packages[unique_identifier]['remaining_subpackages'] -= 1

    def _parse_content(self, datagram):
        package_id = int.from_bytes(datagram[4:8], 'little')
        number_of_subpackages = int.from_bytes(datagram[40:44], 'little')
        subpackage_id = int.from_bytes(datagram[44:48], 'little')
        payload = datagram[48:]

        return package_id, number_of_subpackages, subpackage_id, payload

    def _if_package_completed_handle_and_clean(self, unique_identifier):
        if self._packages[unique_identifier]['remaining_subpackages'] == 0:
            print(f"Package id {unique_identifier} completed!")
            self._response_handler(b''.join((subdata for subdata in self._packages[unique_identifier]['data'])))
            del self._packages[unique_identifier]['data'][:]

    def _parse_datagram(self, datagram, address, reliable=False):
        # Datagram Header: datagram_type, packet_id, hash, number_of_subpackages, subpackage_id
        if self._hash_is_correct(datagram):
            package_id, number_of_subpackages, subpackage_id, payload = self._parse_content(datagram)
            print(f"Received datagram from {address}, packageid:{package_id}, subpackageid{subpackage_id}")
            unique_identifier = self._create_unique_identifier(address, package_id)
            self._check_if_package_already_registered(unique_identifier, number_of_subpackages)
            self._save_package_payload(unique_identifier, package_id, subpackage_id, payload, address, reliable)
            self._if_package_completed_handle_and_clean(unique_identifier)

    def _create_unique_identifier(self, address, package_id):
        unique_identifier = (address[0], address[1], self._package_identifier, package_id)
        self._package_identifier += 1
        return unique_identifier

    def _get_datagram_type(self, datagram):
        return int.from_bytes(datagram[:4], 'little')

    def _listen_loop(self):
        while 1:
            datagram, address = self._socket.recvfrom(4096)

            datagram_type = self._get_datagram_type(datagram)

            # Todo: crear log.txt con los paquetes recibidos

            if datagram_type == self._DATAGRAM_RELIABLE:
                self._parse_datagram(datagram, address, reliable=True)
            elif datagram_type == self._DATAGRAM_NORMAL:
                self._parse_datagram(datagram, address, reliable=False)

    def _hash_is_correct(self, datagram):
        hash_received = datagram[8:40]
        payload = datagram[48:]

        payload_hash = sha256(payload).digest()

        if hash_received != payload_hash:
            return False
        else:
            return True

# Todo: Mirar como hacer que cuando un send acabe, se cierren esos threads.


class Client:
    _DATAGRAM_ACK = 0
    _DATAGRAM_NORMAL = 1
    _DATAGRAM_RELIABLE = 2

    _CHUNK_SIZE = 2048

    _AWAIT_TIME = 0.5
    _SEND_ATTEMPTS = 3

    def __init__(self, address=None, address_port=None, local_port=None):
        self.address = address
        self.address_port = address_port
        self._local_port = local_port
        self._socket = socket(AF_INET, SOCK_DGRAM)
        self._packet_ID = 0
        self._datagrams_waiting_ack = {}
        self._is_completed = False
        self._mutex = Lock()

    def send_data(self, data, datagram_type, is_json=False):
        # If we close the socket, we need to create it again
        if self._is_completed:
            self._socket = socket(AF_INET, SOCK_DGRAM)
        self._is_completed = False

        self._bound_socket() #Todo: Revisar que devuelve true

        if datagram_type == self._DATAGRAM_RELIABLE:
            listen_ack_thread = Thread(target=self._ack_listener)
            listen_ack_thread.start()

        self._divide_and_send(data, datagram_type, is_json)

        if datagram_type == self._DATAGRAM_RELIABLE:
            self._ack_resend_monitor()
        else:
            self._is_completed = True

        self._unbound_socket()

        '''
        if self._bound_socket():

            # Thread listening acks
            if datagram_type == self._DATAGRAM_RELIABLE:
                listen_ack_thread = Thread(target=self._ack_listener)
                listen_ack_thread.start()

            self._divide_and_send(data, datagram_type, is_json)

            # Thread resending subpackages
            if datagram_type == self._DATAGRAM_RELIABLE:
                self._ack_resend_monitor()
            else:
                self._is_completed = True
        '''

    def _divide_and_send(self, data, datagram_type, is_json=False):
        is_reliable = datagram_type == self._DATAGRAM_RELIABLE
        data_to_split = data

        if is_json:
            data_to_split = json.dumps(data_to_split).encode(encoding='utf-8')

        data_splitted = self._split(data_to_split)

        if is_reliable:
            self._initialize_structure_for_reliable(len(data_splitted))

        for i, chunk in enumerate(data_splitted):
            hash = sha256(chunk).digest()
            chunk = datagram_type.to_bytes(4, 'little') + self._packet_ID.to_bytes(4, 'little') + hash \
                    + len(data_splitted).to_bytes(4, 'little') + i.to_bytes(4, 'little') + chunk
            self._socket.sendto(chunk, (self.address, self.address_port))
            if is_reliable:
                self._datagrams_waiting_ack[self._packet_ID]['backup_data'][i] = chunk

        self._packet_ID += 1

    def _initialize_structure_for_reliable(self, data_length):
        if self._packet_ID not in self._datagrams_waiting_ack.keys():
            self._datagrams_waiting_ack[self._packet_ID] = {}
            self._datagrams_waiting_ack[self._packet_ID]['acks'] = [False for _ in range(data_length)]
            self._datagrams_waiting_ack[self._packet_ID]['remaining_acks'] = data_length
            self._datagrams_waiting_ack[self._packet_ID]['backup_data'] = [None for _ in range(data_length)]
            self._datagrams_waiting_ack[self._packet_ID]['remaining_attempts'] = [self._SEND_ATTEMPTS for _ in range(data_length)]

    def _split(self, data):
        result = []
        while len(data) > self._CHUNK_SIZE:
            result.append(data[:self._CHUNK_SIZE])
            data = data[self._CHUNK_SIZE:]
        result.append(data)
        return result

    def _ack_listener(self):
        while not self._is_completed:
            # Todo: Si se cierra al estar completo en el ultimo metodo, esto peta
            try:
                datagram, address = self._socket.recvfrom(4096)
            except OSError as e:
                print(e)

            datagram_type = int.from_bytes(datagram[:4], 'little')
            package_id = int.from_bytes(datagram[4:8], 'little')
            subpackage_id = int.from_bytes(datagram[8:12], 'little')

            if datagram_type == self._DATAGRAM_ACK:
                print(f'ACK received, type: {datagram_type}, Package id:{package_id}, subpackageid:{subpackage_id}')
                self._mark_subpackage(package_id, subpackage_id)

        # Todo: Export data to txt
        self._clean_register()

    def _clean_register(self):
        self._datagrams_waiting_ack.clear()

    def _bound_socket(self):
        #Todo: Mejor manera de mirar si el puerto está ocupado
        try:
            self._socket.bind(('localhost', self._local_port))
            print(f'Socket bound on port {self._local_port}, host {self.address}')
        except OSError as e:
            print(f"Error binding port {self._local_port}, host {self.address}")
            print(e)

    def _unbound_socket(self):
        self._socket.close()
        print(f'Socket closed on port {self._local_port}, host {self.address}')

    def _mark_subpackage(self, package_id, subpackage_id):
        self._mutex.acquire()
        if self._datagrams_waiting_ack[package_id]['remaining_acks'] != 0:
            self._datagrams_waiting_ack[package_id]['acks'][subpackage_id] = True
            self._datagrams_waiting_ack[package_id]['remaining_acks'] -= 1
            self._datagrams_waiting_ack[package_id]['backup_data'][subpackage_id] = None
        else:
            self._is_completed = True
        self._mutex.release()

    def _ack_resend_monitor(self):
        while not self._is_completed:
            time.sleep(0.5)
            for key in self._datagrams_waiting_ack.keys():
                self._mutex.acquire()
                remaining_acks = self._datagrams_waiting_ack[key]['remaining_acks']
                self._mutex.release()

                if remaining_acks != 0:
                    self._resend_data(key)
                else:
                    self._is_completed = True
        print("Closing monitor.")

    #Todo: limpiar
    def _resend_data(self, package_id):
        self._mutex.acquire()
        remaining_acks = [i for i, x in enumerate(self._datagrams_waiting_ack[package_id]['acks']) if not x]
        self._mutex.release()

        if not remaining_acks: #if list is empty
            pass
        else:
            for ack_location in remaining_acks:
                _data_exists = self._datagrams_waiting_ack[package_id].get('backup_data', None)
                if _data_exists is not None:
                    print(f'Resending subpackage {ack_location} of package {package_id}')
                    self._socket.sendto(_data_exists[ack_location],
                                        (self.address, self.address_port))
                    self._datagrams_waiting_ack[package_id]['remaining_attempts'][ack_location] -= 1
                    if self._datagrams_waiting_ack[package_id]['remaining_attempts'][ack_location] == 0:
                        self._datagrams_waiting_ack[package_id]['acks'][ack_location] = True


# Todo: Mirar otra manera de cerrar el socket.

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