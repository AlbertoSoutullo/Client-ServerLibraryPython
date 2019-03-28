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
        print(f"ACK sended to {package_id}, {subpackage_id}")
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
        self._package_ID = 0
        self._reliable_datagrams_info = {}
        self._is_closed = False
        self._mutex = Lock()

    def send_json(self, data, datagram_type, destination):
        json_bytes = json.dumps(data).encode(encoding='utf-8')
        self.send_data(json_bytes, datagram_type, destination)

    def send_data(self, data, datagram_type, destination):
        unique_package_id = self._package_ID #Gurdamos el id por seguridad, ya que igual otros threads mas adelante lo modifican.

        is_reliable = datagram_type == self._DATAGRAM_RELIABLE
        self._bound_socket()

        #Dividir los datos.
        data_splitted = self._split(data)

        #Realizar la estructura de datos
        if is_reliable:
            self._initialize_structure_for_reliable(len(data_splitted), unique_package_id)
            # Thread: Preparar el ack listener
            listen_ack_thread = Thread(target=self._ack_listener, args=[unique_package_id])
            listen_ack_thread.start()

        #Thread: Enviar datos

        self._send(data_splitted, datagram_type, destination, unique_package_id, is_reliable)

        #Activar monitor

        if is_reliable:
            resend_monitor = Thread(target=self._ack_resend_monitor, args=[unique_package_id])
            resend_monitor.start()






        if datagram_type == self._DATAGRAM_RELIABLE:

            listen_ack_thread = Thread(target=self._ack_listener)
            listen_ack_thread.start()

        self._divide_and_send(data, datagram_type, is_json)

        if datagram_type == self._DATAGRAM_RELIABLE:
            resend_monitor = Thread(target=self._ack_resend_monitor)
            resend_monitor.start()
        else:
            self._is_closed = True

        #self._unbound_socket()

    def _send(self, data_splitted, datagram_type, destination, unique_package_id, is_reliable):
        for i, chunk in enumerate(data_splitted):
            hash = sha256(chunk).digest()
            chunk = datagram_type.to_bytes(4, 'little') + unique_package_id.to_bytes(4, 'little') + hash \
                    + len(data_splitted).to_bytes(4, 'little') + i.to_bytes(4, 'little') + chunk
            self._socket.sendto(chunk, destination)
            if is_reliable:
                self._reliable_datagrams_info[unique_package_id]['backup_data'][i] = chunk

        self._mutex.acquire()
        self._package_ID += 1
        self._mutex.release()

    def _initialize_structure_for_reliable(self, data_length, unique_package_id):
        if unique_package_id not in self._reliable_datagrams_info.keys():
            self._reliable_datagrams_info[unique_package_id] = {}
            self._reliable_datagrams_info[unique_package_id]['acks'] = [False for _ in range(data_length)]
            self._reliable_datagrams_info[unique_package_id]['remaining_acks'] = data_length
            self._reliable_datagrams_info[unique_package_id]['backup_data'] = [None for _ in range(data_length)]
            self._reliable_datagrams_info[unique_package_id]['remaining_attempts'] = [self._SEND_ATTEMPTS for _ in range(data_length)]
            self._reliable_datagrams_info[unique_package_id]['last_send_time'] = [None for _ in range(data_length)]

    def _split(self, data):
        result = []
        while len(data) > self._CHUNK_SIZE:
            result.append(data[:self._CHUNK_SIZE])
            data = data[self._CHUNK_SIZE:]
        result.append(data)
        return result


    def _get_number_remaining_acks(self, package_id):
        return self._reliable_datagrams_info[package_id]['remaining_acks']

    #Se creará un ack listener por cada paquete. De esta manera si se están recibiendo 5 paquetes a la vez, se
    #tendrán 5 ack listeners pada aliviar trabajo. Cada uno estará corriendo mientras no se acabae un paquete en concreto.
    def _ack_listener(self, unique_package_id):
        while self._get_number_remaining_acks(unique_package_id) > 0:
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
        self._clean_package_register(unique_package_id)

    def _clean_package_register(self, package_id_listener):
        self._reliable_datagrams_info[package_id_listener].clear()

    def _bound_socket(self):
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
        if self._reliable_datagrams_info[package_id]['remaining_acks'] > 0:
            self._reliable_datagrams_info[package_id]['acks'][subpackage_id] = True
            self._reliable_datagrams_info[package_id]['remaining_acks'] -= 1
            self._reliable_datagrams_info[package_id]['backup_data'][subpackage_id] = None
        self._mutex.release()

    def _ack_resend_monitor(self, unique_package_id):
        while self._get_number_remaining_acks(unique_package_id) > 0:
            time.sleep(0.5)
            for key in self._reliable_datagrams_info.keys():
                self._mutex.acquire()
                remaining_acks = self._reliable_datagrams_info[key]['remaining_acks']
                self._mutex.release()

                if remaining_acks != 0:
                    self._resend_data(key)
                else:
                    self._is_closed = True
        print("Closing monitor.")

    #Todo: limpiar
    def _resend_data(self, package_id):
        self._mutex.acquire()
        remaining_acks = [i for i, x in enumerate(self._reliable_datagrams_info[package_id]['acks']) if not x]
        self._mutex.release()

        if not remaining_acks: #if list is empty
            pass
        else:
            for ack_location in remaining_acks:
                _data_exists = self._reliable_datagrams_info[package_id].get('backup_data', None)
                if _data_exists is not None:
                    print(f'Resending subpackage {ack_location} of package {package_id}')
                    self._socket.sendto(_data_exists[ack_location],
                                        (self.address, self.address_port))
                    self._reliable_datagrams_info[package_id]['remaining_attempts'][ack_location] -= 1
                    if self._reliable_datagrams_info[package_id]['remaining_attempts'][ack_location] == 0:
                        self._reliable_datagrams_info[package_id]['acks'][ack_location] = True



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