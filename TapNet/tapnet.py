from socket import socket, AF_INET, SOCK_DGRAM
from threading import Thread, Lock
from hashlib import sha256

import json
import time


class TapNet:
    DATAGRAM_ACK = 0
    DATAGRAM_NORMAL = 1
    DATAGRAM_RELIABLE = 2

    _CHUNK_SIZE = 2048

    _SEND_ATTEMPTS = 3
    _AWAIT_TIME = 0.5

    def __init__(self, address=None):
        self.address = address  # Server Address
        self.response_handler = None
        self._package_ID = 0  # ID from the next datagram to send
        self._datagrams_awating_ack = {}  # Reliable Datagrams waiting ACK
        self._cache = {}  # Data from packages received
        self._socket = socket(AF_INET, SOCK_DGRAM)
        self._is_bound = False
        self._mutex = Lock()

    def start(self):
        """
        Starts a Server in a Thread.
        """
        print('Starting server on  {}'.format(self.address))
        self._socket.bind(self.address)
        self._is_bound = True

        listen_loop = Thread(target=self._listen_loop)
        listen_loop.start()

    def send_json(self, json_to_send, data_type, to):
        """
        Send a json as binary data.
        :param json_to_send: JSON to send
        :param data_type: ACK, NORMAL or RELIABLE
        :param to: Receiver's address
        """
        json_bytes = json.dumps(json_to_send).encode(encoding='utf-8')
        self.send_data(json_bytes, data_type, to)

    def send_data(self, bytes_to_send, datagram_type, to):
        """
        Send binary data
        :param bytes_to_send: Binary data to send
        :param datagram_type: ACK, NORMAL or RELIABLE
        :param to: Receiver's address
        """
        unique_package_id = self._lock_package_id()
        is_reliable = datagram_type == self.DATAGRAM_RELIABLE

        data_splitted = self._split(bytes_to_send)  # Split binary data

        if is_reliable:
            self._initialize_structure_for_datagram(len(data_splitted), unique_package_id)
            # Thread: ACK Listener
            listen_ack_thread = Thread(target=self._ack_listener, args=[unique_package_id])
            listen_ack_thread.start()

        #Thread: Send data
        self._send(data_splitted, datagram_type, to, unique_package_id, is_reliable)

        #Activate monitor

        if is_reliable:
            resend_monitor = Thread(target=self._ack_resend_monitor, args=[unique_package_id, to])
            resend_monitor.start()

    def _lock_package_id(self):
        """
        Locks a unique id, so race conditions are avoided if sent multiples packages.
        :return: unique package id.
        """
        self._mutex.acquire()
        unique_package_id = self._package_ID
        self._package_ID += 1
        self._mutex.release()
        return unique_package_id

    def _split(self, data):
        """
        Splits binary data into _CHUNK_SIZEs divisions.
        :param data: Binary data to divide
        :return: Array, where each position has _CHUNK_SIZE bytes of data.
        """
        result = []
        while len(data) > self._CHUNK_SIZE:
            result.append(data[:self._CHUNK_SIZE])
            data = data[self._CHUNK_SIZE:]
        result.append(data)
        return result

    def _initialize_structure_for_datagram(self, data_length, unique_package_id):
        """
        Initialize data for a datagram which is waiting ack:
            -acks: Array of booleans that indicates if that subpackage receive it's ack(True) or not(False).
            -remaining_acks: Number of acks left to receive.
            -backup_data: Array where is saved that subpackage datagram, header included, for easier re-send.
            -remaining_attempts: Number of attempts of re-sends if an ACK is not received.
            -last_send_time: Last time that a re-send was done.
            -completed_or_expired: Boolean that says if that package was completed, or maximum attempts was done.

        :param data_length: Number of subpackages
        :param unique_package_id: Unique ID for that package
        """
        if unique_package_id not in self._datagrams_awating_ack.keys():
            self._datagrams_awating_ack[unique_package_id] = {}
            self._datagrams_awating_ack[unique_package_id]['acks'] = [False for _ in range(data_length)]
            self._datagrams_awating_ack[unique_package_id]['remaining_acks'] = data_length
            self._datagrams_awating_ack[unique_package_id]['backup_data'] = [None for _ in range(data_length)]
            self._datagrams_awating_ack[unique_package_id]['remaining_attempts'] = [self._SEND_ATTEMPTS for _ in range(data_length)]
            self._datagrams_awating_ack[unique_package_id]['last_send_time'] = [None for _ in range(data_length)]
            self._datagrams_awating_ack[unique_package_id]['completed_or_expired'] = False

    def _get_is_completed_or_expired(self, package_id):
        """
        Get key value of 'completed_or_expired'. If that key does not exists, its assumed that is completed, since data
        one complete is erased.
        :param package_id: Unique package id.
        :return: Boolean value if that package is completed/expired or not.
        """
        completed_or_expired = self._datagrams_awating_ack[package_id].get('completed_or_expired', True)
        return completed_or_expired

    def _mark_subpackage(self, package_id, subpackage_id):
        """
        Mark a specific subpackage as true. Erases backup data, and reduce the number of remaining acks.
        If there are no more subpackages to receive, that package is completed.
        :param package_id: Unique package id
        :param subpackage_id: Unique subpackage id
        """
        self._mutex.acquire()
        if self._datagrams_awating_ack[package_id]['remaining_acks'] > 0:
            self._datagrams_awating_ack[package_id]['acks'][subpackage_id] = True
            self._datagrams_awating_ack[package_id]['remaining_acks'] -= 1
            self._datagrams_awating_ack[package_id]['backup_data'][subpackage_id] = None
            if self._datagrams_awating_ack[package_id]['remaining_acks'] == 0:
                self._datagrams_awating_ack[package_id]['completed_or_expired'] = True
        self._mutex.release()

    def _clean_package_register(self, package_id_listener):
        """
        Clear all data of a given package waiting acks.
        :param package_id_listener:
        """
        self._datagrams_awating_ack[package_id_listener].clear()

    def _ack_listener(self, unique_package_id):
        """
        Works in a Thread. It runs per package sent. If we send 4 packages at the same time, 4 ack listeners will spawn.
        Each one is "linked" to it's package spawner. If that package is completed, that listener closes.
        :param unique_package_id: Unique package identifier
        """
        if not self._is_bound:
            self._socket.bind(('localhost', 10001))
            self._is_bound = True

        while not self._get_is_completed_or_expired(unique_package_id):

            datagram, address = self._socket.recvfrom(4096)

            datagram_type = int.from_bytes(datagram[:4], 'little')
            package_id = int.from_bytes(datagram[4:8], 'little')
            subpackage_id = int.from_bytes(datagram[8:12], 'little')

            if datagram_type == self.DATAGRAM_ACK:
                print(f'ACK received, type: {datagram_type}, Package id:{package_id}, Subpackage Id:{subpackage_id}')
                self._mark_subpackage(package_id, subpackage_id)

        self._clean_package_register(unique_package_id)

    def _send(self, data_splitted, datagram_type, destination, unique_package_id, is_reliable):
        """
        Send binary data.
        :param data_splitted: Entire array of data to send.
        :param datagram_type: ACK, NORMAL or RELIABLE.
        :param destination: Receiver
        :param unique_package_id: Unique package ID
        :param is_reliable: If is reliable, we save chunk of data and time to send again.
        :return:
        """
        for i, chunk in enumerate(data_splitted):
            hash = sha256(chunk).digest()
            chunk = datagram_type.to_bytes(4, 'little') + unique_package_id.to_bytes(4, 'little') + hash \
                    + len(data_splitted).to_bytes(4, 'little') + i.to_bytes(4, 'little') + chunk
            self._socket.sendto(chunk, destination)
            if is_reliable:
                self._datagrams_awating_ack[unique_package_id]['backup_data'][i] = chunk
                self._datagrams_awating_ack[unique_package_id]['last_send_time'][i] = time.time()

    def _get_datagram_type(self, datagram):
        """
        Return datagram type. ACK, NORMAL or RELIABLE.
        :param datagram: Datagram to check.
        :return: ACK, NORMAL or RELIABLE.
        """
        return int.from_bytes(datagram[:4], 'little')

    def _hash_is_correct(self, datagram):
        """
        Check if hash given is correct.
        :param datagram: Datagram to check.
        :return: True or false if it is correct.
        """
        hash_received = datagram[8:40]
        payload = datagram[48:]

        payload_hash = sha256(payload).digest()

        if hash_received != payload_hash:
            return False
        else:
            return True

    def _parse_content(self, datagram):
        """
        Parse datagram content, extracting data from header and payload.
        :param datagram: Datagram to parse.
        :return: Package ID, Number of Subpackages, Subpackage ID and Payload.
        """
        package_id = int.from_bytes(datagram[4:8], 'little')
        number_of_subpackages = int.from_bytes(datagram[40:44], 'little')
        subpackage_id = int.from_bytes(datagram[44:48], 'little')
        payload = datagram[48:]

        return package_id, number_of_subpackages, subpackage_id, payload

    def _create_unique_identifier(self, address, package_id):
        """
        Creates a unique identifier for each package, compund by Address, Port and unique ID.
        :param address: Sender Adress
        :param package_id: Unique package ID.
        :return: Tuple of (Address, Port, PackageID)
        """
        unique_identifier = (address[0], address[1], package_id)
        return unique_identifier

    def _create_package_cache(self, unique_identifier, number_of_subpackages):
        """
        Creates structure for cache.
        :param unique_identifier: Unique identifier of the package.
        :param number_of_subpackages: Number of subpackages.
        """
        self._cache[unique_identifier] = {}
        self._cache[unique_identifier]['data'] = [None for _ in range(number_of_subpackages)]
        self._cache[unique_identifier]['remaining_subpackages'] = number_of_subpackages

    def _check_if_package_already_registered(self, unique_identifier, number_of_subpackages):
        if unique_identifier not in self._cache.keys():
            self._create_package_cache(unique_identifier, number_of_subpackages)

    def _send_ack(self, package_id, subpackage_id, address):
        """
        Creates and send ACK.
        :param package_id: Package Identifier
        :param subpackage_id: Subpackage ID
        :param address: Receivers ID.
        """
        ack = self.DATAGRAM_ACK.to_bytes(4, 'little') + package_id.to_bytes(4, 'little') + \
              subpackage_id.to_bytes(4, 'little')
        print(f"ACK sent to {package_id}, {subpackage_id}")
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
        Keep listening for entrance petitions.
        """
        while 1:
            datagram, address = self._socket.recvfrom(4096)

            datagram_type = self._get_datagram_type(datagram)

            if datagram_type == self.DATAGRAM_RELIABLE:
                self._parse_datagram(datagram, address, reliable=True)
            elif datagram_type == self.DATAGRAM_NORMAL:
                self._parse_datagram(datagram, address, reliable=False)

    def _ack_resend_monitor(self, unique_package_id, destination):
        """
        Checks for resend packages every 0.5 seconds.
        :param unique_package_id: Unique package Identifier
        :param destination: Destination to resend.
        """
        while not self._get_is_completed_or_expired(unique_package_id):
            time.sleep(0.5)
            self._resend_data(unique_package_id, destination)

        print("Closing monitor.")

    def _get_remaining_ack_list(self, package_id):
        self._mutex.acquire()
        completed_or_expired = self._datagrams_awating_ack[package_id].get('completed_or_expired', True)

        if completed_or_expired:
            remaining_acks_list = None
        else:
            remaining_acks_list = [i for i, x in enumerate(self._datagrams_awating_ack[package_id]['acks']) if not x]
        self._mutex.release()
        return remaining_acks_list

    def _resend_subpackage(self, data, ack_location, package_id, destination):
        print(f'Resending subpackage {ack_location} of package {package_id}')
        self._socket.sendto(data[ack_location], destination)
        self._datagrams_awating_ack[package_id]['remaining_attempts'][ack_location] -= 1
        self._datagrams_awating_ack[package_id]['last_send_time'][ack_location] = time.time()
        if self._datagrams_awating_ack[package_id]['remaining_attempts'][ack_location] == 0:
            self._datagrams_awating_ack[package_id]['acks'][ack_location] = True

    def _resend_data(self, package_id, destination):
        remaining_acks_list = self._get_remaining_ack_list(package_id)

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
                                self._resend_subpackage(_data_exists, ack_location, package_id, destination)

