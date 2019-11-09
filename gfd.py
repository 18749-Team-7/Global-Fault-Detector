import json
import requests
from _thread import *
import threading
import socket

rm_port1 = 10001
rm_port2 = 10002

members_mutex = threading.Lock()


class GlobalFaultDetector:
    def __init__(self, rm_address=('localhost',1000), gfd_hb_interval=1, gfd_port=10001):
        self.lfd_conn
        self.establish_lfd_connection = threading.Thread(target=self.lfd_connection, args=(self.lfd_conn, gfd_port1))
        #this is for heartbeat
        self.rm_address1 = (rm_address, rm_port)
        #this is for status
        self.rm_address2 = (rm_address, rm_port2)
        self.rm_thread = threading.Thread(target=self.establish_RM_connection)
        self.lfd_replica_dict = {}

        print("Establishing connection with Replication Manager")
        self.rm_thread.start()


    def establish_RM_connection(self):
        try:
            # Create a TCP/IP socket for hearbeat
            self.rm_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Bind the socket to the replication port
            server_address = self.rm_address1
            print('Connecting to Replication Manager on rm_address{} port {}'.format(*server_address))
            self.rm_conn.connect(server_address)


            # Create a TCP/IP socket for sending status
            self.rm_conn_2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Bind the socket to the replication port
            server_address = self.rm_address2
            print('Connecting to Replication Manager on rm_address{} port {}'.format(*server_address))
            self.rm_conn_2.connect(server_address)

        except Exception as e:
            print(e)


    def RM_heartbeat_func(self): 
        while True:
            try:
                # Waiting for RM membership data
                while True:
                    RM_heartbeat_msg = "This is a GFD_heartbeat json str"
                    self.rm_conn.sendall(LFD_heartbeat_msg)
                    time.sleep(self.gfd_hb_interval)
            finally:
                # GFD connection errors
                print("RM connection lost")
                # Clean up the connection
                self.rm_conn.close()

    def lfd_service_thread(s, addr):
        # first time save the replica IPs corresponding to LFD
        try:
            s.settimeout(2)
            data = s.recv(BUF_SIZE)
            data = data.decode('utf-8')
            json_data = json.load(data)
            replica_ip = json_data[server_ip]
            replica_status = json_data[status]
            
            members_mutex.acquire()
            if addr not in self.lfd_replica_dict:
                self.lfd_replica_dict[addr] = replica_ip
            members_mutex.release()


        # keep receiving status update from LFD, if you don't hear back from LFD, then replica failed
        while True:      
            try:
                s.settimeout(2)
                data = s.recv(BUF_SIZE)
                data = data.decode('utf-8')
                json_data = json.load(data)
                replica_ip = json_data[server_ip]
                replica_status = json_data[status]
                try:
                    LFD_status_msg =str({"server_ip": replica_ip, "status": replica_status}).encode('utf-8')
                    jsonObj = json.loads(str(LFD_status_msg))
                    self.rm_conn_2.sendall(jsonObj)
                    time.sleep(self.gfd_hb_interval)
                except:
                    continue
            except
                print("timeout from LFD")
                replica_ip = self.lfd_replica_dict[addr]
                replica_status = False
                try:
                    LFD_status_msg =str({"server_ip": replica_ip, "status": replica_status}).encode('utf-8')
                    jsonObj = json.loads(str(LFD_status_msg))
                    self.rm_conn_2.sendall(jsonObj)
                    time.sleep(self.gfd_hb_interval)
                except:
                    continue
               

    def lfd_connection(lfd_conn, gfd_port):
        try:
            # Create a TCP/IP socket
            lfd_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Bind the socket to the replication port
            server_address = ('localhost', gfd_port)
            print('Started listening for LFD on {} port {}'.format(*server_address))
            lfd_conn.bind(server_address)
            
            # Listen for incoming connections
            lfd_conn.listen(5)
            i = 1
            while(True):
                # Accept a new connection
                conn, addr = lfd_conn.accept()
                print("Establish connection with LFD " + str(i))
                # Initiate a client listening thread
                threading.Thread(target=self.lfd_service_thread, args=(conn, addr)).start()
                i+=1

        except KeyboardInterrupt:
            # Closing the server
            lfd_conn.close()




if __name__=="__main__":
    gfd = GlobalFaultDetector()