import json
import requests
# from _thread import *
import threading
import socket
import time
import argparse


BUF_SIZE = 1024

members_mutex = threading.Lock()

BLACK =     "\u001b[30m"
RED =       "\u001b[31m"
GREEN =     "\u001b[32m"
YELLOW =    "\u001b[33m"
BLUE =      "\u001b[34m"
MAGENTA =   "\u001b[35m"
CYAN =      "\u001b[36m"
WHITE =     "\u001b[37m"
RESET =     "\u001b[0m"

class GlobalFaultDetector:
    def __init__(self, rm_address=None, gfd_hb_interval=1):
        self.establish_lfd_connection = threading.Thread(target=self.lfd_connection)

        self.get_host_ip()
        rm_address = self.host_ip

        self.rm_port1 = 10002
        self.rm_port2 = 10001

        self.HB_counter = 0

        #this is for heartbeat
        self.rm_address1 = (rm_address, self.rm_port1)
        #this is for status
        self.rm_address2 = (rm_address, self.rm_port2)
        self.gfd_hb_interval = gfd_hb_interval
        self.gfd_port = 12345
        self.rm_thread = threading.Thread(target=self.establish_RM_connection)
        self.lfd_replica_dict = {}

        print(RED + "Establishing connection with Replication Manager..." + RESET)
        self.rm_thread.start()
        time.sleep(3)
        self.establish_lfd_connection.start()

        print(RED + "Connecting to RM for status..." + RESET)
        # Create a TCP/IP socket for sending status
        self.rm_conn_2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Bind the socket to the replication port
        server_address = self.rm_address2
        print(RED + "Connecting to Replication Manager on rm_address {} port {} ...".format(*server_address) + RESET)
        self.rm_conn_2.connect(server_address)

    def get_host_ip(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        self.host_ip = s.getsockname()[0]

    def establish_RM_connection(self):
        try:
            # Create a TCP/IP socket for hearbeat
            self.rm_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Bind the socket to the replication port
            server_address = self.rm_address1
            print(RED + 'Connecting to Replication Manager on rm_address {} port {} ...'.format(*server_address) + RESET)
            self.rm_conn.connect(server_address)

            self.RM_heartbeat_func()
        except Exception as e:
            print(e)


    def RM_heartbeat_func(self): 
        while 1:
            try:
                # Waiting for RM membership data
                while True:
                    RM_heartbeat_msg = "GFD_heartbeat"
                    RM_heartbeat_msg = RM_heartbeat_msg.encode('utf-8')
                    self.rm_conn.send(RM_heartbeat_msg)
                    print("Sent hearbeat to RM")
                    time.sleep(self.gfd_hb_interval)
            finally:
                # GFD connection errors
                print("RM connection lost")
                # Clean up the connection
                self.rm_conn.close()

    def lfd_service_thread(self, s, addr):
        # first time save the replica IPs corresponding to LFD
        try:
            #s.settimeout(2)
            print(addr)
            data = s.recv(BUF_SIZE)
            data = data.decode('utf-8')
            json_data = json.loads(data)
            replica_ip = json_data["server_ip"]
            replica_status = json_data["status"]
            members_mutex.acquire()
            if addr not in self.lfd_replica_dict:
                self.lfd_replica_dict[addr] = replica_ip
            members_mutex.release()
        except Exception as e:
            print(e)
            return

        # keep receiving status update from LFD, if you don't hear back from LFD, then replica failed
        while True:      
            try:
                print("Received heartbeat from LFD")
                s.settimeout(2)
                data = s.recv(BUF_SIZE)
                data = data.decode('utf-8')
                json_data = json.loads(data)
                replica_ip = json_data["server_ip"]
                replica_status = json_data["status"]
                try:
                    LFD_status_msg =json.dumps({"server_ip": str(replica_ip), "status": replica_status}).encode('utf-8')
                    self.rm_conn_2.sendall(LFD_status_msg)
                    time.sleep(self.gfd_hb_interval)
                except Exception as e:
                    print(e)
                    continue
            except Exception as e:
                print(e)
                replica_ip = self.lfd_replica_dict[addr]
                replica_status = False
                try:
                    LFD_status_msg =json.dumps({"server_ip": replica_ip, "status": replica_status}).encode('utf-8')
                    self.rm_conn_2.sendall(LFD_status_msg)
                    time.sleep(self.gfd_hb_interval)
                except:
                    continue
               


    def lfd_connection(self):
        try:
            print("LISTENING FOR LFD")
            # Create a TCP/IP socket
            lfd_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # # Bind the socket to the replication port
            # host_name = socket.gethostname() 
            # host_ip = socket.gethostbyname(host_name) 

            server_address = (self.host_ip, self.gfd_port)
            # server_address = ('localhost', self.gfd_port)
            # print('Started listening for LFD on {} port {}'.format(*server_address))
            lfd_conn.bind(server_address)  
            # # Listen for incoming connections
            lfd_conn.listen(5)
            i = 1
            while(True):
                # Accept a new connection
                conn, addr = lfd_conn.accept()
                print("Establish connection with LFD " + str(addr) + str(i))
                #Initiate a client listening thread
                threading.Thread(target=self.lfd_service_thread, args=(conn, addr)).start()
                i+=1

        except KeyboardInterrupt:
            # Closing the server
            lfd_conn.close()

if __name__=="__main__":
    gfd = GlobalFaultDetector()