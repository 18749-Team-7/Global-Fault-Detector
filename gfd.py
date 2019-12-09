import json
import requests
import threading
import socket
import time
import argparse
import os


BUF_SIZE = 1024

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
        # Get host IP
        self.get_host_ip()

        # Global Variables
        self.lfd_replica_dict = {}
        self.HB_counter = 0
        self.members_mutex = threading.Lock()
        self.gfd_hb_interval = int(gfd_hb_interval)
        self.gfd_port = 12345

        # Heartbeat RM thread
        self.rm_hb_port = 10002
        self.rm_hb_addr = (self.host_ip, self.rm_hb_port)
        threading.Thread(target=self.establish_RM_HB_connection).start()

        # RM status updating
        time.sleep(2)
        self.rm_status_port = 10001
        self.rm_address2 = (self.host_ip, self.rm_status_port)
        self.init_rm_status_comm()
        
        # LFD listening Thread
        threading.Thread(target=self.init_lfd_comm).start() # Start LFD listening

    def init_rm_status_comm(self):
        # Create a TCP/IP socket for sending status
        self.rm_conn_2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.rm_conn_2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Bind the socket to the replication port
        print(RED + "Connecting to RM for status {} ...".format(self.rm_address2) + RESET)
        try:
            self.rm_conn_2.connect(self.rm_address2)
        except:
            print(RED + "Could not connect to RM for status" + RESET)
            os._exit()

    # Finds the IP on the host
    def get_host_ip(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.connect(("8.8.8.8", 80))
        self.host_ip = s.getsockname()[0]

    # Establishes connection for heartbeating with the RM
    # Uses port: 10002
    def establish_RM_HB_connection(self):
        print(RED + "Establishing connection with Replication Manager..." + RESET)
        try:
            # Create a TCP/IP socket for hearbeat
            self.rm_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.rm_conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            # Bind the socket to the replication port
            print(RED + 'Connecting to Replication Manager on IP: {} ...'.format(self.rm_hb_addr) + RESET)
            self.rm_conn.connect(self.rm_hb_addr)

            self.RM_heartbeat_func()
        except:
            print(RED + "Connection with RM could not be established" + RESET)
            os._exit()


    # Heart beating function for the RM
    def RM_heartbeat_func(self): 
        try:
            # Waiting for RM membership data
            while True:
                RM_heartbeat_msg = "GFD_heartbeat"
                RM_heartbeat_msg = RM_heartbeat_msg.encode('utf-8')
                self.rm_conn.send(RM_heartbeat_msg)
                time.sleep(self.gfd_hb_interval)
        finally:
            # GFD connection errors
            print(RED + "RM connection has been lost" + RESET)
            
            # Clean up the connection
            self.rm_conn.close()

    def lfd_service_thread(self, s, addr):
        # first time save the replica IPs corresponding to LFD
        try:
            #s.settimeout(2)
            data = s.recv(BUF_SIZE)
        except Exception as e:
            print(e)
            return

        lfd_count = 0
        
        data = data.decode('utf-8')
        json_data = json.loads(data)

        replica_ip = json_data["server_ip"]
        replica_status = json_data["status"]

        # Edit the replica_ip_list
        self.members_mutex.acquire()
        if addr not in self.lfd_replica_dict:
            self.lfd_replica_dict[addr] = replica_ip
        self.members_mutex.release()

        # keep receiving status update from LFD, if you don't hear back from LFD, then replica failed
        while True:      
            try:
                # Set timeout to figure death of LFD
                timeout = int(self.gfd_hb_interval + 2)
                s.settimeout(timeout)

                data = s.recv(BUF_SIZE)
                data = data.decode('utf-8')
                json_data = json.loads(data)
                replica_ip = json_data["server_ip"]
                replica_status = json_data["status"]

                print(BLUE + "Received heartbeat from LFD at: {} | Heartbeat count: {}".format(addr, lfd_count) + RESET)
                lfd_count += 1

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
               


    def init_lfd_comm(self):
        try:
            # Create a TCP/IP socket
            lfd_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            lfd_conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            lfd_conn.bind((self.host_ip, self.gfd_port))  
            # Listen for incoming connections
            lfd_conn.listen(5)

            print(RED + "Started listening for LFD on {} port {} ...".format(self.host_ip, self.gfd_port) + RESET)

            i = 1
            while(True):
                # Accept a new connection
                conn, addr = lfd_conn.accept()
                print(RED + "Connected to LFD: {}".format(addr) + RESET)

                # Start the client listening thread
                threading.Thread(target=self.lfd_service_thread, args=(conn, addr)).start()
                i+=1

        except KeyboardInterrupt:
            # Closing the server
            lfd_conn.close()

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-hbf', '--hb_freq', help="Heartbeat Frequency", type=int, default=1)
    # Parse the arguments
    args = parser.parse_args()
    return args

if __name__=="__main__":
        # Extract Arguments from the 
    args = get_args()

    gfd = GlobalFaultDetector(gfd_hb_interval=args.hb_freq)