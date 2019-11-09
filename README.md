# Global Fault Detector
Functions, example code, packet format. Fill in details here


GFD port for LFD1 10001
GFD port for LFD2 10002
GFD port for LFD3 10003

Timeout heartbeat from local fault detector: 2 secs

# GFD expects packet from LFD with the following:
Server IP: string
Status: true or false (Indicates if alive or not)

{
server_ip: “IP addr”
status: “true”
} 
