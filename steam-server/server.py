#!/usr/bin/python

import socket
import sys
import time

if len(sys.argv) < 2:
	fileName = '../the_dataset.txt'
	print 'Using default file ' + fileName
else:
	fileName = sys.argv[1]

HOST = ''
PORT = 8888

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#print 'Socket created'

try:
  s.bind((HOST, PORT))
except socket.error, msg:
  print 'Bind failed. Error Code: ' + str(msg[0]) + ' Message ' + msg[1]
  sys.exit()
  #print 'Socket bind complete'

s.listen(10)
print 'Socket now listening'

conn, addr = s.accept()
print 'Connected with ' + addr[0] + ':' + str(addr[1])

# Read file and send it to client
with open(fileName) as openfileobject:
	ctr = 0
	for line in openfileobject:
		conn.sendall(line)
		# ctr += 1
		# if (ctr % 10000 == 0):
			# print 'paused'
			# time.sleep(1)

print 'done'

conn.recv(1)
conn.close()
s.close()
