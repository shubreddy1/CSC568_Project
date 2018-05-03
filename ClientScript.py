import hashlib
import socket as sk
import sys
import pickle
import random
from socket import *
from sets import Set
from time import sleep
from threading import Thread
from kazoo.client import KazooClient

def confun(socket,message):
	reply = None
	try:
		socket.send(message)
		reply = socket.recv(64)
	except:
		return "Timeout"
	return reply
	
def getsock():
	client_socket = sk.socket(sk.AF_INET, sk.SOCK_STREAM)
	client_socket.setsockopt(sk.SOL_SOCKET, sk.SO_REUSEADDR, 1)
	client_socket.settimeout(3)
	return client_socket
	
if len(sys.argv)<3:
	print "Insifficient arguments"
	exit(1)


myip = sys.argv[1]
serverip = sys.argv[2]
sel_opt = 0
tkey = ""
tval = ""
client_socket = getsock()
while 1:
	print "Server's IP: ",serverip
	sel_opt = input("\nSelect one of the options\n1.Insert values\n2.Get values\n3.Delete values")
	if sel_opt == 1:
		to_send = {}
		numval = input("Enter the number of values you'd like to store\n")
		for x in range(numval):
			tkey = raw_input("Enter the key\n")
			tval = raw_input("Enter the value\n")
			if tkey in to_send:
				print "Value will be overwritten"
			else:
				to_send[tkey] = tval
		to_send = pickle.dumps(to_send)                                 #Serialize values and send them to master server
		alen = len(to_send)
		buff = "0"*(6-len(str(alen)))
		message = "PUT"
		client_socket.connect((serverip,random.randint(60003,60010)))
		reply = confun(client_socket,message)
		#print "Server replied ",reply
		nport = int(reply[7:])
		client_socket.close()
		client_socket = getsock()
		sleep(1.6)
		message = message+buff+str(alen)
		#print "Trying to connect to ",nport," message ",message
		client_socket.connect((serverip,nport))
		client_socket.send(message)
		rep_ans = client_socket.recv(2)
		if rep_ans == "NO":                                             #Slave servers don't store keys, hence will reply with a "NO" to unsupported functions
			print "Functionality unavailable\n"
			client_socket.close()
			client_socket = getsock()
			break
		i_cntr = alen
		sp = 0
		while i_cntr>2048:                                              #Receive the list of values whose insertion failed
			client_socket.send(to_send[sp:sp+2048])
			#print "sent from ",sp," to ",sp+2048
			sp = sp + 2048
			i_cntr = i_cntr - 2048
		client_socket.send(to_send[sp:sp+2048])
		#print "sent from ",sp," to ",sp+2048
		reply = client_socket.recv(9)
		#print "received ",reply
		alen = int(reply)
		i_cntr = alen
		reply = []
		while i_cntr>2048:                                              #Fetch the list of uninserted values
			tstore = client_socket.recv(2048)
			reply.append(tstore)
			i_cntr = i_cntr - 2048
		tstore = client_socket.recv(2048)
		reply.append(tstore)
		client_socket.send("OK")
		client_socket.close()
		#print reply
		leftover = pickle.loads("".join(reply))
		print leftover
		if len(leftover)==0:
			print "All values have been successfully inserted"
		else:
			print "These values have been rejected ",leftover
		client_socket = getsock()
	elif sel_opt == 2:
		to_fetch = []
		numval = input("Enter the number of values you'd like to retrieve\n")           #Fetch the keys to be retrieved
		for x in range(numval):
			tkey = raw_input("Enter the key\n")
			if tkey in to_fetch:
				print "You've already entered this value"
			else:
				to_fetch.append(tkey)
		to_fetch = pickle.dumps(to_fetch)
		alen = len(to_fetch)
		buff = "0"*(6-len(str(alen)))
		message = "GET"
		client_socket.connect((serverip,random.randint(60003,60010)))
		reply = confun(client_socket,message)
		#print "Server replied ",reply
		nport = int(reply[7:])
		client_socket.close()
		client_socket = getsock()
		#print "Trying to connect to ",nport
		sleep(1)
		message = message+buff+str(alen)
		print "sending ",message
		client_socket.connect((serverip,nport))
		client_socket.send(message)
		client_socket.recv(2)                                                                   #serialize and send the values
		i_cntr = alen
		sp = 0
		while i_cntr>2048:
			client_socket.send(to_fetch[sp:sp+2048])
			sp = sp + 2048
			i_cntr = i_cntr - 2048
		client_socket.send(to_fetch[sp:sp+2048])
		reply = client_socket.recv(9)
		#print "received ",reply
		alen = int(reply)
		i_cntr = alen
		reply = []
		while i_cntr>2048:                                                                      #Fetch the result dictionary
			tstore = client_socket.recv(2048)
			reply.append(tstore)
			i_cntr = i_cntr - 2048
		tstore = client_socket.recv(2048)
		reply.append(tstore)
		client_socket.send("OK")                                        
		client_socket.close()
		client_socket = getsock()
		#print reply
		fetched_values = pickle.loads("".join(reply))
		for key in fetched_values:
			print "Fetched ",fetched_values[key]," for key ",key
	elif sel_opt==3:
		#print "delete key stub"
		to_del = []
		numval = input("Enter the number of values you'd like to delete\n")
		for x in range(numval):
			tkey = raw_input("Enter the key\n")
			if tkey in to_del:
				print "Value previously entered"
			else:
				to_del.append(tkey)
		to_del = pickle.dumps(to_del)
		alen = len(to_del)
		buff = "0"*(6-len(str(alen)))
		message = "DEL"
		client_socket.connect((serverip,random.randint(60003,60010)))
		reply = confun(client_socket,message)
		#print "Server replied ",reply
		nport = int(reply[7:])
		client_socket.close()
		client_socket = getsock()
		#print "Trying to connect to ",nport
		sleep(1)
		message = message+buff+str(alen)                        #serialize and send the values
		client_socket.connect((serverip,nport))
		client_socket.send(message)
		rep_ans = client_socket.recv(2)
		if rep_ans == "NO":
			print "Fucntionality unavailable\n"
			client_socket.close()
			client_socket = getsock()
			break
		i_cntr = alen
		sp = 0
		while i_cntr>2048:
			client_socket.send(to_del[sp:sp+2048])
			sp = sp + 2048
			i_cntr = i_cntr - 2048
		client_socket.send(to_del[sp:sp+2048])
		reply = client_socket.recv(9)
		#print "received ",reply
		alen = int(reply)
		i_cntr = alen
		reply = []                                                              #Fetch values whose deletions have failed
		while i_cntr>2048:
			tstore = client_socket.recv(2048)
			reply.append(tstore)
			i_cntr = i_cntr - 2048
		tstore = client_socket.recv(2048)
		reply.append(tstore)
		client_socket.send("OK")
		client_socket.close()
		#print reply
		leftover = pickle.loads("".join(reply))
		if len(leftover)==0:
			print "All values have been successfully deleted"
		else:
			print "These values have been rejected for deletion",leftover
		client_socket = getsock()
	else:
		print "Exiting\n"
		exit(0)
		
