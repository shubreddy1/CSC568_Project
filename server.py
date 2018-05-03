from kazoo.client import KazooClient
import os
import sys
import shutil
import logging
import time
import pickle
import random
import socket as sk
from time import sleep
from threading import Thread
#primary check
logging.basicConfig()

FIRST_FLAG = 1
DELIVERY_PORT = 0

local_dict = {}		#Local store dictionary
oldobj_dict = {}	#Global dictionary
newobj_dict = {}	#Global dictionary
key_availability = {}	#Global dictionary
kzc = KazooClient(hosts = "127.0.0.1:2181")	#To connect to Zookeeper on the default port
kzc.start()					#Making the connection

def hello_thread(server_ip):			#Sends heartbeat messages to Master server
	flag = 1	
	ht = sk.socket(sk.AF_INET, sk.SOCK_DGRAM)
	ht.setsockopt(sk.SOL_SOCKET, sk.SO_REUSEADDR, 1)
	ht_address = (myip,60001)
	ht.bind(ht_address)
	server_address = (server_ip,60001)
	while(1):
		if flag:
			ht.sendto("HELLO",server_address)	#Hello initiates state variable setting and flag creation
			flag = 0
		else:
			ht.sendto("KEEP_ALIVE",server_address)	#Keepalive resets the timer for this client
		sleep(3)

def sync_thread():
	time.sleep(10)
	global local_dict
	global oldobj_dict
	global newobj_dict
	global key_availability
	while 1:
		if kzc.get("/nodes/"+myip)[0] == "1":		#If flag is unset, no state change has happened so sleep
			time.sleep(5)
		else:
			#print "just wokeup ",myip		#On waking up, fetch the new state from zookeeper
			newobj_dict = pickle.loads(kzc.get("/newobj_dict")[0])
			oldobj_dict = pickle.loads(kzc.get("/oldobj_dict")[0])
			key_availability = pickle.loads(kzc.get("/key_availability")[0])
			print newobj_dict,oldobj_dict,key_availability
			for ke in local_dict.keys():
				if myip in oldobj_dict:		#Delete keys which have been rehashed to a different slave node and values which have been actually deleted
					if ke not in oldobj_dict[myip]:
						del local_dict[ke]
			if myip not in newobj_dict:		#Certain edge case
				kzc.set("/nodes/"+myip,"1")
				continue
			to_fetch=[]
			for ke in newobj_dict[myip]:		#Fetch all new values hashed to the current slave node by contacting the nodes which have the values
				if ke not in local_dict:
					to_fetch.append(ke)
			if len(to_fetch)>0:
				res_dict = resolve(to_fetch,key_availability)
				for ke in res_dict:
					local_dict[ke] = res_dict[ke]
			kzc.set("/nodes/"+myip,"1")
			print "Locally stored keys are ",local_dict.keys()

def delivery_thread(myip,port):
	try:
		print "Delivery thread started on port ",port                           #Welcome port to handle incoming requests
		while True:
			delivery_socket = sk.socket(sk.AF_INET, sk.SOCK_STREAM)                         
			delivery_socket.setsockopt(sk.SOL_SOCKET, sk.SO_REUSEADDR, 1)
			delivery_thread_addr = (myip,port)
			delivery_socket.bind(delivery_thread_addr)
			delivery_socket.listen(1)
			print "socket is up"
			connection,address = delivery_socket.accept()
			print "conn accepted"
			request = connection.recv(64)
			print "received request ",request
			rport = random.randint(61100,61200)
			tthrd = Thread(target = request_handler, args = (rport,))
			tthrd.start()
			treply = "REDPRT:"+str(rport)
			print treply
			connection.send(treply)
			tthrd.join()
			connection.close()
	finally:
		print "delivery thread crashed"	

def resolve(key_list, availability_list):                                               #Same as the function in Mserver
	global kzc
	global oldobj_dict
	print "I need to fetch ",key_list
	fetcher_dict = {}
	for key in key_list:
		for ip in availability_list[key]:
			if kzc.exists("/nodes/"+ip):
				if ip in fetcher_dict:
					fetcher_dict[ip].append(key)
				else:
					fetcher_dict[ip] = [key]
				break
	print "I'm fetching these values from these IP addresses now: ",fetcher_dict
	result_dict = {}
	for ip in fetcher_dict:
		if ip not in oldobj_dict:
			break
		to_fetch = fetcher_dict[ip]
		to_fetch = pickle.dumps(to_fetch)
		alen = len(to_fetch)
		buff = "0"*(6-len(str(alen)))
		message = "GET"
		client_socket = sk.socket(sk.AF_INET, sk.SOCK_STREAM)
		client_socket.setsockopt(sk.SOL_SOCKET, sk.SO_REUSEADDR, 1)
		client_socket.settimeout(3)
		client_socket.connect((ip,random.randint(60003,60010)))
		client_socket.send(message)
		reply = client_socket.recv(64)
		#print "Server replied ",reply
		nport = int(reply[7:])
		client_socket.close()
		client_socket = sk.socket(sk.AF_INET, sk.SOCK_STREAM)
		client_socket.setsockopt(sk.SOL_SOCKET, sk.SO_REUSEADDR, 1)
		client_socket.settimeout(3)
		#print "Trying to connect to ",nport
		sleep(1)
		message = message+buff+str(alen)
		print "sending ",message
		client_socket.connect((ip,nport))
		client_socket.send(message)
		client_socket.recv(2)
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
		while i_cntr>2048:
			tstore = client_socket.recv(2048)
			reply.append(tstore)
			i_cntr = i_cntr - 2048
		tstore = client_socket.recv(i_cntr)
		reply.append(tstore)
		client_socket.send("OK")
		client_socket.close()
		#print reply
		fetched_values = pickle.loads("".join(reply))
		for key in fetched_values:
			result_dict[key] = fetched_values[key]
	return result_dict


def request_handler(port):                                      #Handles only "GET" requests
	global key_availability
	#global pending_obj
	handler_socket = sk.socket(sk.AF_INET, sk.SOCK_STREAM)
	handler_socket.setsockopt(sk.SOL_SOCKET, sk.SO_REUSEADDR, 1)
	#port = 60005
	handler_addr = (myip,port)
	handler_socket.bind(handler_addr)
	handler_socket.listen(1)
	print "Handler thread started on port",port
	try:
		connection, address = handler_socket.accept()
		request = connection.recv(9)
		if request[0]=="G":
			print "Getting the incoming list"
			connection.send("OK")
			in_len = int(request[3:])
			i_cntr = in_len
			treply = []
			while i_cntr>2048:
				tstore = connection.recv(2048)
				#print tstore
				treply.append(tstore)
				i_cntr = i_cntr - 2048
			tstore = connection.recv(i_cntr)
			print tstore
			treply.append(tstore)
			#print "Fetched the whole list"
			to_fetch = pickle.loads(str("".join(treply)))
			print "Client is requesting to fetch ",to_fetch
			ret_dict = {}
			pend_list = []
			for tke in to_fetch:
				if tke in key_availability:
					if tke in local_dict:
						ret_dict[tke] = local_dict[tke]
					else:
						pend_list.append(tke)
				else:
					ret_dict[tke] = "NOT AVAILABLE"
			if len(pend_list) == 0:
				ret_dict = pickle.dumps(ret_dict)
				alen = len(ret_dict)
				buff = "0"*(9-len(str(alen)))
				#print "sending ",buff+str(alen)
				connection.send(buff+str(alen))
				i_cntr = alen
				sp = 0
				while i_cntr>2048:
					connection.send(ret_dict[sp:sp+2048])
					sp = sp + 2048
					i_cntr = i_cntr - 2048
				connection.send(ret_dict[sp:sp+2048])
				connection.recv(2)
				connection.close()
			else:
				result = resolve(pend_list,key_availability)
				for res in result:
					ret_dict[res] = result[res]
				ret_dict = pickle.dumps(ret_dict)
				alen = len(ret_dict)
				buff = "0"*(9-len(str(alen)))
				#print "sending ",buff+str(alen)
				connection.send(buff+str(alen))
				i_cntr = alen
				sp = 0
				while i_cntr>2048:
					connection.send(ret_dict[sp:sp+2048])
					sp = sp + 2048
					i_cntr = i_cntr - 2048
				connection.send(ret_dict[sp:sp+2048])
				connection.recv(2)
				connection.close()
		else:
			connection.send("NO")
			connection.close()	#unknown functionality
	except Exception as e:
		print e.message
		print "Request handler crashed, try relaunching the code"


zk=KazooClient(hosts="127.0.0.1:2181")

try:
	zk.start()
except:
	print "Zookeeper is not executing locally"
	exit()

print "connection succeeded","state :",zk.state
#secondary check
if len(sys.argv)<2:
	print "Insufficient arguments"
	exit()

myip = sys.argv[1]
server_ip = sys.argv[2]


def main():
	t1 = Thread(target=hello_thread,args=(server_ip,))
	t2 = Thread(target=sync_thread)
	#t3 = Thread(target=hash_thread)
	#t4 = Thread(target=delivery_thread, args = (myip,))
	t1.setDaemon(True)
	t1.start()
	t2.setDaemon(True)
	#t3.setDaemon(True)
	#t4.setDaemon(True)
	arr=[]
	for x in range(60003,60011):
		tmp = Thread(target=delivery_thread,args=(myip,x,))
		arr.append(tmp)
	t2.start()
	#t3.start()
	for thrd in arr:
		thrd.setDaemon(True)
		thrd.start()
	#t4.start()
	while 1:
		try:
			t1.join(1)
			t2.join(1)
			for thrd in arr:
				thrd.join(5)#
			#t3.join(1)
			#t4.join(1)
		except KeyboardInterrupt:
			t1.kill_received=True
			t2.kill_received=True
			#t3.kill_received=True
			#t4.kill_received=True
			for thrd in arr:
				thrd.kill_received=True

main()

#cleanup code
zk.stop()
zk.close()
