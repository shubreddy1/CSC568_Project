import hashlib
import socket as sk
import sys
import random
import pickle
from socket import *
from sets import Set
from time import sleep
from threading import Thread, Lock
from kazoo.client import KazooClient


kzc = KazooClient(hosts = "127.0.0.1:2181")
kzc.start()
print "current state ",kzc.state
#kzc.delete("/*",recursive=True)
kzc.ensure_path("/oldobj_dict")
kzc.ensure_path("/newobj_dict")
kzc.ensure_path("/key_availability")


TIMER_KA = 20
TIMER_SLP = 30
FLAG_LOCK = 1
glock = Lock()
clients_list = []

del_set = []

key_availability = {}
#key_set = Set()
oldobj_dict = {}
newobj_dict = {}
myip = sys.argv[1]

pending_obj = {}

local_dict = {}
pending_objects = []

def listener_thread(client_list):	#Heartbeat thread, listens to messages from other nodes
	global kzc
	lts = sk.socket(sk.AF_INET, sk.SOCK_DGRAM)	#Socket creation and bind
	lts.setsockopt(sk.SOL_SOCKET, sk.SO_REUSEADDR, 1)
	lts_address = (myip,60001)
	lts.bind(lts_address)
	kzc.ensure_path("/nodes/"+myip)
	kzc.set("/nodes/"+myip,"0")
	clients_list.append([myip,30])
	print "Listener thread started listening on port 60001"
	try:
		while 1:			#Accept UDP messages from nodes in cluster
			data, address = lts.recvfrom(16)
			#print "Incoming connection from ",address[0]," with message ",data
			if data[0] == "H":
				kzc.ensure_path("/nodes/"+address[0])	#Create the global path 
				kzc.set("/nodes/"+address[0],"0")	#Set it to 0
				print "Creating node for ",address[0]
				clients_list.append([address[0],TIMER_KA])
			else:					# Renew the client's age
				for tupl in clients_list:	
					if tupl[0] == address[0]:
						#tupl[1] = TIMER_KA
						clients_list[clients_list.index(tupl)][1] = TIMER_KA
	except:
		print "Error occured"
	finally:
		print "Listener thread crashed"

def checker_thread(client_list):
	global kzc
	global newobj_dict 
	while 1:
		#print "Checker thread is alive"
		#print clients_list
		sleep(5)
		for client in clients_list:
			if client[1]<=0:	#check and decrement timers, if expired perform cleanup
				clients_list.remove(client)
				print client[0]," expired, deleting associated information"
				if client[0] in newobj_dict:
					del newobj_dict[client[0]]
				kzc.delete("/nodes/"+client[0])
			else:
				if client[0] != myip:
					client[1] = client[1] - 5

def request_handler(port):
	global key_availability
	global pending_obj
	global local_dict
	global del_set
	handler_socket = sk.socket(sk.AF_INET, sk.SOCK_STREAM)
	handler_socket.setsockopt(sk.SOL_SOCKET, sk.SO_REUSEADDR, 1)
	handler_addr = (myip,port)
	handler_socket.bind(handler_addr)
	handler_socket.listen(1)
	print "Request handler started on port ",port
	try:
		print "Handling connection"
		connection, address = handler_socket.accept()
		request = connection.recv(9)                                    #Fetch the request, it contains either PUT, GET or DEL
		print request
		if request[0]=="P":
			#print "Getting the incoming dictionary"
			connection.send("OK")
			in_len = int(request[3:])
			i_cntr = 0
			treply = []
			i_cntr=in_len
			while i_cntr>2048:
				tstore = connection.recv(2048)
				#print tstore
				treply.append(tstore)
				i_cntr = i_cntr - 2048
			tstore = connection.recv(i_cntr)
			#print tstore
			treply.append(tstore)
			to_store = pickle.loads(str("".join(treply)))                   #Load the dictionary
			#print "Fetched the whole dictionary ",to_store
			ret_vals = []
			for ke in to_store:
				if ke in local_dict:                                    #if key in local, ignore the insertion
					ret_vals.append(ke)
				else:
					pending_obj[ke] = to_store[ke]          #Else push into local store
					local_dict[ke] = to_store[ke]
			ret_vals = pickle.dumps(ret_vals)
			alen = len(ret_vals)
			buff = "0"*(9-len(str(alen)))
			#print "sending ",buff+str(alen)
			connection.send(buff+str(alen))                                 #Return the leftover values
			i_cntr = alen
			sp = 0
			while i_cntr>2048:
				connection.send(ret_vals[sp:sp+2048])
				sp = sp + 2048
				i_cntr = i_cntr - 2048
			connection.send(ret_vals[sp:sp+2048])
			connection.recv(2)
			connection.close()
		elif request[0]=="G":
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
			#print tstore
			treply.append(tstore)
			#print "Fetched the whole list"
			to_fetch = pickle.loads(str("".join(treply)))
			print "Client is requesting to fetch ",to_fetch
			ret_dict = {}
			pend_list = []
			for tke in to_fetch:
				if tke in key_availability:                                             #If values exist, resolve and set them in result dictionary, else set them to "NOT AVAILABLE"
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
				connection.send(buff+str(alen))                                 #If all values in local, directly set the values and return the result dictionary
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
				for res in result:                                                      #Else, fetch values from other peers and return the result
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
			#delete case
			#print "Getting the incoming dictionary"
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
			#print tstore
			treply.append(tstore)
			to_del = pickle.loads(str("".join(treply)))
			#print "Fetched the whole dictionary ",to_del
			print key_availability,to_del
			ret_vals = []
			for ke in to_del:
				if ke in key_availability:                              #Store values for deletion and return values which cannot be deleted, i.e. the ones which don't exist
					del_set.append(ke)
				else:
					ret_vals.append(ke)
			ret_vals = pickle.dumps(ret_vals)
			alen = len(ret_vals)
			buff = "0"*(9-len(str(alen)))
			#print "sending ",buff+str(alen)
			connection.send(buff+str(alen))
			i_cntr = alen
			sp = 0
			while i_cntr>2048:
				connection.send(ret_vals[sp:sp+2048])
				sp = sp + 2048
				i_cntr = i_cntr - 2048
			connection.send(ret_vals[sp:sp+2048])
			connection.recv(2)
			connection.close()						
	except Exception as e:
		print e.message
	#finally:
		#print "Request handler crashed, try relaunching the code"

def resolve(key_list, availability_list):                                               #Calculates which peers have the existing keys, do a fetch of all of them from each peer and return the resultant dictionary
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
		tstore = client_socket.recv(2048)
		reply.append(tstore)
		client_socket.send("OK")
		client_socket.close()
		#print reply
		fetched_values = pickle.loads("".join(reply))
		for key in fetched_values:
			result_dict[key] = fetched_values[key]
	return result_dict


def sync_thread():                                                      #Makes the local state consistent with the global set state 
	sleep(10)
	global local_dict
	global oldobj_dict
	global newobj_dict
	global key_availability
	global pending_obj
	while 1:
		if kzc.get("/nodes/"+myip)[0] == "1":		#If flag is unset, no state change has happened so sleep
			sleep(15)
		else:
			#print "just wokeup ",myip		#On waking up, fetch the new state from zookeeper
			newobj_dict = pickle.loads(kzc.get("/newobj_dict")[0])
			oldobj_dict = pickle.loads(kzc.get("/oldobj_dict")[0])
			key_availability = pickle.loads(kzc.get("/key_availability")[0])
			#print newobj_dict,oldobj_dict,key_availability
			for ke in local_dict.keys():		#Delete keys which have been rehashed to a different slave node and values which have been actually deleted
				if myip in oldobj_dict:
					if ke not in oldobj_dict[myip] and ke not in newobj_dict[myip] and ke not in pending_obj:
						print "Deleting ",ke
						del local_dict[ke]
			if myip not in newobj_dict:		#Certain edge case
				kzc.set("/nodes/"+myip,"1")
				continue
			print "future state ",newobj_dict[myip]," local has ", local_dict.keys()
			to_fetch = {}
			to_fetch_arr=[]
			for ke in newobj_dict[myip]:		#Fetch all new values hashed to the current slave node by contacting the nodes which have the values
				if ke not in local_dict:
					to_fetch_arr.append(ke)
			if len(to_fetch_arr)>0:
				res_dict = resolve(to_fetch_arr,key_availability)
				for ke in res_dict:
					local_dict[ke] = res_dict[ke]
			kzc.set("/nodes/"+myip,"1")
			print "Locally stored keys are ",local_dict.keys()

def delivery_thread(port):                                      #Handle redirection to request handlers, many instances of this are run to prevent blocking of a single welcome port
	print "Delivery thread started on ",port
	try:
		while True:
			delivery_socket = sk.socket(sk.AF_INET, sk.SOCK_STREAM)
			delivery_socket.setsockopt(sk.SOL_SOCKET, sk.SO_REUSEADDR, 1)
			delivery_thread_addr = (myip,port)
			delivery_socket.bind(delivery_thread_addr)
			delivery_socket.listen(1)
			print "Delivery socket is up"
			connection,address = delivery_socket.accept()
			print "Connection accepted from ",address
			request = connection.recv(64)
			print "Received request",request
			rport = random.randint(62100,62800)
			tthrd = Thread(target = request_handler, args = (rport,))
			tthrd.start()
			treply = "REDPRT:"+str(rport)
			print treply
			connection.send(treply)
			tthrd.join()
			connection.close()
	except Exception as e:
		print e.message
		print "Error in delivery thread"
	finally:
		print "Delivery thread crashed"			

def hash_thread():                                      # Hash all keys to 2 nodes and upload the new state to zookeeper
	print "Hash thread started"
	global key_availability
	global oldobj_dict
	global newobj_dict
	global pending_obj
	global local_dict
	global del_set
	#global glock
	while(1):
		print "Availability", key_availability
		for ke in pending_obj.keys():
			key_availability[ke] = [myip]
			del pending_obj[ke]
		#print "old dict",oldobj_dict
		print "next state",newobj_dict
		oldobj_dict = newobj_dict
		existing_objects = key_availability.keys()
		newobj_dict = myhash(existing_objects,clients_list)
		serial_op1 = pickle.dumps(oldobj_dict)
		serial_op2 = pickle.dumps(newobj_dict)
		serial_op3 = pickle.dumps(key_availability)
		#glock.acquire()
		kzc.set("/newobj_dict",serial_op2)
		kzc.set("/oldobj_dict",serial_op1)
		kzc.set("/key_availability",serial_op3)
		#glock.release()
		for tupl in clients_list:
			print "Setting ",tupl[0]," flag to 1"
			kzc.set("/nodes/"+tupl[0],"0")
		sleep(TIMER_SLP)
		nkey_availability={}
		for tupl in clients_list:
			ip=tupl[0]
			if ip in newobj_dict:
				for key in newobj_dict[ip]:
					if key in nkey_availability:
						nkey_availability[key].append(ip)
					else:
						nkey_availability[key] = [ip]
		for ke in del_set:
			"Deletion working on ",ke
			del nkey_availability[ke]
		del_set=[]
		#glock.acquire()
		key_availability = nkey_availability
		#glock.release()

def myhash(existing_keys, clients_list):                                        #The hash function
	print "Hasing ",existing_keys," over ", clients_list
	base=len(clients_list)
	new_dict={}
	for key in existing_keys:
		indx1 = ord(hashlib.md5(str(key)).digest()[0])%base
		indx2 = (ord(hashlib.md5(str(key)).digest()[0])+1)%base
		if clients_list[indx1][0] in new_dict:
			new_dict[clients_list[indx1][0]].append(key)
		else:
			new_dict[clients_list[indx1][0]] = [key]
		if clients_list[indx2][0] in new_dict:
			new_dict[clients_list[indx2][0]].append(key)
		else:
			new_dict[clients_list[indx2][0]] = [key]
	print "Hashing done, ",new_dict
	return new_dict

def main():                                     #Start the threads to handle input and handle Ctrl+Z
	t1 = Thread(target=listener_thread,args=(clients_list,))
	t2 = Thread(target=checker_thread,args=(clients_list,))
	t3 = Thread(target=hash_thread)
	arr=[]
	for x in range(60003,60011):
		tmp = Thread(target=delivery_thread,args=(x,))
		arr.append(tmp)
	##t4 = Thread(target=delivery_thread)
	t5 = Thread(target=sync_thread)
	t1.setDaemon(True)
	t1.start()
	t2.setDaemon(True)
	t3.setDaemon(True)
	for thrd in arr:
		thrd.setDaemon(True)
		thrd.start()
	##t4.setDaemon(True)
	t5.setDaemon(True)
	t2.start()
	t3.start()
	##t4.start()
	t5.start()
	while 1:
		try:
			t1.join(5)
			t2.join(5)
			t3.join(5)
			for thrd in arr:
				thrd.join(5)
			##t4.join(5)
			t5.join(5)
		except KeyboardInterrupt:
			t1.kill_received=True
			t2.kill_received=True
			t3.kill_received=True
			for thrd in arr:
				thrd.kill_received=True
			##t4.kill_received=True
			t5.kill_received=True

if len(sys.argv)<2:
	print "Insufficient arguments"
	exit(1)

main()                                          #Kill pending threads listening on ports using " sudo lsof -i :xxxx" where port number = xxxx
