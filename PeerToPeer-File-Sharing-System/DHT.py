from cgitb import lookup
from fileinput import filename
import socket 
import threading
import os
import time
import hashlib
from queue import Queue
from json import dumps, loads
#format of messages

# lookup request msg= lookup_req|put_file or get_file or node|filename or nodename| lookupnodeaddr(who called lookup)
# lookup reply msg= lookup_reply|put_file or get_file or node| filename or nodename| successor addr
# lookup reply edge case=lookup_reply_edge_case| curr node addr
# sendfile msg= send_file|put or get or backup or successor|filename|filesize               	#initial msg to setup receive file fn in receiver node
#reqfile msg=request_file|filename|addr of node who request file
#ping_msg= ping|addr of node who sends it| backupfile list of successor it maintains
#ping_reply_msg = ping_reply|predecessor of node who sends it|success of node who sends it      #this helps to fix recv node successor and succesor'successor
#new_predecessor_msg= new_predecessor|type=ping or join| addr of node who sends it     #if its join type then we also have to do file transfer
# bye_msg= bye|succ or pre| predecessor addr or successor addr           #helps to fix pointers of successor and pred of leaving nodes


class Node:
	def __init__(self, host, port):
		self.stop = False #kills ping and listner
		self.host = host
		self.port = port
		self.M = 16
		self.N = 2**self.M
		self.key = self.hasher(host+str(port))
		# You will need to kill this thread when leaving, to do so just set self.stop = True
		threading.Thread(target = self.listener).start()
		self.files = []
		self.backUpFiles = [] #predecessor maintain backup of successor
		if not os.path.exists(host+"_"+str(port)):
			os.mkdir(host+"_"+str(port))
		'''
		------------------------------------------------------------------------------------
		DO NOT EDIT ANYTHING ABOVE THIS LINE
		'''
		# Set value of the following variables appropriately to pass Intialization test
		self.successor = (host,port)
		self.predecessor = (host,port)
		self.number_of_pings=0 #keep track of failed pings
		
		threading.Thread(target = self.ping).start() #start thread of ping fn which send ping
		
		
		# additional state variables
		self.filename=Queue() #used in get fn to return filename

		self.successor_s_successor=tuple() #used in failture tolerance
		



	def hasher(self, key):
		'''
		DO NOT EDIT THIS FUNCTION.
		You can use this function as follow:
			For a node: self.hasher(node.host+str(node.port))
			For a file: self.hasher(file)
		'''
		return int(hashlib.md5(key.encode()).hexdigest(), 16) % self.N


	def handleConnection(self, client, addr):
		'''
		 Function to handle each inbound connection, called as a thread from the listener.
		'''
		
		recv_msg=client.recv(5000).decode()  
		recv_msg_list=recv_msg.split("|")
		msg_type=recv_msg_list[0]  #lookup, send file, request file, ping, ping_reply, 
		msg_content=recv_msg_list[1:]
		
		if msg_type=="lookup_req":  #received from predecessor to continue lookup
			self.lookup(msg_content)
		elif msg_type=="lookup_reply": # receive by node who wanted its successor or file's successor
			self.process_lookup_reply(msg_content)
		elif msg_type=="new_predecessor":  #received from predecessor node to update predecessor of its successor
			
			self.process_new_predecessor(msg_content)
					
		elif msg_type=="lookup_reply_edge_case":  #special lookup reply when edge case: node joinig a hash ring of single node
			self.successor=tuple(loads(msg_content[-1]))
			self.predecessor=tuple(loads(msg_content[-1]))

		elif msg_type=="ping": #received from predecessor

			self.process_ping_msg(msg_content)
			
		elif msg_type=="ping_reply": #received from succesor who was pinged
			self.process_ping_reply(msg_content)

		elif msg_type=="send_file": # in put case sent from node whoinitialized put, in get case sent from node having file in dht
			self.process_sendfile_msg(msg_content,client)
			
		elif msg_type=="request_file": #sent from node who initialized get to the node who have file
			self.process_requestfile_msg(msg_content)
			
		elif msg_type=="bye":
			self.process_bye_msg(msg_content,client)
			
		

	def listener(self):
		'''
		We have already created a listener for you, any connection made by other nodes will be accepted here.
		For every inbound connection we spin a new thread in the form of handleConnection function. You do not need
		to edit this function. If needed you can edit signature of handleConnection function, but nothing more.
		'''
		listener = socket.socket()
		listener.bind((self.host, self.port))
		listener.listen(10)
		while not self.stop:
			client, addr = listener.accept()
			threading.Thread(target = self.handleConnection, args = (client, addr)).start()
		print ("Shutting down node:", self.host, self.port)
		try:
			listener.shutdown(2)
			listener.close()
		except:
			listener.close()

	def join(self, joiningAddr):
		'''
		This function handles the logic of a node joining. This function should do a lot of things such as:
		Update successor, predecessor, getting files, back up files. SEE MANUAL FOR DETAILS.
		'''
		if joiningAddr=="":
			return #already node succesor and predecessor initiliazed to itself
		else:
			lookup_req_msg="lookup_req|"+"node|"+"nodename|"+dumps((self.host,self.port)) #nodename field just added to keep consistent with file lookup message
			soc=socket.socket()
			soc.connect(joiningAddr)
			soc.send(lookup_req_msg.encode())
		# keep making lookup_req_msg with same content to successor node till reach successor node
		#then lookup reply send to the node who initialized this lookup (called join fn) we get its address from lookup_req_msg
		#lookup reply gets processed by process_lookup_reply which fixes succesor of new node and send new_predeccesor msg to successor to fix its predecessor
		#ping and ping reply is used to fix old predecessor succesor. done in fn process_ping reply
		#new_predecessor msg is again used this time to fix new nodes predecessor. this message send by fn process_ping reply


	def put(self, fileName):
		'''
		This function should first find node responsible for the file given by fileName, then send the file over the socket to that node
		Responsible node should then replicate the file on appropriate node. SEE MANUAL FOR DETAILS. Responsible node should save the files
		in directory given by host_port e.g. "localhost_20007/file.py".
		'''
		lookup_req_msg="lookup_req|"+"put_file|"+fileName+"|"+dumps((self.host,self.port))
		soc=socket.socket()
		soc.connect(self.successor)
		soc.send(lookup_req_msg.encode())
		#lookup req keeps getting forwarded till find the right successor adr
		#lookup reply is send back to us with right successor adr where we will put file
		# lookup reply get processed in process_lookup_reply fn and then it puts file in the right successor
		

		
	def get(self, fileName):
		'''
		This function finds node responsible for file given by fileName, gets the file from responsible node, saves it in current directory
		i.e. "./file.py" and returns the name of file. If the file is not present on the network, return None.
		'''
		
		lookup_req_msg="lookup_req|"+"get_file|"+fileName+"|"+dumps((self.host,self.port))
		soc=socket.socket()
		soc.connect(self.successor)
		soc.send(lookup_req_msg.encode())
		filename=self.filename.get()
		
		if filename==" ": #can be empty if file donot exist at the right successor because it was never stored there
			
			return None
		else:
			return filename
		# send lookup req to get right succ who stores the file
		# we get a lookupreply of type get file which gets processed in process_lookup_reply fn
		#there we send the right successor a request file msg 
		#then that right successor on receiving request file msg send the file to use
		#it uses sendfile with type get file msg which does is along receiiving file it adds filename into self.filename queue which is acquired here in get fn
		#this filename is then returned
		
	def leave(self):
		'''
		When called leave, a node should gracefully leave the network i.e. it should update its predecessor that it is leaving
		it should send its share of file to the new responsible node, close all the threads and leave. You can close listener thread
		by setting self.stop flag to True
		'''
		
		soc_pre=socket.socket()
		soc_pre.connect(self.predecessor)
		deleted_files_lst=[]

		for filename in self.files: #send your share of files to succ and delete them from here
			path=self.host+"_"+str(self.port)+"\\"+filename
			fileSize = os.path.getsize(path)
			sendfile_msg="send_file|"+"put|"+filename+"|"+str(fileSize)
			soc_suc=socket.socket()
			soc_suc.connect(self.successor)
			soc_suc.send(sendfile_msg.encode())
			self.sendFile(soc_suc,path)
			
			deleted_files_lst.append(filename)
		for filename in deleted_files_lst:
			self.files.remove(filename)
			
		
		bye_successor_msg="bye|"+"successor|"+dumps(self.predecessor) #send bye messages to succ and pre to fix there pointers 
		bye_predecessor_msg="bye|"+"predecessor|"+dumps(self.successor)
		
		soc_suc=socket.socket()
		soc_suc.connect(self.successor)
		soc_suc.send(bye_successor_msg.encode())
		soc_pre.send(bye_predecessor_msg.encode())
		
		self.stop=	True




	def sendFile(self, soc, fileName):
		
		try:
			file=open(fileName,"rb")
			contentChunk = file.read()
			
			soc.send(contentChunk)
			
		except:
			pass

	def recieveFile(self, soc, fileName,fileSize):
		
		try:
			
			file = open(fileName, "wb")
			
			contentChunk = soc.recv(4060)
		
		except:
			pass
			
		file.write(contentChunk)
		file.close()

	def kill(self):
		# DO NOT EDIT THIS, used for code testing
	
		self.stop = True
		
		

	def lookup(self,msg_content):
		#lookup for file or node
		lookup_type=msg_content[0] #file or node
		lookup_node_addr=tuple(loads(msg_content[-1]))	#(host,port) msg content[-1] just have new node addr or node looking for file
		find_hash=None
		if lookup_type=="put_file" or lookup_type=="get_file":
			find_hash=self.hasher(msg_content[1]) #filename
		else: #lookup for node
			find_hash=self.hasher(lookup_node_addr[0]+str(lookup_node_addr[1]))
		
		
		curr_node_hash=self.hasher(self.host+str(self.port))
		successor_node_hash=self.hasher(self.successor[0]+str(self.successor[1]))
		
		 
		if self.successor==self.predecessor and self.predecessor==(self.host,self.port): #means only one node in ring #edge case
			#if lookup_node_hash>curr_node_hash:

			self.successor=lookup_node_addr
			self.predecessor=lookup_node_addr
			lookup_reply_msg="lookup_reply_edge_case|"+dumps((self.host,self.port))
			soc=socket.socket()
			soc.connect(lookup_node_addr)
			soc.send(lookup_reply_msg.encode())

		else: #general case
			if find_hash==curr_node_hash: #should never happen and run ignore this if
				lookup_reply_msg="lookup_reply|"+"|".join(msg_content[0:2])+"|"+dumps((self.host,self.port))
				soc=socket.socket()
				soc.connect(lookup_node_addr)
				soc.send(lookup_reply_msg.encode())
			elif find_hash>curr_node_hash and find_hash<successor_node_hash:
				lookup_reply_msg="lookup_reply|"+"|".join(msg_content[0:2])+"|"+dumps(self.successor)
				soc=socket.socket()
				soc.connect(lookup_node_addr)
				soc.send(lookup_reply_msg.encode())
			elif successor_node_hash<curr_node_hash and (find_hash>curr_node_hash or find_hash<successor_node_hash): #edge case in lookup between max and zero point
				lookup_reply_msg="lookup_reply|"+"|".join(msg_content[0:2])+"|"+dumps(self.successor)
				soc=socket.socket()
				soc.connect(lookup_node_addr)
				soc.send(lookup_reply_msg.encode())
			else:
				#above conditions find succesor and send lookup reply
				#this one continue lookup req
				lookup_req_msg="lookup_req|"+"|".join(msg_content) # look_up_req= type|lookup_type|maybe filename or nodename|lookupaddr
				soc=socket.socket()
				soc.connect(self.successor)
				soc.send(lookup_req_msg.encode())
	

	def process_lookup_reply(self,msg_content): #can be for put fn , can be for get fn, can be for join fn
		lookup_reply_type=msg_content[0]
		successor_addr=tuple(loads(msg_content[-1]))
		if lookup_reply_type=="put_file":
			filename=msg_content[1]
			soc=socket.socket()
			soc.connect(successor_addr)
			fileSize = os.path.getsize(filename)

			sendfile_msg="send_file|"+"put|"+filename+"|"+str(fileSize)  #initial msg to setup receive file fn in receiver node
			soc.send(sendfile_msg.encode())
			
			self.sendFile(soc,filename)
		elif lookup_reply_type=="get_file":
			filename=msg_content[1]
			request_file_msg="request_file|"+filename+"|"+dumps((self.host,self.port))
			soc=socket.socket()
			soc.connect(successor_addr)
			soc.send(request_file_msg.encode())
		else: #if its to join node
			# thenode  who make lookup req to joining addr gets this reply
			self.successor=successor_addr
			new_predecessor_msg="new_predecessor|"+"join|"+dumps((self.host,self.port)) #helps tofixpredecessor of thesuccessor of new node
			soc=socket.socket()
			soc.connect(self.successor)
			soc.send(new_predecessor_msg.encode())
	
	def ping(self):
		
		while not self.stop:
			
			
			try:	
				if self.number_of_pings<2: #after three ping run else  cond
					self.number_of_pings+=1
					ping_msg="ping|"+dumps((self.host,self.port))+"|"+dumps(self.backUpFiles)
					soc=socket.socket()
					soc.connect(self.successor)
					soc.send(ping_msg.encode())
					
					time.sleep(0.3) 
					
				else:
					#succesor failed
					self.successor=self.successor_s_successor
					
					new_predecessor_msg="new_predecessor|"+"ping|"+dumps((self.host,self.port)) #helps tofixpredecessor of thesuccessr's successor, as succesor has failed
					
					soc=socket.socket()
					soc.connect(self.successor)
					soc.send(new_predecessor_msg.encode())
					
					sendfile_msg="send_file|"+"successor|"+dumps(self.backUpFiles) #backup file are successor files and as the successor have failed send them to successor's successor
					
					new_soc=socket.socket()
					new_soc.connect(self.successor)
					new_soc.send(sendfile_msg.encode())
					self.backUpFiles=[]
					self.successor_s_successor=tuple()
					self.number_of_pings=0
					
			except:
				self.number_of_pings+=1	
			
			
				

	def process_ping_reply(self,msg_content):
		self.number_of_pings-=1                    #used to confirm ping successful
		if self.successor_s_successor!=tuple(loads(msg_content[-1])):
			self.successor_s_successor=tuple(loads(msg_content[-1])) #update successor's succesor
		predecessor_changed=(tuple(loads(msg_content[0])) != (self.host,self.port)) #update predecessor , this happen when new node join so old predecessor's successor needs to be fixed
		if predecessor_changed:
			self.successor=tuple(loads(msg_content[0]) )
			#use same new predecessor msg(used to update succesor of new node) to update predecesssor of new node
			new_predecessor_msg="new_predecessor|"+"ping|"+dumps((self.host,self.port)) #helps tofixpredecessor of thesuccessor of new node
			soc=socket.socket()
			soc.connect(self.successor)
			soc.send(new_predecessor_msg.encode())
	
	def process_new_predecessor(self,msg_content):
		new_predecessor_addr=tuple(loads(msg_content[-1]))
		old_predecessor_addr=self.predecessor
		self.predecessor=new_predecessor_addr

		#if type ping that means we are just fixing predecessor of newly joined node
		#if type join then we are fixing predecessor of new node's successor and also
		#if type join then have to share files with new node and delete the files from here
		if msg_content[0]=="join":
			new_predecessor_hash=self.hasher(new_predecessor_addr[0]+str(new_predecessor_addr[1]))
			old_predecessor_hash=self.hasher(old_predecessor_addr[0]+str(old_predecessor_addr[1]))
			curr_node_hash=self.hasher(self.host+str(self.port))
			
			deleted_files_lst=[]
			for filename in self.files:
				file_hash=self.hasher(filename)
				
				if old_predecessor_hash>curr_node_hash: #edge case inlookup
					if new_predecessor_hash>old_predecessor_hash:
						if file_hash>old_predecessor_hash and file_hash<=new_predecessor_hash:
							
							path=self.host+"_"+str(self.port)+"\\"+filename
							fileSize = os.path.getsize(path)
							sendfile_msg="send_file|"+"put|"+filename+"|"+str(fileSize)
							soc=socket.socket()
							soc.connect(new_predecessor_addr)
							soc.send(sendfile_msg.encode())
							self.sendFile(soc,path)
							deleted_files_lst.append(filename)
					else:
						if file_hash>old_predecessor_hash or (file_hash>=0 and file_hash<=new_predecessor_hash):
							
							path=self.host+"_"+str(self.port)+"\\"+filename
							fileSize = os.path.getsize(path)
							sendfile_msg="send_file|"+"put|"+filename+"|"+str(fileSize)
							soc=socket.socket()
							soc.connect(new_predecessor_addr)
							soc.send(sendfile_msg.encode())
							self.sendFile(soc,path)
							deleted_files_lst.append(filename)
				else: #general case
					if file_hash<= new_predecessor_hash:
						path=self.host+"_"+str(self.port)+"\\"+filename
						fileSize = os.path.getsize(path)
						sendfile_msg="send_file|"+"put|"+filename+"|"+str(fileSize)
						soc=socket.socket()
						soc.connect(new_predecessor_addr)
						soc.send(sendfile_msg.encode())
					
						self.sendFile(soc,path)
						deleted_files_lst.append(filename)
				
				
			for filename in deleted_files_lst:
				self.files.remove(filename)
	
	def process_ping_msg(self,msg_content):
		soc=socket.socket()
			
		soc.connect(tuple(loads(msg_content[0])))

		predecessor_backup_files=loads(msg_content[1])
		predecessor_backup_files.sort()
		self.files.sort()
		
		#new_backup_files=[]
		if self.files != predecessor_backup_files:
			sendfile_msg="send_file|"+"backup|"+dumps(self.files)
			soc.send(sendfile_msg.encode())
		
		ping_reply_msg="ping_reply|"+dumps(self.predecessor)+"|"+dumps(self.successor)
		new_soc=socket.socket()
		try:
			new_soc.connect(tuple(loads(msg_content[0])))
				
			new_soc.send(ping_reply_msg.encode())
		except:
			pass
	
	def process_sendfile_msg(self,msg_content,client):
		send_file_type=msg_content[0]
		if send_file_type=="put":

			self.files.append(msg_content[1]) #append in files structure
			filesize=msg_content[2]
			path=self.host+"_"+str(self.port)+"\\" +msg_content[1]
			self.recieveFile(client,path,filesize)  
		if send_file_type=="backup":
			#update backup files if successor's files changed. This happens regularly along ping
			incoming_backup_files=loads(msg_content[1])
			self.backUpFiles=incoming_backup_files
			
		if send_file_type=="successor": #this is received by a node's successor_s_successor when successor fail. he receives the backup files
			
			self.files.extend(loads(msg_content[1]))
			
		if send_file_type=="get":
			filename=msg_content[1]
			
			if filename!=" ":
				
				filesize=msg_content[2]
				path=msg_content[1]#"."+"\\" +msg_content[1]
				self.recieveFile(client,path,filesize)
			self.filename.put(filename) #get fn at halt as it needs filename to return
	
	def process_requestfile_msg(self,msg_content):
		filename=msg_content[0]
		get_req_addr=tuple(loads(msg_content[1]))
		soc=socket.socket()
		soc.connect(get_req_addr)
		if filename in self.files:
			path=self.host+"_"+str(self.port)+"\\"+filename
			fileSize = os.path.getsize(path)
			
			
			sendfile_msg="send_file|"+"get|"+filename+"|"+str(fileSize)
			soc.send(sendfile_msg.encode())
			
			self.sendFile(soc,path)
		else:
			sendfile_msg="send_file|"+"get|"+" "+"|"+" " #when no file send it empty this will be detected
			soc.send(sendfile_msg.encode())
	
	def process_bye_msg(self,msg_content,client):
		if msg_content[0]=="successor":
			
			reply="get_lost" #get lost msg helps to come out of listner loop of leaving node, which is stuck at recv and see that self.stop is true
			client.send(reply.encode())
			new_predecessor=tuple(loads(msg_content[-1]))
			
			self.predecessor=new_predecessor
		if msg_content[0]=="predecessor":
			reply="get_lost"
			client.send(reply.encode())
			
			new_successor=tuple(loads(msg_content[-1]))
			
			self.successor=new_successor
		