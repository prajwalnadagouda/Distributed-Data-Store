from concurrent import futures

import grpc
import filesend_pb2_grpc
import filesend_pb2

import pandas as pd
from pathlib import Path
import datetime

from kazoo.client import KazooClient
from kazoo.security import make_digest_acl_credential, make_acl

import configparser
import threading
import time
from glob import glob 
import shutil

import os
if os.environ.get('https_proxy'):
 del os.environ['https_proxy']
if os.environ.get('http_proxy'):
 del os.environ['http_proxy']

from myhash import ConsistentHash
from myETL import split_csv_file
from termcolor import colored


class RouteService(filesend_pb2_grpc.RouteServiceServicer):
    # for intra team communication
    def finalquery(self, request, context):
        print(colored("Serving data to team cluster", 'magenta'))
        print(colored(str(request),'magenta'))
        daterange=str(request.payload.decode('utf-8'))
        daterange=daterange.split(":")
        if(len(daterange)==0):
            return filesend_pb2.Route(id=3)
        if(daterange[1]):
            startdate=daterange[0].replace("/","-")
            enddate=daterange[1].replace("/","-")
            trafficode=daterange[2]
        else:
            startdate=daterange[0].replace("/","-")
            enddate=daterange[0].replace("/","-")
            trafficode=daterange[2]
        start_date=datetime.datetime.strptime(startdate,'%Y-%m-%d').date()
        end_date=datetime.datetime.strptime(enddate,'%Y-%m-%d').date()
        delta = datetime.timedelta(days=1)
        csv_files=[]
        firstflag=0
        while (start_date <= end_date):
            filefound= False
            csv_files.append(str(start_date))
            try:
                if(trafficode==""):
                    tosendfiles=(glob("./content/data/"+str(start_date)+"*.csv"))
                    for tosend in tosendfiles:
                        file = open(tosend, 'rb')
                        filefound= True
                        if(firstflag==0):
                            firstflag=1
                        else:
                            next(file)
                        while True:
                            chunk = file.read(4000000)
                            if not chunk: 
                                break
                            yield filesend_pb2.Route(payload=chunk)
                else:
                    file = open("./content/data/"+str(start_date)+"-"+trafficode+".csv", 'rb')
                    filefound= True
                    if(firstflag==0):
                        firstflag=1
                    else:
                        next(file)
                    while True:
                        chunk = file.read(4000000)
                        if not chunk: 
                            break
                        yield filesend_pb2.Route(payload=chunk)
            except:
                yield filesend_pb2.Route(path="none")
            start_date += delta

    # talking to client and other teams
    def query(self, request, context):
        print(colored("Serving data to team cluster", 'yellow'))
        print(colored(str(request),'yellow'))
        daterange=str(request.payload.decode('utf-8'))
        daterange=daterange.split(":")
        if(len(daterange)==0):
            return filesend_pb2.Route(id=3,path="none")
        if(daterange[1]):
            startdate=daterange[0].replace("/","-")
            enddate=daterange[1].replace("/","-")
            trafficode=daterange[2]
        else:
            startdate=daterange[0].replace("/","-")
            enddate=daterange[0].replace("/","-")
            trafficode=daterange[2]
        start_date=datetime.datetime.strptime(startdate,'%Y-%m-%d').date()
        end_date=datetime.datetime.strptime(enddate,'%Y-%m-%d').date()
        delta = datetime.timedelta(days=1)
        csv_files=[]
        firstflag=0
        while (start_date <= end_date):
            filefound= False
            csv_files.append(str(start_date))
            try:
                if(trafficode==""):
                    tosendfiles=(glob("./content/data/"+str(start_date)+"*.csv"))
                    if(not tosendfiles):
                        raise Exception("Date not in this server")
                    for tosend in tosendfiles:
                        file = open(tosend, 'rb')
                        filefound= True
                        if(firstflag==0):
                            firstflag=1
                        else:
                            next(file)
                        while True:
                            chunk = file.read(4000000)
                            if not chunk: 
                                break
                            yield filesend_pb2.Route(payload=chunk)
                else:
                    file = open("./content/data/"+str(start_date)+"-"+trafficode+".csv", 'rb')
                    filefound= True
                    if(firstflag==0):
                        firstflag=1
                    else:
                        next(file)
                    while True:
                        chunk = file.read(4000000)
                        if not chunk: 
                            break
                        yield filesend_pb2.Route(payload=chunk)
            except:
                targetservers=ch.get_servers(str(start_date)+".csv")
                for targetserver in targetservers:
                    if(filefound):
                        break
                    if(targetserver==str(IPAddress)+":"+str(PortNumber)):
                        continue
                    try:
                        with grpc.insecure_channel(targetserver) as channel:
                            stub = filesend_pb2_grpc.RouteServiceStub(channel)
                            responses = stub.finalquery(filesend_pb2.Route(id=1, origin =1,payload=bytes((str(start_date).replace("-","/")+"::"), 'utf-8')))
                            for response in responses:
                                try:
                                    if(response.path=="none"):
                                        pass
                                        # print("calling other friends for",start_date)
                                    else:
                                        filefound=True
                                        yield filesend_pb2.Route(payload=response.payload)
                                except:
                                    filefound=True
                                    yield filesend_pb2.Route(payload=response.payload)
                    except Exception as e:
                        pass
            if(not filefound):
                print(colored("Not available in our cluster. Ask other teams", 'red'))
                
            start_date += delta
            continue

        return filesend_pb2.Route(path="none")

    #for clients to upload
    def upload(self, request, context):
        print(colored("Got request from the clients to store", 'yellow'))

        now = datetime.datetime.now() 
        date_time = now.strftime("%m-%d-%Y%H:%M:%S")
        binary_file = open("./content/toload/"+date_time+".csv", "wb")
        for i in request:
            binary_file.write(i.payload)
        x=threading.Thread(target=file_ETL,args=() )
        x.start()
        return filesend_pb2.Route(id=1)

    #for intra team store
    def finalfilestore(self, request, context):
        print(colored("Got request from team to store", 'magenta'))
        for eachrequest in request:
            binary_file = open("./content/data/"+eachrequest.path, "wb")
            binary_file.write(eachrequest.payload)
        return filesend_pb2.Route(id=1)



#helper for file_spread
def file_split(CONTENT_FILE_NAME):
    file = open(CONTENT_FILE_NAME, 'rb')
    while True:
        chunk = file.read(4000000)
        if not chunk: 
            break
        res= filesend_pb2.Route(path=CONTENT_FILE_NAME.split("/")[-1], payload=chunk)
        yield res   


#Called after upload->ETL - 
def file_spread():
    tomovefiles=(glob("./content/tomove/*.csv"))
    for eachfile in tomovefiles:
        year=eachfile.split("/")[-1]
        year=year.split("-")
        year=year[0]+"-"+year[1]+"-"+year[2]+".csv"
        targetservers=ch.get_servers(year)
        for targetserver in targetservers:
            with grpc.insecure_channel(str(targetserver)) as channel:
                    stub = filesend_pb2_grpc.RouteServiceStub(channel)
                    pay=file_split(eachfile)
                    response= stub.finalfilestore(pay)

    for file in tomovefiles:
        src_path = file
        dst_path = "./content/moved/"+file.split("/")[-1]
        shutil.move(src_path, dst_path)
        

# Called right after upload action - Splits the files in format
def file_ETL():
    toloadfiles=(glob("./content/toload/*.csv"))
    for toloadfile in toloadfiles:
        split_csv_file(toloadfile,"./content/tomove/")
    file_spread()
    for file in toloadfiles:
        src_path = file
        dst_path = "./content/loaded/"+file.split("/")[-1]
        shutil.move(src_path, dst_path)
	  
def server(zk):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    filesend_pb2_grpc.add_RouteServiceServicer_to_server(RouteService(), server)
    server.add_insecure_port('[::]:50051')
    print("gRPC starting")
    server.start()
    server.wait_for_termination()


time.sleep(1)

# reading the config file
config = configparser.ConfigParser()
config.read('config.ini')
ZooIPAddress=config['ZOOKEEPER']['IPAddress']
ZooPortNumber=config['ZOOKEEPER']['PortNumber']
IPAddress=config['SYSTEM']['IPAddress']
PortNumber=config['SYSTEM']['PortNumber']
# print(ZooIPAddress+":"+ZooPortNumber)


from kazoo.client import KazooState
def my_listener(state):
    if state == KazooState.LOST:
        time.sleep(4)
        zooconnect()
    elif state == KazooState.SUSPENDED:
        pass
    else:
        pass
      
def zooconnect():
    global zk
    zk = KazooClient(hosts=ZooIPAddress+":"+ZooPortNumber)
    zk.start()
    while True:
        if(not zk.exists("/available/"+IPAddress+":"+PortNumber)):
            break
    zk.create("/available/"+IPAddress+":"+PortNumber,ephemeral=True)
    # zk.add_auth("digest","cmpe:275")
    zk.add_listener(my_listener)
    return zk

zk=zooconnect()


#How many replications needed
replication_factor = 2
ch = ConsistentHash(replication_factor)

serverlist=[]
children = zk.get_children('/available')
for child in children:
    ch.add_server(child)
    serverlist.append(child)

def movingafterchange():
    tomovefiles=(glob("./content/data/*.csv"))
    for eachfile in tomovefiles:
        year=eachfile.split("/")[-1]
        year=year.split("-")
        year=year[0]+"-"+year[1]+"-"+year[2]+".csv"
        targetservers=ch.get_servers(year)
        flag=1
        for targetserver in targetservers:
            if(targetserver==str(IPAddress)+":"+str(PortNumber)):
                flag=0
                continue
            try:
                with grpc.insecure_channel(str(targetserver)) as channel:
                    stub = filesend_pb2_grpc.RouteServiceStub(channel)
                    pay=file_split(eachfile)
                    response= stub.finalfilestore(pay)
            except Exception as e:
                pass
        if(flag):
            src_path = eachfile
            dst_path = "./content/rehashed/"+eachfile.split("/")[-1]
            shutil.move(src_path, dst_path)


@zk.ChildrenWatch("/available")
def watch_children(children):
    availableservers = zk.get_children('/available')
    availableserverslen = len(availableservers)
    if((len((ch.__dict__)['ring'])/replication_factor) >availableserverslen):
        
        for i in (list(set(serverlist) - set(availableservers))):
            ch.remove_server(i)
            serverlist.remove(i)
        print(colored("Server got removed", 'red'))

    else:
        for i in (list(set(availableservers) - set(serverlist))):
            ch.add_server(i)
            serverlist.append(i)
        print(colored("Server got added", 'green'))
    print(colored(str(ch.__dict__),"green"))
    movingafterchange()

server(zk)