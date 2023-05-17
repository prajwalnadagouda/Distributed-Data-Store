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

class RouteService(filesend_pb2_grpc.RouteServiceServicer):
    def finalquery(self, request, context):
        print("Got request4 " + str(request))
        daterange=str(request.payload.decode('utf-8'))
        print((daterange))
        daterange=daterange.split(",")
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
        print("--<",startdate,enddate,trafficode)
        start_date=datetime.datetime.strptime(startdate,'%Y-%m-%d').date()
        end_date=datetime.datetime.strptime(enddate,'%Y-%m-%d').date()
        delta = datetime.timedelta(days=1)
        csv_files=[]
        firstflag=0
        while (start_date <= end_date):
            csv_files.append(str(start_date))
            try:
                file = open("./content/data/"+str(start_date)+".csv", 'rb')
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



    def query(self, request, context):
        print("Got request3 " + str(request))
        daterange=str(request.payload.decode('utf-8'))
        print((daterange))
        daterange=daterange.split(",")
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
            
        
        print("--<",startdate,enddate,trafficode)
        start_date=datetime.datetime.strptime(startdate,'%Y-%m-%d').date()
        end_date=datetime.datetime.strptime(enddate,'%Y-%m-%d').date()
        delta = datetime.timedelta(days=1)
        csv_files=[]
        firstflag=0
        while (start_date <= end_date):
            csv_files.append(str(start_date))
            try:
                file = open("./content/data/"+str(start_date)+".csv", 'rb')
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
                filefound=False
                for targetserver in targetservers:
                    print(targetserver,str(IPAddress)+":"+str(PortNumber))
                    if(filefound):
                        break
                    if(targetserver==str(IPAddress)+":"+str(PortNumber)):
                        continue
                    try:
                        with grpc.insecure_channel(targetserver) as channel:
                            stub = filesend_pb2_grpc.RouteServiceStub(channel)
                            responses = stub.finalquery(filesend_pb2.Route(id=1, origin =1,payload=bytes((str(start_date).replace("-","/")+",,"), 'utf-8')))
                            for response in responses:
                                try:
                                    if(response.path=="none"):
                                        print("calling other friends for",start_date)
                                    else:
                                        yield filesend_pb2.Route(payload=response.payload)
                                except:
                                    yield filesend_pb2.Route(payload=response.payload)
                    except Exception as e:
                        print("wassi",e)
                print("->-",start_date,targetservers)
                print("file not available")
            start_date += delta
            continue

        return filesend_pb2.Route(path="none")

    def upload(self, request, context):
        print("Got request2 " + str(request))
        binary_file = open("./content/toload/something.csv", "wb")
        for i in request:
            binary_file.write(i.payload)
        x=threading.Thread(target=file_ETL,args=() )
        x.start()
        return filesend_pb2.Route(id=1)

    def finalfilestore(self, request, context):
        print("Got request1 " + str(request))
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
    print(tomovefiles)
    for eachfile in tomovefiles:
        year=eachfile.split("/")[-1]
        targetservers=ch.get_servers(year)
        print(year,targetservers)
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
        df = pd.read_csv(toloadfile)
        cols = df.columns
        for i in set(df['Issue Date']): # for classified by years files
            j=i
            j=j.split("/")
            j=j[2]+"-"+j[0]+"-"+j[1]
            # i=str(i).replace("/","-")
            filename = "./content/tomove/"+j+".csv"
            print(filename)
            df.loc[df['Issue Date'] == i].to_csv(filename,index=False,columns=cols)
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
        # Register somewhere that the session was lost
    elif state == KazooState.SUSPENDED:
        pass
        # Handle being disconnected from Zookeeper
    else:
        pass
        # Handle being connected/reconnected to Zookeeper
      
def zooconnect():
    global zk
    zk = KazooClient(hosts=ZooIPAddress+":"+ZooPortNumber)
    # zk = KazooClient(hosts='10.0.1.1:2191') #change it to the zookeeper address
    zk.start()
    while True:
        if(not zk.exists("/available/"+IPAddress+":"+PortNumber)):
            break
    zk.create("/available/"+IPAddress+":"+PortNumber,ephemeral=True)
    # zk.add_auth("digest","cmpe:275")
    zk.add_listener(my_listener)
    return zk

zk=zooconnect()




replication_factor = 2
ch = ConsistentHash(replication_factor)
# ch.add_server(IPAddress+":"+PortNumber)


serverlist=[]
children = zk.get_children('/available')
for child in children:
    ch.add_server(child)
    serverlist.append(child)

def movingafterchange():
    tomovefiles=(glob("./content/data/*.csv"))
    print(tomovefiles)
    for eachfile in tomovefiles:
        year=eachfile.split("/")[-1]
        targetservers=ch.get_servers(year)
        print(year,targetservers)
        flag=1
        for targetserver in targetservers:
            print("see this")
            if(targetserver==str(IPAddress)+":"+str(PortNumber)):
                flag=0
                continue
            try:
                with grpc.insecure_channel(str(targetserver)) as channel:
                    stub = filesend_pb2_grpc.RouteServiceStub(channel)
                    pay=file_split(eachfile)
                    response= stub.finalfilestore(pay)
            except Exception as e:
                print(e)
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
        print("deleted")
        print("serverlist",serverlist)
    else:
        for i in (list(set(availableservers) - set(serverlist))):
            ch.add_server(i)
            serverlist.append(i)
        print("added")
        print("serverlist",serverlist)
    print(ch.__dict__)
    # print("------->",children)
    movingafterchange()

server(zk)