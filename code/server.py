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
        print("-->>>",start_date,end_date)
        while (start_date <= end_date):
            csv_files.append(str(start_date))
            try:
                file = open("./content/data/"+str(start_date)+".csv", 'rb')
            except:
                targetservers=ch.get_servers(str(start_date)+".csv")
                for targetserver in targetservers:
                    print(targetserver,str(IPAddress)+":"+str(PortNumber))
                    if(targetserver==str(IPAddress)+":"+str(PortNumber)):
                        continue
                    try:
                        with grpc.insecure_channel(targetserver) as channel:
                            stub = filesend_pb2_grpc.RouteServiceStub(channel)
                            responses = stub.query(filesend_pb2.Route(id=1, origin =1,payload=bytes((str(start_date).replace("-","/")+",,"), 'utf-8')))
                            for response in responses:
                                print("hiiiiii")
                                yield filesend_pb2.Route(payload=response.payload)
                    except Exception as e:

                        print(e)
                print("->-",start_date,targetservers)
                print("file not available")
                start_date += delta
                continue
            if(firstflag==0):
                firstflag=1
            else:
                next(file)
            while True:
                chunk = file.read(4000000)
                if not chunk: 
                    break
                yield filesend_pb2.Route(payload=chunk)
            start_date += delta
        return filesend_pb2.Route(id=3)

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
print(ZooIPAddress+":"+ZooPortNumber)
zk = KazooClient(hosts=ZooIPAddress+":"+ZooPortNumber)
# zk = KazooClient(hosts='10.0.1.1:2191') #change it to the zookeeper address
zk.start()
zk.create("/available/"+IPAddress+":"+PortNumber,ephemeral=True)
# zk.add_auth("digest","cmpe:275")




replication_factor = 2
ch = ConsistentHash(replication_factor)
# ch.add_server(IPAddress+":"+PortNumber)


children = zk.get_children('/available')
for child in children:
    ch.add_server(child)

@zk.ChildrenWatch("/available")
def watch_children(children):
    availableservers = zk.get_children('/available')
    availableserverslen = len(availableservers)
    if((len((ch.__dict__)['ring'])/replication_factor) >availableserverslen):
        print("deleted")
    else:
        print("added")  
    print(ch.__dict__)
    print("------->",children)
server(zk)