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

class RouteService(filesend_pb2_grpc.RouteServiceServicer):
    def query(self, request, context):
        print("Got request " + str(request))
        daterange=request.path
        daterange=daterange.split(",")
        if(len(daterange)==0):
            return filesend_pb2.Route(id=3)
        if(daterange[0]==daterange[1]):
            startdate=daterange[0].replace("/","-")
            enddate=daterange[0].replace("/","-")
            trafficode=daterange[2]
        else:
            startdate=daterange[0].replace("/","-")
            enddate=daterange[1].replace("/","-")
            trafficode=daterange[2]
        
        print(startdate,enddate,trafficode)
        start_date=datetime.datetime.strptime(startdate,'%Y-%m-%d').date()
        end_date=datetime.datetime.strptime(enddate,'%Y-%m-%d').date()
        delta = datetime.timedelta(days=1)
        csv_files=[]
        firstflag=0
        while (start_date < end_date):
            csv_files.append(str(start_date))
            try:
                file = open("./content/data/"+str(start_date)+".csv", 'rb')
            except:
                print("file nahi hain bhai")
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
        print("Got request " + str(request))
        binary_file = open("./content/toload/something.csv", "wb")
        for i in request:
            binary_file.write(i.payload)
        x=threading.Thread(target=file_ETL,args=() )
        x.start()
        return filesend_pb2.Route(id=1)

    def finalfilestore(self, request, context):
        print("Got request " + str(request))
        for eachrequest in request:
            binary_file = open("./content/data/"+eachrequest.path, "wb")
            binary_file.write(eachrequest.payload)
        return filesend_pb2.Route(id=1)
    
def file_split(CONTENT_FILE_NAME):
    file = open(CONTENT_FILE_NAME, 'rb')
    while True:
        chunk = file.read(4000000)
        if not chunk: 
            break
        res= filesend_pb2.Route(path=CONTENT_FILE_NAME.split("/")[-1], payload=chunk)
        yield res   

def file_move(connection,filelist):
    with grpc.insecure_channel(str(connection)) as channel:
        for CONTENT_FILE_NAME in filelist:
            print("----",CONTENT_FILE_NAME)
            stub = filesend_pb2_grpc.RouteServiceStub(channel)
            pay=file_split(CONTENT_FILE_NAME)
            print("yeh pay hai",pay)
            response= stub.finalfilestore(pay)
            print(response)
        for file in filelist:
            src_path = file
            dst_path = "./content/moved/"+file.split("/")[-1]
            shutil.move(src_path, dst_path)

def file_spread():
    tomovefiles=(glob("./content/tomove/*.csv"))
    children = zk.get_children("/available")
    print(children)
    i=0
    servercount=len(children)
    if(len(children)!=0):
        for child in children:
            print("---->",servercount,tomovefiles ,tomovefiles[i::servercount])
            file_move(child,tomovefiles[i::servercount])
            i=i+1
        

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
server(zk)