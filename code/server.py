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


class RouteService(filesend_pb2_grpc.RouteServiceServicer):
    def request(self, request, context):
        print("Got request " + str(request))
        daterange=request.path
        daterange=daterange.split("-")
        if(len(daterange)==0):
            return filesend_pb2.Route(id=3)
        if(len(daterange)==1):
            startdate=daterange[0].replace("/","-")
            enddate=daterange[0].replace("/","-")
        else:
            startdate=daterange[0].replace("/","-")
            enddate=daterange[1].replace("/","-")
        
        print(startdate,enddate)
        start_date=datetime.datetime.strptime(startdate,'%Y-%m-%d').date()
        end_date=datetime.datetime.strptime(enddate,'%Y-%m-%d').date()
        delta = datetime.timedelta(days=1)
        csv_files=[]
        while (start_date <= end_date):
            csv_files.append(str(start_date))
            start_date += delta
        print(csv_files)

        firstflag=0
        for filetocombine in csv_files:
            file = open("../ETL/main/"+filetocombine+".csv", 'rb')
            if(firstflag==0):
                firstflag=1
            else:
                next(file)
            while True:
                chunk = file.read(4000000)
                if not chunk: 
                    break
                yield filesend_pb2.Route(payload=chunk)


        # CONTENT_FILE_NAME="../dataset/Parking_Violations_Issued_-_Fiscal_Year_2014.csv"
        # file = open(CONTENT_FILE_NAME, 'rb')
        # while True:
        #     chunk = file.read(4000000)
        #     if not chunk: 
        #         break 
        #     print(type(chunk))
            # yield filesend_pb2.Route(payload=chunk)
        return filesend_pb2.Route(id=3)

    def filewrite(self, request, context):
        print("Got request " + str(request))
        binary_file = open("toload/writetest.csv", "wb")
        for i in request:
            binary_file.write(i.payload)
        x=threading.Thread(target=file_split,args=() )
        x.start()
        return filesend_pb2.Route(id=1)
    

def file_split():
    toloadfiles=(glob("./toload/*.csv"))
    for toloadfile in toloadfiles:
        df = pd.read_csv(toloadfile)
        cols = df.columns
        for i in set(df['Issue Date']): # for classified by years files
            j=i
            j=j.split("/")
            j=j[2]+"-"+j[0]+"-"+j[1]
            # i=str(i).replace("/","-")
            filename = "./tomove/"+j+".csv"
            print(filename)
            df.loc[df['Issue Date'] == i].to_csv(filename,index=False,columns=cols)
	  
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