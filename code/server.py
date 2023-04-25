from concurrent import futures

import grpc
import filesend_pb2_grpc
import filesend_pb2

import pandas as pd
from pathlib import Path
import datetime


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
        # print(len(request.payload))
        binary_file = open("writetest.csv", "wb")
        for i in request:
            # print((i.payload))
            binary_file.write(i.payload)
        return filesend_pb2.Route(id=1)
	  
def server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    filesend_pb2_grpc.add_RouteServiceServicer_to_server(RouteService(), server)
    server.add_insecure_port('[::]:50051')
    print("gRPC starting")
    server.start()
    server.wait_for_termination()

server()