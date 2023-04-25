import grpc

import filesend_pb2_grpc
import filesend_pb2

def filesplit(CONTENT_FILE_NAME):
    file = open(CONTENT_FILE_NAME, 'rb')
    while True:
        chunk = file.read(4000000)
        if not chunk: 
            break
        res= filesend_pb2.Route(payload=chunk)
        yield res
def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = filesend_pb2_grpc.RouteServiceStub(channel)
        CONTENT_FILE_NAME="../dataset/Parking_Violations_Issued_-_Fiscal_Year_2014.csv"
        pay=filesplit(CONTENT_FILE_NAME)
        response= stub.filewrite(pay)
        print(response)
run()