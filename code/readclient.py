import grpc

import filesend_pb2_grpc
import filesend_pb2

def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = filesend_pb2_grpc.RouteServiceStub(channel)
        responses = stub.request(filesend_pb2.Route(id=1, origin =1,path="2013/03/04-2013/03/06"))
        
        
        binary_file = open("readtest.csv", "wb")
        for response in responses:
            # print("Greeter client received following from server: ",(response.payload))
            binary_file.write(response.payload)
            # Close file
        binary_file.close()
run()