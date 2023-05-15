import grpc

import filesend_pb2_grpc
import filesend_pb2

def run():
    with grpc.insecure_channel('10.0.12.3:50052') as channel:
        stub = filesend_pb2_grpc.RouteServiceStub(channel)
        responses = stub.query(filesend_pb2.Route(id=1, origin =1,payload=b"2013/03/03,2013/03/06,"))
        
        binary_file = open("./content/dump/readtest.csv", "wb")
        print("hi")
        for response in responses:
            # print("Greeter client received following from server: ",(response.payload))
            binary_file.write(response.payload)
            print(response.payload)
            # Close file
        binary_file.close()
run()