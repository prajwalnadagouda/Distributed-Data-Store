import grpc

import filesend_pb2_grpc
import filesend_pb2

def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = filesend_pb2_grpc.RouteServiceStub(channel)
        responses = stub.request(filesend_pb2.Route(id=1, origin =1))
        for response in responses:
            print("Greeter client received following from server: ",len(response.payload))
run()