from concurrent import futures

import grpc
import filesend_pb2_grpc
import filesend_pb2

class RouteService(filesend_pb2_grpc.RouteServiceServicer):
    def request(self, request, context):
        print("Got request " + str(request))
        print(len(request.payload))
        CONTENT_FILE_NAME="../../dataset/Parking_Violations_Issued_-_Fiscal_Year_2014.csv"
        file = open(CONTENT_FILE_NAME, 'rb')
        while True:
            chunk = file.read(4000000)
            if not chunk: 
                break 
            # print(chunk)
            yield filesend_pb2.Route(payload=chunk)
        # return filesend_pb2.Route(id=3)

    def filewrite(self, request, context):
        print("Got request " + str(request))
        # print(len(request.payload))
        for i in request:
            print(len(i.payload))
        return filesend_pb2.Route(id=1)
	  
def server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    filesend_pb2_grpc.add_RouteServiceServicer_to_server(RouteService(), server)
    server.add_insecure_port('[::]:50051')
    print("gRPC starting")
    server.start()
    server.wait_for_termination()

server()