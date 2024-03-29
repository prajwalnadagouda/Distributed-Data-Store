# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import filesend_pb2 as filesend__pb2


class RouteServiceStub(object):
    """a service interface (contract)

    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.query = channel.unary_stream(
                '/route.RouteService/query',
                request_serializer=filesend__pb2.Route.SerializeToString,
                response_deserializer=filesend__pb2.Route.FromString,
                )
        self.upload = channel.stream_unary(
                '/route.RouteService/upload',
                request_serializer=filesend__pb2.Route.SerializeToString,
                response_deserializer=filesend__pb2.Route.FromString,
                )
        self.finalfilestore = channel.stream_unary(
                '/route.RouteService/finalfilestore',
                request_serializer=filesend__pb2.Route.SerializeToString,
                response_deserializer=filesend__pb2.Route.FromString,
                )
        self.finalquery = channel.unary_stream(
                '/route.RouteService/finalquery',
                request_serializer=filesend__pb2.Route.SerializeToString,
                response_deserializer=filesend__pb2.Route.FromString,
                )


class RouteServiceServicer(object):
    """a service interface (contract)

    """

    def query(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def upload(self, request_iterator, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def finalfilestore(self, request_iterator, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def finalquery(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_RouteServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'query': grpc.unary_stream_rpc_method_handler(
                    servicer.query,
                    request_deserializer=filesend__pb2.Route.FromString,
                    response_serializer=filesend__pb2.Route.SerializeToString,
            ),
            'upload': grpc.stream_unary_rpc_method_handler(
                    servicer.upload,
                    request_deserializer=filesend__pb2.Route.FromString,
                    response_serializer=filesend__pb2.Route.SerializeToString,
            ),
            'finalfilestore': grpc.stream_unary_rpc_method_handler(
                    servicer.finalfilestore,
                    request_deserializer=filesend__pb2.Route.FromString,
                    response_serializer=filesend__pb2.Route.SerializeToString,
            ),
            'finalquery': grpc.unary_stream_rpc_method_handler(
                    servicer.finalquery,
                    request_deserializer=filesend__pb2.Route.FromString,
                    response_serializer=filesend__pb2.Route.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'route.RouteService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class RouteService(object):
    """a service interface (contract)

    """

    @staticmethod
    def query(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(request, target, '/route.RouteService/query',
            filesend__pb2.Route.SerializeToString,
            filesend__pb2.Route.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def upload(request_iterator,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.stream_unary(request_iterator, target, '/route.RouteService/upload',
            filesend__pb2.Route.SerializeToString,
            filesend__pb2.Route.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def finalfilestore(request_iterator,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.stream_unary(request_iterator, target, '/route.RouteService/finalfilestore',
            filesend__pb2.Route.SerializeToString,
            filesend__pb2.Route.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def finalquery(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(request, target, '/route.RouteService/finalquery',
            filesend__pb2.Route.SerializeToString,
            filesend__pb2.Route.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
