import threading
from concurrent import futures

import sys
import grpc
from queue import Queue

import mqtt_pubsub
import interface_pb2  # Import the generated protobuf Python code
import interface_pb2_grpc  # Import the generated gRPC stubs
from manipulateDictionary import ManipulateDictionary


class KeyValueStoreServicer(interface_pb2_grpc.KeyValueStoreServicer):
    dictionary = None

    def __init__(self):
        self.dictionary = ManipulateDictionary()

    def Get(self, request, context):
        key = request.key
        version = request.ver

        if key == '':
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Unable to pass an empty key')
            raise grpc.RpcError

        key_returned, value_returned, version_returned = (
            self.dictionary.getByKeyVersion(key=key, version=version)
        )

        try:
            response = interface_pb2.KeyValueVersionReply(
                key=key_returned,
                val=value_returned,
                ver=version_returned
            )

            return response
        except grpc.RpcError as e:
            context.set_code(e.code())
            context.set_details(str(e.details()))
            raise grpc.RpcError

    def GetRange(self, request, context):
        from_key = request.fr.key
        from_version = request.fr.ver

        to_key = request.to.key
        to_version = request.to.ver

        if from_key == '' or to_key == '':
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Unable to pass an empty key')
            raise grpc.RpcError

        dict_range = self.dictionary.getRangeByKeyVersion(
            from_key, to_key,
            from_version, to_version
        )

        try:
            for key, values in dict_range.items():
                for version, value in values:
                    response = interface_pb2.KeyValueVersionReply(
                        key=key,
                        val=value,
                        ver=version
                    )

                    yield response
        except grpc.RpcError as e:
            context.set_code(e.code())
            context.set_details(str(e.details()))
            raise grpc.RpcError

    def GetAll(self, request_iterator, context):
        keys, versions, response = [], [], []

        for request in request_iterator:
            key = request.key
            version = request.ver

            if key == '':
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details('Unable to pass an empty key')
                raise grpc.RpcError

            if version == '':
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details('Unable to pass an empty version')
                raise grpc.RpcError

            keys.append(key)
            versions.append(version)

        try:
            for key, version in zip(keys, versions):
                version_returned, value_returned = self.dictionary.getAllInRange(key, version)

                response = interface_pb2.KeyValueVersionReply(
                    key=key,
                    val=value_returned,
                    ver=version_returned
                )

                yield response
        except grpc.RpcError as e:
            context.set_code(e.code())
            context.set_details(str(e.details()))
            raise grpc.RpcError

    def Put(self, request, context):
        key = request.key
        value = request.val

        if key == '':
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Unable to pass an empty key')
            raise grpc.RpcError

        if value == '':
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Unable to pass an empty value')
            raise grpc.RpcError

        key_returned, old_value_returned, old_version_returned, new_version_returned = \
            self.dictionary.insertAndUpdate(key, value)

        try:
            mqtt_pubsub.pub_insert(key, value, new_version_returned)
        except Exception as e:
            context.set_code('Internal error')
            context.set_details('Problem with cross-server operation')
            raise grpc.RpcError

        try:
            if old_version_returned <= 0:
                response = interface_pb2.PutReply(
                    key=key_returned,
                    ver=new_version_returned
                )
            else:
                response = interface_pb2.PutReply(
                    key=key_returned,
                    old_val=old_value_returned,
                    old_ver=old_version_returned,
                    ver=new_version_returned
                )

            return response
        except grpc.RpcError as e:
            context.set_code(e.code())
            context.set_details(str(e.details()))
            raise grpc.RpcError

    def PutAll(self, request_iterator, context):
        keys, values, response = [], [], []

        for request in request_iterator:
            key = request.key
            value = request.val

            if key == '':
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details('Unable to pass an empty key')
                raise grpc.RpcError

            if value == '':
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details('Unable to pass an empty value')
                raise grpc.RpcError

            keys.append(key)
            values.append(value)

        try:
            for key, value in zip(keys, values):
                key_returned, old_value_returned, old_version_returned, new_version_returned = \
                    self.dictionary.insertAndUpdate(key, value)

                try:
                    mqtt_pubsub.pub_insert(key, value, new_version_returned)
                except Exception as e:
                    context.set_code('Internal error')
                    context.set_details('Problem with cross-server operation')
                    raise grpc.RpcError

                if old_value_returned == '':
                    response.append(interface_pb2.PutReply(
                        key=key_returned,
                        ver=new_version_returned
                    ))
                else:
                    response.append(interface_pb2.PutReply(
                        key=key_returned,
                        old_val=old_value_returned,
                        old_ver=old_version_returned,
                        ver=new_version_returned
                    ))

            return iter(response)
        except grpc.RpcError as e:
            context.set_code(e.code())
            context.set_details(str(e.details()))
            raise grpc.RpcError

    def Del(self, request, context):
        key = request.key

        if key == '':
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Unable to pass an empty key')
            raise grpc.RpcError

        key_returned, last_value, last_version = self.dictionary.delete(key)

        if key_returned != '':
            try:
                mqtt_pubsub.pub_delete(key)
            except Exception as e:
                context.set_code('Internal error')
                context.set_details('Problem with cross-server operation')
                raise grpc.RpcError

        try:
            response = interface_pb2.KeyValueVersionReply(
                key=key,
                val=last_value,
                ver=last_version
            )

            return response
        except grpc.RpcError as e:
            context.set_code(e.code())
            context.set_details(str(e.details()))
            raise grpc.RpcError

    def DelRange(self, request, context):
        from_key = request.fr.key
        to_key = request.to.key

        if from_key == '' or to_key == '':
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Unable to pass an empty key')
            raise grpc.RpcError

        dict_range = self.dictionary.delRange(from_key, to_key)

        try:
            for key, values in dict_range.items():
                try:
                    mqtt_pubsub.pub_delete(key)
                except Exception as e:
                    context.set_code('Internal error')
                    context.set_details('Problem with cross-server operation')
                    raise grpc.RpcError

                for version, value in values:
                    response = interface_pb2.KeyValueVersionReply(
                        key=key,
                        val=value,
                        ver=version
                    )

                    yield response
        except grpc.RpcError as e:
            context.set_code(e.code())
            context.set_details(str(e.details()))
            raise grpc.RpcError

    def DelAll(self, request_iterator, context):
        keys = []

        for request in request_iterator:
            key = request.key

            if key == '':
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details('Unable to pass an empty key')
                raise grpc.RpcError

            keys.append(key)

        try:
            for key in keys:
                key_returned, value_returned, version_returned = self.dictionary.delAll(key)

                if key_returned != '':
                    try:
                        mqtt_pubsub.pub_delete(key)
                    except Exception as e:
                        context.set_code('Internal error')
                        context.set_details('Problem with cross-server operation')
                        raise grpc.RpcError

                response = interface_pb2.KeyValueVersionReply(
                    key=key_returned,
                    val=value_returned,
                    ver=version_returned
                )

                yield response
        except grpc.RpcError as e:
            context.set_code(e.code())
            context.set_details(str(e.details()))
            raise grpc.RpcError

    def Trim(self, request, context):
        key = request.key

        if key == '':
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Unable to pass an empty key')
            raise grpc.RpcError

        key_returned, last_value, last_version = self.dictionary.trim(key)

        if key_returned != '':
            try:
                mqtt_pubsub.pub_trim(key)
            except Exception as e:
                context.set_code('Internal error')
                context.set_details('Problem with cross-server operation')
                raise grpc.RpcError

        try:
            response = interface_pb2.KeyValueVersionReply(
                key=key_returned,
                val=last_value,
                ver=last_version
            )

            return response
        except grpc.RpcError as e:
            context.set_code(e.code())
            context.set_details(str(e.details()))
            raise grpc.RpcError


def sync_queue(class_work: KeyValueStoreServicer) -> None:
    while True:
        msg_q = mqtt_pubsub.get_queue()

        while not msg_q.empty():
            msg = str(msg_q.get())

            msg = msg.split(',')

            topic = msg[0]

            if topic == 'projeto-sd/insert':
                class_work.dictionary.insertAndUpdateMQTT(msg[1], msg[3], int(msg[2]))
            elif topic == 'projeto-sd/delete':
                class_work.dictionary.delete(msg[1])
            elif topic == 'projeto-sd/trim':
                class_work.dictionary.trim(msg[1])


def serve(port: int) -> None:
    class_work = KeyValueStoreServicer()

    try:
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        interface_pb2_grpc.add_KeyValueStoreServicer_to_server(class_work, server)
        server.add_insecure_port(f'localhost:{port}')  # Set the server port
    except grpc.RpcError as e:
        print(f'Error during gRPC startup: {e}')

    server.start()

    print('Server listening on port ' + str(port) + '...')

    mqttc = mqtt_pubsub.mqttClient()

    thread_mqtt = threading.Thread(target=sync_queue, args=(class_work,))
    thread_mqtt.start()

    rc = mqttc.run()

    print("rc: " + str(rc))

    thread_mqtt.join()
    server.wait_for_termination()


if __name__ == '__main__':
    try:
        port = int(sys.argv[1])
    except Exception as e:
        port = 50051

    serve(port)
