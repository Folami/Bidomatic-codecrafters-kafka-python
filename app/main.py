import asyncio
import socket  # noqa: F401
import struct
import sys
from .metadata import Metadata


TAG_BUFFER = int(0).to_bytes(1, byteorder="big")
DEFAULT_THROTTLE_TIME = int(0).to_bytes(4, byteorder="big")
ERRORS = {
    "ok": int(0).to_bytes(2, byteorder="big"),
    "error": int(35).to_bytes(2, byteorder="big"),
}

class BaseKafka(object):
    @staticmethod
    def _create_message(message: bytes):
        message_size = len(message)
        message_bytes = message_size.to_bytes(4, byteorder="big")
        return message_bytes + message

    @staticmethod
    def _remove_tag_buffer(buffer: bytes):
        return buffer[1:]

    @staticmethod
    def _parse_string(buffer: bytes):
        length = int.from_bytes(buffer[:2], byteorder="big")
        string = buffer[2 : 2 + length].decode("utf-8")
        string_parse_result = (string, buffer[2 + length :])
        return string_parse_result

    @staticmethod
    def _parse_array(buffer: bytes, func):
        arr_length = int.from_bytes(buffer[:1], byteorder="big") - 1
        arr_buffer = buffer[1:]
        for _ in range(arr_length):
            item_length = int.from_bytes(arr_buffer[:1], byteorder="big")
            item_buffer = arr_buffer[1:item_length]
            func(item_buffer)
            arr_buffer = arr_buffer[item_length + 1 :]
        return arr_buffer

class KafkaHeader(BaseKafka):
    def __init__(self, data: bytes):
        self.length = data[0:4]
        self.key = data[4:6]
        self.key_int = int.from_bytes(self.key, byteorder="big")
        self.version = data[6:8]
        self.version_int = int.from_bytes(self.version, byteorder="big")
        self.id = data[8:12]
        self.client, buffer = self._parse_string(data[12:])
        buffer = self._remove_tag_buffer(buffer)
        self.body = buffer

class ApiRequest(BaseKafka):
    # The class "constructor" - It's actually an initializer
    def __init__(self, version_int: int, id: bytes):
        self.version_int = version_int
        self.id = id
        self.message = self._create_message(self.construct_message())

    def add_api_version(self, string, api_version, mini, maximum):
        string += api_version
        string += int(mini).to_bytes(2)
        string += int(maximum).to_bytes(2)
        return string

    def construct_message(self):
        body = self.error_handler()
        apis = b""
        apis += struct.pack(">b", 3)  # Compact array length (3 entries)
        apis += struct.pack(">hhhh", 18, 0, 4, 0)    # ApiVersions
        apis += struct.pack(">hhhh", 1, 0, 16, 0)   # Fetch
        apis += struct.pack(">hhhh", 75, 0, 0, 0)   # DescribeTopicPartitions
        body += apis                            # Append APIs array
        body += struct.pack(">i", 0)             # throttle_time_ms
        body += struct.pack(">b", 0)             # tag_buffer
        return body
    
    def error_handler(self):
        if 0 <= self.version_int <= 4:
            return ERRORS["ok"]
        else:
            return ERRORS["error"]

class TopicRequest(BaseKafka):
    # The class "constructor" - It's actually an initializer
    def __init__(self, correlation_id, body, metadata):
        self.id = correlation_id
        self.body = body
        self.topics = []
        buffer = self._parse_array(body, self.parse_topics)
        self.limit = buffer[0:4]
        self.cursor = buffer[4:5]
        self.available_topics = metadata.topics
        self.partitions = metadata.partitions
        self.message = self._create_message(self.construct_message())

    def parse_topics(self, item_buffer):
        decoded_topic = item_buffer.decode("utf-8")
        self.topics.append(decoded_topic)

    def add_api_version(self, string, api_version, mini, maximum):
        string += api_version
        string += int(mini).to_bytes(2)
        string += int(maximum).to_bytes(2)
        return string

    def create_topic_item(self, topic):
        available = topic in self.available_topics
        topic_buffer = b""
        # two byte error code
        if available:
            topic_buffer += struct.pack(">h", 0)
        else:
            topic_buffer += struct.pack(">h", 3)
        # string length
        topic_buffer += struct.pack(">b", len(topic) + 1)
        # encode string
        topic_buffer += struct.pack(f">{len(topic)}s", topic)
        # topic id
        uuid_str = self.available_topics[topic]["uuid"]
        # Convert to a UUID object and then to bytes
        uuid_bytes = uuid_str.bytes
        # Pack the 16-byte binary UUID
        topic_buffer += struct.pack("16s", uuid_bytes)
        # is internal false
        topic_buffer += struct.pack(">b", 0)
        # empty partition array
        topic_buffer += struct.pack(
            ">b", len(self.available_topics[topic]["partitions"]) + 1
        )
        if available:
            print(self.available_topics[topic])
            for id in self.available_topics[topic]["partitions"]:
                print(self.partitions[id])
                topic_buffer += self.add_partition(self.partitions[id])
        # permissions
        topic_buffer += struct.pack(">I", 0x00000DF8)
        # tag buffer
        topic_buffer += struct.pack(">b", 0)
        return topic_buffer

    def add_partition(self, partition):
        ret = b""
        # error code
        ret += struct.pack(">h", 0)
        # index
        ret += struct.pack(">I", int.from_bytes(partition["id"]))
        # leader
        ret += struct.pack(">I", int.from_bytes(partition["leader"]))
        # leader_epoch
        ret += struct.pack(">I", int.from_bytes(partition["leader_epoch"]))
        ret += struct.pack(">b", 0)
        ret += struct.pack(">b", 0)
        ret += struct.pack(">b", 0)
        ret += struct.pack(">b", 0)
        ret += struct.pack(">b", 0)
        ret += struct.pack(">b", 0)
        return ret

    def construct_message(self):
        header = self.id
        header += TAG_BUFFER
        # array length
        topics_buffer = int(len(self.topics) + 1).to_bytes(1)
        # encode topic
        topics_buffer += self.create_topic_item(self.topics[0].encode("utf-8"))
        topics_buffer += struct.pack(">B", 0xFF)
        topics_buffer += struct.pack(">b", 0)
        return header + DEFAULT_THROTTLE_TIME + topics_buffer

    def error_handler(self):
        version = int.from_bytes(self.version, byteorder="big")
        if 0 <= version <= 4:
            return ERRORS["ok"]
        else:
            return ERRORS["error"]


class DescribeTopicPartitionsRequest(BaseKafka):
    def __init__(self, correlation_id, body, metadata):
        self.id = correlation_id
        self.body = body
        self.topics = []
        buffer = self._parse_array(body, self.parse_topics)
        self.cursor = buffer[0:1]  # Extract cursor
        buffer = self._remove_tag_buffer(buffer)
        self.available_topics = metadata.topics
        self.partitions = metadata.partitions
        self.message = self._create_message(self.construct_message())

    def parse_topics(self, item_buffer):
        self.topics.append(item_buffer.decode("utf-8"))

    def create_topic_item(self, topic):
        available = topic in self.available_topics
        topic_buffer = b""
        # two byte error code
        if available:
            topic_buffer += struct.pack(">h", 0)  # Success
        else:
            topic_buffer += struct.pack(">h", 3)  # UNKNOWN_TOPIC_OR_PARTITION
        # string length and topic name
        topic_buffer += struct.pack(">b", len(topic) + 1)  # Compact string length
        topic_buffer += struct.pack(f">{len(topic)}s", topic)  # Topic name
        # topic id (UUID)
        if available:
            uuid_str = self.available_topics[topic]["uuid"]
            uuid_bytes = uuid_str.bytes
        else:
            # Use all zeros for unknown topics
            uuid_bytes = bytes(16)
        topic_buffer += struct.pack("16s", uuid_bytes)
        # is_internal flag (false)
        topic_buffer += struct.pack(">b", 0)
        # partitions array
        if available and self.available_topics[topic]["partitions"]:
            # Add 1 for compact array format
            topic_buffer += struct.pack(">b", len(self.available_topics[topic]["partitions"]) + 1)
            for id in self.available_topics[topic]["partitions"]:
                topic_buffer += self.add_partition(self.partitions[id])
        else:
            # Empty array (just the length byte)
            topic_buffer += struct.pack(">b", 1)
        # topic_authorized_operations
        topic_buffer += struct.pack(">I", 0x00000DF8)
        # tag buffer
        topic_buffer += struct.pack(">b", 0)
        return topic_buffer

    def add_partition(self, partition):
        ret = b""
        # error code
        ret += struct.pack(">h", 0)
        # partition index
        ret += struct.pack(">I", int.from_bytes(partition["id"]))
        # leader
        ret += struct.pack(">I", int.from_bytes(partition["leader"]))
        # leader_epoch
        ret += struct.pack(">I", int.from_bytes(partition["leader_epoch"]))
        # replica_nodes (empty array)
        ret += struct.pack(">b", 1)
        # isr_nodes (empty array)
        ret += struct.pack(">b", 1)
        # eligible_leader_replicas (empty array)
        ret += struct.pack(">b", 1)
        # last_known_elr (empty array)
        ret += struct.pack(">b", 1)
        # offline_replicas (empty array)
        ret += struct.pack(">b", 1)
        # tagged fields
        ret += struct.pack(">b", 0)

        return ret

    def construct_message(self):
        header = self.id
        header += TAG_BUFFER  # Tagged fields in header
        # Response body
        body = DEFAULT_THROTTLE_TIME  # throttle_time_ms: 0
        # Topics array (compact format)
        body += int(len(self.topics) + 1).to_bytes(1)  # Array length
        # Add all topic information
        for topic in self.topics:
            body += self.create_topic_item(topic.encode("utf-8"))
        # Add cursor (null cursor)
        body += struct.pack(">B", 0xFF)  # 0xFF indicates a null cursor
        # Tagged fields at end of response
        body += TAG_BUFFER

        return header + body


async def client_handler(metadata, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    while True:
        data = await reader.read(1024)
        if not data:
            break
        header = KafkaHeader(data)
        if header.key_int == 18:
            request = ApiRequest(header.version_int, header.id)
            body = request.message  # Get only the body
        elif header.key_int == 75:  # DescribeTopicPartitions API
            request = DescribeTopicPartitionsRequest(header.id, header.body, metadata)
            body = request.message
        else:
            request = TopicRequest(header.id, header.body, metadata)
            body = request.message

        # Prepend the header and the length of the body
        length = len(body)
        length_prefix = length.to_bytes(4, byteorder="big")
        response = header.id + struct.pack(">b", 0) + body #The header also includes correlation ID and tag buffer
        response = length_prefix + response
        writer.write(response)
        await writer.drain()
    writer.close()
    await writer.wait_closed()


async def run_server(metadata, port, host):
    server = await asyncio.start_server(
        # Use await for start_server
        lambda r,
        w: client_handler(metadata, r, w),
        host, port,
        reuse_port=True
    )
    addr = server.sockets[0].getsockname() if server.sockets else ("unknown", 0)
    print(f"Server listening on {addr[0]}:{addr[1]}...")
    # Ensures server.close() and server.wait_closed() on exit or cancellation
    async with server:
        # Use await for serve_forever
        await server.serve_forever()


async def main():
    port = 9092
    host = "localhost"
    # Use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")
    metadata_log_path = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
    with open(metadata_log_path, "rb") as f:
        data = f.read()
        metadata = Metadata(data)
        f.close()
    print(metadata.topics)
    # Call run_server as a coroutine
    await run_server(metadata, port, host)

asyncio.run(main())