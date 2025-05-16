import asyncio
import socket  # noqa: F401
import struct
from .metadata import Metadata

ERRORS = {
    "ok": int(0).to_bytes(2, byteorder="big"),
    "error": int(35).to_bytes(2, byteorder="big"),
}
TAG_BUFFER = int(0).to_bytes(1)
DEFAULT_THROTTLE_TIME = int(0).to_bytes(4)

class BaseKafka(object):
    @staticmethod
    def _create_message(message: bytes):
        return int(len(message)).to_bytes(4, byteorder="big") + message
    
    @staticmethod
    def _remove_tag_buffer(buffer: bytes):
        return buffer[1:]

    @staticmethod
    def _parse_string(buffer: bytes):
        length = int.from_bytes(buffer[:2], byteorder="big")
        string = buffer[2 : 2 + length].decode("utf-8")
        return (string, buffer[2 + length :])

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
        body = self.id
        body += self.error_handler()
        apis = b""
        apis += struct.pack(">b", 3)
        apis += struct.pack(">hhhb", 18, 4, 18, 0)
        apis += struct.pack(">hhhb", 75, 0, 0, 0)
        body += apis
        body += struct.pack(">Ib", 4, 0)
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
        buffer = self._remove_tag_buffer(buffer)
        self.available_topics = metadata.topics
        self.partitions = metadata.partitions
        self.message = self._create_message(self.construct_message())

    def parse_topics(self, item_buffer):
        self.topics.append(item_buffer.decode("utf-8"))

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

async def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")
    server = await asyncio.start_server(handler, "localhost", 9092)
    print("Server listening...")
    async with server:
        await server.serve_forever()

async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    with open(
        "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log",
        "rb",
    ) as f:
        data = f.read()
        m = Metadata(data)
        f.close()
    print(m.topics)
    while True:
        data = await reader.read(1024)
        if not data:
            break
        header = KafkaHeader(data)
        if header.key_int == 18:
            message = ApiRequest(header.version_int, header.id).message
        else:
            request = TopicRequest(header.id, header.body, m)
            message = request.message
        writer.write(message)
        await writer.drain()
    writer.close()
    await writer.wait_closed()

APIS = {18: [0, 4], 75: [0, 0]}
def add_api_version(string, api_version, mini, maximum):
    string += api_version
    string += int(mini).to_bytes(2)
    string += int(maximum).to_bytes(2)
    return string

def create_message(id, key, version_in_bytes):
    tag_buffer = b"\x00"
    throttle_time_ms = 0
    version = int.from_bytes(version_in_bytes, byteorder="big")
    ok = int(0).to_bytes(2, byteorder="big")
    error = int(35).to_bytes(2, byteorder="big")
    body = ""
    if 0 <= version <= 4:
        body = ok
    else:
        body = error
    apis = b""
    apis += struct.pack(">b", 3)
    apis += struct.pack(">hhhb", 18, 4, 18, 0)
    apis += struct.pack(">hhhb", 75, 0, 0, 0)
    # apis = add_api_version(bytes(), key, 4, 18)
    # apis = add_api_version(apis, int(75).to_bytes(2), 0, 0)
    # body += int(3).to_bytes(2, byteorder="big")
    body += apis
    body += tag_buffer
    body += throttle_time_ms.to_bytes(4)
    response_length = len(id) + len(body)
    return int(response_length).to_bytes(4, byteorder="big") + id + body


asyncio.run(main())