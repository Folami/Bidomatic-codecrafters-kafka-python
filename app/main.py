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
    def __init__(self, version_int: int, correlation_id: bytes):
        self.version_int = version_int
        self.correlation_id = correlation_id
        self.message = self._create_message(self.construct_message())
    
    def construct_message(self):
        payload = self.correlation_id  # Correlation ID (4 bytes)
        payload += self.error_handler()  # Error code (2 bytes)
        # API keys array (compact format: count = number of entries+1, here 3 entries => 4)
        payload += struct.pack(">b", 4)
        # ApiVersions entry (key 18, min 0, max 4)
        payload += struct.pack(">h", 18)  
        payload += struct.pack(">h", 0)   
        payload += struct.pack(">h", 4)   
        payload += struct.pack(">b", 0)   # Tagged fields
        # Fetch entry (key 1, min 0, max 16)
        payload += struct.pack(">h", 1)   
        payload += struct.pack(">h", 0)   
        payload += struct.pack(">h", 16)  
        payload += struct.pack(">b", 0)   
        # DescribeTopicPartitions entry (key 75, min 0, max 0)
        payload += struct.pack(">h", 75)  
        payload += struct.pack(">h", 0)   
        payload += struct.pack(">h", 0)   
        payload += struct.pack(">b", 0)   
        # Throttle time (4 bytes)
        payload += struct.pack(">I", 0)
        # Final tagged fields at end (1 byte)
        payload += struct.pack(">b", 0)
        return payload

    def error_handler(self):
        if 0 <= self.version_int <= 4:
            return ERRORS["ok"]
        else:
            return ERRORS["error"]

class FetchRequest(BaseKafka):
    def __init__(self, version_int: int, correlation_id: bytes, request_body: bytes):
        self.version_int = version_int
        self.correlation_id = correlation_id
        self.request_body = request_body # We're not parsing this yet
        self.parsed_topic_id = b'\x00' * 16 # Default to a zero UUID if parsing fails or no topics
        self.parsed_partition_index = 0 # Default partition index
        self._parse_fetch_request()
        self.message = self._create_message(self.construct_response())

    def _parse_fetch_request(self):
        # Simplified parsing for Fetch Request v12+ (flexible versions)
        # We are interested in the first topic's ID and its first partition's index.
        # Fetch Request:
        # ... other fields ...
        # Topics (COMPACT_ARRAY)
        #   TopicId (UUID)
        #   Partitions (COMPACT_ARRAY)
        #     Partition (INT32)
        #     CurrentLeaderEpoch (INT32)
        #     FetchOffset (INT64)
        #     LogStartOffset (INT64)
        #     PartitionMaxBytes (INT32)
        #     TAG_BUFFER
        #   TAG_BUFFER
        # ...
        
        buffer = self.request_body
        # Skip ReplicaId (INT32), MaxWaitMs (INT32), MinBytes (INT32), MaxBytes (INT32)
        # For v16, these are present.
        # For flexible versions (v12+), there might be tagged fields before topics.
        # We'll make a simplifying assumption for this stage that we can find the topics array.
        # A more robust parser would handle tagged fields and version differences.

        # Assuming we've skipped to the topics array (this is a simplification)
        # Let's find the first Uvarint for the array length.
        # A robust parser would read Uvarint properly. For now, assume it's 1 byte if small.
        # Skip fields until we find the topics array. For v16, after ClusterId (nullable string), ReplicaEpoch (int32), RackId (string)
        # This is highly simplified and will need to be made robust later.
        # For this stage, let's assume the first topic ID starts at a known offset for the test case.
        # A common pattern for the first topic ID in a v12+ request:
        # ReplicaId (4), MaxWait (4), MinBytes (4), MaxBytes (4), IsolationLevel (1), SessionId (4), SessionEpoch (4) = 25 bytes
        # Topics Array Length (Uvarint, assume 1 byte for 1 topic = 0x02)
        # TopicId (16 bytes)
        # Partitions Array Length (Uvarint, assume 1 byte for 1 partition = 0x02)
        # PartitionIndex (INT32)

        # This is a placeholder for robust parsing. For this stage, we'll assume the test sends
        # a structure where we can directly pick out the topic ID and partition index
        # for the *first* topic and *first* partition.

        # A more correct parsing would involve checking request_version and iterating through fields.
        # For now, let's assume the test sends a single topic, and we can extract its ID.
        # The structure of FetchRequest v16:
        # Header
        # ReplicaId (INT32) - for followers, -1 for consumers
        # MaxWaitTime (INT32)
        # MinBytes (INT32)
        # MaxBytes (INT32)
        # IsolationLevel (INT8)
        # SessionId (INT32)
        # SessionEpoch (INT32)
        # Topics (COMPACT_ARRAY of FetchTopic)
        #   TopicId (UUID)
        #   Partitions (COMPACT_ARRAY of FetchPartition)
        #     Partition (INT32)
        #     CurrentLeaderEpoch (INT32)
        #     FetchOffset (INT64)
        #     LogStartOffset (INT64)
        #     PartitionMaxBytes (INT32)
        #     TAG_BUFFER
        #   TAG_BUFFER
        # ForgottenTopicsData (COMPACT_ARRAY of ForgottenTopic)
        # RackId (COMPACT_STRING)
        # TAG_BUFFER

        try:
            offset = 0
            # Skip ReplicaId, MaxWait, MinBytes, MaxBytes, IsolationLevel, SessionId, SessionEpoch
            offset += 4 + 4 + 4 + 4 + 1 + 4 + 4 # 25 bytes

            # Topics Array Length (CompactArray Uvarint)
            # Assuming it's 1 topic, so length is 2 (1 entry + 1)
            topics_array_len_byte = self.request_body[offset]
            offset += 1
            num_topics = topics_array_len_byte -1

            if num_topics > 0:
                # First TopicId (UUID - 16 bytes)
                self.parsed_topic_id = self.request_body[offset : offset + 16]
                offset += 16

                # Partitions Array Length for the first topic (CompactArray Uvarint)
                # Assuming 1 partition, so length is 2 (1 entry + 1)
                partitions_array_len_byte = self.request_body[offset]
                offset += 1
                num_partitions = partitions_array_len_byte - 1

                if num_partitions > 0:
                    # First Partition Index (INT32)
                    self.parsed_partition_index = int.from_bytes(self.request_body[offset : offset + 4], byteorder="big")
                    # offset += 4 # Not needed further for this stage
        except IndexError:
            print("Error parsing FetchRequest: not enough data for expected fields.", file=sys.stderr)
            # Keep default parsed_topic_id (zero UUID) and parsed_partition_index (0)
        except Exception as e:
            print(f"Unexpected error during FetchRequest parsing: {e}", file=sys.stderr)

    def construct_response(self):
        # FetchResponse V16
        # See: kafka-Fetch-API-Protocol-Summary.md or https://kafka.apache.org/protocol.html#The_Messages_Fetch
        
        payload = self.correlation_id # Correlation ID (4 bytes)
        # Header Tagged Fields (Uvarint, 0 means no tagged fields)
        # For FetchResponse v9+, the header can have tagged fields.
        # We'll assume 0 tagged fields for simplicity in this stage.
        payload += TAG_BUFFER # (1 byte)

        # Response Body
        response_body = b""
        response_body += struct.pack(">i", 0) # ThrottleTimeMs (INT32)
        response_body += struct.pack(">h", 0) # ErrorCode (INT16) - Top-level error code
        response_body += struct.pack(">i", 0) # SessionID (INT32)
        # Responses array (CompactArray of FetchableTopicResponse)
        # For this stage, if a topic was requested (even if unknown), we respond for it.
        # The test sends 1 topic.
        response_body += struct.pack(">b", 2) # Array length (1 entry + 1 for compact format)

        # FetchableTopicResponse
        response_body += self.parsed_topic_id # TopicID (UUID - 16 bytes)
        # Partitions array (CompactArray of FetchPartitionResponse)
        response_body += struct.pack(">b", 2) # Array length (1 partition entry + 1)
        
        # FetchPartitionResponse
        response_body += struct.pack(">i", self.parsed_partition_index) # Partition Index (INT32)
        response_body += struct.pack(">h", 100) # ErrorCode (INT16) - UNKNOWN_TOPIC_ID (or UNKNOWN_TOPIC_OR_PARTITION if more general)
                                               # Kafka uses 3 for UNKNOWN_TOPIC_OR_PARTITION.
                                               # The test specifically asks for 100 (UNKNOWN_TOPIC_ID).
        response_body += struct.pack(">q", -1)  # HighWatermark (INT64)
        response_body += struct.pack(">q", -1)  # LastStableOffset (INT64)
        response_body += struct.pack(">q", -1)  # LogStartOffset (INT64)
        response_body += struct.pack(">b", 1)   # AbortedTransactions (CompactArray - empty, so length 1 (0+1))
        response_body += struct.pack(">i", -1)  # PreferredReadReplica (INT32)
        response_body += struct.pack(">b", 1)   # RecordSet (CompactBytes - empty, so length 1 (0+1))
        response_body += TAG_BUFFER # Tagged Fields for FetchPartitionResponse
        response_body += TAG_BUFFER # Tagged Fields for FetchableTopicResponse

        # Tagged Fields at the end of the response body
        response_body += TAG_BUFFER # (1 byte)
        
        return payload + response_body

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
        if header.key_int == 18:  # ApiVersions
            request = ApiRequest(header.version_int, header.id)
            message = request.message
            writer.write(message)
        elif header.key_int == 1: # Fetch API
            request = FetchRequest(header.version_int, header.id, header.body)
            message = request.message
            writer.write(message)
        elif header.key_int == 75:  # DescribeTopicPartitions API
            request = DescribeTopicPartitionsRequest(header.id, header.body, metadata)
            message = request.message
            writer.write(message)
        else:
            request = TopicRequest(header.id, header.body, metadata)
            message = request.message
            writer.write(message)
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
