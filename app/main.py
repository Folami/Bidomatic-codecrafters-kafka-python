"""
Kafka Clone - Multi-API Handler

This module implements a simple Kafka-like broker that supports multiple API requests.
It reads incoming requests (which may be sent sequentially on the same connection),
parses the header (including client_id), and sends back appropriate responses.

Supported APIs:
  - ApiVersions (api_key 18):
    - The broker supports versions 0 to 4 for the ApiVersions request itself.
    - If the request_api_version is outside this range, an error code 35 ("UNSUPPORTED_VERSION") is returned.
    - A successful response lists support for ApiVersions (key 18, v0-v4) and DescribeTopicPartitions (key 75, v0).
  - DescribeTopicPartitions (api_key 75, version 0):
    - Only version 0 of this API is processed.
    - For any topic requested, the server currently responds as if the topic is unknown.
    - The response structure mimics a flexible format to align with tester expectations,
      even though the request is v0.

Request Header Parsing:
  The server expects a standard Kafka request header:
    - message_size: INT32 (Total size of the message payload following this field)
    - api_key: INT16
    - api_version: INT16
    - correlation_id: INT32
    - client_id: STRING (Kafka v0 format: INT16 length + UTF-8 bytes)
  The `read_request_header` function parses these fields and calculates the size of the
  actual request body.

Response Encoding Details:

  1. ApiVersions (api_key 18) Response:
     - The response is built in a Kafka flexible message format.
     - Header (5 bytes):
         - correlation_id: INT32
         - Header Tagged Fields: Compact Bytes (1 byte, 0x00 for 0 tags)
     - Success Body (error_code 0, 22 bytes):
         - error_code: INT16 (0)
         - api_keys (Compact Array):
             - Length: 1 byte (0x03, indicating 2 entries + 1)
             - Entry 1 (ApiVersions, key 18):
                 - api_key: INT16 (18)
                 - min_version: INT16 (0)
                 - max_version: INT16 (4)
                 - Tagged Fields: Compact Bytes (1 byte, 0x00)
             - Entry 2 (DescribeTopicPartitions, key 75):
                 - api_key: INT16 (75)
                 - min_version: INT16 (0)
                 - max_version: INT16 (0)
                 - Tagged Fields: Compact Bytes (1 byte, 0x00)
         - throttle_time_ms: INT32 (0)
         - Response Tagged Fields: Compact Bytes (1 byte, 0x00)
     - Error Body (error_code 35 - UNSUPPORTED_VERSION, 8 bytes):
         - error_code: INT16 (35)
         - api_keys (Compact Array):
             - Length: 1 byte (0x01, indicating 0 entries + 1)
         - throttle_time_ms: INT32 (0)
         - Response Tagged Fields: Compact Bytes (1 byte, 0x00)
     - Total message size (excluding the initial 4-byte message_size field itself):
         - Success: 5 (header) + 22 (body) = 27 bytes.
         - Error:   5 (header) + 8 (body)  = 13 bytes.

  2. DescribeTopicPartitions (api_key 75, version 0) Response for Unknown Topic:
     - The response mimics a flexible format based on tester expectations.
     - Header (5 bytes):
         - correlation_id: INT32
         - Header Tagged Fields: Compact Bytes (1 byte, 0x00 for 0 tags)
     - Body (for an unknown topic, e.g., with an empty topic name in response):
         - throttle_time_ms: INT32 (0) (4 bytes)
         - topics_array_len_byte: Compact Array Length (1 byte, 0x02 for 1 topic entry)
         - Topic Entry:
             - error_code: INT16 (3 - UNKNOWN_TOPIC_OR_PARTITION) (2 bytes)
             - topic_name_len_byte: Compact String Length (1 byte, e.g., 0x01 for empty name)
             - topic_name: UTF-8 bytes (e.g., empty)
             - topic_id: UUID (16 zero bytes)
             - is_internal: BOOLEAN (1 byte, 0 - hardcoded from imain.py)
             - partition_array_len_byte: Compact Array Length (1 byte, 0x01 for 0 partitions)
             - topic_authorized_operations: INT32 (0x00000df8 - hardcoded from imain.py) (4 bytes)
             - topic_tag_buffer: Compact Bytes (1 byte, 0x00)
         - cursor_byte: 1 byte (0x00 - hardcoded, as main.py doesn't parse this from request)
         - response_tag_buffer: Compact Bytes (1 byte, 0x00)
     - Example total body size (for empty topic name):
       4 + 1 + 2 + 1 + 0 + 16 + 1 + 1 + 4 + 1 + 1 + 1 = 33 bytes.
     - Example total message size (excluding initial 4-byte message_size field):
       5 (header) + 33 (body) = 38 bytes.

Client Handling:
  - The `handle_client` function processes requests from a connected client in a loop.
  - It uses helper functions to parse request headers and specific request bodies.
  - It delegates response construction to API-specific builder functions.
  - Each client connection is handled in a separate thread.
"""



import socket  # noqa: F401
import threading
import struct
import os
import json


class ClusterMetadataReader:
    """
    Reads and parses Kafka's cluster metadata from the __cluster_metadata topic log file.
    Extracts topic information including names, UUIDs, and partition details.
    """
    def __init__(self, log_path="/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"):
        self.log_path = log_path
        self.topics = {}  # {topic_name: {uuid, partitions}}
        self.load_metadata()
    
    def load_metadata(self):
        """Load and parse the cluster metadata log file."""
        if not os.path.exists(self.log_path):
            print(f"Warning: Metadata log file not found at {self.log_path}")
            return
        
        try:
            with open(self.log_path, "rb") as f:
                # Skip file header and navigate through record batches
                # This is a simplified parser that looks for topic metadata records
                data = f.read()
                pos = 0
                
                # Skip over file header
                while pos < len(data):
                    # Look for topic metadata records
                    # This is a simplified approach - in production, you'd parse the full record batch format
                    if pos + 8 < len(data):
                        record_size = int.from_bytes(data[pos:pos+4], byteorder="big")
                        if record_size <= 0 or record_size > 10000000:  # Sanity check
                            pos += 1
                            continue
                        
                        # Try to find topic metadata records by looking for JSON-like structures
                        record_data = data[pos:pos+record_size]
                        try:
                            # Look for topic name and UUID patterns
                            if b"topicName" in record_data and b"topicUuid" in record_data:
                                # Extract topic information
                                self._extract_topic_info(record_data)
                        except Exception as e:
                            pass  # Skip malformed records
                        
                        pos += record_size
                    else:
                        pos += 1
                
                print(f"Loaded metadata for {len(self.topics)} topics")
        except Exception as e:
            print(f"Error reading metadata: {e}")
    
    def _extract_topic_info(self, record_data):
        """Extract topic information from a record."""
        try:
            # Find topic name
            name_start = record_data.find(b"topicName") + 12  # Skip past field name and quotes
            if name_start > 12:  # Found
                name_end = record_data.find(b'"', name_start)
                if name_end > name_start:
                    topic_name = record_data[name_start:name_end].decode('utf-8')
                    
                    # Find UUID
                    uuid_start = record_data.find(b"topicUuid") + 12
                    if uuid_start > 12:
                        # UUID is 16 bytes, extract it
                        uuid_pos = uuid_start
                        while uuid_pos < len(record_data) and not (48 <= record_data[uuid_pos] <= 57 or 
                                                                  65 <= record_data[uuid_pos] <= 90 or 
                                                                  97 <= record_data[uuid_pos] <= 122):
                            uuid_pos += 1
                        
                        if uuid_pos < len(record_data):
                            uuid_end = uuid_pos
                            while uuid_end < len(record_data) and (48 <= record_data[uuid_end] <= 57 or 
                                                                 65 <= record_data[uuid_end] <= 90 or 
                                                                 97 <= record_data[uuid_end] <= 122):
                                uuid_end += 1
                            
                            uuid_str = record_data[uuid_pos:uuid_end].decode('utf-8')
                            
                            # Convert UUID string to bytes
                            uuid_bytes = bytes.fromhex(uuid_str.replace('-', ''))
                            
                            # Find partition info
                            partitions = []
                            partition_start = record_data.find(b"partitions")
                            if partition_start > 0:
                                # Extract partition ID, leader, etc.
                                # For simplicity, we'll assume partition 0 with leader 0
                                partitions.append({
                                    "partition_index": 0,
                                    "leader_id": 0,
                                    "leader_epoch": 0,
                                    "replicas": [0],
                                    "isr": [0]
                                })
                            
                            self.topics[topic_name] = {
                                "uuid": uuid_bytes,
                                "partitions": partitions
                            }
                            print(f"Found topic: {topic_name}, UUID: {uuid_str}")
        except Exception as e:
            print(f"Error extracting topic info: {e}")
    
    def topic_exists(self, topic_name):
        """Check if a topic exists in the metadata."""
        return topic_name in self.topics
    
    def get_topic_info(self, topic_name):
        """Get information about a topic if it exists."""
        return self.topics.get(topic_name)


def response_api_key_75(id, cursor, array_length, length, topic_name):
    """
    Constructs a DescribeTopicPartitions response for a topic.
    This function builds a response structure that includes elements
    typically found in flexible Kafka message formats (v1+), even though
    it's intended for a v0 request scenario.

    Args:
        id (int): The correlation ID from the request header.
        cursor (bytes): A single byte value used in the response body structure.
        array_length (int): An integer used to encode a compact array length in the body.
        length (int): An integer used to encode a compact string length in the body.
        topic_name (bytes): The raw bytes of the topic name to include in the response.
    """
    tag_buffer = b"\x00"
    # Header: correlation_id + tag_buffer (flexible header)
    response_header = id.to_bytes(4, byteorder="big") + tag_buffer

    # Check if topic exists in metadata
    topic_name_str = topic_name.decode('utf-8', errors='ignore')
    topic_info = metadata_reader.get_topic_info(topic_name_str)
    
    if topic_info:
        # Topic exists
        error_code = int(0).to_bytes(2, byteorder="big")  # SUCCESS
        topic_id = topic_info["uuid"]
        
        # Build response body
        throttle_time_ms = int(0).to_bytes(4, byteorder="big")
        is_internal = int(0).to_bytes(1, byteorder="big")
        topic_authorized_operations = b"\x00\x00\x0d\xf8"
        
        # Build partitions array
        if topic_info["partitions"]:
            partition = topic_info["partitions"][0]
            partition_array = b"\x02"  # 1 partition + 1
            partition_data = (
                partition["partition_index"].to_bytes(4, byteorder="big") +  # partition_index
                int(0).to_bytes(2, byteorder="big") +  # error_code
                partition["leader_id"].to_bytes(4, byteorder="big") +  # leader_id
                partition["leader_epoch"].to_bytes(4, byteorder="big") +  # leader_epoch
                
                # replica_nodes array
                int(len(partition["replicas"]) + 1).to_bytes(1, byteorder="big")
            )
            
            # Add each replica
            for replica in partition["replicas"]:
                partition_data += replica.to_bytes(4, byteorder="big")
            
            # isr_nodes array
            partition_data += int(len(partition["isr"]) + 1).to_bytes(1, byteorder="big")
            
            # Add each ISR
            for isr in partition["isr"]:
                partition_data += isr.to_bytes(4, byteorder="big")
            
            # offline_replicas (empty array)
            partition_data += b"\x01"  # 0 elements + 1
            
            # Tagged fields
            partition_data += tag_buffer
        else:
            # No partitions
            partition_array = b"\x01"  # 0 partitions + 1
            partition_data = b""
    else:
        # Topic doesn't exist
        error_code = int(3).to_bytes(2, byteorder="big")  # UNKNOWN_TOPIC_OR_PARTITION
        topic_id = int(0).to_bytes(16, byteorder="big")
        partition_array = b"\x01"  # 0 partitions + 1
        partition_data = b""
        
        throttle_time_ms = int(0).to_bytes(4, byteorder="big")
        is_internal = int(0).to_bytes(1, byteorder="big")
        topic_authorized_operations = b"\x00\x00\x0d\xf8"
    
    response_body = (
        throttle_time_ms
        + int(array_length).to_bytes(1, byteorder="big")
        + error_code
        + int(length).to_bytes(1, byteorder="big")
        + topic_name
        + topic_id
        + is_internal
        + partition_array
        + partition_data
        + topic_authorized_operations
        + tag_buffer
        + cursor 
        + tag_buffer  # Response tag buffer
    )
    
    total_len = len(response_header) + len(response_body)
    return int(total_len).to_bytes(4, byteorder="big") + response_header + response_body

def create_msg(id, api_key: int, error_code: int = 0): # Note: api_key parameter is unused internally
    """
    Constructs an ApiVersions response.
    This function builds a response structure that includes elements
    typically found in flexible Kafka message formats (v1+).

    Args:
        id (int): The correlation ID from the request header.
        api_key (int): The API key from the request header (unused in response construction).
        error_code (int): The error code to include in the response (e.g., 0 for success, 35 for unsupported version).
    logic for this specific response. It handles error_code internally based on api_version
    in the calling context, but takes it as a parameter here to match imain.py's signature.
    """
    response_header = id.to_bytes(4, byteorder="big")
    err = error_code.to_bytes(2, byteorder="big")
    api_key_bytes = api_key.to_bytes(2, byteorder="big")
    min_version_api_18, max_version_api_18, min_version_api_75, max_version_api_75 = (
        0,
        4,
        0,
        0,
    )
    tag_buffer = b"\x00"
    num_api_keys = int(3).to_bytes(1, byteorder="big")
    # Note: The ApiVersions response should list ApiKey 18 itself, not the requested api_key
    describe_topic_partition_api = int(75).to_bytes(2, byteorder="big")
    throttle_time_ms = 0
    response_body = (
        err
        + num_api_keys
        + api_key_bytes
        + min_version_api_18.to_bytes(2)
        + max_version_api_18.to_bytes(2)
        + tag_buffer
        + describe_topic_partition_api
        + min_version_api_75.to_bytes(2)
        + max_version_api_75.to_bytes(2)
        + tag_buffer
        + throttle_time_ms.to_bytes(4, byteorder="big")
        + tag_buffer
    )
    total_len = len(response_header) + len(response_body)
    return int(total_len).to_bytes(4, byteorder="big") + response_header + response_body

# --- Client Handling Logic ---

def handle_client(client):
    """
    Handles an individual client connection.

    Processes multiple sequential requests from a single client socket.
    Reads each request, parses its header and relevant body parts, and
    sends back an appropriate response based on the API key.

    Args:
        client (socket.socket): The client socket connection.
    """
    def parse_request_header(request: bytes) -> tuple[int, int, int]:
        """Parse Kafka request header: api_key, api_version, correlation_id."""
        api_key = int.from_bytes(request[4:6], byteorder="big")
        api_version = int.from_bytes(request[6:8], byteorder="big")
        correlation_id = int.from_bytes(request[8:12], byteorder="big")
        print(f"parse_request_header: api_key={api_key}, api_version={api_version}, correlation_id={correlation_id}")
        return api_key, api_version, correlation_id

    def parse_client_id(request: bytes) -> tuple[str, int, bytes]:
        """
        Parses the client ID field from the request bytes.
        Assumes the client ID field starts at byte offset 12 (after message_size, api_key, api_version, correlation_id).

        Args:
            request (bytes): The raw bytes of the incoming request.

        Returns:
            tuple[str, int, bytes]: A tuple containing the decoded client ID string,
                                     the length of the client ID as read from the bytes,
                                     and the single byte immediately following the client ID field.
        Note: This parsing is specific to the structure assumed by imain.py's handle function.
        """
        client_id_len = int.from_bytes(request[12:14], byteorder="big")
        print(f"parse_client_id: client_id_len={client_id_len}")
        if client_id_len > 0:
            client_id = request[14:14 + client_id_len].decode('utf-8', errors='ignore')
            tagged = request[14 + client_id_len]
        else:
            client_id = ""
            tagged = [14]  # Preserve original (unused) list assignment
        return client_id, client_id_len, tagged

    def parse_describe_topic_partitions(request: bytes, client_id_len: int) -> tuple[int, int, bytes, bytes]:
        """
        Parses specific fields from the DescribeTopicPartitions request body.
        Assumes the body structure starts immediately after the client ID field
        and a single byte following it.

        Args:
            request (bytes): The raw bytes of the incoming request.
            client_id_len (int): The length of the client ID field (including length prefix).
        Returns array_length (topics count), topic_name_length (as read),
        topic_name (bytes), and cursor (byte).
        """
        array_len_finder = 14 + client_id_len + 1
        array_length = request[array_len_finder]
        topic_name_length = request[array_len_finder + 1]
        topic_name_starter = array_len_finder + 2
        topic_name = bytes(request[topic_name_starter:topic_name_starter + (topic_name_length - 1)])
        cursor_length = topic_name_starter + topic_name_length + 4
        cursor = request[cursor_length]
        cursor_bytes = int(cursor).to_bytes(1, byteorder="big")
        print(f"P_DTP_R: array_length={array_length}, topic_name_length={topic_name_length}, "
              f"topic_name={topic_name.decode('utf-8', errors='ignore')}, cursor={cursor}")
        return array_length, topic_name_length, topic_name, cursor_bytes

    try:
        while True:
            request = client.recv(1024)
            if not request:
                break
            api_key, api_version, correlation_id = parse_request_header(request)
            if api_key == 75:
                client_id, client_id_len, tagged = parse_client_id(request)
                array_length, topic_name_length, topic_name, cursor_bytes = parse_describe_topic_partitions(request, client_id_len)
                response = response_api_key_75(
                    correlation_id,
                    cursor_bytes,
                    array_length,
                    topic_name_length,
                    topic_name,
                )
                client.sendall(response)
            else:
                version = {0, 1, 2, 3, 4}
                error_code = 0 if api_version in version else 35
                response = create_msg(correlation_id, api_key, error_code)
                client.sendall(response)
    except Exception as e:
        print(f"Except Error Handling Client: {e}")
    finally:
        client.close()
        

# --- Server Logic ---

def run_server(port):
    """
    Runs the Kafka clone server.

    Binds to the specified port and continuously listens for incoming
    client connections. Each new connection is handled in a separate thread.

    Args:
        port (int): The port number to listen on.
    """
    server = socket.create_server(("localhost", port), reuse_port=True)
    print(f"Server listening on port {port}")
    try:
        while True:
            client_socket, client_address = server.accept()
            print(f"Connection from {client_address} has been established!")
            # Spawn a thread for each client.
            thread = threading.Thread(target=handle_client, args=(client_socket,), daemon=True)
            thread.start()
    except KeyboardInterrupt:
        print("\nShutting down server...")
    finally:
        server.close()
        print("Server closed.")


# --- Main Execution ---

def run():
    """
    Main entry point for the Kafka clone application.
    Initializes the server and starts listening for connections on port 9092.
    """
    port = 9092
    print(f"Starting server on port {port}...")
    print("Logs from your program will appear here!")
    try:
        run_server(port)
    except Exception as e:
        print(f"Server failed to start or run: {e}")


if __name__ == "__main__":
    # Initialize the metadata reader
    metadata_reader = ClusterMetadataReader()
    run()
