"""
Kafka Clone - APIVersions Handler

This module implements a simple Kafka broker that supports the ApiVersions request.
It reads incoming requests (which may be sent sequentially on the same connection),
parses the header, and sends back a response.

Supported ApiVersions:
  - Only the ApiVersions API (api_key 18) is processed.
  - The broker supports versions 0 to 4. If the request_api_version is outside this range,
    an error code 35 ("UNSUPPORTED_VERSION") is returned.
    
Response encoding:
  The response is built in Kafka's flexible (compact) message format for ApiVersions v4:
    - error_code: INT16 (2 bytes)
    - api_keys: compact array:
         Length: Unsigned varint (1 byte); for one entry, this equals (1+1)=2.
         Entry: api_key (INT16), min_version (INT16), max_version (INT16),
                entry TAG_BUFFER (1 byte = 0x00)
    - throttle_time_ms: INT32 (4 bytes, value 0)
    - response TAG_BUFFER: compact bytes (empty, encoded as 1 byte 0x00)
    
Overall, the response body length is 15 bytes. Since a 4‐byte correlation id is prepended,
the message length field (the first 4 bytes) is 19.
Combined with the 4-byte message_length field, the total transmission is 23 bytes.
"""

import socket
import struct
import threading


def encode_big_endian(fmt, n):
    """
    Encodes a number 'n' using the given format string 'fmt'
    in big-endian order.
    """
    return struct.pack(fmt, n)

def decode_big_endian(fmt, data):
    """
    Decodes data using the given format string 'fmt'
    in big-endian order.
    """
    return struct.unpack(fmt, data)

def encode_string(s):
    """
    Encodes a string in Kafka v0 format:
      - 2 bytes for length followed by UTF-8 encoded bytes.
      - If s is None, encodes as int16 -1.
    """
    if s is None:
        return encode_big_endian('>h', -1)
    encoded = s.encode('utf-8')
    return encode_big_endian('>h', len(encoded)) + encoded

def parse_describe_topic_partitions_request(sock, remaining):
    # For v0, assume the request body begins with the topic string.
    topic_len_bytes = read_n_bytes(sock, 2)
    topic_len = decode_big_endian('>h', topic_len_bytes)[0]
    topic_bytes = read_n_bytes(sock, topic_len)
    # Discard any extra bytes (if any) from the request body.
    bytes_read = 2 + topic_len
    if remaining > bytes_read:
        discard_remaining_request(sock, remaining, bytes_read)
    return topic_bytes.decode('utf-8')

def build_describe_topic_partitions_response(correlation_id, topic):
    """
    Constructs a DescribeTopicPartitions response for an unknown topic.
    The response body is:
      - error_code: INT16 (2 bytes) = 3 (UNKNOWN_TOPIC_OR_PARTITION)
      - topic_name: string (2 bytes length + UTF-8 bytes; same as in request)
      - topic_id: 16 bytes of zero
      - partitions: array of partitions:
            int32 count (4 bytes), here 0 for empty.
    The full response is: message_length (4 bytes) + correlation_id (4 bytes) + body.
    """
    error_code = 3  # UNKNOWN_TOPIC_OR_PARTITION
    topic_field = encode_string(topic)
    topic_id = b'\x00' * 16
    partitions = encode_big_endian('>i', 0)  # empty partitions array (count = 0)
    body = encode_big_endian('>h', error_code) + topic_field + topic_id + partitions
    msg_size = 4 + len(body)  # correlation_id (4 bytes) + body
    response = encode_big_endian('>i', msg_size)
    response += encode_big_endian('>i', correlation_id)
    response += body
    return response

def read_n_bytes(sock, n):
    """
    Reads exactly n bytes from the socket.
    Returns the data read or raises an IOError if unable.
    """
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise IOError("Expected {} bytes, got {} bytes".format(n, len(data)))
        data += chunk
    return data


def read_request_header(sock):
    """
    Reads the request header from the socket.
    Header layout:
      - 4 bytes: request message size (size of data following this field)
      - 2 bytes: request_api_key (INT16)
      - 2 bytes: request_api_version (INT16)
      - 4 bytes: correlation_id (INT32, signed)
    Returns:
      (api_key, api_version, correlation_id, remaining_size)
    where remaining_size = request_size - 8.
    """
    # Read 4-byte size field.
    request_size_bytes = read_n_bytes(sock, 4)
    request_size = decode_big_endian('>i', request_size_bytes)[0]
    
    # Read remaining 8 bytes: api_key, api_version, correlation_id.
    header_bytes = read_n_bytes(sock, 8)
    # Use '>hhi' to decode correlation_id as a signed int.
    api_key, api_version, correlation_id = decode_big_endian('>hhi', header_bytes)
    
    remaining_size = request_size - 8
    return api_key, api_version, correlation_id, remaining_size


def discard_remaining_request(sock, total_bytes, bytes_read):
    """
    Discards the remaining bytes in a request.
    """
    remaining = total_bytes - bytes_read
    while remaining > 0:
        chunk = sock.recv(min(remaining, 4096))
        if not chunk:
            break
        remaining -= len(chunk)


def build_api_versions_response(api_key, api_version, correlation_id):
    """
    Constructs the ApiVersions response.
    For api_key 18:
      - If api_version is not between 0 and 4, error_code 35 is used.
      - Otherwise, error_code is 0 and the response includes:
            • an entry for ApiVersions (api_key 18, min_version 0, max_version 4)
            • an entry for DescribeTopicPartitions (api_key 75, min_version 0, max_version 0)
    Response format for a successful response:
      - error_code: INT16 (2 bytes)
      - api_keys (compact array): 1 byte (for two elements, value = 3)
      - Entry 1 (7 bytes): api_key (INT16), min_version (INT16), max_version (INT16), TAG_BUFFER (1 byte, 0x00)
      - Entry 2 (7 bytes): api_key (INT16), min_version (INT16), max_version (INT16), TAG_BUFFER (1 byte, 0x00)
      - throttle_time_ms: INT32 (4 bytes, value 0)
      - overall TAG_BUFFER: 1 byte (0x00)
      
      Total successful body size = 2 + 1 + 7 + 7 + 4 + 1 = 22 bytes.
      The full response is: message_length (4 bytes) + correlation_id (4 bytes) + body.
    For an error response, the body layout remains unchanged.
    """
    error_code = 35 if (api_version < 0 or api_version > 4) else 0

    if error_code != 0:
        # Error body: 2 + 1 + 4 + 1 = 8 bytes.
        body = encode_big_endian('>h', error_code)
        body += encode_big_endian('>B', 0)       # compact array length = 0
        body += encode_big_endian('>i', 0)       # throttle_time_ms = 0
        body += b'\x00'                        # TAG_BUFFER
    else:
        # Successful body:
        # error_code (2 bytes)
        # compact array length (1 byte) = 3 (i.e. 2 elements + 1)
        # Entry 1: ApiVersions (api_key 18) + 2 (min_version) + 2 (max_version) + 1 = 7 bytes
        # Entry 2: DescribeTopicPartitions (api_key 75) + 2 (min_version) + 2 (max_version) + 1 = 7 bytes
        # throttle_time_ms (4 bytes)
        # overall TAG_BUFFER (1 byte)
        # Total = 2 + 1 + 7 + 7 + 4 + 1 = 22 bytes
        body = encode_big_endian('>h', error_code)
        body += encode_big_endian('>B', 3)       # compact array length = 3 (2 entries + 1)
        
        # Entry for ApiVersions (api_key 18)
        entry1 = encode_big_endian('>h', 18)       # api_key = 18
        entry1 += encode_big_endian('>h', 0)         # min_version = 0
        entry1 += encode_big_endian('>h', 4)         # max_version = 4
        entry1 += b'\x00'                          # TAG_BUFFER
        
        # Entry for DescribeTopicPartitions (api_key 75)
        entry2 = encode_big_endian('>h', 75)       # api_key = 75
        entry2 += encode_big_endian('>h', 0)         # min_version = 0
        entry2 += encode_big_endian('>h', 0)         # max_version = 0
        entry2 += b'\x00'                          # TAG_BUFFER
        
        body += entry1 + entry2
        body += encode_big_endian('>i', 0)         # throttle_time_ms = 0
        body += b'\x00'                          # overall TAG_BUFFER

    # Total payload after message_length field: correlation_id (4 bytes) + body.
    msg_size = 4 + len(body)
    response = encode_big_endian('>i', msg_size)             # message_length field
    response += encode_big_endian('>i', correlation_id)        # correlation_id
    response += body
    return response


def handle_client(client_socket):
    """
    Processes multiple sequential requests from a single client.
    Reads each request header, discards any extra request data, and sends back a response.
    """
    try:
        while True:
            try:
                api_key, api_version, correlation_id, remaining_size = read_request_header(client_socket)
                print(f"Received api_key: {api_key}, api_version: {api_version}, correlation_id: {correlation_id}")
                
                # Discard any extra request bytes.
                if api_key == 18:
                    if remaining_size > 0:
                        discard_remaining_request(client_socket, remaining_size, 0)
                    response = build_api_versions_response(api_key, api_version, correlation_id)
                    client_socket.sendall(response)
                    print(f"Sent ApiVersions response ({len(response)} bytes)")
                elif api_key == 75:
                    # DescribeTopicPartitions request (v0)
                    topic = parse_describe_topic_partitions_request(client_socket, remaining_size)
                    response = build_describe_topic_partitions_response(correlation_id, topic)
                    client_socket.sendall(response)
                    print(f"Sent DescribeTopicPartitions response ({len(response)} bytes)")
                else:
                    print(f"Unknown api_key: {api_key}, no response sent")
            except IOError:
                # Likely client disconnected.
                break
            except Exception as e:
                print(f"Error processing request: {e}")
                break
    finally:
        try:
            client_socket.shutdown(socket.SHUT_WR)
        except Exception:
            pass
        client_socket.close()
        print("Client connection closed.")


def run_server(port):
    """
    Runs the Kafka clone server on the specified port.
    Accepts new client connections concurrently, and delegates request processing to handle_client().
    """
    server = socket.create_server(("localhost", port), reuse_port=True)
    print("Server listening on port", port)
    
    try:
        while True:
            client_socket, client_address = server.accept()
            print(f"Connection from {client_address} has been established!")
            # Spawn a thread for each client.
            thread = threading.Thread(target=handle_client, args=(client_socket,))
            thread.start()
    finally:
        server.close()
        print("Server closed.")


def run():
    """
    Main entry point.
    Starts the server and prints logs for each request and response.
    """
    port = 9092
    print(f"Starting server on port {port}...")
    print("Logs from your program will appear here!")
    try:
        run_server(port)
    except Exception as e:
        print("Error:", e)


if __name__ == "__main__":
    run()
