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


# --- Encoding/Decoding Helpers ---

def encode_big_endian(fmt, n):
    """
    Encodes number 'n' using format 'fmt' in big-endian.
    The format 'fmt' is a string that specifies the data type.
    """
    return struct.pack(f'>{fmt}', n)

def decode_big_endian(fmt, data):
    """
    Decodes data using format 'fmt' in big-endian.
    The data must be a bytes object of the correct size.
    """
    return struct.unpack(f'>{fmt}', data)

def encode_string(s):
    """
    Encodes a string in Kafka v0 format:
      - 2 bytes for length followed by UTF-8 encoded bytes.
      - If s is None, encodes as int16 -1.
    """
    if s is None:
        return encode_big_endian('h', -1)
    encoded = s.encode('utf-8')
    return encode_big_endian('h', len(encoded)) + encoded

def decode_string(data, offset):
    """
    Decodes a Kafka v0 string from data starting at offset.
    Returns the decoded string and the new offset.
    """
    length = decode_big_endian('h', data[offset:offset+2])[0]
    if length == -1:
        return None, offset + 2
    start = offset + 2
    end = start + length
    return data[start:end].decode('utf-8'), end

def read_string(sock):
    """
    Reads a Kafka v0 string from the socket.
    Returns the decoded string and the number of bytes read.
    """
    len_bytes = read_n_bytes(sock, 2)
    length = decode_big_endian('h', len_bytes)[0]
    print(f"read_string: Length bytes={len_bytes.hex()}, Length={length}")
    if length == -1:
        return None, 2
    string_bytes = read_n_bytes(sock, length)
    print(f"read_string: Read {length} bytes, Data={string_bytes.hex()}")
    try:
        return string_bytes.decode('utf-8'), 2 + length
    except UnicodeDecodeError:
        print(f"read_string: Invalid UTF-8 data: {string_bytes.hex()}")
        return "", 2 + length  # Fallback to empty string

def encode_fixed_string(s, length):
    """
    Encodes the string s as UTF-8 and pads (or truncates) it to a fixed length.
    """
    encoded = s.encode('utf-8')
    if len(encoded) > length:
        encoded = encoded[:length]
    # Pad with null bytes on the right
    return encoded + b'\x00' * (length - len(encoded))

# --- Socket Reading Helpers ---

def read_n_bytes(sock, n):
    """
    Reads exactly n bytes from the socket.
    Returns the data read or raises an IOError if unable.
    """
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise IOError(f"Connection closed while trying to read {n} bytes. Got {len(data)} bytes.")
        data += chunk
    print(f"read_n_bytes: Requested={n}, Read={len(data)}, Data={data.hex()}")
    return data

def discard_remaining_bytes(sock, remaining):
    """
    Discards the remaining bytes in a request.
    """
    while remaining > 0:
        chunk_size = min(remaining, 4096)
        chunk = sock.recv(chunk_size)
        if not chunk:
            raise IOError(f"Connection closed while discarding {remaining} bytes.")
        remaining -= len(chunk)

# --- Request Header Parsing ---

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
    # 1. Read Message Size (4 bytes)
    size_bytes = read_n_bytes(sock, 4)
    request_total_size = decode_big_endian('i', size_bytes)[0]

    # 2. Read ApiKey, ApiVersion, CorrelationId (2 + 2 + 4 = 8 bytes)
    api_corr_bytes = read_n_bytes(sock, 8)
    api_key, api_version, correlation_id = decode_big_endian('h h i', api_corr_bytes)

    # 3. Read ClientId (v0 String: INT16 length + bytes)
    client_id, client_id_bytes_read = read_string(sock)

    # Calculate remaining size for the actual request *body*
    header_size = 8 + client_id_bytes_read # ApiKey/Ver/Corr + ClientId
    request_body_size = request_total_size - header_size

    print(f"read_request_header: TotalSize={request_total_size}, HeaderSize={header_size}, BodySize={request_body_size}")
    return api_key, api_version, correlation_id, client_id, request_body_size

# --- API Specific Request Parsing ---

def parse_describe_topic_partitions_request(sock, body_size):
    """
    Parses the body of a DescribeTopicPartitions v0 request.
    Expected body format:
      - topicsCount: INT16
      - For each topic:
            • topic_name: Kafka v0 string (2-byte length + UTF-8 bytes)
            • partitionsCount: INT32
            • For each partition: INT32 partition id
    Returns the topic name of the first topic (or "" if none found).
    """
    if body_size < 2:
        print(f"parse_describe_topic_partitions_request: Invalid body size {body_size}")
        return ""
    
    # Read topicsCount (INT16)
    topics_count_bytes = read_n_bytes(sock, 2)
    topics_count = decode_big_endian('h', topics_count_bytes)[0]
    bytes_read = 2

    print(f"parse_describe_topic_partitions_request: Reading {topics_count} topics")
    first_topic = None

    for i in range(topics_count):
        # Read topic name length (INT16)
        name_len_bytes = read_n_bytes(sock, 2)
        name_len = decode_big_endian('h', name_len_bytes)[0]
        bytes_read += 2

        # Validate length
        if name_len < 0 or name_len > body_size - bytes_read:
            print(f"parse_describe_topic_partitions_request: Invalid topic name length: {name_len}")
            break

        # Read topic name bytes
        name_bytes = read_n_bytes(sock, name_len)
        try:
            topic_name = name_bytes.decode('utf-8')
            if first_topic is None:
                first_topic = topic_name
        except UnicodeDecodeError:
            print(f"parse_describe_topic_partitions_request: Invalid UTF-8 in topic name")
            if first_topic is None:
                first_topic = ""
        bytes_read += name_len

        # Read partitionsCount (INT32)
        if body_size - bytes_read < 4:
            print("parse_describe_topic_partitions_request: Not enough bytes for partitions count")
            break
        partitions_count_bytes = read_n_bytes(sock, 4)
        partitions_count = decode_big_endian('i', partitions_count_bytes)[0]
        bytes_read += 4

        # Skip partition IDs
        if partitions_count > 0:
            skip_bytes = partitions_count * 4
            if skip_bytes > body_size - bytes_read:
                print(f"parse_describe_topic_partitions_request: Not enough bytes for {partitions_count} partitions")
                break
            discard_remaining_bytes(sock, skip_bytes)
            bytes_read += skip_bytes

    # Discard any remaining bytes
    if body_size > bytes_read:
        discard_remaining_bytes(sock, body_size - bytes_read)

    print(f"parse_describe_topic_partitions_request: Parsed topic='{first_topic}'")
    return first_topic if first_topic is not None else ""


def build_describe_topic_partitions_response(correlation_id, topic):
    """
    Constructs a DescribeTopicPartitions (v0) response for an unknown topic.
    Response Body format:
       - error_code: INT16 (2 bytes) = 3 (UNKNOWN_TOPIC_OR_PARTITION)
       - topic_name: fixed 96-byte field (UTF-8 encoded, padded with zeros)
       - topic_id: 16 bytes of zeros (UUID all zeros)
       - partitions_count: INT32 (4 bytes) = 0 (empty array)
    The full response is: message_length (4 bytes) + correlation_id (4 bytes) + body.
    Total = 4 + 4 + (2+96+16+4) = 126 bytes.
    """
    error_code = 3  # UNKNOWN_TOPIC_OR_PARTITION
    if not isinstance(topic, str):
        topic = ""
    print(f"build_describe_topic_partitions_response: topic_name='{topic}'")
    
    topic_bytes = topic.encode('utf-8')
    # Create a fixed 96-byte topic field
    topicField = encode_fixed_string(topic, 96)
    
    # Fixed topic_id: 16 bytes zeros
    topicId = b'\x00' * 16

    # Body size: error_code (2) + topic_field (96) + topic_id (16) + partitions_count (4)
    bodySize = 2 + 96 + 16 + 4
    buffer = bytearray(4 + 4 + bodySize)
    # Use big-endian
    buf = memoryview(buffer)
    
    # message_length = correlation_id (4) + body size
    buf[:4] = encode_big_endian('i', 4 + bodySize)
    buf[4:8] = encode_big_endian('i', correlation_id)
    offset = 8
    buf[offset:offset+2] = encode_big_endian('h', error_code); offset += 2
    buf[offset:offset+96] = topicField; offset += 96
    buf[offset:offset+16] = topicId; offset += 16
    buf[offset:offset+4] = encode_big_endian('i', 0); offset += 4

    return bytes(buffer)

# --- API Specific Response Building ---

def build_api_versions_response(correlation_id, api_version_requested):
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
    supported_api_versions_key = 18
    supported_api_versions_min = 0
    supported_api_versions_max = 4

    supported_describe_topic_key = 75
    supported_describe_topic_min = 0
    supported_describe_topic_max = 0 # Only v0 supported

    error_code = 0
    # Check if the *requested* version for ApiVersions itself is supported
    if api_version_requested < supported_api_versions_min or api_version_requested > supported_api_versions_max:
        error_code = 35 # UNSUPPORTED_VERSION

    body = b""
    if error_code != 0:
        # Error response body (flexible format)
        body += encode_big_endian('h', error_code) # Error Code
        body += b'\x01' # Compact Array Length: 0 elements (encoded as 0+1)
        body += encode_big_endian('i', 0) # Throttle Time MS
        body += b'\x00' # Tagged Fields (0 tags)
    else:
        # Success response body (flexible format)
        body += encode_big_endian('h', error_code) # Error Code (0)
        # Compact Array for ApiKeys: Length = 2 elements (encoded as 2+1 = 3)
        body += b'\x03'

        # Entry 1: ApiVersions (Key 18)
        body += encode_big_endian('h', supported_api_versions_key)
        body += encode_big_endian('h', supported_api_versions_min)
        body += encode_big_endian('h', supported_api_versions_max)
        body += b'\x00' # Tagged Fields for this entry (0 tags)

        # Entry 2: DescribeTopicPartitions (Key 75)
        body += encode_big_endian('h', supported_describe_topic_key)
        body += encode_big_endian('h', supported_describe_topic_min)
        body += encode_big_endian('h', supported_describe_topic_max)
        body += b'\x00' # Tagged Fields for this entry (0 tags)

        body += encode_big_endian('i', 0) # Throttle Time MS
        body += b'\x00' # Tagged Fields for the response (0 tags)

    # Response Header v1 (Correlation ID + Tagged Fields)
    header = encode_big_endian('i', correlation_id)
    header += b'\x00' # Tagged Fields for header (0 tags)

    # Full Response: Size + Header + Body
    message_size = len(header) + len(body)
    response = encode_big_endian('i', message_size) + header + body
    return response

# --- Client Handling Logic ---

def handle_client(client_socket):
    """
    Processes multiple sequential requests from a single client.
    Reads each request header, discards any extra request data, and sends back a response.
    """
    client_addr = client_socket.getpeername()
    print(f"Connection from {client_addr} established!")
    try:
        while True:
            # Read the common request header fields
            api_key, api_version, correlation_id, client_id, request_body_size = read_request_header(client_socket)
            print(f"[{client_addr}] Received Request: ApiKey={api_key}, ApiVersion={api_version}, CorrelationId={correlation_id}, ClientId='{client_id}', BodySize={request_body_size}")

            response = None
            if api_key == 18: # ApiVersions
                # ApiVersions request body is usually empty or has tagged fields we ignore for now
                if request_body_size > 0:
                    print(f"[{client_addr}] Discarding {request_body_size} bytes from ApiVersions request body.")
                    discard_remaining_bytes(client_socket, request_body_size)
                response = build_api_versions_response(correlation_id, api_version)
                print(f"[{client_addr}] Sending ApiVersions response ({len(response)} bytes)")

            elif api_key == 75: # DescribeTopicPartitions
                if api_version == 0:
                    topic_name = parse_describe_topic_partitions_request(client_socket, request_body_size)
                    print(f"[{client_addr}] Parsed DescribeTopicPartitions v0 request for topic: '{topic_name}'")
                    # For now, always respond with UNKNOWN_TOPIC
                    response = build_describe_topic_partitions_response(correlation_id, topic_name)
                    print(f"[{client_addr}] Sending DescribeTopicPartitions v0 (Unknown Topic) response ({len(response)} bytes)")
                else:
                    print(f"[{client_addr}] Unsupported DescribeTopicPartitions version: {api_version}. Discarding body.")
                    if request_body_size > 0:
                        discard_remaining_bytes(client_socket, request_body_size)
                    # Ideally, send an UNSUPPORTED_VERSION error response matching the requested version's schema
                    # For simplicity now, we just won't respond.

            else:
                print(f"[{client_addr}] Unsupported ApiKey: {api_key}. Discarding body.")
                if request_body_size > 0:
                    discard_remaining_bytes(client_socket, request_body_size)
                # No response for unknown keys for now

            if response:
                client_socket.sendall(response)

    except IOError as e:
        print(f"[{client_addr}] I/O Error: {e}. Closing connection.")
    except Exception as e:
        print(f"[{client_addr}] Unexpected error: {e}. Closing connection.")
    finally:
        try:
            # Attempt graceful shutdown
            client_socket.shutdown(socket.SHUT_WR)
        except OSError:
            pass # Ignore error if socket already closed
        finally:
            client_socket.close()
            print(f"[{client_addr}] Connection closed.")

# --- Server Logic ---

def run_server(port):
    """
    Runs the Kafka clone server on the specified port.
    Accepts new client connections concurrently, and delegates request processing to handle_client().
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
    Main entry point.
    Starts the server and prints logs for each request and response.
    """
    port = 9092
    print(f"Starting server on port {port}...")
    print("Logs from your program will appear here!")
    try:
        run_server(port)
    except Exception as e:
        print(f"Server failed to start or run: {e}")


if __name__ == "__main__":
    run()
