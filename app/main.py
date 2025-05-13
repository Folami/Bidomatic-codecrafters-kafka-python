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
    if length == -1: # Nullable string is null
        return None, 2
    if length < 0: # Invalid length for any string type (non-nullable or nullable non-null)
        print(f"read_string: Invalid negative length {length} (and not -1 for null). Treating as empty string.")
        # Consumed 2 bytes for length, return empty string.
        return "", 2
    
    # If length is 0, it's an empty string, read_n_bytes(sock, 0) will correctly return b""
    string_bytes = read_n_bytes(sock, length)
    print(f"read_string: Read {length} bytes for string payload, Data={string_bytes.hex()}")
    try:
        return string_bytes.decode('utf-8'), 2 + length
    except UnicodeDecodeError:
        print(f"read_string: Invalid UTF-8 data: {string_bytes.hex()}")
        return "", 2 + length  # Fallback to empty string

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
        print(f"P_DTP_R: Body too small for topicsCount ({body_size} bytes).")
        if body_size > 0:
            discard_remaining_bytes(sock, body_size)
        return ""

    bytes_consumed = 0
    first_topic_name_found = None

    # Read topicsCount (INT16)
    topics_count_bytes = read_n_bytes(sock, 2)
    topics_count = decode_big_endian('h', topics_count_bytes)[0]
    bytes_consumed += 2

    if topics_count < 0: # Invalid count
        print(f"P_DTP_R: Invalid topics_count {topics_count}.")
        if body_size - bytes_consumed > 0:
            discard_remaining_bytes(sock, body_size - bytes_consumed)
        return ""

    print(f"P_DTP_R: Expecting {topics_count} topics.")

    for i in range(topics_count):
        # Parse Topic Name (STRING)
        if body_size - bytes_consumed < 2: # Not enough for name length field
            print(f"P_DTP_R: Not enough data for topic_name length (topic {i+1}). Remaining: {body_size - bytes_consumed}")
            break
        name_len_bytes = read_n_bytes(sock, 2)
        bytes_consumed += 2
        name_len = decode_big_endian('h', name_len_bytes)[0]

        current_topic_name_str = ""
        if name_len < 0: # Kafka STRING length must be >= 0
            print(f"P_DTP_R: Invalid topic_name length {name_len} for topic {i+1}. STRING requires >= 0.")
            # Malformed. Treat as empty name for this topic.
        elif name_len > 0: # Only read payload if length > 0
            if name_len > body_size - bytes_consumed:
                print(f"P_DTP_R: Stated topic_name length {name_len} for topic {i+1} exceeds remaining body bytes {body_size - bytes_consumed}. Malformed.")
                if first_topic_name_found is None: first_topic_name_found = "" # Ensure it's set if this was the first
                break # Stop parsing topics, rest of body will be discarded.
            
            name_payload_bytes = read_n_bytes(sock, name_len)
            bytes_consumed += name_len
            try:
                current_topic_name_str = name_payload_bytes.decode('utf-8')
            except UnicodeDecodeError:
                print(f"P_DTP_R: Topic_name for topic {i+1} had UTF-8 decode error. Treating as empty.")
                # current_topic_name_str remains ""
        # If name_len == 0, current_topic_name_str is already ""
        
        if first_topic_name_found is None:
            first_topic_name_found = current_topic_name_str
        
        # Parse Partitions for this topic
        if body_size - bytes_consumed < 4: # Not enough for partitionsCount field
            print(f"P_DTP_R: Not enough data for partitionsCount (topic {i+1}). Remaining: {body_size - bytes_consumed}")
            break
        partitions_count_bytes = read_n_bytes(sock, 4)
        bytes_consumed += 4
        partitions_count = decode_big_endian('i', partitions_count_bytes)[0]

        if partitions_count < 0:
            print(f"P_DTP_R: Invalid partitions_count {partitions_count} for topic {i+1}.")
            break # Malformed. Stop parsing topics.
        
        num_partition_ids_to_read = partitions_count # If count is 0, this is 0.
        bytes_for_partition_ids = num_partition_ids_to_read * 4

        if bytes_for_partition_ids > 0: # Only try to read if count > 0
            if bytes_for_partition_ids > body_size - bytes_consumed:
                print(f"P_DTP_R: Stated {bytes_for_partition_ids} bytes for partition IDs (topic {i+1}) exceeds remaining body bytes {body_size - bytes_consumed}. Malformed.")
                break # Malformed. Stop parsing topics.
            
            # We just discard partition IDs as per problem spec for now
            discard_remaining_bytes(sock, bytes_for_partition_ids) 
            bytes_consumed += bytes_for_partition_ids
    
    # Consume any remaining bytes specified by body_size, if not fully parsed.
    if body_size > bytes_consumed:
        print(f"P_DTP_R: Discarding {body_size - bytes_consumed} unparsed bytes from request body.")
        discard_remaining_bytes(sock, body_size - bytes_consumed)
    elif body_size < bytes_consumed:
        print(f"P_DTP_R: WARNING - Consumed {bytes_consumed} bytes, but body_size was {body_size}.")

    parsed_topic_name = first_topic_name_found if first_topic_name_found is not None else ""
    print(f"P_DTP_R: Parsed first topic='{parsed_topic_name}'")
    return parsed_topic_name


def build_describe_topic_partitions_response(correlation_id, topic_name_str):
    """
    Constructs a DescribeTopicPartitions (v0) response for an unknown topic.
    Response Body format:
       - error_code: INT16 (2 bytes) = 3 (UNKNOWN_TOPIC_OR_PARTITION)
       - topic_name: STRING (INT16 length + UTF-8 bytes)
       - topic_id: 16 bytes of zeros (UUID all zeros)
       - partitions_count: INT32 (4 bytes) = 0 (empty array)
    The full response is: message_length (4 bytes) + correlation_id (4 bytes) + body.
    """
    error_code = 3  # UNKNOWN_TOPIC_OR_PARTITION
    
    if not isinstance(topic_name_str, str):
        topic_name_str = "" # Default to empty string if not a string
    print(f"build_describe_topic_partitions_response: topic_name='{topic_name_str}' for response.")

    error_code_bytes = encode_big_endian('h', error_code)
    # encode_string prepends INT16 length to the UTF-8 encoded string bytes
    topic_name_encoded_as_kafka_string = encode_string(topic_name_str)
    topic_id_bytes = b'\x00' * 16  # Nil UUID (16 zero bytes)
    partitions_count_bytes = encode_big_endian('i', 0) # partitions_count = 0

    response_body = error_code_bytes + \
                    topic_name_encoded_as_kafka_string + \
                    topic_id_bytes + \
                    partitions_count_bytes
    
    # Response Header for v0 is just the Correlation ID
    response_header = encode_big_endian('i', correlation_id)
    
    message_size = len(response_header) + len(response_body)
    
    full_response = encode_big_endian('i', message_size) + response_header + response_body
    
    print(f"build_describe_topic_partitions_response: Sending response. MessageSize={message_size}, HeaderLen={len(response_header)}, BodyLen={len(response_body)}")
    return full_response

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
