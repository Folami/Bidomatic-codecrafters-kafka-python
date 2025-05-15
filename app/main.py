import socket
import threading

def encode_big_endian(value: int, size: int) -> bytes:
    """Encode an integer to big-endian bytes with the specified size."""
    return value.to_bytes(size, byteorder="big")

def encode_kafka_string(s: bytes) -> bytes:
    """Encode a byte string as a Kafka v0 STRING (2-byte length + bytes)."""
    length = len(s)
    return encode_big_endian(length, 2) + s

def read_n_bytes(sock: socket.socket, n: int) -> bytes:
    """Read exactly n bytes from the socket, raising IOError on failure."""
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise IOError(f"Connection closed while reading {n} bytes. Got {len(data)} bytes.")
        data += chunk
    print(f"read_n_bytes: Read {len(data)} bytes: {data.hex()}")
    return data

def parse_request_header(data: bytes) -> tuple[int, int, int, int]:
    """Parse Kafka request header: message_size, api_key, api_version, correlation_id."""
    message_size = int.from_bytes(data[:4], byteorder="big")
    api_key = int.from_bytes(data[4:6], byteorder="big")
    api_version = int.from_bytes(data[6:8], byteorder="big")
    correlation_id = int.from_bytes(data[8:12], byteorder="big")
    print(f"parse_request_header: message_size={message_size}, api_key={api_key}, api_version={api_version}, correlation_id={correlation_id}")
    return message_size, api_key, api_version, correlation_id

def parse_client_id(data: bytes, offset: int) -> tuple[bytes, int]:
    """Parse Kafka v0 STRING for client_id, returning the client_id and bytes consumed."""
    client_id_len = int.from_bytes(data[offset:offset+2], byteorder="big")
    print(f"parse_client_id: client_id_len={client_id_len}")
    if client_id_len <= 0:
        return b"", 2
    client_id = data[offset+2:offset+2+client_id_len]
    return client_id, 2 + client_id_len

def parse_describe_topic_partitions(data: bytes, offset: int) -> tuple[bytes, int, int]:
    """Parse DescribeTopicPartitions v0 request body, returning first topic name, array length, and cursor."""
    array_len_finder = offset
    array_length = int.from_bytes(data[array_len_finder:array_len_finder+1], byteorder="big")
    print(f"P_DTP_R: array_length={array_length}")
    topic_name_length = int.from_bytes(data[array_len_finder+1:array_len_finder+2], byteorder="big")
    print(f"P_DTP_R: topic_name_length={topic_name_length}")
    topic_name_start = array_len_finder + 2
    topic_name = data[topic_name_start:topic_name_start+topic_name_length]
    cursor_pos = topic_name_start + topic_name_length + 4
    cursor = int.from_bytes(data[cursor_pos:cursor_pos+1], byteorder="big") if cursor_pos < len(data) else 0
    print(f"P_DTP_R: topic_name={topic_name.decode('utf-8', errors='ignore')}, cursor={cursor}")
    return topic_name, array_length, cursor

def build_api_versions_response(correlation_id: int, api_version: int) -> bytes:
    """Build ApiVersions response, returning error 35 for unsupported versions."""
    supported_versions = {0, 1, 2, 3, 4}
    error_code = 0 if api_version in supported_versions else 35
    response_header = encode_big_endian(correlation_id, 4)
    tag_buffer = b"\x00"

    if error_code != 0:
        body = (
            encode_big_endian(error_code, 2) +
            b"\x01" +  # Empty compact array
            encode_big_endian(0, 4) +  # throttle_time_ms
            tag_buffer
        )
    else:
        body = (
            encode_big_endian(0, 2) +  # error_code
            b"\x03" +  # Compact array with 2 entries
            encode_big_endian(18, 2) + encode_big_endian(0, 2) + encode_big_endian(4, 2) + tag_buffer +  # ApiVersions
            encode_big_endian(75, 2) + encode_big_endian(0, 2) + encode_big_endian(0, 2) + tag_buffer +  # DescribeTopicPartitions
            encode_big_endian(0, 4) +  # throttle_time_ms
            tag_buffer
        )

    message_size = len(response_header) + len(body)
    return encode_big_endian(message_size, 4) + response_header + body

def build_describe_topic_partitions_response(correlation_id: int, topic_name: bytes, array_length: int, cursor: int) -> bytes:
    """Build DescribeTopicPartitions v0 response for an unknown topic."""
    tag_buffer = b"\x00"
    response_header = encode_big_endian(correlation_id, 4) + tag_buffer
    body = (
        encode_big_endian(0, 4) +  # throttle_time_ms
        encode_big_endian(array_length, 1) +
        encode_big_endian(3, 2) +  # error_code (UNKNOWN_TOPIC_OR_PARTITION)
        encode_kafka_string(topic_name) +
        encode_big_endian(0, 16) +  # topic_id (nil UUID)
        b"\x00" +  # is_internal
        b"\x01" +  # Empty partition array (compact)
        b"\x00\x00\x0d\xf8" +  # topic_authorized_operations
        tag_buffer +
        encode_big_endian(cursor, 1) +
        tag_buffer
    )
    message_size = len(response_header) + len(body)
    print(f"build_describe_topic_partitions_response: message_size={message_size}, topic_name={topic_name.decode('utf-8', errors='ignore')}")
    return encode_big_endian(message_size, 4) + response_header + body

def handle_client(client_socket: socket.socket):
    """Handle sequential requests from a single client."""
    client_addr = client_socket.getpeername()
    print(f"Connection from {client_addr} established!")
    try:
        while True:
            data = read_n_bytes(client_socket, 1024)
            if not data:
                break

            message_size, api_key, api_version, correlation_id = parse_request_header(data)
            client_id_offset = 12
            client_id, client_id_bytes = parse_client_id(data, client_id_offset)
            body_offset = client_id_offset + client_id_bytes + 1  # Skip tagged field

            if api_key == 75:
                topic_name, array_length, cursor = parse_describe_topic_partitions(data, body_offset)
                response = build_describe_topic_partitions_response(correlation_id, topic_name, array_length, cursor)
                print(f"[{client_addr}] Sending DescribeTopicPartitions response for topic: {topic_name.decode('utf-8', errors='ignore')}")
            else:
                response = build_api_versions_response(correlation_id, api_version)
                print(f"[{client_addr}] Sending ApiVersions response")

            client_socket.sendall(response)
    except Exception as e:
        print(f"[{client_addr}] Error handling client: {e}")
    finally:
        client_socket.close()
        print(f"[{client_addr}] Connection closed.")

def run_server(port: int):
    """Run Kafka clone server on the specified port, handling concurrent clients."""
    server = socket.create_server(("localhost", port), reuse_port=True)
    print(f"Server listening on port {port}")
    try:
        while True:
            client_socket, client_address = server.accept()
            print(f"Connection from {client_address} established!")
            thread = threading.Thread(target=handle_client, args=(client_socket,), daemon=True)
            thread.start()
    except KeyboardInterrupt:
        print("\nShutting down server...")
    finally:
        server.close()
        print("Server closed.")

def main():
    """Main entry point to start the Kafka clone server."""
    port = 9092
    print(f"Starting server on port {port}...")
    print("Logs from your program will appear here!")
    try:
        run_server(port)
    except Exception as e:
        print(f"Server failed to start or run: {e}")

if __name__ == "__main__":
    main()