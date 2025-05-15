import socket
import threading
import struct

def encode_big_endian(value: int, size: int) -> bytes:
    """
    Encode an integer to big-endian bytes with the specified size.
    """
    return value.to_bytes(size, byteorder="big")

def encode_kafka_string(s: bytes) -> bytes:
    """
    Encode a byte string as a Kafka v0 STRING (2-byte length + bytes).
    """
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
    return data

def parse_request_header(data: bytes) -> tuple[int, int, int, int]:
    """Parse Kafka request header: message_size, api_key, api_version, correlation_id."""
    message_size = int.from_bytes(data[:4], byteorder="big")
    api_key = int.from_bytes(data[4:6], byteorder="big")
    api_version = int.from_bytes(data[6:8], byteorder="big")
    correlation_id = int.from_bytes(data[8:12], byteorder="big")
    return message_size, api_key, api_version, correlation_id

def parse_client_id(data: bytes, offset: int) -> tuple[bytes, int]:
    """Parse Kafka v0 STRING for client_id, returning the client_id and bytes consumed."""
    client_id_len = int.from_bytes(data[offset:offset+2], byteorder="big")
    if client_id_len <= 0:
        return b"", 2
    client_id = data[offset+2:offset+2+client_id_len]
    return client_id, 2 + client_id_len

def parse_describe_topic_partitions(data: bytes, offset: int, body_size: int) -> tuple[bytes, int]:
    """Parse DescribeTopicPartitions v0 request body, returning first topic name and cursor."""
    bytes_consumed = 0
    if body_size < 2:
        print(f"P_DTP_R: Body too small for topics_count ({body_size} bytes).")
        return b"", 0

    topics_count = int.from_bytes(data[offset:offset+2], byteorder="big")
    bytes_consumed += 2
    if topics_count < 0:
        print(f"P_DTP_R: Invalid topics_count {topics_count}.")
        return b"", 0

    print(f"P_DTP_R: Expecting {topics_count} topics.")
    topic_name = b""
    cursor = 0

    for i in range(topics_count):
        if body_size - bytes_consumed < 2:
            print(f"P_DTP_R: Not enough data for topic_name length (topic {i+1}).")
            break
        topic_name_len = int.from_bytes(data[offset+bytes_consumed:offset+bytes_consumed+2], byteorder="big")
        bytes_consumed += 2
        if topic_name_len < 0:
            print(f"P_DTP_R: Invalid topic_name length {topic_name_len} for topic {i+1}.")
            continue
        if topic_name_len > body_size - bytes_consumed:
            print(f"P_DTP_R: Topic_name length {topic_name_len} exceeds remaining bytes.")
            break

        topic_name = data[offset+bytes_consumed:offset+bytes_consumed+topic_name_len]
        bytes_consumed += topic_name_len

        if body_size - bytes_consumed < 4:
            print(f"P_DTP_R: Not enough data for partitions_count (topic {i+1}).")
            break
        partitions_count = int.from_bytes(data[offset+bytes_consumed:offset+bytes_consumed+4], byteorder="big")
        bytes_consumed += 4
        if partitions_count < 0:
            print(f"P_DTP_R: Invalid partitions_count {partitions_count} for topic {i+1}.")
            break

        partition_bytes = partitions_count * 4
        if partition_bytes > body_size - bytes_consumed:
            print(f"P_DTP_R: Partition IDs exceed remaining bytes for topic {i+1}.")
            break
        bytes_consumed += partition_bytes

        cursor = int.from_bytes(data[offset+bytes_consumed:offset+bytes_consumed+1], byteorder="big")
        bytes_consumed += 1
        break  # Only process first topic

    return topic_name, cursor

def build_api_versions_response(correlation_id: int, api_version: int) -> bytes:
    """Build ApiVersions response, returning error 35 for unsupported versions."""
    supported_versions = {0, 1, 2, 3, 4}
    error_code = 0 if api_version in supported_versions else 35
    tag_buffer = b"\x00"
    response_header = encode_big_endian(correlation_id, 4) + tag_buffer

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

def build_describe_topic_partitions_response(correlation_id: int, topic_name: bytes, cursor: int) -> bytes:
    """Build DescribeTopicPartitions v0 response for an unknown topic."""
    tag_buffer = b"\x00"
    response_header = encode_big_endian(correlation_id, 4) + tag_buffer
    body = (
        encode_big_endian(0, 4) +  # throttle_time_ms
        b"\x01" +  # Array length (1 topic)
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
            body_offset = client_id_offset + client_id_bytes
            body_size = message_size - (body_offset - 4)

            if api_key == 75:
                topic_name, cursor = parse_describe_topic_partitions(data, body_offset, body_size)
                response = build_describe_topic_partitions_response(correlation_id, topic_name, cursor)
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