import socket
import struct

def encode_unsigned_varint(n):
    # For our small numbers, n < 128 so one byte is enough.
    return struct.pack('>B', n)

def main():
    # Debug log.
    print("Logs from your program will appear here!")

    server = socket.create_server(("localhost", 9092), reuse_port=True)
    client_socket, client_address = server.accept()
    print(f"Connection from {client_address} has been established!")

    # Read the request size (first 4 bytes)
    request_size_bytes = b""
    while len(request_size_bytes) < 4:
        chunk = client_socket.recv(4 - len(request_size_bytes))
        if not chunk:
            break
        request_size_bytes += chunk
    if len(request_size_bytes) < 4:
        print("Incomplete request size received!")
        client_socket.close()
        server.close()
        return
    request_size = struct.unpack('>i', request_size_bytes)[0]
    print(f"Received request size: {request_size}")

    # Read the rest of the request (header + body)
    # Header v2:
    #   INT16 request_api_key (2 bytes)
    #   INT16 request_api_version (2 bytes)
    #   INT32 correlation_id (4 bytes)
    header_prefix_len = 2 + 2 + 4
    request_data = b""
    while len(request_data) < header_prefix_len:
        chunk = client_socket.recv(header_prefix_len - len(request_data))
        if not chunk:
            break
        request_data += chunk
    if len(request_data) < header_prefix_len:
        print("Incomplete header prefix received!")
        client_socket.close()
        server.close()
        return
    # Unpack the essential header fields.
    api_key, api_version, correlation_id = struct.unpack('>h h i', request_data[:header_prefix_len])
    print(f"Received api_key: {api_key}, api_version: {api_version}, correlation_id: {correlation_id}")

    # Read and discard the rest of the request.
    bytes_read_so_far = header_prefix_len
    bytes_to_discard = request_size - bytes_read_so_far
    if bytes_to_discard > 0:
        print(f"Discarding {bytes_to_discard} bytes from request (client_id, tagged_fields, etc.)")
        while bytes_to_discard > 0:
            chunk = client_socket.recv(min(bytes_to_discard, 4096))
            if not chunk:
                break
            bytes_to_discard -= len(chunk)

    # Build ApiVersions response.
    if api_key == 18:  # ApiVersions Key
        # If the requested api_version is unsupported, error_code = 35.
        error_code = 35 if (api_version < 0 or api_version > 4) else 0

        if error_code != 0:
            # Error response: minimal body.
            # For error, we'll send: error_code (2 bytes), an empty api_keys array (encoded as 0 as compact array),
            # throttle_time_ms (4 bytes), and empty TAG_BUFFER (1 byte).
            body = struct.pack('>h', error_code)
            body += encode_unsigned_varint(0)  # empty compact array (length 0)
            body += struct.pack('>i', 0)         # throttle_time_ms = 0
            body += b'\x00'                      # empty TAG_BUFFER
        else:
            # Success response.
            # Encode error_code.
            body = struct.pack('>h', 0)
            # Encode api_keys as a compact array.
            # For one element, the compact array length is number_of_elements + 1 = 2.
            body += encode_unsigned_varint(2)
            # Encode one ApiVersion entry:
            #  api_key (INT16), min_version (INT16), max_version (INT16)
            entry = struct.pack('>h', 18) + struct.pack('>h', 0) + struct.pack('>h', 4)
            # For the element's TAG_BUFFER, use empty compact bytes (unsigned varint for length 0).
            entry += b'\x00'
            body += entry
            # Encode throttle_time_ms.
            body += struct.pack('>i', 0)
            # Encode TAG_BUFFER for the response (empty).
            body += b'\x00'

        # Total message size is size of correlation_id (4 bytes) + len(body).
        msg_size = 4 + len(body)
        # Construct complete response: message_size (4 bytes) + correlation_id (4 bytes) + body.
        response = struct.pack('>i', msg_size) + struct.pack('>i', correlation_id) + body
        client_socket.sendall(response)
        print(f"Sending response ({len(response)} bytes)")

    client_socket.shutdown(socket.SHUT_WR)
    client_socket.close()
    server.close()
    print("Connection closed.")

if __name__ == "__main__":
    main()
