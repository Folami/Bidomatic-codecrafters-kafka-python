import socket
import struct

def main():
    # Debug log.
    print("Logs from your program will appear here!")

    server = socket.create_server(("localhost", 9092), reuse_port=True)
    client_socket, client_address = server.accept()
    print(f"Connection from {client_address} has been established!")
    
    # Read the first 12 bytes from the request:
    # 4 bytes: message_size (unused)
    # Header v2:
    #   INT16 request_api_key (2 bytes)
    #   INT16 request_api_version (2 bytes)
    #   INT32 correlation_id (4 bytes)
    request_header = b""
    while len(request_header) < 12:
        chunk = client_socket.recv(12 - len(request_header))
        if not chunk:
            break
        request_header += chunk

    if len(request_header) < 12:
        print("Incomplete header received!")
        client_socket.close()
        server.close()
        return

    # Unpack in big-endian order: INT32, INT16, INT16, INT32.
    message_size, api_key, api_version, correlation_id = struct.unpack('>i h h i', request_header)
    print(f"Received correlation_id: {correlation_id}, requested api_version: {api_version}")

    # Determine error_code:
    # Broker supports ApiVersions request versions 0 to 4.
    error_code = 35 if (api_version < 0 or api_version > 4) else 0

    if error_code != 0:
        # In error case, send a minimal response body (error_code only).
        body = struct.pack('>h', error_code)
        total_bytes = 4 + len(body)  # 4 bytes for correlation_id + body
        response = struct.pack('>i i', total_bytes, correlation_id) + body
    else:
        # Build response body for a valid "ApiVersions" (v4) request.
        # Fixed layout (12 bytes):
        #  - error_code: 2 bytes (int16), here 0
        #  - Array length: 4 bytes (int32), here 1
        #  - One ApiVersion entry (6 bytes):
        #       api_key: 2 bytes (int16) - 18 for ApiVersions
        #       min_version: 2 bytes (int16) - 0
        #       max_version: 2 bytes (int16) - 4
        fixed_body = struct.pack('>h i h h h', 0, 1, 18, 0, 4)
        
        # Append TAG_BUFFER for flexible version (v4):
        # For empty tagged fields, encode as array length 0
        tag_buffer = struct.pack('>B', 0)  # Single byte with value 0
        body = fixed_body + tag_buffer
        
        # Total bytes after message_size = 4 (correlation_id) + len(body)
        total_bytes = 4 + len(body)
        response = struct.pack('>i i', total_bytes, correlation_id) + body

    # Send response.
    client_socket.sendall(response)
    client_socket.shutdown(socket.SHUT_WR)
    client_socket.close()
    server.close()

if __name__ == "__main__":
    main()
