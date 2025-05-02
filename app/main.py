import socket
import struct

def main():
    # Debug log.
    print("Logs from your program will appear here!")

    server = socket.create_server(("localhost", 9092), reuse_port=True)
    client_socket, client_address = server.accept()
    print(f"Connection from {client_address} has been established!")
    
    # Read the first 12 bytes from the request:
    # 4 bytes: message_size (we don't use this value).
    # Next, request header v2:
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
    # If the requested ApiVersions request version is unsupported (broker supports versions 0 to 4),
    # then error_code should be 35 ("UNSUPPORTED_VERSION"); otherwise, 0.
    error_code = 35 if (api_version < 0 or api_version > 4) else 0

    # For a valid "ApiVersions" request (error_code == 0), we build a response body with one entry.
    # Layout of response body:
    # - error_code: 2 bytes (int16)
    # - Array length: 4 bytes (int32); here we use 1
    # - One ApiVersion entry (6 bytes):
    #      api_key: 2 bytes (int16) - use 18 for ApiVersions.
    #      min_version: 2 bytes (int16) - for example, 0.
    #      max_version: 2 bytes (int16) - must be at least 4.
    #
    # Total body length = 2 + 4 + 6 = 12 bytes.
    # The response header is 4 bytes (correlation_id), so total bytes after message_size = 16.
    # That means the first 4 bytes (message_size field) must contain 16.
    
    if error_code != 0:
        # In case of error (unsupported version) we send a minimal response body: error_code only.
        body = struct.pack('>h', error_code)
        # The header still includes the correlation_id.
        total_bytes = 4 + len(body)  # 4 bytes for correlation_id + body
        response = struct.pack('>i i', total_bytes, correlation_id) + body
    else:
        # Build response body.
        api_versions_count = 1
        api_key_entry = 18   # ApiVersions API key.
        min_version = 0
        max_version = 4
        body = struct.pack('>h i h h', 0, api_versions_count, api_key_entry, min_version) + struct.pack('>h', max_version)
        total_bytes = 4 + len(body)  # 4 bytes correlation_id + 12 bytes body = 16
        response = struct.pack('>i i', total_bytes, correlation_id) + body

    client_socket.sendall(response)
    # Gracefully shutdown the write side so the client can read the complete response.
    client_socket.shutdown(socket.SHUT_WR)
    
    client_socket.close()
    server.close()

if __name__ == "__main__":
    main()
