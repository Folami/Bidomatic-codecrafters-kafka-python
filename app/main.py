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
    print(f"Received correlation_id: {correlation_id}")

    # Build response:
    # message_size: 4 bytes (any value works; we'll use 0)
    # correlation_id: from the request header.
    msg_size = struct.pack('>i', 0)
    resp_corr_id = struct.pack('>i', correlation_id)
    response = msg_size + resp_corr_id

    client_socket.sendall(response)
    # Gracefully shutdown the write side so the client can read the full response.
    client_socket.shutdown(socket.SHUT_WR)
    
    client_socket.close()
    server.close()

if __name__ == "__main__":
    main()
