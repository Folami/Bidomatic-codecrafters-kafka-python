import socket
import struct

def main():
    # Debug log.
    print("Logs from your program will appear here!")

    server = socket.create_server(("localhost", 9092), reuse_port=True)
    client_socket, client_address = server.accept()
    print(f"Connection from {client_address} has been established!")
    
    # For this stage:
    # - message_size: 4 bytes (any value works; we'll use 0)
    # - correlation_id: hard-coded to 7
    # All integers are 32-bit signed integers in big-endian order.
    message_size = struct.pack('>i', 0)
    correlation_id = struct.pack('>i', 7)
    response = message_size + correlation_id

    client_socket.sendall(response)
    
    client_socket.close()
    server.close()

if __name__ == "__main__":
    main()


#In this implementation, when a client connects and sends a request, the server responds with an eight-byte message:
#- Four bytes for the message size (here set to 0 for simplicity)
#- Four bytes for the correlation ID (7)
