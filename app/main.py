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
      - Otherwise, error_code is 0 and the response includes one entry 
        for ApiVersions (api_key 18, min_version 0, max_version 4).
      
    The response format:
      - Body:
          error_code: INT16 (2 bytes)
          api_keys (compact array): 1 byte (should be 2 for one element + 1)
          For success: one ApiVersion entry:
              api_key (INT16), min_version (INT16), max_version (INT16), TAG_BUFFER (1 byte, 0x00)
          throttle_time_ms: INT32 (4 bytes, 0)
          overall TAG_BUFFER: 1 byte (0x00)
      - Total successful body size: 15 bytes.
      - Full response: message_length (4 bytes) + correlation_id (4 bytes) + body.
    """
    error_code = 35 if (api_version < 0 or api_version > 4) else 0

    if error_code != 0:
        # Error body: 2 + 1 + 4 + 1 = 8 bytes.
        body = encode_big_endian('>h', error_code)
        body += encode_big_endian('>B', 0)  # compact array length = 0
        body += encode_big_endian('>i', 0)  # throttle_time_ms = 0
        body += b'\x00'                   # TAG_BUFFER
    else:
        # Successful body: 2 + 1 + (2+2+2+1) + 4 + 1 = 15 bytes.
        body = encode_big_endian('>h', error_code)
        # Fix: set compact array length to 2 (1 element + 1)
        body += encode_big_endian('>B', 2)
        entry = encode_big_endian('>h', 18) # api_key = 18
        entry += encode_big_endian('>h', 0)  # min_version = 0
        entry += encode_big_endian('>h', 4)  # max_version = 4
        entry += b'\x00'                   # entry TAG_BUFFER
        body += entry
        body += encode_big_endian('>i', 0)   # throttle_time_ms = 0
        body += b'\x00'                    # overall TAG_BUFFER

    # Total payload after message length field is: correlation_id (4 bytes) + body.
    msg_size = 4 + len(body)
    response = encode_big_endian('>i', msg_size)        # message_length
    response += encode_big_endian('>i', correlation_id)   # correlation_id
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
                if remaining_size > 0:
                    discard_remaining_request(client_socket, remaining_size, 0)
                
                # Process ApiVersions request.
                if api_key == 18:
                    response = build_api_versions_response(api_key, api_version, correlation_id)
                    client_socket.sendall(response)
                    print(f"Sent response ({len(response)} bytes)")
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
