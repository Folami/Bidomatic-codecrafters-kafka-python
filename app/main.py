"""
Kafka Clone - APIVersions Handler

This module implements a simple Kafka broker that supports the ApiVersions request.
It reads incoming requests (which may be sent sequentially on the same connection),
parses the header, discards the request body, and sends back a response.

Supported ApiVersions:
  - Only the ApiVersions API (api_key 18) is processed.
  - The broker supports versions 0 to 4. If the request_api_version is outside this range,
    an error code 35 ("UNSUPPORTED_VERSION") is returned.
    
Response encoding:
  The response is built in Kafka's flexible (compact) message format for ApiVersions v4:
    - error_code: INT16 (2 bytes)
    - api_keys: compact array:
         Length: Unsigned varint (1 byte); for one entry, this equals (1+1)=2.
         Entry: api_key (INT16), min_version (INT16), max_version (INT16), entry TAG_BUFFER (1 byte = 0x00)
    - throttle_time_ms: INT32 (4 bytes, value 0)
    - Response TAG_BUFFER: compact bytes (empty, encoded as 1 byte 0x00)
    
Overall, the response body length is 15 bytes. Since a 4‐byte correlation id is prepended,
the message length field (the first 4 bytes) is 19.
Combined with the 4-byte message_length field, the total transmission is 23 bytes.
"""

import socket
import struct

def encode_unsigned_varint(n):
    """
    Encodes a small unsigned integer as a varint.
    For n < 128, it's encoded in a single byte.
    """
    return struct.pack('>B', n)

def read_n_bytes(sock, n):
    """
    Reads exactly n bytes from the socket.
    Returns the n bytes or raises an IOError if the connection is closed early.
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
    The header consists of:
      - 4 bytes: request message size (unused)
      - 2 bytes: request_api_key (INT16)
      - 2 bytes: request_api_version (INT16)
      - 4 bytes: correlation_id (INT32)
    Returns:
      (request_api_key, request_api_version, correlation_id, request_size)
    """
    # Read first 4 bytes for message size.
    request_size_bytes = read_n_bytes(sock, 4)
    request_size = struct.unpack('>i', request_size_bytes)[0]
    # Read header prefix (8 bytes) for api_key, api_version, correlation_id.
    header_prefix_len = 2 + 2 + 4
    header_prefix_bytes = read_n_bytes(sock, header_prefix_len)
    api_key, api_version, correlation_id = struct.unpack('>h h i', header_prefix_bytes)
    return api_key, api_version, correlation_id, request_size

def discard_remaining_request(sock, total_request_size, bytes_read):
    """
    Discards remaining bytes from the request (e.g., client_id, tagged_fields, etc.)
    """
    remaining = total_request_size - bytes_read
    while remaining > 0:
        chunk = sock.recv(min(remaining, 4096))
        if not chunk:
            break
        remaining -= len(chunk)

def build_api_versions_response(api_key, api_version, correlation_id):
    """
    Constructs the ApiVersions response.
    For api_key 18:
      - If request_api_version is unsupported (< 0 or > 4), error_code is 35.
      - Otherwise, error_code is 0 and the response includes one API entry for 
        ApiVersions (api_key 18, min_version 0, max_version 4).
    
    The response is encoded in Kafka's flexible format:
      - Response body:
            error_code: INT16 (2 bytes)
            api_keys: compact array:
                Length (unsigned varint): for one element, value = 2 (0x02)
                One ApiVersion entry:
                   api_key (INT16), min_version (INT16), max_version (INT16), TAG_BUFFER (1 byte, 0x00)
            throttle_time_ms: INT32 (4 bytes, value 0)
            response TAG_BUFFER: 1 byte (0x00)
      - The complete payload (after the message_length field) consists of 4 bytes correlation_id followed by 15 bytes body.
      - Thus message length = 4 + 15 = 19.
      
    Returns the complete response bytes.
    """
    error_code = 35 if (api_version < 0 or api_version > 4) else 0
    
    if error_code != 0:
        # Error response body: 2 + 1 + 4 + 1 = 8 bytes.
        body = struct.pack('>h', error_code)
        body += encode_unsigned_varint(0)        # empty array
        body += struct.pack('>i', 0)               # throttle_time_ms
        body += b'\x00'                          # response TAG_BUFFER
    else:
        # Successful response body: 2 + 1 + (2+2+2+1) + 4 + 1 = 15 bytes.
        body = struct.pack('>h', 0)                # error_code = 0
        body += encode_unsigned_varint(2)          # compact array length = 2 (one element + 1)
        entry = struct.pack('>h', 18)              # api_key = 18
        entry += struct.pack('>h', 0)              # min_version = 0
        entry += struct.pack('>h', 4)              # max_version = 4
        entry += b'\x00'                         # entry TAG_BUFFER = empty
        body += entry
        body += struct.pack('>i', 0)               # throttle_time_ms = 0
        body += b'\x00'                          # overall TAG_BUFFER = empty
        
    # Message (after the 4-byte message_length field) is: 4 bytes (correlation_id) + body.
    msg_size = 4 + len(body)
    response = struct.pack('>i', msg_size)         # message length field
    response += struct.pack('>i', correlation_id)    # correlation id
    response += body
    return response

def run_server():
    """
    Runs the Kafka clone server on port 9092, processing multiple sequential
    ApiVersions (v4) requests from the same client.
    
    For each request:
      - Reads a fixed header (12 bytes: 4 byte length field and 8 bytes header prefix)
      - Discards any extra request data (if present)
      - Sends an ApiVersions response based on the api_version and correlation_id.
    
    Continues processing until the client closes the connection.
    """
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    print("Server listening on port 9092")
    
    client_socket, client_address = server.accept()
    print(f"Connection from {client_address} has been established!")
    
    try:
        while True:
            try:
                api_key, api_version, correlation_id, request_size = read_request_header(client_socket)
            except IOError:
                # Likely the client has closed the connection.
                break
            print(f"Received api_key: {api_key}, api_version: {api_version}, correlation_id: {correlation_id}")
            print(f"Received request size: {request_size}")
            
            # NOTE: The request_size field does not include the initial 4-byte length field.
            # We've already read 8 bytes (the header prefix) after the length field.
            # Therefore, the extra (body) bytes to discard = request_size - 8.
            discard_remaining_request(client_socket, request_size, 8)
            
            if api_key == 18:
                response = build_api_versions_response(api_key, api_version, correlation_id)
                client_socket.sendall(response)
                print(f"Sent response ({len(response)} bytes)")
            else:
                print("Unknown api_key, no response sent.")
    except Exception as e:
        print("Error while processing requests:", e)
    finally:
        try:
            client_socket.shutdown(socket.SHUT_WR)
        except:
            pass
        client_socket.close()
        server.close()
        print("Connection closed.")

def run():
    """
    Main entry point for the script.
    Sets up the server and starts listening for incoming connections.
    Prints logs to the console for each request and response.
    Handles any exceptions that occur during processing.
    """
    print("Logs from your program will appear here!")
    try:
        run_server()
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    run()


# The code is designed to run as a standalone script.
# It will start a server that listens for incoming connections on port 9092.
# The server will process ApiVersions requests and respond accordingly.
# The server will continue running until interrupted or the client closes the connection.
# The server is designed to handle multiple requests from the same client sequentially.
# The server will print logs to the console for each request received and response sent.
# The server will also handle any exceptions that occur during processing and print error messages.
# The server will close the connection gracefully when finished.
# The server is designed to be simple and easy to understand, focusing on the ApiVersions request handling.
# The server is not intended to be a complete Kafka implementation, but rather a simplified version for educational purposes.
# The server is designed to be run in a controlled environment for testing and development purposes.
# The server is not intended for production use and should not be used in a production environment.
