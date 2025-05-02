import socket
import struct

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
    #   COMPACT_STRING client_id (...)
    #   TAG_BUFFER tagged_fields (...)
    # We only need the first 8 bytes of the header for this stage after the size
    header_prefix_len = 2 + 2 + 4 # api_key + api_version + correlation_id
    request_data = b""
    # We read only the necessary header part first, ignoring client_id and tagged_fields for now
    # as ApiVersions request body is empty for v0-v4.
    # The total request size includes header + body. ApiVersions v4 has an empty body.
    # So we need to read request_size bytes in total after the size field.
    # Let's read the essential header part first (8 bytes).
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

    # Unpack the essential header fields: INT16, INT16, INT32.
    api_key, api_version, correlation_id = struct.unpack('>h h i', request_data[:header_prefix_len])
    print(f"Received api_key: {api_key}, api_version: {api_version}, correlation_id: {correlation_id}")

    # Read and discard the rest of the request body if any (client_id, tagged_fields, request body)
    # For ApiVersions v4, the request body is empty, but client_id and tagged_fields exist in header.
    bytes_read_so_far = header_prefix_len
    bytes_to_discard = request_size - bytes_read_so_far
    if bytes_to_discard > 0:
        print(f"Discarding {bytes_to_discard} bytes from request (client_id, tagged_fields, etc.)")
        while bytes_to_discard > 0:
            chunk = client_socket.recv(min(bytes_to_discard, 4096))
            if not chunk:
                print("Connection closed while discarding request data.")
                break
            bytes_to_discard -= len(chunk)

    # --- Start Response Logic ---

    # Check if the request is for ApiVersions (key 18)
    if api_key == 18: # ApiVersions Key
        # Determine error_code:
        # Broker supports ApiVersions request versions 0 to 4.
        error_code = 0 # Assume success initially
        # Note: Kafka protocol versions are inclusive. 0 <= api_version <= 4 is valid.
        # The check in the original code was correct.
        if api_version < 0 or api_version > 4:
             error_code = 35 # UNSUPPORTED_VERSION

        if error_code != 0:
            # In error case, send a minimal response body (error_code only).
            # ApiVersions Response Body (for error): ErrorCode (INT16)
            # For v3+, it also includes ThrottleTimeMs (INT32) and Tagged Fields (TAG_BUFFER)
            # Let's stick to the minimal required for the error code itself first.
            # However, the client expects a v3+ structure if it requested v3+
            body = struct.pack('>h', error_code) # Error Code
            if api_version >= 3:
                 body += struct.pack('>i', 0) # ThrottleTimeMs
                 body += b'\x00' # Tagged Fields (empty)
            # Note: Even in error cases, the array might be expected by some clients.
            # Let's refine: Send full structure but with error code set.
            body = (
                struct.pack('>h', error_code) +       # error_code
                struct.pack('>i', 0) +                # api_keys array length = 0
                # No entries if length is 0
                struct.pack('>i', 0) +                # throttle_time_ms = 0
                b'\x00'                               # tagged_fields (empty)
            )

        else:
            # Build response body for a valid "ApiVersions" (v3+) request:
            # - error_code: INT16 (2 bytes)
            # - api_keys array: COMPACT_ARRAY_LEGACY (INT32 length prefix)
            #     - length: INT32 (4 bytes)
            #     - entries: array of:
            #         - api_key: INT16 (2 bytes)
            #         - min_version: INT16 (2 bytes)
            #         - max_version: INT16 (2 bytes)
            # - throttle_time_ms: INT32 (4 bytes) [v1+]
            # - tagged_fields: TAG_BUFFER [v3+] (starts with UNSIGNED_VARINT count)
            body = (
                struct.pack('>h', 0) +                # error_code = 0
                struct.pack('>i', 1) +                # api_keys array length = 1 (INT32 for COMPACT_ARRAY_LEGACY)
                struct.pack('>h h h', 18, 0, 4) +     # one ApiVersion entry (ApiKey=18, Min=0, Max=4)
                struct.pack('>i', 0) +                # throttle_time_ms = 0
                b'\x00'                               # tagged_fields (NumTaggedFields=0 encoded as UNSIGNED_VARINT)
            )

        # Response Header v1 (since Request Header was v2):
        #   CorrelationID (INT32)
        #   Tagged Fields (TAG_BUFFER)
        header_correlation_id = struct.pack('>i', correlation_id)
        header_tagged_fields = b'\x00' # Empty tagged fields for header (NumTaggedFields=0 encoded as UNSIGNED_VARINT)

        # Message Size = Length of (CorrelationID + Header Tagged Fields + Body)
        message_size = len(header_correlation_id) + len(header_tagged_fields) + len(body)

        # Response = MessageSize + Header + Body
        response = struct.pack('>i', message_size) + header_correlation_id + header_tagged_fields + body

    else:
        # Handle other API keys (unsupported for now)
        # We can send back an UNSUPPORTED_API_KEY error or just close connection
        # For now, let's just close. In a real broker, we'd send an error response.
        print(f"Unsupported api_key: {api_key}")
        response = b"" # Or construct an error response if needed

    # Send response if one was constructed
    if response:
        print(f"Sending response ({len(response)} bytes)")
        client_socket.sendall(response)
    else:
        print("No response generated.")

    # Gracefully close connection
    try:
        client_socket.shutdown(socket.SHUT_WR)
    except OSError as e:
        print(f"Socket shutdown error: {e}") # Ignore errors if socket already closed
    client_socket.close()
    server.close() # Close server socket after handling one connection for this simple example
    print("Connection closed.")


if __name__ == "__main__":
    main()
