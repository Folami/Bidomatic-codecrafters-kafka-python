MSB_SET_MASK = 0b10000000
REMOVE_MSB_MASK = 0b01111111

class ByteParser:
    def __init__(self, data: bytes):
        self.data = data
        self.index = 0  # Current position in the byte stream
        self.finished = False

    def eof(self):
        return self.index == len(self.data)

    def check_is_finished(self):
        self.finished = self.index == len(self.data)

    def read(self, num_bytes: int) -> bytes:
        """Read num_bytes from the current position."""
        if self.index + num_bytes > len(self.data):
            raise ValueError("Not enough bytes to read")
        result = self.data[self.index : self.index + num_bytes]
        return result

    def consume(self, num_bytes: int) -> bytes:
        """Read num_bytes from the current position."""
        if self.index + num_bytes > len(self.data):
            raise ValueError("Not enough bytes to read")
        result = self.data[self.index : self.index + num_bytes]
        self.index += num_bytes
        self.check_is_finished()
        return result

    def skip(self, num_bytes: int) -> None:
        """Skip num_bytes in the stream by advancing the index."""
        if self.index + num_bytes > len(self.data):
            raise ValueError("Not enough bytes to skip")
        self.index += num_bytes
        self.check_is_finished()

    def remaining(self) -> int:
        """Return the number of remaining bytes."""
        return len(self.data) - self.index

    def reset(self) -> None:
        """Reset the index to the start."""
        self.index = 0
        self.finished = False

    def consume_svarint(self, signed=True):
        shift = 0
        value = 0
        aux = MSB_SET_MASK
        index = self.index
        record = b""
        while aux & MSB_SET_MASK:
            aux = self.data[index]
            record += aux.to_bytes()
            value += (aux & REMOVE_MSB_MASK) << shift
            index += 1
            shift += 7
        if signed:
            lsb = value & 0x01
            if lsb:
                value = -1 * ((value + 1) >> 1)
            else:
                value = value >> 1
        self.index = index
        self.check_is_finished()
        return value