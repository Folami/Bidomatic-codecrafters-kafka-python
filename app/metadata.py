import uuid
from collections import defaultdict
from.parser import ByteParser

BASE_UUID = uuid.UUID("00000000-0000-0000-0000-000000000000")

class Metadata:
    def __init__(self, file):
        self.parser = ByteParser(file)
        self.batches = 0
        self.topics = defaultdict(lambda: {"uuid": BASE_UUID, "partitions": []})
        self.partitions = {}
        self.parse_log_file()
        for id in self.partitions.keys():
            topic_uuids = self.partitions[id]["topics"]
            for uuid in topic_uuids:
                for topic in self.topics:
                    if uuid == self.topics[topic]["uuid"]:
                        self.topics[topic]["partitions"].append(id)

    def parse_log_file(self):
        batches = self.separate_batches()
        for batch in batches:
            self.parse_batch(batch)

    def separate_batches(self):
        parser = self.parser
        batches = []
        while not parser.finished:
            offset = parser.consume(8)
            print(f"offset {int.from_bytes(offset)}")
            self.batches += 1
            batch_length = int.from_bytes(parser.consume(4))
            batches.append(parser.consume(batch_length))
        return batches

    def parse_batch(self, batch):
        parser = ByteParser(batch)
        ple = parser.consume(4)
        mb = parser.consume(1)
        crc = parser.consume(4)
        type = parser.consume(2)
        offset = parser.consume(4)
        length = int.from_bytes(offset) + 1
        created_at = parser.consume(8)
        updated_at = parser.consume(8)
        p_id = parser.consume(8)
        p_epoch = parser.consume(2)
        base_sequence = parser.consume(4)
        record_count = parser.consume(4)
        records = self.separate_records(parser)
        for record in records:
            self.parse_record(record)

    def separate_records(self, parser):
        records = []
        while not parser.eof():
            length = parser.consume_svarint()
            record = parser.consume(length)
            records.append(record)
        return records

    def parse_record(self, batch):
        parser = ByteParser(batch)
        attribute = parser.consume(1)
        timestamp_delta = parser.consume_svarint()
        offset_delta = parser.consume_svarint()
        key_length = parser.consume_svarint()
        if key_length != -1:
            parser.read(1)
        value_length = parser.consume_svarint()
        value = parser.consume(value_length)
        self.parse_value(value)

    def parse_value(self, value):
        parser = ByteParser(value)
        frame_version = parser.consume(1)
        type = int.from_bytes(parser.consume(1))
        match type:
            case 2:
                self.parse_topic(parser)
            case 3:
                self.parse_partition(parser)

    def parse_topic(self, parser):
        version = parser.consume(1)
        length_of_name = parser.consume_svarint(False) - 1
        topic_name = parser.consume(length_of_name)
        raw_uuid = parser.consume(16)
        self.topics[topic_name] = {"uuid": uuid.UUID(bytes=raw_uuid), "partitions": []}

    def parse_partition(self, parser):
        version = parser.consume(1)
        partition_id = parser.consume(4)
        raw_uuid = parser.consume(16)
        length_of_replica_array = parser.consume_svarint(False) - 1
        replicas = self.digest_array(parser, parser.consume_svarint(False) - 1, 4)
        in_sync = self.digest_array(parser, parser.consume_svarint(False) - 1, 4)
        removing = self.digest_array(parser, parser.consume_svarint(False) - 1, 4)
        adding = self.digest_array(parser, parser.consume_svarint(False) - 1, 4)
        leader = parser.consume(4)
        epoch = parser.consume(4)
        partition_epoch = parser.consume(4)
        directories = self.digest_array(parser, parser.consume_svarint(False) - 1, 4)
        if partition_id in self.partitions:
            self.partitions[partition_id]["topics"].append(uuid.UUID(bytes=raw_uuid))
        else:
            self.partitions[partition_id] = {
                "topics": [uuid.UUID(bytes=raw_uuid)],
                "id": partition_id,
                "leader": leader,
                "leader_epoch": epoch,
            }

    def digest_array(self, parser, length, size_per_item):
        ret = []
        for _ in range(length):
            ret.append(parser.consume(size_per_item))
        return ret