import sys
from time import sleep
import uuid
from collections import MutableMapping
import capnp
import tango_capnp
from kafka import KafkaClient, KafkaConsumer, SimpleProducer
from kafka.common import ConsumerTimeout

BROKERS = ['192.168.33.10:9092', '192.168.33.11:9092', '192.168.33.12:9092']

class TangoRuntime(object):
    def __init__(self, topic):
        self.topic = topic
        self.consumer = KafkaConsumer(topic,
                         bootstrap_servers=BROKERS,
                         consumer_timeout_ms=0)
        self.producer = SimpleProducer(KafkaClient(BROKERS))
        
    def query_helper(self, obj):
        # catch up to tail of log and apply all pending updates
        try:
            for message in self.consumer:
                # message value is raw byte string -- decode if necessary!
                # e.g., for unicode: `message.value.decode('utf-8')`
                obj.apply(message.value)
                print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                     message.offset, message.key,
                                                     message.value))
        except ConsumerTimeout:
            pass

    def update_helper(self, data):
        response = self.producer.send_messages(bytes(self.topic), bytes(data))[0]
        print "topic: %s\npartition: %s\nerror: %s\noffset: %s" % (response.topic, response.partition, response.error, response.offset)

class TangoMap(MutableMapping):
    def __init__(self, runtime):
        self.runtime = runtime
        self.dict = dict()

    def apply(self, encoded_record):
        tango_record = tango_capnp.TangoMapRecord.from_bytes(encoded_record)
        which = tango_record.payload.which()
        if which == 'update':
            self.dict[tango_record.payload.update.key] = tango_record.payload.update.value
        elif which == 'delete':
            key = tango_record.payload.delete.key
            # if we initiated the delete locally, then we already deleted it from our dict
            if key in self.dict:
                del self.dict[key]
        else:
            raise UnknownRecordType()

    def make_update_record(self, key, value):
        update_record = tango_capnp.TangoMapRecord.new_message()
        update = update_record.payload.init('update')
        update.key = key
        update.value = value
        return update_record.to_bytes()

    def make_delete_record(self, key):
        delete_record = tango_capnp.TangoMapRecord.new_message()
        delete = delete_record.payload.init('delete')
        delete.key = key
        return delete_record.to_bytes()

    def __getitem__(self, key):
        self.runtime.query_helper(self)
        return self.dict.get(key)

    def __setitem__(self, key, value):
        self.runtime.update_helper(self.make_update_record(key, value))
        self.dict[key] = value

    def __delitem__(self, key):
        self.runtime.update_helper(self.make_delete_record(key))
        del self.dict[key]

    def __iter__(self):
        self.runtime.query_helper(self)
        return iter(self.dict)

    def __len__(self):
        self.runtime.query_helper(self)
        return len(self.dict)


class UnknownRecordType(Exception):
    pass

if __name__ == "__main__":
    runtime = TangoRuntime('tango-topic-1')
    tango_map = TangoMap(runtime)
    for k, v in tango_map.iteritems():
        print k, v
    tango_map["foo"] = "bar"
    for k, v in tango_map.iteritems():
        print k, v
    tango_map["foo"] = "baz"
    for k, v in tango_map.iteritems():
        print k, v
    tango_map["quux"] = "fnord"
    for k, v in tango_map.iteritems():
        print k, v
    del tango_map["foo"]
    for k, v in tango_map.iteritems():
        print k, v
