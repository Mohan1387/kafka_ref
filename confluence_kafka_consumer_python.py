from confluent_kafka import Consumer, KafkaException, KafkaError
import sys
import getopt
import logging
from pprint import pformat

broker = '127.0.0.1:9092'
group = 'testid'
topics = ['first_topic']
# Consumer configuration
# See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
conf = {'bootstrap.servers': broker,
    'group.id': group,
        'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest', 'auto.commit.enable' : True, 'auto.commit.interval.ms' : 1000}
        }

# Create logger for consumer (logs will be emitted when poll() is called)
logger = logging.getLogger('consumer')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
logger.addHandler(handler)

# Create Consumer instance
# Hint: try debug='fetch' to generate some log messages
c = Consumer(conf, logger=logger)

def print_assignment(consumer, partitions):
   print('Assignment:', partitions)

# Subscribe to topics
c.subscribe(topics, on_assign=print_assignment)

# Read messages from Kafka, print to stdout
try:
    while True:
        msg = c.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            # Error or event
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                sys.stderr.write('%% %s [%d] reached end at offset %d\n' % (msg.topic(), msg.partition(), msg.offset()))
            else:
                # Error
                raise KafkaException(msg.error())
        else:
            # Proper message
            sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' % (msg.topic(), msg.partition(), msg.offset(),str(msg.key())))
            print(msg.value())

except KeyboardInterrupt:
    sys.stderr.write('%% Aborted by user\n')

finally:
    # Close down consumer to commit final offsets.
    c.close()
 
# to run the program
# python pythonconsumer.py



