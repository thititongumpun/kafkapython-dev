from json import dumps
import sys
from kafka import KafkaConsumer
from line import sendMsgToLine
# from kafka.errors import KafkaError
# consumer = KafkaConsumer('test1',
#                           group_id='group.id',
#                           bootstrap_servers=['TSITLRBK1:9093', 'TSITLRZK1:9093'],
#                           security_protocol='SASL_PLAINTEXT',
#                           sasl_mechanism='PLAIN',                          
#                           sasl_plain_username='trc_platform',
#                           sasl_plain_password='p@ssw0rd',
#                           auto_offset_reset='earliest', enable_auto_commit=False)

#ssl
consumer = KafkaConsumer('test1',
                          group_id='group.id',
                          bootstrap_servers=['TSITLRBK1:9091', 'TSITLRZK1:9091'],
                          security_protocol='SASL_SSL',
                          ssl_check_hostname=True,
                          ssl_cafile='./certs/ca.crt',
                          ssl_certfile='./certs/kafka_broker.crt',
                          ssl_keyfile='./certs/kafka_broker_key.pem',
                          api_version=(2,0,2),
                          # value_deserializer=lambda x: dumps(x).encode('utf-8'),
                          sasl_mechanism='PLAIN',                          
                          sasl_plain_username='trc_platform',
                          sasl_plain_password='p@ssw0rd',
                          auto_offset_reset='earliest', enable_auto_commit=False)
# for msg in consumer:
#   print (msg)

def consume_msg():
    try:
        for message in consumer:
          sendMsgToLine(message.topic, message.partition, message.offset, message.key, message.value)
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise
consume_msg()

