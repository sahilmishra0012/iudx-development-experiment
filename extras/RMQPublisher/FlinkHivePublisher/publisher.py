import pika
import datetime
import pytz
import json
IST = pytz.timezone('Asia/Kolkata')

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost', 5672))
channel = connection.channel()

exchange_name = "adaptor-test"
queue_name = "adaptor-test"
routing_key = 'adaptor-test'


channel.exchange_declare(exchange=exchange_name, exchange_type='direct')

result = channel.queue_declare(queue=queue_name)
channel.queue_bind(exchange=exchange_name, queue=result.method.queue)


for i in range(1000):
    data = {}
    # data['date'] = str(datetime.datetime.now())
    data['date'] = "test"
    data['name'] = 'HelloSahil!'+str(i)
    message = json.dumps(data)
    channel.basic_publish(exchange=exchange_name,
                        routing_key=routing_key, body=message)
    print(f" [x] Sent[{i}]: {message} \t {exchange_name} -> {queue_name}")


