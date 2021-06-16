import pika
import sys
import time
import numpy as np
import random
import json


def generate_sine(points, threshold=0.08):
	time = np.arange(0, points/10, 0.1);
	pure = np.sin(time)
	noise = np.random.normal(0, 1, points)
	signal = pure + noise*threshold
	return signal

def generate_cat():
	classes = ["A", "B", "C", "D", "E"]
	return random.choice(classes)


def main():
	exchange_name = "adaptor-test"
	queue_name = "adaptor-test"
	routing_key = 'adaptor-test'

	connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
	channel = connection.channel()

	channel.exchange_declare(exchange=exchange_name, exchange_type='direct')

	result = channel.queue_declare(queue=queue_name)
	channel.queue_bind(exchange=exchange_name, queue=result.method.queue)


	sine_signal = generate_sine(iterations)

	for i in range(iterations):    
		data = {}
		data['key'] = i
		data['signal'] = float(round(sine_signal[i], 4))
		data['deviceId'] = generate_cat()
		message = json.dumps(data)

		channel.basic_publish(exchange=exchange_name, routing_key=routing_key, body=message)
		print(f" [x] Sent[{i}]: {message} \t {exchange_name} -> {queue_name}")



if __name__ == "__main__":
	iterations = 10
	main()
