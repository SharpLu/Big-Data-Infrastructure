from kafka import KafkaConsumer
consumer = KafkaConsumer('stock-analyzer',bootstrap_servers='192.168.99.100:9092')
for msg in consumer:
	print(msg)