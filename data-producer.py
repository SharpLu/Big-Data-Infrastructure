# 1 connect to any kafka broker
# 2 fetch stock price every second

from googlefinance import getQuotes
from kafka import KafkaProducer
from kafka.errors import KafkaError
import argparse
import logging
import json
import time
import schedule
import atexit

#logging configuration
logging.basicConfig()
logger=logging.getLogger('data-producer')
logger.setLevel(logging.DEBUG)

symbol ='AAPL'
kafka_broker='192.168.99.100:9092'
topic ='stock-analyzer'

def fetch_price(producer,symbol):
    logger.debug('Start to fetch price for %s' % symbol)
    price= json.dumps(getQuotes(symbol))
    producer.send(topic=topic,value=price, timestamp_ms=time.time())
    logger.debug('sent stock price for %s, price is %s' % (symbol,price))

def shut_down(producer):
    logger.debug('exit program')
    producer.flush(10)
    producer.close()
    logger.debug('Kafka producer closed, exiting')

if __name__ == '__main__':
    # - setup command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('symbol', help='the stock symbol, such as AAPL')
    parser.add_argument('kafka_broker', help='the location of the kafka broker')
    parser.add_argument('topic', help='the kafka topic push to')

    # - parse arguments
    args = parser.parse_args()
    symbol=args.symbol
    topic = args.topic
    kafka_broker = args.kafka_broker

    logger.debug('stock symbox is %s' % symbol)

    producer = KafkaProducer(
        bootstrap_servers=kafka_broker
    )

    producer.send(topic=topic,value='Hello from Bittiger')
    # fetch_price(producer,'AAPL')


    schedule.every(1).second.do(fetch_price, producer, symbol)

    # - setup proper shutdown hook
    atexit.register(shut_down, producer)

    while True:
        schedule.run_pending()
        time.sleep(1)