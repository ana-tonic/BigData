import csv
import time
from kafka import KafkaProducer
from json import dumps
import sys

if len(sys.argv) < 2:
    print("Please provide the name of the CSV file to open")
    sys.exit()
    
filename = sys.argv[1]
producer = KafkaProducer(bootstrap_servers='kafka:9092')

while True:
    with open(filename) as file:
        reader = csv.DictReader(file)
        for row in reader:
            message = dumps(row)
            producer.send('topic1', message.encode('utf-8'))
            time.sleep(0.001)
            print(message.encode('utf-8'))
