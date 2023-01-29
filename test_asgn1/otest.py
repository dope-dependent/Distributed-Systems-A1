import threading
import time
from random import randint
from myqueue import myConsumer, myProducer

topic_1 = "T-1"
topic_2 = "T-2"
topic_3 = "T-3"

def consumer_func(consumer_id, topics : list):
    consumer = myConsumer(topics = topics, broker = "http://localhost:5000")
    

def producer_func(producer_id, topics: list):
    # producer = myProducer(topics = topics, broker = "http://localhost:5000")
    file_name = "./test_asgn1/producer_" + str(producer_id) + ".txt"
    print(file_name)
    # log_file = open(file_name, "r")

    # for line in log_file:

    #     sleep_time = rand(20,60) / 60
    #     time.sleep(sleep_time)

    #     print(line)

producer_func(1, [])