from fileinput import filename
import threading
import time
from random import randint
from myqueue import myConsumer, myProducer

topic_1 = "T-1"
topic_2 = "T-2"
topic_3 = "T-3"

def consumer_func(consumer_id, topics : list):
    file_name = "./test_asgn1/consumer_" + str(consumer_id) + "_small.txt"
    log_file = open(file_name,"w")
    consumer = myConsumer(topics = topics, broker = "http://localhost:5000")
    
    while True:

        for topic in topics:
            time.sleep(1)
            log = consumer.getNextMessage(topic)
            if log is not None:
                log_file.write(log)

def producer_func(producer_id, topics: list):

    # if producer_id == 2:
        # time.sleep(10)

    producer = myProducer(topics = topics, broker = "http://localhost:5000")
    file_name = "./test_asgn1/producer_" + str(producer_id) + "_small.txt"
    log_file = open(file_name, "r")

    for log in log_file:

        sleep_time = randint(20,60) / 60
        time.sleep(sleep_time*2)
        topic = log.split()[3]
        producer.sendNewMessage(topic, log) 



# t1 = threading.Thread(target=producer_func, args=(1,['T-1', 'T-2', 'T-3']))
# t2 = threading.Thread(target=producer_func, args=(2,['T-1', 'T-3']))

t7 = threading.Thread(target=consumer_func, args=(2,['T-1', 'T-3']))
t8 = threading.Thread(target=consumer_func, args=(3,['T-1']))



# t1.start()
# t2.start()
t7.start()
t8.start()


# t1.join()
# t2.join()

t7.join()
t8.join()
