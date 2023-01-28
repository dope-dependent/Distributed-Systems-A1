from urllib import response
import requests

class myConsumer:
    def __init__(self, topics: list or None, broker: str):
        self.register = {}
        self.broker = broker
        if topics is not None:
            self.registerForTopics(topics) 


    def registerForTopics(self, topics:list):
        for topic in topics:
            if topic not in self.register:
                url = self.broker + '/consumer/register'
                response = requests.post(url, json = {'topic':topic}, headers = {'Content-Type': 'application/json'})
                
                if response.status_code == 200:
                    self.register[topic] = response.json()['consumer_id']
                
                else:
                    print(f"Error in registering consumer: {topic} => {response.json()['message']}")


    def login(self, topics: dict):
        for topic in topics.keys():
            if topic not in self.register:
                url = self.broker + '/login'
                response = requests.get(url, json = {'topic':topic, 'id': topics[topic], 'type': "consumer"}, headers = {'Content-Type': 'application/json'})
                if response.status_code == 200:
                    self.register[topic] = topics[topic]
                else:
                    print(f"Error in logging in with topic {topic} => {response.json()['message']}")
            else:
                print(f'Already logged in with topic {topic}')

    def getID(self, topic: list or str):
        rv = dict()
        if type(topic) == str:
            if topic in self.register:
                rv[topic] = self.register[topic] 
            else:
                print(f'Error in getting ID => {topic} not registered by the consumer')
                rv[topic] = None
        else:
            for topix in topic:
                if topix in self.register:
                    rv[topix] = self.register[topix] 
                else:
                    print(f'Error in getting ID => {topix} not registered by the consumer')
                    rv[topix] = None

        return rv

    def getQueueSize(self, topic: str):
        url = self.broker + '/size'
        if topic not in self.register:
            print("Error in getting queue size => Topic not subscribed by the consumer")
            return None

        response = requests.get(url,json = {'topic':topic, 'consumer_id': self.register[topic]})

        if response.status_code == 200:
            return response.json()['size']

        else:
            print("Error in getting queue size => " + response.json()['message'])
            return None
    
    def getNextMessage(self, topic: str):
        url = self.broker + '/consumer/consume'
        if topic not in self.register:
            print("Error in getting next message => Topic not subscribed by the consumer")
            return None
        
        response = requests.get(url, json = {'topic': topic, 'consumer_id': self.register[topic]})

        if response.status_code == 200:
            return response.json()['message']
        
        else:
            print('Error in getting next message => ' + response.json()['message'])
            return None
        
    def getAllTopics(self):
        url = self.broker + '/topics'
        response = requests.get(url)

        return response.json()['topics']

class myProducer:
    def __init__(self, topics: list or None, broker: str):
        self.register = {}
        self.broker = broker
        if topics is not None:
            self.registerForTopics(topics) 
    

    def registerForTopics(self, topics:list):
        for topic in topics:
            if topic not in self.register:
                url = self.broker + '/producer/register'
                response = requests.post(url, json = {'topic':topic}, headers = {'Content-Type': 'application/json'})
                
                if response.status_code == 200:
                    self.register[topic] = response.json()['producer_id']
                
                else:
                    print(f"Error in creating topic: {topic} => {response.json()['message']}")

    def login(self, topics: dict):
        for topic in topics.keys():
            if topic not in self.register:
                url = self.broker + '/login'
                response = requests.get(url, json = {'topic':topic, 'id': topics[topic], 'type': "producer"}, headers = {'Content-Type': 'application/json'})
                if response.status_code == 200:
                    self.register[topic] = topics[topic]
                else:
                    print(response)
                    print(f"Error in logging in with topic {topic} => {response.json()['message']}")
            else:
                print(f'Already logged in with topic {topic}')

    def getID(self, topic: list or str):
        rv = dict()
        if type(topic) == str:
            if topic in self.register:
                rv[topic] = self.register[topic] 
            else:
                print(f'Error in getting ID => {topic} not registered by the producer')
                rv[topic] = None
        else:
            for topix in topic:
                if topix in self.register:
                    rv[topix] = self.register[topix] 
                else:
                    print(f'Error in getting ID => {topix} not registered by the producer')
                    rv[topix] = None

        return rv
    
    def sendNewMessage(self, topic: str, message: str):
        url = self.broker + '/producer/produce'
        if topic not in self.register:
            print("Error in producing message => Topic not subscribed by the producer")
            return None
        
        response = requests.post(url, json = {'topic': topic, 'producer_id': self.register[topic], 'message': message})

        if response.status_code == 200:
            return None
        
        else:
            print('Error in sending message => ' + response.json()['message'])
            return None


    def getAllTopics(self):
        url = self.broker + '/topics'
        response = requests.get(url)

        return response.json()['topics']
 