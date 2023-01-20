from flask import Flask, request, jsonify
import json
from collections import deque

app = Flask(__name__)



# class Topics:
all_topics = {}

class Topic:
    def __init__(self, name):
        self.id = len(all_topics)+1
        self.consumerIndex = 0
        self.producerIndex = 0
        self.name = name
        self.consumers = set()
        self.producers = set()
        self.queue = deque()

    def createConsumer(self):
        id = (self.id * 10**12 + self.consumerIndex) * 10 + 1
        self.consumerIndex += 1
        self.consumers.add(id)

        return id

    def createProducer(self):
        id = (self.id * 10**12 + self.producerIndex) * 10
        self.producerIndex += 1
        self.producers.add(id)

        return id

# a
@app.route("/topics", methods = ["POST"])
def createTopic():
    data = request.json
    if "name" in data:
        if data["name"] in all_topics:
            response = app.response_class(
                response=json.dumps({
                    "status": "failure", 
                    "message": f'Bad request: name already present'
                }),
                status=500,
                mimetype='application/json'
            )
        else:
            response = app.response_class(
                response=json.dumps({
                    "status": "success", 
                    "message": f'Topic {data["name"]} created successfully'
                }),
                status=200,
                mimetype='application/json'
            )
            
            all_topics[data["name"]] = Topic(data["name"])
    else:
        response = app.response_class(
            response=json.dumps({
                "status": "failure", 
                "message": f'Bad request: name not sent'
            }),
            status=500,
            mimetype='application/json'
        )
    print(response)
    return response   

# b
@app.route("/topics", methods = ['GET'])
def ListTopics():
    response = app.response_class(
        response=json.dumps({
            "topics": [x.name for x in list(all_topics.values())]
        }),
        status=200,
        mimetype='application/json'
    )
    return response

# c

@app.route("/consumer/register", methods = ["POST"])
def registerConsumer():
    data = request.json
    if "topic" in data and data["topic"] in all_topics:

        consumer_id = all_topics[data["topic"]].createConsumer()

        response = app.response_class(
            response=json.dumps({
                "status": "success", 
                "consumer_id": consumer_id
            }),
            status=200,
            mimetype='application/json'
        )
            
    else:
        response = app.response_class(
            response=json.dumps({
                "status": "failure", 
                "message": f'Bad request: topic not sent or topic not present'
            }),
            status=500,
            mimetype='application/json'
        )
    print(response)
    return response


# d
@app.route("/producer/register", methods = ["POST"])
def registerProducer():
    data = request.json
    if "topic" in data:
        if data["topic"] not in all_topics:
            all_topics[data["topic"]] = Topic(data["topic"])

        producer_id = all_topics[data["topic"]].createProducer()

        response = app.response_class(
            response=json.dumps({
                "status": "success", 
                "producer_id": producer_id
            }),
            status=200,
            mimetype='application/json'
        )
            
    else:
        response = app.response_class(
            response=json.dumps({
                "status": "failure", 
                "message": f'Bad request: topic not sent'
            }),
            status=500,
            mimetype='application/json'
        )
    print(response)
    return response


# e
@app.route("/producer/produce", methods = ["POST"])
def enqueueMessage():
    data = request.json
    if "topic" in data and "producer_id" in data and "message" in data:
        if data["topic"] not in all_topics:
            response = app.response_class(
                response=json.dumps({
                    "status": "failure", 
                    "message": f'Bad request: topic not present'
                }),
                status=500,
                mimetype='application/json'
            )
        
        elif data["producer_id"] not in all_topics[data["topic"]].producers:
            response = app.response_class(
                response=json.dumps({
                    "status": "failure", 
                    "message": f'Bad request: topic not subscribed by producer'
                }),
                status=500,
                mimetype='application/json'
            )
        
        else:
            all_topics[data["topic"]].queue.appendleft(data["message"])
            print(all_topics[data["topic"]].queue)
            response = app.response_class(
                response=json.dumps({
                    "status": "success"
                }),
                status=200,
                mimetype='application/json'
            )
            
    else:
        response = app.response_class(
            response=json.dumps({
                "status": "failure", 
                "message": f'Bad request: topic not sent'
            }),
            status=500,
            mimetype='application/json'
        )
    print(response)
    return response


# f

@app.route('/consumer/consume',methods=["GET"])
def dequeueMessage():
    data = request.json
    if "topic" in data and "consumer_id" in data:
        if data["topic"] not in all_topics:
            response = app.response_class(
                response=json.dumps({
                    "status": "failure", 
                    "message": f'Bad request: topic not present'
                }),
                status=500,
                mimetype='application/json'
            )
        
        elif data["consumer_id"] not in all_topics[data["topic"]].consumers:
            response = app.response_class(
                response=json.dumps({
                    "status": "failure", 
                    "message": f'Bad request: topic not subscribed by consumer'
                }),
                status=500,
                mimetype='application/json'
            )
        
        else:
            message = all_topics[data["topic"]].queue.pop()
            print(message)
            response = app.response_class(
                response=json.dumps({
                    "status": "success",
                    "message": message
                }),
                status=200,
                mimetype='application/json'
            )
            
    else:
        response = app.response_class(
            response=json.dumps({
                "status": "failure", 
                "message": f'Bad request: topic not sent'
            }),
            status=500,
            mimetype='application/json'
        )
    print(response)
    return response

# g

@app.route('/size',methods = ['GET'])
def size():
    data = request.json
    if "topic" in data and "consumer_id" in data:
        if data["topic"] not in all_topics:
            response = app.response_class(
                response=json.dumps({
                    "status": "failure", 
                    "message": f'Bad request: topic not present'
                }),
                status=500,
                mimetype='application/json'
            )
        
        elif data["consumer_id"] not in all_topics[data["topic"]].consumers:
            response = app.response_class(
                response=json.dumps({
                    "status": "failure", 
                    "message": f'Bad request: topic not subscribed by consumer'
                }),
                status=500,
                mimetype='application/json'
            )
        
        else:
            lenQueue = len(all_topics[data["topic"]].queue)
            print(lenQueue)
            response = app.response_class(
                response=json.dumps({
                    "status": "success",
                    "size": lenQueue
                }),
                status=200,
                mimetype='application/json'
            )
            
    else:
        response = app.response_class(
            response=json.dumps({
                "status": "failure", 
                "message": f'Bad request: topic not sent'
            }),
            status=500,
            mimetype='application/json'
        )
    print(response)
    return response


@app.route("/")
def home():
    return "Hello, World!"
    
if __name__ == "__main__":
    app.run(debug=True)