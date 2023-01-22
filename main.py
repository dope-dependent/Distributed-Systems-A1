from curses.ascii import CR
from flask import Flask, request, jsonify
import json
from collections import deque
import psycopg2

app = Flask(__name__)
global conn

def createResponse(r, status):
    response = app.response_class(
                response=json.dumps(r),
                status=status,
                mimetype='application/json'
            )
    
    return response

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
    cursor = conn.cursor()

    if "name" in data:

        cursor.execute("SELECT * FROM all_topics WHERE topicName = %s",(data["name"],))

        if len(cursor.fetchall()) != 0:
            response = createResponse({
                        "status": "failure", 
                        "message": f'Bad request: name already present'
                    }, 500)
            
        else:
            response = createResponse({
                        "status": "success", 
                        "message": f'Topic {data["name"]} created successfully'
                    }, 200)
            cursor.execute("""SELECT COUNT (*) FROM all_topics""")
            id = cursor.fetchone()[0]+1
            print(id)
            cursor.execute("INSERT INTO all_topics  VALUES (%s, %s, %s, %s)",
                            (id,data["name"],0,0))

            # cursor.execute("CREATE TABLE %s ")

            
            
    else:
        response = createResponse({
                        "status": "failure", 
                        "message": f'Bad request: name not sent'
                    },500)
        
    print(response)
    cursor.close()
    return response   

# b
@app.route("/topics", methods = ['GET'])
def ListTopics():
    cursor = conn.cursor()
    cursor.execute("SELECT topicName FROM all_topics")
    all_topics = cursor.fetchall()
    response = createResponse({
                "topics": all_topics[0]
            }, 200)

    return response

# c

@app.route("/consumer/register", methods = ["POST"])
def registerConsumer():
    data = request.json
    if "topic" in data and data["topic"] in all_topics:

        consumer_id = all_topics[data["topic"]].createConsumer()

        response = createResponse({
                "status": "success", 
                "consumer_id": consumer_id
            },200)
            
    else:
        response = createResponse({
                "status": "failure", 
                "message": f'Bad request: topic not sent or topic not present'
            },500)
    
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

        response = createResponse({
                "status": "success", 
                "producer_id": producer_id
            },200)
            
    else:
        response = createResponse({
                "status": "failure", 
                "message": f'Bad request: topic not sent'
            },500)

    print(response)
    return response


# e
@app.route("/producer/produce", methods = ["POST"])
def enqueueMessage():
    data = request.json
    if "topic" in data and "producer_id" in data and "message" in data:
        if data["topic"] not in all_topics:
            response = createResponse({
                    "status": "failure", 
                    "message": f'Bad request: topic not present'
                },500)
        
        
        elif data["producer_id"] not in all_topics[data["topic"]].producers:
            response = createResponse({
                    "status": "failure", 
                    "message": f'Bad request: topic not subscribed by producer'
                },500)
        
        
        else:
            all_topics[data["topic"]].queue.appendleft(data["message"])
            print(all_topics[data["topic"]].queue)
            response = createResponse({
                    "status": "success"
                },200)
            
    else:
        response = createResponse({
                "status": "failure", 
                "message": f'Bad request: topic not sent'
            },500)
    print(response)
    return response


# f

@app.route('/consumer/consume',methods=["GET"])
def dequeueMessage():
    data = request.json
    if "topic" in data and "consumer_id" in data:
        if data["topic"] not in all_topics:
            response = createResponse({
                    "status": "failure", 
                    "message": f'Bad request: topic not present'
                },500)
            
        
        elif data["consumer_id"] not in all_topics[data["topic"]].consumers:
            response = createResponse({
                    "status": "failure", 
                    "message": f'Bad request: topic not subscribed by consumer'
                },500)
        
        else:
            message = all_topics[data["topic"]].queue.pop()
            print(message)
            response = createResponse({
                        "status": "success",
                        "message": message
                    },200)
            
    else:
        response = createResponse({
                    "status": "failure", 
                    "message": f'Bad request: topic not sent'
                },500)

    print(response)
    return response

# g

@app.route('/size',methods = ['GET'])
def size():
    print(conn)
    data = request.json
    if "topic" in data and "consumer_id" in data:
        if data["topic"] not in all_topics:
            response = createResponse({
                    "status": "failure", 
                    "message": f'Bad request: topic not present'
                },500)
        
        elif data["consumer_id"] not in all_topics[data["topic"]].consumers:
            response = createResponse({
                    "status": "failure", 
                    "message": f'Bad request: topic not subscribed by consumer'
                },500)
        
        else:
            lenQueue = len(all_topics[data["topic"]].queue)
            print(lenQueue)
            response = createResponse({
                    "status": "success",
                    "size": lenQueue
                },200)
            
    else:
        response = createResponse({
                "status": "failure", 
                "message": f'Bad request: topic not sent'
            },500)
        
    print(response)
    return response


@app.route("/")
def home():
    return "Hello, World!"
    
if __name__ == "__main__":
    conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="admin"
        )

    conn.autocommit = True
    cursor = conn.cursor()
    
    cursor.execute("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'""")
    
    for table in cursor.fetchall():
        print(table)
    cursor.close()
    app.run(debug=True)