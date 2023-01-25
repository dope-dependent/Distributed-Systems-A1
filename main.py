from flask import Flask, request
import json
import psycopg2
from psycopg2 import sql

app = Flask(__name__)
global conn

# TODO Replace raw indexing by CID indexing as given below
# TODO Close exit on termination of main.py file
# TODO (Optional) Move the three Request Classes to a separate file (requires remving the app dependency)
# TODO Provide option to create tables all_topics, all_consumers from scratch

def BadRequestResponse(message: str = ""):
    resp = app.response_class(
                response=json.dumps({
                    "status": "failure", 
                    "message": 'Bad Request: ' + message
                }),
                status = 400,
                mimetype = 'application/json'
            )
    return resp

def ServerErrorResponse(message: str = ""):
    resp = app.response_class(
                response=json.dumps({
                    "status": "failure", 
                    "message": 'Server Error: ' + message
                }),
                status = 500,
                mimetype = 'application/json'
            )
    return resp

def GoodResponse(response: dict = {}):
    resp = app.response_class(
                response=json.dumps(response), 
                status = 200,
                mimetype = 'application/json'
            )
    
    return resp

CID = {
    'topicid'   : 0,
    'topicname' : 1,
    'consumerid': 2, 
    'producerid': 3,
    'tailid'    : 4 
}

def checkValidityOfID(id: int, topic: str, client: str):
    ind = 3 if client == "producer" else 2
    cursor = conn.cursor()
    try: 
        cursor.execute("""SELECT * FROM all_topics WHERE topicName = %s""",(topic,))
    except:
        return ServerErrorResponse(), None
    
    result = cursor.fetchall()
    cursor.close()

    if len(result) == 0:
        return BadRequestResponse('topic not present in database'), None

    table_id = (id // 10) % (10 ** 12)
    table_topic = id // (10 ** 13)
    flag = id % 2
    correct_flag = 1 if client == "producer" else 0
    if table_topic != result[0][0] or table_id >= result[0][ind]: 
        return BadRequestResponse(f'topic not subscribed by {client}'), None
    else:
        return None, result


# A
@app.route("/topics", methods = ["POST"])
def createTopic():
    data = request.json

    if "name" in data:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM all_topics WHERE topicName = %s",(data["name"],))
        if len(cursor.fetchall()) != 0:
            response = ServerErrorResponse('topic already present')
        
        else:
            response = GoodResponse({
                        "status": "success", 
                        "message": f'Topic {data["name"]} created successfully'
                    })
            
            cursor.execute("""SELECT COUNT (*) FROM all_topics""")
            id = cursor.fetchone()[0]+1

            cursor.execute("INSERT INTO all_topics (topicID, topicName, producerID, consumerID, tailID) VALUES (%s, %s, %s, %s, %s)",
                            (id, data["name"], 0, 0, 1))

            cursor.execute(sql.SQL("""CREATE TABLE {table_name} (
                messageid BIGINT PRIMARY KEY, 
                message TEXT
            )""").format(table_name = sql.Identifier(data['name'])))
        
        cursor.close()
    else:
        response = BadRequestResponse('topic not sent')
        
    print(response)
    return response   

# B
@app.route("/topics", methods = ['GET'])
def ListTopics():
    cursor = conn.cursor()
    cursor.execute("SELECT topicName FROM all_topics")
    all_topics = cursor.fetchall()
    cursor.close()
    return GoodResponse({"topics": all_topics[0]})

# C
@app.route("/consumer/register", methods = ["POST"])
def registerConsumer():
    data = request.json

    if "topic" in data:
        cursor = conn.cursor()
        cursor.execute("""SELECT * FROM all_topics WHERE topicName = %s""", (data['topic'],))
        ids = cursor.fetchall()
        cursor.close()

        if len(ids) != 0:
            consumerID = ids[0][2]
            topicID = ids[0][0]
            returnID = (topicID * (10 ** 12) + consumerID) * 10

            response = GoodResponse({"status": "success", "consumer_id": returnID})

            cursor = conn.cursor()
            cursor.execute("""UPDATE all_topics SET consumerID = %s WHERE topicName = %s""", (consumerID + 1, data['topic']))
            cursor.execute("""INSERT INTO all_consumers (consumerid, queueoffset) VALUES (%s, %s)""", (returnID, 1))
            cursor.close()
        
        else: 
            response = ServerErrorResponse('topic not present in the database')
            
    else:
        response = BadRequestResponse('topic not sent')
    
    cursor.close()
    print(response)
    print()
    return response


# D
@app.route("/producer/register", methods = ["POST"])
def registerProducer():
    data = request.json
    if "topic" in data:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM all_topics WHERE topicName = %s", (data["topic"],))
        result = cursor.fetchall()
        if len(result) == 0:    # Create topic if this topic is not present
            cursor.execute("""SELECT COUNT (*) FROM all_topics""")
            topicID = cursor.fetchone()[0] + 1
            producerID = 0
            
            cursor.execute("INSERT INTO all_topics (topicID, topicName, producerID, consumerID, tailID) VALUES (%s, %s, %s, %s, %s)",
                            (id, data["topic"], 0, 0, 1))

            cursor.execute(sql.SQL("""CREATE TABLE {table_name} (
                messageid BIGINT PRIMARY KEY, 
                message TEXT
            )""").format(table_name = sql.Identifier(data['topic'])))
        
        else:
            topicID = result[0][0]
            producerID = result[0][3]


        cursor.execute("""UPDATE all_topics SET producerID = %s WHERE topicName = %s""", (producerID + 1, data['topic']))
        cursor.close()

        returnID = (topicID * (10 ** 12) + producerID) * 10 + 1
        response = GoodResponse({"status": "success", "producer_id": returnID})  
    
    else:
        response = BadRequestResponse('topic not sent')

    return response


# E
@app.route("/producer/produce", methods = ["POST"])
def enqueueMessage():
    data = request.json
    if "topic" in data and "producer_id" in data and "message" in data:
        # Check for data['topic']
        resp, result = checkValidityOfID(data['producer_id'], data['topic'], "producer")
        if result is None: return resp

        # Head id
        tid = result[0][4]

        col_names = sql.SQL(',').join(sql.Identifier(n) for n in ['messageid', 'message'])
        col_values = sql.SQL(',').join(sql.Literal(n) for n in [tid, data['message']])

        cursor = conn.cursor()
        cursor.execute(sql.SQL("INSERT INTO {table_name} ({col_names}) VALUES ({col_values})").format(table_name = sql.Identifier(data['topic']), 
                                    col_names = col_names, 
                                    col_values = col_values))

        cursor.execute("UPDATE all_topics SET tailID = %s WHERE topicName = %s", (tid + 1, data['topic']))
        cursor.close()    
        
        response = GoodResponse({"status": "success"})
    else:

        response = BadRequestResponse('topic or producer id not sent')
    
    print(response)
    return response


# F
@app.route('/consumer/consume', methods = ["GET"])
def dequeueMessage():
    data = request.json
    if "topic" in data and "consumer_id" in data:
        cursor = conn.cursor()
        resp, result = checkValidityOfID(id = data['consumer_id'], topic = data['topic'], client = "consumer")
        
        if result is not None: return resp
        
        cursor.execute("""SELECT * FROM all_consumers WHERE consumerID = %s""",(data["consumer_id"],))

        hid = cursor.fetchone()[1]
        tid = result[0][4]

        if hid == tid:
            response = ServerErrorResponse('consumer is up to date')
        
        else:
            cursor.execute(sql.SQL("""SELECT message 
                                        FROM {table_name} 
                                        WHERE messageid = {hid}""").format(table_name = sql.Identifier(data['topic']), 
                                        hid = sql.Literal(hid)))

            message = cursor.fetchall()[0][0]

            cursor.execute("UPDATE all_consumers SET queueoffset = %s WHERE consumerID = %s", (hid + 1, data['consumer_id']))
            
            response = GoodResponse({"status": "success", "message": message})
        
        cursor.close()
    else:
        response = BadRequestResponse('topic or consumer id not sent')

    print(response)
    return response

# G
@app.route('/size', methods = ['GET'])
def size():
    print(conn)
    data = request.json
    cursor = conn.cursor()
    if "topic" in data and "consumer_id" in data:
        resp, result = checkValidityOfID(data['consumer_id'], data['topic'], "consumer")
        if result is None: return resp
        
        cursor.execute("SELECT tailID FROM all_topics WHERE topicName = %s",(data['topic'],))

        tid = cursor.fetchone()[0]

        cursor.execute(sql.SQL("SELECT queueoffset FROM all_consumers WHERE consumerID={consumerID}").
                    format(consumerID = sql.Literal(data['consumer_id'])))

        queueoffset = cursor.fetchone()[0]
        response = GoodResponse({"status": "success", "size": tid - queueoffset})
            
    else:
        response = BadRequestResponse('topic or consumer id not sent')
    
    cursor.close()
    print(response)
    return response


@app.route('/login', methods = ['GET'])
def login():
    data = request.json
    if "topic" in data and "id" in data and "type" in data:
        resp, query = checkValidityOfID(data['id'], data['topic'], data['type'])
        return GoodResponse({"status": "success"}) if query is not None else resp

    else:
        return BadRequestResponse('topic or consumer id not sent')


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