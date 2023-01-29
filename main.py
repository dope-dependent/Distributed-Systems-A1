import threading
from flask import Flask, request
import json
import psycopg2
from psycopg2 import sql

app = Flask(__name__)


global sem
sem = threading.Semaphore() # Semaphore for parallel executions

global MAX_TOPICS
MAX_TOPICS = 100000 # Power of 10 only (CAREFUL!!!)

global conn


# TODO Replace raw indexing by CID indexing as given below
# TODO Close exit on termination of main.py file
# TODO cursor.rollback()

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


def CheckValidityOfID(id: int, topic: str, client: str):
    ind = 3  if client == "producer" else 2
    cursor = conn.cursor()
    sem.acquire()
    try: 
        cursor.execute("""SELECT * FROM all_topics WHERE topicname = %s""",(topic,))
        result = cursor.fetchall()
        sem.release()
        cursor.close()
    except:
        sem.release()
        cursor.close()
        return ServerErrorResponse('error in accessing server'), None
      
    if len(result) == 0:
        return BadRequestResponse('topic not present in database'), None

    table_id = id // (10 * MAX_TOPICS)
    table_topic = (id // 10) % (MAX_TOPICS)
    flag = id % 2
    correct_flag = 1 if client == "producer" else 0
    if table_topic != result[0][0] or table_id >= result[0][ind]: 
        return BadRequestResponse(f'topic not subscribed by {client}'), None
    else:
        return None, result


# A
@app.route("/topics", methods = ["POST"])
def CreateTopic():
    data = request.json

    if "name" in data:
        cursor = conn.cursor()
        sem.acquire()
        cursor.execute("SELECT * FROM all_topics WHERE topicname = %s",(data["name"],))
        if len(cursor.fetchall()) != 0:
            sem.release()
            cursor.close()
            response = ServerErrorResponse('topic already present')

        else:
            try:
                response = GoodResponse({
                            "status": "success", 
                            "message": f'Topic {data["name"]} created successfully'
                        })
                
                cursor.execute("""SELECT COUNT (*) FROM all_topics""")
                id = cursor.fetchone()[0]+1
                cursor.execute("INSERT INTO all_topics (topicid, topicname, producers, consumers, tailid) VALUES (%s, %s, %s, %s, %s)",
                                (id, data["name"], 1, 1, 1))
                cursor.execute(sql.SQL("""CREATE TABLE {table_name} (
                    messageid BIGINT PRIMARY KEY, 
                    message TEXT
                )""").format(table_name = sql.Identifier(data['name'])))
                conn.commit()
            except:
                response = ServerErrorResponse("failed to add topic to database")
            finally:
                cursor.close()
                sem.release()
    else:
        response = BadRequestResponse('topic not sent')
    
    print(response)
    return response   

# B
@app.route("/topics", methods = ['GET'])
def ListTopics():
    cursor = conn.cursor()
    sem.acquire() #
    try:
        cursor.execute("SELECT topicname FROM all_topics")
        all_topics = cursor.fetchall()
        response = GoodResponse({"topics": [t[0] for t in all_topics]})
    except:
        response = ServerErrorResponse('error while getting all topics')
    finally:
        cursor.close()
        sem.release() #
    return response

# C
@app.route("/consumer/register", methods = ["POST"])
def RegisterConsumer():
    data = request.json
    if "topic" in data:
        cursor = conn.cursor()
        sem.acquire()
        try:
            cursor.execute("""SELECT * FROM all_topics WHERE topicname = %s""", (data['topic'],))
            ids = cursor.fetchall()
        except:
            ids = []
        finally:
            cursor.close()
        
        if len(ids) != 0:
            cursor = conn.cursor()
            try:
                consumerID = ids[0][2]
                topicID = ids[0][0]
                returnID = (consumerID * (MAX_TOPICS) + topicID) * 10

                response = GoodResponse({"status": "success", "consumer_id": returnID})
                cursor.execute("""UPDATE all_topics SET consumers = %s WHERE topicname = %s""", (consumerID + 1, data['topic']))
                cursor.execute("""INSERT INTO all_consumers (consumerid, queueoffset) VALUES (%s, %s)""", (str(returnID), 1))
                conn.commit()
            except:
                response = ServerErrorResponse('error in registering the consumer')
            finally:
                cursor.close()
                sem.release()
        else: 
            sem.release()
            response = ServerErrorResponse('topic not present in the database')
            
    else:
        response = BadRequestResponse('topic not sent')
    
    print(response)
    return response

# D
@app.route("/producer/register", methods = ["POST"])
def RegisterProducer():
    data = request.json
    if "topic" in data:
        cursor = conn.cursor()
        sem.acquire()
        try: 
            cursor.execute("SELECT * FROM all_topics WHERE topicname = %s", (data["topic"],))
            result = cursor.fetchall()
        except:
            result = []
        finally:
            cursor.close()
        
        cursor = conn.cursor()
        try:
            print(sem)

            if len(result) == 0:    # Create topic if this topic is not present
                cursor.execute("""SELECT COUNT (*) FROM all_topics""")
                topicID = cursor.fetchone()[0] + 1
                producerID = 1
                
                cursor.execute("INSERT INTO all_topics (topicid, topicname, producers, consumers, tailid) VALUES (%s, %s, %s, %s, %s)",
                                (topicID, data["topic"], 1, 1, 1))

                cursor.execute(sql.SQL("""CREATE TABLE {table_name} (
                    messageid BIGINT PRIMARY KEY, 
                    message TEXT
                )""").format(table_name = sql.Identifier(data['topic'])))
                
            else:
                topicID = result[0][0]
                producerID = result[0][3]

            cursor.execute("""UPDATE all_topics SET producers = %s WHERE topicname = %s""", (producerID + 1, data['topic']))
            conn.commit()
            returnID = (producerID * (MAX_TOPICS) + topicID) * 10 + 1
            response = GoodResponse({"status": "success", "producer_id": returnID})  
        
        except:
            response = ServerErrorResponse('error in registering producer')
        finally:
            cursor.close()
            sem.release()
    else:
        response = BadRequestResponse('topic not sent')

    return response


# E
@app.route("/producer/produce", methods = ["POST"])
def EnqueueMessage():
    data = request.json
    if "topic" in data and "producer_id" in data and "message" in data:
        # Check for data['topic']
        resp, result = CheckValidityOfID(data['producer_id'], data['topic'], "producer")
        if result is None: return resp
        # Get tailid of the topic
        tid = result[0][4]
        col_names = sql.SQL(',').join(sql.Identifier(n) for n in ['messageid', 'message'])
        col_values = sql.SQL(',').join(sql.Literal(n) for n in [tid, data['message']])
        
        sem.acquire()
        cursor = conn.cursor() 
        try:
            cursor.execute(sql.SQL("INSERT INTO {table_name} ({col_names}) VALUES ({col_values})").format(table_name = sql.Identifier(data['topic']), 
                                        col_names = col_names, 
                                        col_values = col_values))

            cursor.execute("UPDATE all_topics SET tailid = %s WHERE topicname = %s", (tid + 1, data['topic']))
            conn.commit()
            response = GoodResponse({"status": "success"})

        except Exception as e:
            print(e)
            response = ServerErrorResponse('error in adding message to the queue')
        
        finally:
            cursor.close()  
            sem.release()
    
    else:
        response = BadRequestResponse('topic or producer id not sent')
    
    print(response)
    return response


# F
@app.route('/consumer/consume', methods = ["GET"])
def DequeueMessage():
    data = request.json
    if "topic" in data and "consumer_id" in data:
        resp, result = CheckValidityOfID(id = data['consumer_id'], topic = data['topic'], client = "consumer")
        if result is None: return resp
        cursor = conn.cursor()
        sem.acquire()
        try: 
            cursor.execute("""SELECT * FROM all_consumers WHERE consumerid = %s""",(str(data["consumer_id"]),))
            hid = cursor.fetchone()[1]
        except:
            sem.release()
            cursor.close()
            return ServerErrorResponse('error in accessing all_consumers table')
        
        tid = result[0][4]
        if hid == tid:
            response = ServerErrorResponse('consumer is up to date')
        else:
            try:
                cursor.execute(sql.SQL("""SELECT message 
                                            FROM {table_name} 
                                            WHERE messageid = {hid}""").format(table_name = sql.Identifier(data['topic']), 
                                            hid = sql.Literal(hid)))

                message = cursor.fetchall()[0][0]
                cursor.execute("UPDATE all_consumers SET queueoffset = %s WHERE consumerid = %s", (hid + 1, str(data['consumer_id'])))
                response = GoodResponse({"status": "success", "message": message})
                conn.commit()
            except:
                response = ServerErrorResponse('error in fetching message from the queue')

        sem.release()
        cursor.close()
    
    else:
        response = BadRequestResponse('topic or consumer id not sent')

    print(response)
    return response

# G
@app.route('/size', methods = ['GET'])
def Size():
    print(conn)
    data = request.json
    if "topic" in data and "consumer_id" in data:
        resp, result = CheckValidityOfID(data['consumer_id'], data['topic'], "consumer")
        if result is None: return resp
        
        sem.acquire()
        cursor = conn.cursor()
        try: 
            cursor.execute("SELECT tailid FROM all_topics WHERE topicname = %s",(data['topic'],))
            tid = cursor.fetchone()[0]
            cursor.execute(sql.SQL("SELECT queueoffset FROM all_consumers WHERE consumerid={consumerID}").
                        format(consumerID = sql.Literal(str(data['consumer_id']))))
            conn.commit()
            queueoffset = cursor.fetchone()[0]
            response = GoodResponse({"status": "success", "size": tid - queueoffset})
        
        except: 
            response = ServerErrorResponse('consumer is up to date')
        finally:
            cursor.close()
            sem.release()
    else:
        response = BadRequestResponse('topic or consumer id not sent')
    
    print(response)
    return response


@app.route('/login', methods = ['GET'])
def Login():
    data = request.json
    if "topic" in data and "id" in data and "type" in data:
        resp, query = CheckValidityOfID(data['id'], data['topic'], data['type'])
        return GoodResponse({"status": "success"}) if query is not None else resp
    else:
        return BadRequestResponse('topic or consumer id not sent')


@app.route("/")
def home():
    return "Hello, World!"
    
if __name__ == "__main__":

    DB_NAME = 'dist_queue'

    conn = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="admin",
        )

    conn.autocommit = True # Only to create the database, new connection will be created
   
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s",(DB_NAME,))
    exists = cursor.fetchone()
    if not exists:
        cursor.execute(sql.SQL("CREATE DATABASE {db_name}")
                .format(db_name = sql.Identifier(DB_NAME)))
        cursor.close()
        conn.close()

        conn = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="admin",
            dbname = DB_NAME
        )

        cursor = conn.cursor()
        cursor.execute("""CREATE TABLE all_topics (
                topicid INT,
                topicname VARCHAR(255) PRIMARY KEY,
                consumers BIGINT,
                producers BIGINT,
                tailid BIGINT
                )""")

        cursor.execute("""CREATE TABLE all_consumers(
            consumerid VARCHAR(255) PRIMARY KEY,
            queueoffset BIGINT)""")
        conn.commit()

    cursor.close()
    conn.close()


    conn = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="admin",
            dbname = DB_NAME
        )

    cursor = conn.cursor()
    cursor.execute("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'""")

    all_tables = cursor.fetchall()
    print(all_tables)


    app.run(debug=True)