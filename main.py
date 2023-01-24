from flask import Flask, request, jsonify
import json
import psycopg2
from psycopg2 import sql

app = Flask(__name__)
global conn

def createResponse(r, status):
    response = app.response_class(
                response=json.dumps(r),
                status=status,
                mimetype='application/json'
            )
    
    return response

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
            cursor.execute("INSERT INTO all_topics (topicID, topicName, producerID, consumerID, tailID) VALUES (%s, %s, %s, %s, %s)",
                            (id, data["name"], 0, 0, 1))

            cursor.execute(sql.SQL("""CREATE TABLE {table_name} (
                messageid BIGINT PRIMARY KEY, 
                message TEXT
            )""").format(table_name = sql.Identifier(data['name'])))
            
    else:
        response = createResponse({
                        "status": "failure", 
                        "message": f'Bad request: name not sent'
                    },500)
        
    cursor.close()
    print(response)
    print()
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
    cursor = conn.cursor()

    if "topic" in data:
        cursor.execute("""SELECT * FROM all_topics WHERE topicName = %s""", (data['topic'],))
        ids = cursor.fetchall()
        if len(ids) != 0:
            consumer_id = ids[0][2]
            topic_id = ids[0][0]
            return_id = (topic_id * (10 ** 12) + consumer_id) * 10
            response = createResponse({
                "status": "success", 
                "consumer_id": return_id
            },200)
            nid = consumer_id + 1
            cursor.execute("""UPDATE all_topics SET consumerID = %s WHERE topicName = %s""", (nid, data['topic']))
            cursor.execute("""INSERT INTO all_consumers (consumerid, queueoffset) VALUES (%s, %s)""",(return_id,1))
        
        else:
            response = createResponse({
                "status": "failure", 
                "message": f'Bad request: topic not sent or topic not present'
            },500)
            
    else:
        response = createResponse({
            "status": "failure", 
            "message": f'Bad request: topic not sent or topic not present'
        },500)
    
    cursor.close()
    print(response)
    print()
    return response


# d
@app.route("/producer/register", methods = ["POST"])
def registerProducer():
    data = request.json
    cursor = conn.cursor()
    if "topic" in data:
        cursor.execute("SELECT * FROM all_topics WHERE topicName = %s",(data["topic"],))

        if len(cursor.fetchall()) == 0:
            cursor.execute("""SELECT COUNT (*) FROM all_topics""")
            
            id = cursor.fetchone()[0]+1
            
            cursor.execute("INSERT INTO all_topics (topicID, topicName, producerID, consumerID, headID, tailID) VALUES (%s, %s, %s, %s, %s, %s)",
                            (id, data["topic"], 0, 0, 0, 0))

            cursor.execute(sql.SQL("""CREATE TABLE {table_name} (
                messageid BIGINT PRIMARY KEY, 
                message TEXT
            )""").format(table_name = sql.Identifier(data['topic'])))

        cursor.execute("""SELECT * FROM all_topics WHERE topicName = %s""", (data['topic'],))
        result = cursor.fetchall()[0]
        topic_id = result[0]
        producer_id = result[3]
        nid = producer_id + 1
        
        cursor.execute("""UPDATE all_topics SET producerID = %s WHERE topicName = %s""", (nid, data['topic']))
        return_id = (topic_id * (10 ** 12) + producer_id) * 10 + 1
        response = createResponse({
            "status": "success", 
            "producer_id": return_id
        },200)
            
    else:
        response = createResponse({
                "status": "failure", 
                "message": f'Bad request: topic not sent'
            },500)

    cursor.close()
    print(response)
    print()
    return response


# e
@app.route("/producer/produce", methods = ["POST"])
def enqueueMessage():
    data = request.json
    cursor = conn.cursor()
    if "topic" in data and "producer_id" in data and "message" in data:
        # Check for data['topic']
        cursor.execute("SELECT * FROM all_topics WHERE topicName = %s", (data['topic'],))
        result = cursor.fetchall()
        if len(result) == 0:
            response = createResponse({
                "status": "failure", 
                "message": f'Bad request: topic not present'
            },500)
            cursor.close()
            return response
        
        # Check for producer_id 
        pid = result[0][3]
        if pid >= data['producer_id']:
            response = createResponse({
                    "status": "failure", 
                    "message": f'Bad request: topic not subscribed by producer'
                },500)
            cursor.close()
            return response
        
        # Head id
        tid = result[0][4]
        col_names = sql.SQL(',').join(sql.Identifier(n) for n in ['messageid', 'message'])
        col_values = sql.SQL(',').join(sql.Literal(n) for n in [tid, data['message']])
        cursor.execute(sql.SQL("INSERT INTO {table_name} ({col_names}) VALUES ({col_values})").format(table_name = sql.Identifier(data['topic']), 
                                    col_names = col_names, 
                                    col_values = col_values))

        cursor.execute("UPDATE all_topics SET tailID = %s WHERE topicName = %s", (tid + 1, data['topic']))
        
        response = createResponse({
                "status": "success"
            },200)
            
    else:
        response = createResponse({
                "status": "failure", 
                "message": f'Bad request: topic not sent'
            },500)
    
    cursor.close()
    print(response)
    print()
    return response


# f

@app.route('/consumer/consume',methods=["GET"])
def dequeueMessage():
    data = request.json
    cursor = conn.cursor()
    if "topic" in data and "consumer_id" in data:
        cursor.execute("SELECT * FROM all_topics WHERE topicName = %s", (data['topic'],))
        result = cursor.fetchall()
        if len(result) == 0:
            response = createResponse({
                "status": "failure", 
                "message": f'Bad request: topic not present'
            },500)
            cursor.close()
            return response
        
        pid = result[0][2]
        if pid >= data['consumer_id']:
            response = createResponse({
                    "status": "failure", 
                    "message": f'Bad request: topic not subscribed by consumer'
                },500)
            cursor.close()
            return response
        
        cursor.execute("""SELECT * FROM all_consumers WHERE consumerID = %s""",(data["consumer_id"],))
        hid = cursor.fetchone()[1]
        tid = result[0][4]
        if hid == tid:
            response = createResponse({
                    "status": "failure", 
                    "message": f'Server Error: Consumer is up to date with all entries'
                },400)
        else:
            cursor.execute(sql.SQL("""SELECT message 
                                        FROM {table_name} 
                                        WHERE messageid = {hid}""").format(table_name = sql.Identifier(data['topic']), 
                                        hid = sql.Literal(hid)))

            message = cursor.fetchall()[0][0]

            cursor.execute("UPDATE all_consumers SET queueoffset = %s WHERE consumerID = %s", (hid + 1, data['consumer_id']))
            
            response = createResponse({
                    "status": "success", 
                    "message": message
                },200)
            
    else:
        response = createResponse({
                    "status": "failure", 
                    "message": f'Bad request: topic not sent'
                },500)

    cursor.close()
    print(response)
    return response

# g

@app.route('/size',methods = ['GET'])
def size():
    print(conn)
    data = request.json
    cursor = conn.cursor()
    if "topic" in data and "consumer_id" in data:
        cursor.execute("SELECT * FROM all_topics WHERE topicName = %s", (data['topic'],))
        result = cursor.fetchall()
        if len(result) == 0:
            response = createResponse({
                "status": "failure", 
                "message": f'Bad request: topic not present'
            },500)
            cursor.close()
            return response

        
        pid = result[0][2]
        if pid >= data['consumer_id']:
            response = createResponse({
                    "status": "failure", 
                    "message": f'Bad request: topic not subscribed by producer'
                },500)
            cursor.close()
            return response
        
        
        cursor.execute("SELECT tailID FROM all_topics WHERE topicName = %s",(data['topic'],))
        tid = cursor.fetchone()[0]
        cursor.execute(sql.SQL("SELECT queueoffset FROM all_consumers WHERE consumerID={consumerID}").
                    format(consumerID=sql.Literal(data['consumer_id'])))
        queueoffset = cursor.fetchone()[0]
        response = createResponse({
                "status": "success",
                "size": tid - queueoffset
            },200)
            
    else:
        response = createResponse({
                "status": "failure", 
                "message": f'Bad request: topic not sent'
            },500)
    
    cursor.close()
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