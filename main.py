from flask import Flask, request, jsonify
import json

app = Flask(__name__)

all_topics = []

# a
@app.route("/topics", methods = ["POST"])
def createTopic():
    data = request.json
    if "name" in data:
        response = app.response_class(
            response=json.dumps({
                "status": "success", 
                "message": f'Topic {data["name"]} created successfully'
            }),
            status=200,
            mimetype='application/json'
        )
        all_topics.append(data["name"])
        # TODO: Check for query        
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
            "topics": all_topics
        }),
        status=200,
        mimetype='application/json'
    )
    return response

@app.route("/")
def home():
    return "Hello, World!"
    
if __name__ == "__main__":
    app.run(debug=True)