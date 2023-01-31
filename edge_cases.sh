# registering the same topic again, should return error for the second request
curl -X POST -d '{"name": "producer_signup"}' -H 'Content-Type: application/json' http://127.0.0.1:5000/topics
curl -X POST -d '{"name": "producer_signup"}' -H 'Content-Type: application/json' http://127.0.0.1:5000/topics


# sending request with wrong parameters, should return error
curl -X POST -d '{"names": "producer_signup"}' -H 'Content-Type: application/json' http://127.0.0.1:5000/topics


# sending request without payload, should return error
curl -X POST -H 'Content-Type: application/json' http://127.0.0.1:5000/topics


# register consumer for a non existent topic, should return error
curl -X POST -d '{"topic": "non_existent_topic"}' -H 'Content-Type: application/json' http://127.0.0.1:5000/consumer/register

# register consumer GET request, should return error
curl -X GET -d '{"topic": "producer_signup"}' -H 'Content-Type: application/json' http://127.0.0.1:5000/consumer/register

# register consumer request with wrong parameter, should fail
curl -X POST -d '{"wrong_param": "non_existent_topic"}' -H 'Content-Type: application/json' http://127.0.0.1:5000/consumer/register

# register producer with non-existent topic, should register topics and send success message
curl -X POST -d '{"topic": "non_existent_topic_1"}' -H 'Content-Type: application/json' http://127.0.0.1:5000/producer/register


# register producer request with wrong parameter, should fail
curl -X POST -d '{"wrong_param": "non_existent_topic_1"}' -H 'Content-Type: application/json' http://127.0.0.1:5000/producer/register

# produce message with wrong id, should fail
curl -X POST -d '{"topic": "producer_signup","producer_id":1, "message": "hello1"}' -H 'Content-Type: application/json' http://127.0.0.1:5000/producer/produce

# produce message with non existent topic, should fail
curl -X POST -d '{"topic": "non_existent_topic","producer_id":1000011, "message": "hello1"}' -H 'Content-Type: application/json' http://127.0.0.1:5000/producer/produce


# consume message with wrong id, should fail
curl -X GET  -d '{"topic": "producer_signup","consumer_id":100}' -H 'Content-Type: application/json' http://127.0.0.1:5000/consumer/consume

# consume message POST request, should fail
curl -X POST  -d '{"topic": "producer_signup","consumer_id":1000010}' -H 'Content-Type: application/json' http://127.0.0.1:5000/consumer/consume


# consume message for topic not subscribed
curl -X POST  -d '{"topic": "producer_signup","consumer_id":1000020}' -H 'Content-Type: application/json' http://127.0.0.1:5000/consumer/consume

