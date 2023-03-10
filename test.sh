# #create topic
# curl -X POST -d '{"name": "producer_signup"}' -H 'Content-Type: application/json' http://127.0.0.1:5000/topics

#create producer and consumer
curl -X POST -d '{"topic": "producer_signup"}' -H 'Content-Type: application/json' http://127.0.0.1:5000/producer/register
# curl -X POST -d '{"topic": "producer_signup"}' -H 'Content-Type: application/json' http://127.0.0.1:5000/consumer/register

# #produce two messages
# curl -X POST -d '{"topic": "producer_signup","producer_id":10000000000001, "message": "hello1"}' -H 'Content-Type: application/json' http://127.0.0.1:5000/producer/produce
# curl -X POST -d '{"topic": "producer_signup","producer_id":10000000000001, "message": "hello2"}' -H 'Content-Type: application/json' http://127.0.0.1:5000/producer/produce


# #check size to be 2
# curl -X GET  -d '{"topic": "producer_signup","consumer_id":10000000000000}' -H 'Content-Type: application/json' http://127.0.0.1:5000/size

# first message to be retrieved
# curl -X GET  -d '{"topic": "producer_signup","consumer_id":10000000000000}' -H 'Content-Type: application/json' http://127.0.0.1:5000/consumer/consume

# #queue size should change to 1
# curl -X GET  -d '{"topic": "producer_signup","consumer_id":10000000000000}' -H 'Content-Type: application/json' http://127.0.0.1:5000/size

# Retrieve Second Message
# curl -X GET  -d '{"topic": "producer_signup","consumer_id":10000000000000}' -H 'Content-Type: application/json' http://127.0.0.1:5000/consumer/consume

# Check size should be zero
# curl -X GET  -d '{"topic": "producer_signup","consumer_id":10000000000000}' -H 'Content-Type: application/json' http://127.0.0.1:5000/size
