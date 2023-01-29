# Documentation
This section contains the entire documentation of the `myConsumer` and `myProducer` classes, which are basically client-level classes making interactions with the distributed queue easier for users. 

## The `myConsumer` class for Consumers
The `myConsumer.register` attribute stores a dictionary of all the topics and IDs of the consumer. 
### Creation of a `myConsumer` instance
A `myConsumer` object can be called by specifying a list of topics, which the consumer needs to subscribe to. This list can be empty, which means that the consumer does not need to subscribe to any topic currently. 
```
a = myConsumer(topics = ['t1', 't2'], broker = 'localhost:5000')
b = myConsumer(topics = [], broker = 'localhost:6000')
```
If one or more topics in this list are not present in the database, they are NOT created, instead an error message is printed on the console. However, creation of IDs for topics present in the database continues normally. 
### `myConsumer.registerForTopics(list)`: Registering for more topics
Registering for more topics is allowed. A consumer can register for more topics by calling the `registerForTopics` function.
```
a = myConsumer(topics = ['t1', 't2'], broker = 'localhost:5000')
a.registerForTopics(['t3'])
```
This function first checks if the topic is not already registered, if it is it is simply ignored (and a message is printed on the console). Otherwise, we register the consumer for this topic

### `myConsumer.login(dict)`: Logging in
For consumers to login with their topic-specific IDs to a given topic, they can use the login functionality
```
a = myConsumer(topics['t1', 't2'], broker = 'localhost:5000')
a.login({
			't3': 4920342034, 
			't4': 4932053405
			})
```
The above command checks if these IDs are valid and then adds these ids to the consumer. 
 
### `myConsumer.getID(topic)`: Getting the Consumer IDs for a topic
This function is used to retrieve the topic-specific consumer ID. It returns a `dict` with topic names as keys and IDs as values.
```
a = myConsumer(topics['t1', 't2'], broker = 'localhost:5000')
id_dict = a.getID('t1')	# Returns dict()
id_dict2 = a.getID(['t1', 't2']) # Returns dict()
```

### `myConsumer.getQueueSize(topic)`: Get the size of queue for a particular topic
This function is used to find how many messages are left in the topic-specific queue for this consumer. It returns an `int`.
```
a = myConsumer(topics['t1', 't2'], broker = 'localhost:5000')
x = a.getQueueSize('t1')	# Returns int
```

### `myConsumer.getNextMessage(topic)`: Get the next message in the topic-specific queue
This function returns the next message in the topic queue for this consumer and returns `None` if there is no such message.
```
a = myConsumer(topics['t1', 't2'], broker = 'localhost:5000')
msg = a.getNextMessage('t1')	# Returns str or None
```

### `myConsumer.getAllTopics()`: Returns a list of all topics
This function returns the list of all topics. 
```
a = myConsumer(topics['t1', 't2'], broker = 'localhost:5000')
topics = a.getAllTopics('t1')	# Returns list
```

## The `myProducer` class for Producers
Similar to the consumer, the `myProducer.register` attribute stores a dictionary of all the topics and IDs of the producer.
 
### Creation of a `myProducer` instance
A `myProducer` object can be called by specifying a list of topics, which the producer needs to subscribe to. This list can be empty, which means that the producer does not need to subscribe to any topic currently. 
```
a = myProducer(topics = ['t1', 't2'], broker = 'localhost:5000')
b = myProducer(topics = [], broker = 'localhost:6000')
```
If one or more topics in this list are not present in the database, they are NOT created, instead an error message is printed on the console. However, creation of IDs for topics present in the database continues normally. 

### `myProducer.registerForTopics(list)`: Registering for more topics
Registering for more topics is allowed. A producer can register for more topics by calling the `registerForTopics` function.
```
a = myProducer(topics = ['t1', 't2'], broker = 'localhost:5000')
a.registerForTopics(['t3'])
```
This function first checks if the topic is not already registered, if it is it is simply ignored (and a message is printed on the console). Otherwise, we register the producer for this topic and generate an ID.

### `myProducer.login(dict)`: Logging in
For producers to login with their topic-specific IDs to a given topic, they can use the login functionality
```
a = myProducer(topics['t1', 't2'], broker = 'localhost:5000')
a.login({
			't3': 4920342034, 
			't4': 4932053405
			})
```
The above command checks if these IDs are valid and then adds these ids to the producer. 
 
### `myProducer.getID(topic)`: Getting the Producer IDs for a topic
This function is used to retrieve the topic-specific producer ID. It returns a `dict` with topic names as keys and IDs as values.
```
a = myProducer(topics['t1', 't2'], broker = 'localhost:5000')
id_dict = a.getID('t1')	# Returns dict()
id_dict2 = a.getID(['t1', 't2']) # Returns dict()
```

### `myProducer.sendNewMessage(topic, message)`: Send a message
This function sends a message to the topic specific queue. If the message is not sent, an error message is printed. 
```
a = myProducer(topics['t1', 't2'], broker = 'localhost:5000')
a.sendNewMessage('t1')	# Returns None
```

### `myProducer.getAllTopics()`: Returns a list of all topics
This function returns the list of all topics. 
```
a = myProducer(topics['t1', 't2'], broker = 'localhost:5000')
topics = a.getAllTopics('t1')	# Returns list
```

