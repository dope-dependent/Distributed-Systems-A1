from myqueue import myProducer
from myqueue import myConsumer


p1 = myProducer(topics = ['user_logout', 'user_message','user_signup'], broker = 'http://localhost:5000')
c1 = myConsumer(topics = ['user_login', 'failure_lol'], broker = 'http://localhost:5000')



if 'user_login' in list(c1.register.keys()):
    print('Consumer Creation Test PASSED')
else:
    print('Consumer Creation Test FAILED')


if set(['user_logout', 'user_login','user_signup']) == set(list(p1.register.keys())):
    print('Producer Creation Test PASSED')
else:
    print('Producer Creation Test FAILED')


print(p1.register)
print(c1.register)


initialQueueSize = c1.getQueueSize(topic = 'user_login')

p1.sendNewMessage(topic = 'sjdflsjkdflkjsdlkf', message = 'jksdjflksdjflksdjf')
p1.sendNewMessage(topic = 'user_logout', message = 'Hello, I need to logout')
p1.sendNewMessage(topic = 'user_login', message='Hello, I need to login')
p1.sendNewMessage(topic = 'user_login', message='Hello, I need to login (2)')

if c1.getQueueSize(topic = 'user_login') - initialQueueSize == 2:
    print("Message Creation Test PASSED")
else:
    print("Message Creation Test FAILED")

c1.getNextMessage(topic = 'user_login')

if c1.getQueueSize(topic = 'user_login') - initialQueueSize == 1:
    print("Message Consumption Test PASSED")
else:
    print("Message Consumption Test FAILED")

print(p1.getAllTopics())

c2 = myConsumer(topics=[],broker = 'http://localhost:5000')

c2_id = c1.register['user_login']

c2.login({'user_login': c2_id})

if c1.getQueueSize(topic = 'user_login') == c2.getQueueSize(topic = 'user_login'):
    print("Consumer Login Test PASSED")
else:
    print("Consumer Login Test FAILED")

c2.login({'user_logout': 1000020})

