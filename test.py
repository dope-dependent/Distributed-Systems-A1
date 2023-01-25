from myqueue import myProducer
from myqueue import myConsumer

# p1 = myProducer(topics = ['user_logout', 'user_login'], broker = 'http://localhost:5000')
# c1 = myConsumer(topics = ['user_login', 'failure_lol'], broker = 'http://localhost:5000')

# p1.sendNewMessage(topic = 'sjdflsjkdflkjsdlkf', message = 'jksdjflksdjflksdjf')
# p1.sendNewMessage(topic = 'user_logout', message = 'Hello, I need to logout')
# p1.sendNewMessage(topic = 'user_login', message='Hello, I need to login')

# print(c1.getQueueSize(topic = 'user_login'))
# print(c1.getNextMessage(topic = 'user_login'))
# print(c1.getNextMessage(topic = 'user_login'))
# print(c1.getNextMessage(topic = 'user_login'))
# print(c1.getNextMessage(topic = 'user_login'))
# print(c1.getNextMessage(topic = 'user_login'))
# print(c1.getQueueSize(topic = 'user_login'))

# # p1.sendNewMessage(topic = 'user_login', message='Hello, I need to login (2)')

# print(c1.getNextMessage(topic = 'user_login'))
# print(c1.getQueueSize(topic = 'user_login'))

p1 = myProducer([], broker = 'http://localhost:5000')
p1.login({
    "producer_signup": 10000000000001,
    "phjklgjdkfgj": 92384092309482309480
})

print(p1.register)
