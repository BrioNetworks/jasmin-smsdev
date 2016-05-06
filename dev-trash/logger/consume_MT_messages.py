""" Simple mod from the original gist submitted by @zoufou
https://gist.github.com/zoufou/5701d71bf6e404d17cb4

Mod by: @pguillem
 
Purpose:
- Integrates PySQLPool lib to handle MySQL
- Visit https://pythonhosted.org/PySQLPool/tutorial.html
 
Requirements:
- Pip
- libmysqlclient-dev
- PySQLPool library
 
To prepare in Debian:
  apt-get install libmysqlclient-dev
  pip install PySQLPool

TO-DO:
- Wrap the MySQL execs in a try/catch to track any exeptions
 
 
Watch for comments to suit your needs
"""

import pickle
from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor
from twisted.internet.protocol import ClientCreator
from twisted.python import log
 
from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate
 
import txamqp.spec
 
#Mysql conn pool handler
import PySQLPool
 
 
@inlineCallbacks
def gotConnection(conn, username, password):
    #print "Connected to broker."
    yield conn.authenticate(username, password)
 
    print "Authenticated. Ready to receive messages"
    chan = yield conn.channel(1)
    yield chan.channel_open()
 
    yield chan.queue_declare(queue="someQueueName")
 
    # Bind to submit.sm.* and submit.sm.resp.* routes
    yield chan.queue_bind(queue="someQueueName", exchange="messaging", routing_key='submit.sm.*')
    yield chan.queue_bind(queue="someQueueName", exchange="messaging", routing_key='submit.sm.resp.*')
 
    yield chan.basic_consume(queue='someQueueName', no_ack=True, consumer_tag="someTag")
    queue = yield conn.queue("someTag")
 
 
    #Build Mysql connection pool
    PySQLPool.getNewPool().maxActiveConnections = 20  #Set how many reusable conns to buffer in the pool
    print "Pooling 20 connections"
    
    #Connection parameters - Fill this info with your MySQL server connection parameters
    mysqlconn = PySQLPool.getNewConnection(
        username='mysql_user', 
        password='mysql_password.', 
        host='server_host', 
        db='database_name')
        
    print "Connected to MySQL"
    queryp = PySQLPool.getNewQuery(mysqlconn)
 
    # Wait for messages
    # This can be done through a callback ...
    while True:
        msg = yield queue.get()
        props = msg.content.properties
        pdu = pickle.loads(msg.content.body)
 
        if msg.routing_key[:15] == 'submit.sm.resp.':
                #print 'SubmitSMResp: status: %s, msgid: %s' % (pdu.status,
                #       props['message-id'])
                
                #Update a record in mysql according to your own table. This will fire upon receiving a PDU response. 
                #Make sure you already have a matching sms record to update.
                
                queryp.Query("UPDATE table_name SET status='%s' WHERE messageid='%s'" % (pdu.status,props['message-id']))
                PySQLPool.commitPool() #Very important, always execute a commit, autocommit doesn´t work well here
                
        elif msg.routing_key[:10] == 'submit.sm.':
                
                #print 'SubmitSM: from %s to %s, content: %s, msgid: %s' % (pdu.params['source_addr'],
                #       pdu.params['destination_addr'],
                #       pdu.params['short_message'],
                #       props['message-id'])
                
                # This will fire every time a message is sent to the SumbitSM queue.
                # Create a record with the messagesent msg
                
                queryp.Query("INSERT INTO table_name (messageid,carrier,date,dst,src,status,accountcode,cost,sale,plan_name,amaflags,content) VALUES ('%s','Carrier',NOW(),'%s','%s','8','00000','0.0','0.0','plan_name','some_status','%s') " % (props['message-id'], pdu.params['destination_addr'], pdu.params['source_addr'], pdu.params['short_message']) )
                
                """
                The previous query works for the following table structure:
                    id INT primary_key auto_increment
                    messageid VARCHAR(128)
                    carrier VARCHAR
                    date DATETIME
                    dst VARCHAR(15)
                    src VARCHAR(15)
                    status VARCHAR(10)
                    accountcode INT
                    cost FLOAT
                    sale FLOAT
                    plan_name VARCHAR(25)
                    amaflags VARCHAR(10)
                    content VARCHAR(160)
                """
                
                PySQLPool.commitPool() # Remember to Commit
        else:
                print 'unknown route'
 
 
    # A clean way to tear down and stop
    yield chan.basic_cancel("someTag")
    yield chan.channel_close()
    chan0 = yield conn.channel(0)
    yield chan0.connection_close()
 
    reactor.stop()
 
 
if __name__ == "__main__":
    """
    This example will connect to RabbitMQ broker and consume from two route keys:
      - submit.sm.*: All messages sent through SMPP Connectors
      - submit.sm.resp.*: More relevant than SubmitSM because it contains the sending status
 
    Note:
      - Messages consumed from submit.sm.resp.* are not verbose enough, they contain only message-id and status
      - Message content can be obtained from submit.sm.*, the message-id will be the same when consuming from submit.sm.resp.*,
        it is used for mapping.
      - Billing information is contained in messages consumed from submit.sm.*
      - This is a proof of concept, saying anyone can consume from any topic in Jasmin's exchange hack a
        third party business, more information here: http://docs.jasminsms.com/en/latest/messaging/index.html
    """
 
    host = '127.0.0.1'
    port = 5672
    vhost = '/'
    username = 'guest'
    password = 'guest'
    spec_file = '/etc/jasmin/resource/amqp0-9-1.xml'
 
    spec = txamqp.spec.load(spec_file)
 
    # Connect and authenticate
    d = ClientCreator(reactor,
        AMQClient,
        delegate=TwistedDelegate(),
        vhost=vhost,
        spec=spec).connectTCP(host, port)
    d.addCallback(gotConnection, username, password)
 
    def whoops(err):
        if reactor.running:
            log.err(err)
            reactor.stop()
 
    d.addErrback(whoops)
 
    reactor.run()