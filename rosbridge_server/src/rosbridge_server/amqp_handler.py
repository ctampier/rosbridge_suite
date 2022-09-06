import rospy
import uuid
import re

from rosauth.srv import Authentication

import functools
import pika

from rosbridge_library.rosbridge_protocol import RosbridgeProtocol
from rosbridge_library.util import json

def amqp_topic_name(ros_topic_name):
    topic_name = ros_topic_name
    # Remove first slash if present
    if topic_name[0] == "/":
        topic_name = topic_name[1:]
    # Change all subsequent slashes for points
    return topic_name.replace("/", ".")

class RosbridgeAmqpClient():
    """
    AMQP client interface (needs AMQP server) for rosbridge
    """

    client_id_seed = 0
    clients_connected = 0
    authenticate = False

    # list of parameters
    client_command_destination = 'client-command' # application's publisher channel
    server_command_destination = 'server-command' # application's subscriber channel
    data_exchange_type = 'topic'
    reconnect_delay = -1
    use_history = False
    history_length = 100
    # The following are passed on to RosbridgeProtocol
    # defragmentation.py:
    fragment_timeout = 600                  # seconds
    # protocol.py:
    delay_between_messages = 0              # seconds
    max_message_size = None                 # bytes
    unregister_timeout = 10.0               # seconds

    def __init__(self, amqp_url):
        # Define parameters
        self._connection = None
        self._channel = None

        self.was_consuming = False

        self._stopping = False
        self._consumer_tags = []
        self._url = amqp_url
        self._consuming = False
        self._prefetch_count = 1

    def connect(self):
        self._connection = pika.SelectConnection(
            pika.URLParameters(self._url),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)

    def on_connection_open(self, connection):
        self.open_channel()

    def on_connection_open_error(self, connection, err):
        cls = self.__class__
        rospy.logerr('Connection open failed, reopening in %d seconds: %s', cls.reconnect_delay, err)
        self._connection.ioloop.call_later(cls.reconnect_delay, self._connection.ioloop.stop)

    def on_connection_closed(self, connection, reason_code, reason_text):
        if not hasattr(self, 'protocol'):
            return  # Closed before connection was opened.
        cls = self.__class__
        cls.clients_connected -= 1
        self.protocol.finish()
        if cls.client_manager:
            cls.client_manager.remove_client(self.client_id, self.host_ip)
        rospy.loginfo("Message Broker disconnected. %d total connections.", cls.clients_connected)

        self._channel = None
        if self._stopping:
            self._connection.ioloop.stop()
        elif cls.reconnect_delay >= 0:
            rospy.loginfo("Will try to reconnect afer %d seconds", cls.reconnect_delay)
            self._connection.ioloop.call_later(cls.reconnect_delay, self._connection.ioloop.stop)

    def open_channel(self):
        rospy.loginfo('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        cls = self.__class__
        rospy.loginfo('Channel opened')
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)
        self.setup_queue("amq.topic", cls.client_command_destination, cls.client_command_destination)
        parameters = {
            "fragment_timeout": cls.fragment_timeout,
            "delay_between_messages": cls.delay_between_messages,
            "max_message_size": cls.max_message_size,
            "unregister_timeout": cls.unregister_timeout
        }
        try:
            # Initialize Rosbridge protocol
            self.protocol = RosbridgeProtocol(cls.client_id_seed, parameters=parameters)
            self.protocol.outgoing = self.send_message
            self.authenticated = False
            cls.client_id_seed += 1
            cls.clients_connected += 1
            self.client_id = uuid.uuid4()
            self.host_ip = self._connection.params._host
            if cls.client_manager:
                cls.client_manager.add_client(self.client_id, self.host_ip)
                
        except Exception as exc:
            rospy.logerr("Unable to accept incoming connection.  Reason: %s", str(exc))
        rospy.loginfo("Message Broker connected.  %d total connections.", cls.clients_connected)

        if cls.authenticate:
            rospy.loginfo("Awaiting proper authentication...")

    def on_channel_closed(self, channel, reply_code, reply_text):
        rospy.logerr('Channel %i was closed: %s', channel, reply_text)
        self._consuming = False
        self._channel = None
        if 'NOT_FOUND' in reply_text:
            # Search for an exchange name in the error text
            exchange_name = re.findall(r"exchange '([^']*)'", reply_text)[0]
            if exchange_name:
                # Translate to topic name
                topic_name = '/' + exchange_name.replace(".", "/")
                # Forward unsubscribe to the found topic to the protocol
                msg = {'op': 'unsubscribe', 'topic': topic_name}
                self.protocol.incoming(json.dumps(msg))
        if not self._stopping:
            self._connection.close()

    def setup_exchange(self, exchange_type, exchange_name, sub=False, queue_name="", routing_key=""):
        cb = None
        if sub:
            cb = functools.partial(self.on_exchange_declareok,
                                exchange_name=exchange_name,
                                queue_name=queue_name,
                                routing_key=routing_key)
        self._channel.exchange_declare(exchange=exchange_name,
                                       exchange_type=exchange_type,
                                       auto_delete=True,
                                       callback=cb)

    def on_exchange_declareok(self, frame, exchange_name, queue_name, routing_key):
        self.setup_queue(exchange_name, queue_name, routing_key)

    def setup_queue(self, exchange_name, queue_name, routing_key):
        cb = functools.partial(self.on_queue_declareok,
                               exchange_name=exchange_name,
                               queue_name=queue_name,
                               routing_key=routing_key)
        self._channel.queue_declare(queue=queue_name, auto_delete=True, callback=cb)

    def on_queue_declareok(self, frame, exchange_name, queue_name, routing_key):
        cb = functools.partial(self.on_bindok, queue_name=queue_name)
        self._channel.queue_bind(
            queue=queue_name,
            exchange=exchange_name,
            routing_key=routing_key,
            callback=cb)

    def on_bindok(self, frame, queue_name):
        self.set_qos(queue_name)

    def set_qos(self, queue_name):
        cb = functools.partial(self.on_basic_qos_ok, queue_name=queue_name)
        self._channel.basic_qos(
            prefetch_count=self._prefetch_count, callback=cb)

    def on_basic_qos_ok(self, frame, queue_name):
        self.start_consuming(queue_name)

    def start_consuming(self, queue_name):
        consumer = self._channel.basic_consume(
            queue=queue_name,
            consumer_callback=self.on_message
        )
        self._consumer_tags.append({queue_name: consumer})
        self.was_consuming = True
        self._consuming = True

    def stop_consuming(self, queue_name):
        if self._channel:
            for consumer in self._consumer_tags:
                if queue_name in consumer:
                    self._channel.basic_cancel(consumer[queue_name])
                    break

    def on_message(self, channel, basic_deliver, properties, body):
        cls = self.__class__
        msg = json.loads(body)
        # check if we need to authenticate
        if cls.authenticate and not self.authenticated:
            try:
                if msg['op'] == 'auth':
                    # check the authorization information
                    auth_srv = rospy.ServiceProxy('authenticate', Authentication)
                    resp = auth_srv(msg['mac'], msg['client'], msg['dest'],
                                                  msg['rand'], rospy.Time(msg['t']), msg['level'],
                                                  rospy.Time(msg['end']))
                    self.authenticated = resp.authenticated
                    if self.authenticated:
                        rospy.loginfo("Client %d has authenticated.", self.protocol.client_id)
                        return
                # if we are here, no valid authentication was given
                rospy.logwarn("Client %d did not authenticate. Closing connection.",
                              self.protocol.client_id)
                self._connection.ioloop.stop()
            except:
                # proper error will be handled in the protocol class
                self.protocol.incoming(body)
        # no authentication required
        else:
            # If the client advertises a topic, create a stomp-subscriber to it
            if msg['op'] == 'advertise':
                self.setup_exchange(cls.data_exchange_type, 
                                    amqp_topic_name(msg['topic']), 
                                    True, 
                                    amqp_topic_name(msg['topic']), 
                                    amqp_topic_name(msg['topic']))
            # If the client unadvertises a topic, delete the stomp-subscriber to it
            if msg['op'] == 'unadvertise':
                self.stop_consuming(amqp_topic_name(msg['topic']))
            # If the client subscribes a topic, create a new exchange in the server for it
            # if msg['op'] == 'subscribe':
            #     # Use a topic exchange by default
            #     exchange_type = 'topic'
            #     exchange_args = None
            #     # Use an x-recent-history exchange if using history
            #     if cls.use_history:
            #         exchange_type = 'x-recent-history'
            #         exchange_args = { "x-recent-history-length": cls.history_length }
            #     try:
            #         # Check if the exchange already exists
            #         channel.exchange_declare(
            #             exchange=amqp_topic_name(msg['topic']),
            #             passive=True
            #         )
            #     except:
            #         # Create the exchange if it does't exist
            #         channel.exchange_declare(
            #             exchange=amqp_topic_name(msg['topic']),
            #             exchange_type=exchange_type,
            #             auto_delete=True,
            #             arguments=exchange_args
            #         )
            # If it's a client command, acknowledge the message
            if cls.client_command_destination == basic_deliver.exchange:
                self._channel.basic_ack(basic_deliver.delivery_tag)
            # Forward the message to the protocol
            self.protocol.incoming(body)

    def on_disconnected(self):
        if not hasattr(self, 'protocol'):
            return  # Closed before connection was opened.
        cls = self.__class__
        cls.clients_connected -= 1
        self.protocol.finish()
        if cls.client_manager:
            cls.client_manager.remove_client(self.client_id, self.host_ip)
        rospy.loginfo("Message Broker disconnected. %d total connections.", cls.clients_connected)
        if cls.reconnect_delay >= 0:
            rospy.loginfo("Will try to reconnect afer %d seconds", cls.reconnect_delay)
            rospy.sleep(cls.reconnect_delay)
            self.connect()

    def send_message(self, message):
        if self._channel is None or not self._channel.is_open:
            return

        cls = self.__class__
        # For published data, switch to the topic name in the message with the configured prefix.
        destination = cls.server_command_destination
        msg = json.loads(message)
        if msg['op'] == 'publish':
            destination = amqp_topic_name(msg['topic'])
        
        properties = pika.BasicProperties(app_id='rosbridge-amqp-client',
                                          content_type='application/json')

        self._channel.basic_publish("amq.topic", destination, message, properties, mandatory=True)

    def run(self):
        while not self._stopping:
            self._connection = None

            try:
                self.connect()
                self._connection.ioloop.start()
            except KeyboardInterrupt:
                self.stop()
                if (self._connection is not None and
                        not self._connection.is_closed):
                    self._connection.ioloop.start()

        rospy.loginfo('Stopped')

    def stop(self):
        rospy.loginfo('Stopping')
        self._stopping = True
        if self._channel is not None:
            rospy.loginfo('Closing the channel')
            self._channel.close()
        if self._connection is not None:
            rospy.loginfo('Closing connection')
            self._connection.close()