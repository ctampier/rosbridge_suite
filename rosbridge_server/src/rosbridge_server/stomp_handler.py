import rospy
import uuid
import re

from rosauth.srv import Authentication

import stomp
import pika

from rosbridge_library.rosbridge_protocol import RosbridgeProtocol
from rosbridge_library.util import json

def stomp_topic_name(ros_topic_name):
    topic_name = ros_topic_name
    # Remove first slash if present
    if topic_name[0] == "/":
        topic_name = topic_name[1:]
    # Change all subsequent slashes for points
    return topic_name.replace("/", ".")

class RosbridgeStompSocket(stomp.ConnectionListener):
    """
    STOMP client interface (needs STOMP server) for rosbridge
    """

    client_id_seed = 0
    clients_connected = 0
    authenticate = False

    # list of parameters
    client_command_destination = '/topic/client-command' # application's publisher channel
    server_command_destination = '/topic/server-command' # application's subscriber channel
    data_destination_prefix = '/exchange/'
    reconnect_delay = -1
    use_history = True
    history_length = 100
    # The following are passed on to RosbridgeProtocol
    # defragmentation.py:
    fragment_timeout = 600                  # seconds
    # protocol.py:
    delay_between_messages = 0              # seconds
    max_message_size = None                 # bytes
    unregister_timeout = 10.0               # seconds

    def __init__(self, conn, user, password):
        # Define parameters
        self.conn = conn
        self.user = user
        self.password = password

    def connect(self):
        self.conn.connect(self.user, self.password, wait=True)

    def on_connected(self, headers, body):
        cls = self.__class__
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
            # STOMP subscription to rosbridge client commands
            self.conn.subscribe(
                destination=cls.client_command_destination,
                id=cls.client_command_destination+str(self.protocol.client_id),
                ack='client'
            )
            self.authenticated = False
            cls.client_id_seed += 1
            cls.clients_connected += 1
            self.client_id = uuid.uuid4()
            self.host_ip = self.conn.transport.current_host_and_port[0]
            if cls.client_manager:
                cls.client_manager.add_client(self.client_id, self.host_ip)
                
        except Exception as exc:
            rospy.logerr("Unable to accept incoming connection.  Reason: %s", str(exc))
        rospy.loginfo("Message Broker connected.  %d total connections.", cls.clients_connected)

        if cls.authenticate:
            rospy.loginfo("Awaiting proper authentication...")

    def on_error(self, headers, body):
        rospy.logerr('Received an error: "%s"', body)
        if 'NOT_FOUND' in body:
            # Search for an exchange name in the error text
            exchange_name = re.findall(r"exchange '([^']*)'", body)[0]
            if exchange_name:
                # Translate to topic name
                topic_name = '/' + exchange_name.replace(".", "/")
                # Forward unsubscribe to the found topic to the protocol
                msg = {'op': 'unsubscribe', 'topic': topic_name}
                self.protocol.incoming(json.dumps(msg))

    def on_message(self, headers, body):
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
                self.disconnect()
            except:
                # proper error will be handled in the protocol class
                self.protocol.incoming(body)
        # no authentication required
        else:
            # If the client advertises a topic, create a stomp-subscriber to it
            if msg['op'] == 'advertise':
                self.conn.subscribe(
                    destination=self.data_destination_prefix+stomp_topic_name(msg['topic']),
                    id=msg['topic']+str(self.protocol.client_id),
                    ack='auto'
                )
            # If the client unadvertises a topic, delete the stomp-subscriber to it
            if msg['op'] == 'unadvertise':
                self.conn.unsubscribe(
                    destination=self.data_destination_prefix+stomp_topic_name(msg['topic']),
                    id=msg['topic']+str(self.protocol.client_id)
                )
            # If the client subscribes a topic, create a new exchange in the server for it
            if msg['op'] == 'subscribe':
                # Connect with pika to create an exchange
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=self.conn.transport.current_host_and_port[0]
                    )
                )
                channel = connection.channel()
                # Use a topic exchange by default
                exchange_type = 'topic'
                exchange_args = None
                # Use an x-recent-history exchange if using history
                if cls.use_history:
                    exchange_type = 'x-recent-history'
                    exchange_args = { "x-recent-history-length": cls.history_length }
                try:
                    # Check if the exchange already exists
                    channel.exchange_declare(
                        exchange=stomp_topic_name(msg['topic']),
                        passive=True
                    )
                except:
                    # Create the exchange if it does't exist
                    channel.exchange_declare(
                        exchange=stomp_topic_name(msg['topic']),
                        exchange_type=exchange_type,
                        auto_delete=True,
                        arguments=exchange_args
                    )
                connection.close()
            # If it's a client command, acknowledge the message
            if cls.client_command_destination in headers['destination']:
                self.conn.ack(headers['message-id'], cls.client_command_destination+str(self.protocol.client_id))
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
        cls = self.__class__
        # For published data, switch to the topic name in the message with the configured prefix.
        destination = cls.server_command_destination
        msg = json.loads(message)
        if msg['op'] == 'publish':
            destination = self.data_destination_prefix + stomp_topic_name(msg['topic'])
        self.conn.send(destination, message, headers={"mandatory": True})
