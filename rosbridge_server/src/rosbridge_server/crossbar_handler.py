import rospy
import uuid

from rosauth.srv import Authentication
from rosbridge_library.rosbridge_protocol import RosbridgeProtocol
from rosbridge_library.util import json

from twisted.internet.defer import inlineCallbacks
from autobahn.twisted.wamp import ApplicationSession


def crossbar_topic_name(ros_topic_name):
    topic_name = ros_topic_name
    # Remove first slash if present
    if topic_name[0] == "/":
        topic_name = topic_name[1:]
    # Change all subsequent slashes for points
    return topic_name.replace("/", ".")

class RosbridgeCrossbarClient(ApplicationSession):
    """
    Crossbar client interface (needs crossbar server) for rosbridge
    """

    client_id_seed = 0
    clients_connected = 0
    authenticate = False

    # list of parameters
    client_command_destination = 'client_command' # application's publisher channel
    server_command_destination = 'server_command' # application's subscriber channel
    # The following are passed on to RosbridgeProtocol
    # defragmentation.py:
    fragment_timeout = 600                  # seconds
    # protocol.py:
    delay_between_messages = 0              # seconds
    max_message_size = None                 # bytes
    unregister_timeout = 10.0               # seconds

    def __init__(self, config, host_ip):
        ApplicationSession.__init__(self, config)
        self.topic_subs = []
        self.host_ip = host_ip

    @inlineCallbacks
    def onJoin(self, details):
        rospy.loginfo('Session joined: {}'.format(details))
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
            # Crossbar subscription to rosbridge client commands
            command_sub = yield self.subscribe(self.on_event, cls.client_command_destination)
            print(f"Subscribed to {cls.client_command_destination} with {command_sub.id}")
            self.authenticated = False
            cls.client_id_seed += 1
            cls.clients_connected += 1
            self.client_id = uuid.uuid4()
            if cls.client_manager:
                cls.client_manager.add_client(self.client_id, self.host_ip)
                
        except Exception as exc:
            rospy.logerr("Unable to accept incoming connection.  Reason: %s", str(exc))
        rospy.loginfo("Message Broker connected.  %d total connections.", cls.clients_connected)

        if cls.authenticate:
            rospy.loginfo("Awaiting proper authentication...")

    @inlineCallbacks
    def on_event(self, message):
        cls = self.__class__
        msg = json.loads(message)
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
                self.protocol.incoming(message)
        # no authentication required
        else:
            # If the client advertises a topic, create a crossbar-subscriber to it
            if msg['op'] == 'advertise':
                sub = yield self.subscribe(self.on_event, crossbar_topic_name(msg['topic']))
                self.topic_subs.append(sub)
            # If the client unadvertises a topic, delete the crossbar-subscriber to it
            if msg['op'] == 'unadvertise':
                for sub in self.topic_subs:
                    if sub.topic == crossbar_topic_name(msg['topic']):
                        sub.unsubscribe()
                        break
            # Forward the message to the protocol
            self.protocol.incoming(message)

    def onDisconnect(self):
        if not hasattr(self, 'protocol'):
            return  # Closed before connection was opened.
        cls = self.__class__
        cls.clients_connected -= 1
        self.protocol.finish()
        if cls.client_manager:
            cls.client_manager.remove_client(self.client_id, self.host_ip)
        rospy.loginfo("Message Broker disconnected. %d total connections.", cls.clients_connected)

    def send_message(self, message):
        cls = self.__class__
        # For published data, switch to the topic name in the message with the configured prefix.
        destination = cls.server_command_destination
        msg = json.loads(message)
        if msg['op'] == 'publish':
            destination = crossbar_topic_name(msg['topic'])
        self.publish(destination, message)
