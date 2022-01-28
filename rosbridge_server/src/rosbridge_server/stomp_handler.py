import rospy
import uuid

from rosauth.srv import Authentication

import stomp

from rosbridge_library.rosbridge_protocol import RosbridgeProtocol
from rosbridge_library.util import json


class RosbridgeStompSocket(stomp.ConnectionListener):
    """
    STOMP client interface (needs STOMP server) for rosbridge
    """

    client_id_seed = 0
    clients_connected = 0
    authenticate = False

    # list of parameters
    sub_stomp_topic = 'pub' # application's publisher channel
    pub_stomp_topic = 'sub' # application's subscriber channel
    reconnect_delay = -1
    # The following are passed on to RosbridgeProtocol
    # defragmentation.py:
    fragment_timeout = 600                  # seconds
    # protocol.py:
    delay_between_messages = 0              # seconds
    max_message_size = None                 # bytes
    unregister_timeout = 10.0               # seconds

    def __init__(self, conn, user, password):
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
            self.protocol = RosbridgeProtocol(cls.client_id_seed, parameters=parameters)
            self.protocol.outgoing = self.send_message
            self.conn.subscribe(destination='/topic/'+self.sub_stomp_topic, id=self.protocol.client_id, ack='auto')
            self.authenticated = False
            cls.client_id_seed += 1
            cls.clients_connected += 1
            self.client_id = uuid.uuid4()
            self.host_ip = self.conn.transport.current_host_and_port[0]
            if cls.client_manager:
                cls.client_manager.add_client(self.client_id, self.host_ip)
                
        except Exception as exc:
            rospy.logerr("Unable to accept incoming connection.  Reason: %s", str(exc))
        rospy.loginfo("Client connected.  %d clients total.", cls.clients_connected)
        if cls.authenticate:
            rospy.loginfo("Awaiting proper authentication...")

    def on_error(self, headers, body):
        rospy.logerr('Received an error: "%s"', body)

    def on_message(self, headers, body):
        cls = self.__class__
        # check if we need to authenticate
        if cls.authenticate and not self.authenticated:
            try:
                msg = json.loads(body)

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
        else:
            # no authentication required
            self.protocol.incoming(body)

    def on_disconnected(self):
        if not hasattr(self, 'protocol'):
            return  # Closed before connection was opened.
        cls = self.__class__
        cls.clients_connected -= 1

        if cls.client_manager:
            cls.client_manager.remove_client(self.client_id, self.host_ip)
        rospy.loginfo("Client disconnected. %d clients total.", cls.clients_connected)
        if cls.reconnect_delay >= 0:
            rospy.loginfo("Will try to reconnect afer %d seconds", cls.reconnect_delay)
            rospy.sleep(cls.reconnect_delay)
            self.connect()

    def send_message(self, message):
        cls = self.__class__
        self.conn.send('/topic/'+cls.pub_stomp_topic, message)
