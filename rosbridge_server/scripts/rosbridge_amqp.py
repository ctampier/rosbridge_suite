#!/usr/bin/env python

from __future__ import print_function
import rospy
import sys

from rosbridge_server import ClientManager
from rosbridge_server import RosbridgeAmqpClient

from rosbridge_library.capabilities.advertise import Advertise
from rosbridge_library.capabilities.publish import Publish
from rosbridge_library.capabilities.subscribe import Subscribe
from rosbridge_library.capabilities.advertise_service import AdvertiseService
from rosbridge_library.capabilities.unadvertise_service import UnadvertiseService
from rosbridge_library.capabilities.call_service import CallService

from functools import partial
import time

def shutdown_hook(amqpClient):
    amqpClient.stop()

if __name__ == "__main__":
    rospy.init_node("rosbridge_websocket")

    ##################################################
    # Parameter handling                             #
    ##################################################
    RosbridgeAmqpClient.reconnect_delay = rospy.get_param('~reconnect_delay', -1.0)  # seconds
    RosbridgeAmqpClient.use_history = rospy.get_param('~use_history', True)
    RosbridgeAmqpClient.history_length = rospy.get_param('~history_length', 100)
    heartbeat_producer = rospy.get_param('~heartbeat_producer', 0) # milliseconds
    heartbeat_consumer = rospy.get_param('~heartbeat_consumer', 0) # milliseconds
    user = rospy.get_param('~user', 'guest')
    password = rospy.get_param('~password', 'guest')

    # get RosbridgeProtocol parameters
    RosbridgeAmqpClient.fragment_timeout = rospy.get_param('~fragment_timeout',
                                                          RosbridgeAmqpClient.fragment_timeout)
    RosbridgeAmqpClient.delay_between_messages = rospy.get_param('~delay_between_messages',
                                                                RosbridgeAmqpClient.delay_between_messages)
    RosbridgeAmqpClient.max_message_size = rospy.get_param('~max_message_size',
                                                          RosbridgeAmqpClient.max_message_size)
    RosbridgeAmqpClient.unregister_timeout = rospy.get_param('~unregister_timeout',
                                                          RosbridgeAmqpClient.unregister_timeout)

    if RosbridgeAmqpClient.max_message_size == "None":
        RosbridgeAmqpClient.max_message_size = None

    # if authentication should be used
    RosbridgeAmqpClient.authenticate = rospy.get_param('~authenticate', False)
    port = rospy.get_param('~port', 5672)
    host = rospy.get_param('~host', "localhost")

    RosbridgeAmqpClient.client_manager = ClientManager()

    # Get the glob strings and parse them as arrays.
    RosbridgeAmqpClient.topics_glob = [
        element.strip().strip("'")
        for element in rospy.get_param('~topics_glob', '')[1:-1].split(',')
        if len(element.strip().strip("'")) > 0]
    RosbridgeAmqpClient.services_glob = [
        element.strip().strip("'")
        for element in rospy.get_param('~services_glob', '')[1:-1].split(',')
        if len(element.strip().strip("'")) > 0]
    RosbridgeAmqpClient.params_glob = [
        element.strip().strip("'")
        for element in rospy.get_param('~params_glob', '')[1:-1].split(',')
        if len(element.strip().strip("'")) > 0]

    if "--port" in sys.argv:
        idx = sys.argv.index("--port")+1
        if idx < len(sys.argv):
            port = int(sys.argv[idx])
        else:
            print("--port argument provided without a value.")
            sys.exit(-1)

    if "--host" in sys.argv:
        idx = sys.argv.index("--host")+1
        if idx < len(sys.argv):
            host = str(sys.argv[idx])
        else:
            print("--host argument provided without a value.")
            sys.exit(-1)

    if "--reconnect_delay" in sys.argv:
        idx = sys.argv.index("--reconnect_delay") + 1
        if idx < len(sys.argv):
            RosbridgeAmqpClient.reconnect_delay = float(sys.argv[idx])
        else:
            print("--reconnect_delay argument provided without a value.")
            sys.exit(-1)

    if "--use_history" in sys.argv:
        RosbridgeAmqpClient.use_history = True

    if "--history_length" in sys.argv:
        idx = sys.argv.index("--history_length") + 1
        if idx < len(sys.argv):
            RosbridgeAmqpClient.history_length = int(sys.argv[idx])
        else:
            print("--history_length argument provided without a value.")
            sys.exit(-1)

    if "--fragment_timeout" in sys.argv:
        idx = sys.argv.index("--fragment_timeout") + 1
        if idx < len(sys.argv):
            RosbridgeAmqpClient.fragment_timeout = int(sys.argv[idx])
        else:
            print("--fragment_timeout argument provided without a value.")
            sys.exit(-1)

    if "--delay_between_messages" in sys.argv:
        idx = sys.argv.index("--delay_between_messages") + 1
        if idx < len(sys.argv):
            RosbridgeAmqpClient.delay_between_messages = float(sys.argv[idx])
        else:
            print("--delay_between_messages argument provided without a value.")
            sys.exit(-1)

    if "--max_message_size" in sys.argv:
        idx = sys.argv.index("--max_message_size") + 1
        if idx < len(sys.argv):
            value = sys.argv[idx]
            if value == "None":
                RosbridgeAmqpClient.max_message_size = None
            else:
                RosbridgeAmqpClient.max_message_size = int(value)
        else:
            print("--max_message_size argument provided without a value. (can be None or <Integer>)")
            sys.exit(-1)

    if "--unregister_timeout" in sys.argv:
        idx = sys.argv.index("--unregister_timeout") + 1
        if idx < len(sys.argv):
            RosbridgeAmqpClient.unregister_timeout = float(sys.argv[idx])
        else:
            print("--unregister_timeout argument provided without a value.")
            sys.exit(-1)

    if "--topics_glob" in sys.argv:
        idx = sys.argv.index("--topics_glob") + 1
        if idx < len(sys.argv):
            value = sys.argv[idx]
            if value == "None":
                RosbridgeAmqpClient.topics_glob = []
            else:
                RosbridgeAmqpClient.topics_glob = [element.strip().strip("'") for element in value[1:-1].split(',')]
        else:
            print("--topics_glob argument provided without a value. (can be None or a list)")
            sys.exit(-1)

    if "--services_glob" in sys.argv:
        idx = sys.argv.index("--services_glob") + 1
        if idx < len(sys.argv):
            value = sys.argv[idx]
            if value == "None":
                RosbridgeAmqpClient.services_glob = []
            else:
                RosbridgeAmqpClient.services_glob = [element.strip().strip("'") for element in value[1:-1].split(',')]
        else:
            print("--services_glob argument provided without a value. (can be None or a list)")
            sys.exit(-1)

    if "--params_glob" in sys.argv:
        idx = sys.argv.index("--params_glob") + 1
        if idx < len(sys.argv):
            value = sys.argv[idx]
            if value == "None":
                RosbridgeAmqpClient.params_glob = []
            else:
                RosbridgeAmqpClient.params_glob = [element.strip().strip("'") for element in value[1:-1].split(',')]
        else:
            print("--params_glob argument provided without a value. (can be None or a list)")
            sys.exit(-1)

    if "--heartbeat_producer" in sys.argv:
        idx = sys.argv.index("--heartbeat_producer") + 1
        if idx < len(sys.argv):
            heartbeat_producer = int(sys.argv[idx])
        else:
            print("--heartbeat_producer argument provided without a value.")
            sys.exit(-1)

    if "--heartbeat_consumer" in sys.argv:
        idx = sys.argv.index("--heartbeat_consumer") + 1
        if idx < len(sys.argv):
            heartbeat_consumer = int(sys.argv[idx])
        else:
            print("--heartbeat_consumer argument provided without a value.")
            sys.exit(-1)

    if "--user" in sys.argv:
        idx = sys.argv.index("--user") + 1
        if idx < len(sys.argv):
            user = sys.argv[idx]
        else:
            print("--user argument provided without a value.")
            sys.exit(-1)

    if "--password" in sys.argv:
        idx = sys.argv.index("--password") + 1
        if idx < len(sys.argv):
            password = sys.argv[idx]
        else:
            print("--password argument provided without a value.")
            sys.exit(-1)

    # To be able to access the list of topics and services, you must be able to access the rosapi services.
    if RosbridgeAmqpClient.services_glob:
        RosbridgeAmqpClient.services_glob.append("/rosapi/*")

    Subscribe.topics_glob = RosbridgeAmqpClient.topics_glob
    Advertise.topics_glob = RosbridgeAmqpClient.topics_glob
    Publish.topics_glob = RosbridgeAmqpClient.topics_glob
    AdvertiseService.services_glob = RosbridgeAmqpClient.services_glob
    UnadvertiseService.services_glob = RosbridgeAmqpClient.services_glob
    CallService.services_glob = RosbridgeAmqpClient.services_glob

    ##################################################
    # Done with parameter handling                   #
    ##################################################    

    amqpClient = RosbridgeAmqpClient(
        f'amqp://{user}:{password}@{host}:{port}/%2F?connection_attempts=3&heartbeat=3600'
    )
    amqpClient.run()
    rospy.on_shutdown(partial(shutdown_hook, amqpClient))
    rospy.spin()
