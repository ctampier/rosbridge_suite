#!/usr/bin/env python

from __future__ import print_function
import rospy
import sys

from twisted.internet import reactor
from autobahn.wamp.types import ComponentConfig
from autobahn.twisted.wamp import ApplicationRunner
from rosbridge_server import ClientManager
from rosbridge_server import RosbridgeCrossbarClient

from rosbridge_library.capabilities.advertise import Advertise
from rosbridge_library.capabilities.publish import Publish
from rosbridge_library.capabilities.subscribe import Subscribe
from rosbridge_library.capabilities.advertise_service import AdvertiseService
from rosbridge_library.capabilities.unadvertise_service import UnadvertiseService
from rosbridge_library.capabilities.call_service import CallService


def shutdown_hook():
    if reactor.running:
        reactor.stop()
    else:
        rospy.logwarn("Can't stop the reactor, it wasn't running")
        


if __name__ == "__main__":
    rospy.init_node("rosbridge_websocket")

    ##################################################
    # Parameter handling                             #
    ##################################################

    # get RosbridgeProtocol parameters
    RosbridgeCrossbarClient.fragment_timeout = rospy.get_param('~fragment_timeout',
                                                          RosbridgeCrossbarClient.fragment_timeout)
    RosbridgeCrossbarClient.delay_between_messages = rospy.get_param('~delay_between_messages',
                                                                RosbridgeCrossbarClient.delay_between_messages)
    RosbridgeCrossbarClient.max_message_size = rospy.get_param('~max_message_size',
                                                          RosbridgeCrossbarClient.max_message_size)
    RosbridgeCrossbarClient.unregister_timeout = rospy.get_param('~unregister_timeout',
                                                          RosbridgeCrossbarClient.unregister_timeout)

    if RosbridgeCrossbarClient.max_message_size == "None":
        RosbridgeCrossbarClient.max_message_size = None

    # if authentication should be used
    RosbridgeCrossbarClient.authenticate = rospy.get_param('~authenticate', False)
    port = rospy.get_param('~port', 8080)
    host = rospy.get_param('~host', "localhost")

    RosbridgeCrossbarClient.client_manager = ClientManager()

    # Get the glob strings and parse them as arrays.
    RosbridgeCrossbarClient.topics_glob = [
        element.strip().strip("'")
        for element in rospy.get_param('~topics_glob', '')[1:-1].split(',')
        if len(element.strip().strip("'")) > 0]
    RosbridgeCrossbarClient.services_glob = [
        element.strip().strip("'")
        for element in rospy.get_param('~services_glob', '')[1:-1].split(',')
        if len(element.strip().strip("'")) > 0]
    RosbridgeCrossbarClient.params_glob = [
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

    if "--fragment_timeout" in sys.argv:
        idx = sys.argv.index("--fragment_timeout") + 1
        if idx < len(sys.argv):
            RosbridgeCrossbarClient.fragment_timeout = int(sys.argv[idx])
        else:
            print("--fragment_timeout argument provided without a value.")
            sys.exit(-1)

    if "--delay_between_messages" in sys.argv:
        idx = sys.argv.index("--delay_between_messages") + 1
        if idx < len(sys.argv):
            RosbridgeCrossbarClient.delay_between_messages = float(sys.argv[idx])
        else:
            print("--delay_between_messages argument provided without a value.")
            sys.exit(-1)

    if "--max_message_size" in sys.argv:
        idx = sys.argv.index("--max_message_size") + 1
        if idx < len(sys.argv):
            value = sys.argv[idx]
            if value == "None":
                RosbridgeCrossbarClient.max_message_size = None
            else:
                RosbridgeCrossbarClient.max_message_size = int(value)
        else:
            print("--max_message_size argument provided without a value. (can be None or <Integer>)")
            sys.exit(-1)

    if "--unregister_timeout" in sys.argv:
        idx = sys.argv.index("--unregister_timeout") + 1
        if idx < len(sys.argv):
            RosbridgeCrossbarClient.unregister_timeout = float(sys.argv[idx])
        else:
            print("--unregister_timeout argument provided without a value.")
            sys.exit(-1)

    if "--topics_glob" in sys.argv:
        idx = sys.argv.index("--topics_glob") + 1
        if idx < len(sys.argv):
            value = sys.argv[idx]
            if value == "None":
                RosbridgeCrossbarClient.topics_glob = []
            else:
                RosbridgeCrossbarClient.topics_glob = [element.strip().strip("'") for element in value[1:-1].split(',')]
        else:
            print("--topics_glob argument provided without a value. (can be None or a list)")
            sys.exit(-1)

    if "--services_glob" in sys.argv:
        idx = sys.argv.index("--services_glob") + 1
        if idx < len(sys.argv):
            value = sys.argv[idx]
            if value == "None":
                RosbridgeCrossbarClient.services_glob = []
            else:
                RosbridgeCrossbarClient.services_glob = [element.strip().strip("'") for element in value[1:-1].split(',')]
        else:
            print("--services_glob argument provided without a value. (can be None or a list)")
            sys.exit(-1)

    if "--params_glob" in sys.argv:
        idx = sys.argv.index("--params_glob") + 1
        if idx < len(sys.argv):
            value = sys.argv[idx]
            if value == "None":
                RosbridgeCrossbarClient.params_glob = []
            else:
                RosbridgeCrossbarClient.params_glob = [element.strip().strip("'") for element in value[1:-1].split(',')]
        else:
            print("--params_glob argument provided without a value. (can be None or a list)")
            sys.exit(-1)

    # To be able to access the list of topics and services, you must be able to access the rosapi services.
    if RosbridgeCrossbarClient.services_glob:
        RosbridgeCrossbarClient.services_glob.append("/rosapi/*")

    Subscribe.topics_glob = RosbridgeCrossbarClient.topics_glob
    Advertise.topics_glob = RosbridgeCrossbarClient.topics_glob
    Publish.topics_glob = RosbridgeCrossbarClient.topics_glob
    AdvertiseService.services_glob = RosbridgeCrossbarClient.services_glob
    UnadvertiseService.services_glob = RosbridgeCrossbarClient.services_glob
    CallService.services_glob = RosbridgeCrossbarClient.services_glob

    ##################################################
    # Done with parameter handling                   #
    ##################################################    

    rospy.on_shutdown(shutdown_hook)
    session = RosbridgeCrossbarClient(ComponentConfig("webgui", {}), host)
    runner = ApplicationRunner(
        url=f"ws://{host}:{port}/ws", 
        realm="webgui", 
        extra=dict(
            host = host,
            max_events = 5,
        )
    )
    runner.run(session, auto_reconnect=True)
    rospy.loginfo('Rosbridge Crossbar connected to {}:{}'.format(host, port))
    rospy.spin()
