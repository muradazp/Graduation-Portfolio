###############################################
# Author: Aniruddha Gokhale
# Updated by: Patrick Muradaz
# Vanderbilt University
# Purpose: Subscriber middleware for PAs
# Semester: Spring 2023
###############################################
#
# Here is what this middleware should do
# (1) it must maintain the ZMQ sockets, one in the REQ role to talk to the 
#     Discovery service and one in the SUB role to receive topic data 
# (2) It must, on behalf of the application logic, register the subscriber 
#     application with the discovery service. To that end, it must use the 
#     protobuf-generated serialization code to send the appropriate message 
#     with the contents to the discovery service.
# (3) On behalf of the subscriber appln, it must use the ZMQ setsockopt method 
#     to subscribe to all the user-supplied topics of interest. 
# (4) Since it is a receiver, the middleware object will maintain a poller 
#     and even loop waiting for some subscription to show up(or response from 
#     Discovery service).
# (5) On receipt of a subscription, determine which topic it is and let the 
#     application level handle the incoming data. To that end, you may need to 
#     make an upcall to the application-level object.
#
# Import statements
import sys, os, zmq, json, time
sys.path.append(os.getcwd())
from Apps.Common.common import handle_exception, \
  send_message, register
from Apps.Common import discovery_pb2
from kazoo.client import KazooClient
from kazoo.recipe.watchers import ChildrenWatch
from kazoo.recipe.watchers import DataWatch

"""Subscriber Middleware class"""
class SubscriberMW():

  """constructor"""
  def __init__(self, logger):
    self.logger = logger  # internal logger for print statements
    self.req = None       # will be a ZMQ REQ socket for register with discovery
    self.sub = None       # will be a ZMQ REQ socket for subscriptions
    self.poller = None    # used to wait on incoming subscriptions
    self.addr = None      # advertised IP address (might not be necessary)
    self.port = None      # port num (might not be necessary)
    self.name = None      # name of this publisher application
    self.topiclist = None # the list of topics we care about
    self.pubs = None      # the array of our current publishers
    self.discovery = None # the current connect string for discovery
    self.min_hist = None  # the minimum history we need from our pubs
    self.got_hist = None  # used to determine if we have received the pub hist yet or not

  """configure/initialize"""
  def configure(self, args):
    try:
      self.logger.debug("SubscriberMW::configure")
      # retrieve our advertised IP addr and the publication port num
      self.port = args.port
      self.addr = args.addr
      self.name = args.name
      self.min_hist = int(args.history)
      self.got_hist = {}
      self.pubs = []
      # setup ZMQ
      context = zmq.Context()
      self.poller = zmq.Poller()
      # Now setup the sockets
      self.req = context.socket(zmq.REQ)
      self.sub = context.socket(zmq.SUB)
      self.poller.register(self.req, zmq.POLLIN)
      # Now setup the zookeeper kazoo client
      self.zkc = KazooClient(hosts='10.0.0.1:2181')
      self.zkc.start()
    except Exception as e: handle_exception(e)
    
  """register with the discovery service"""
  def register(self, topiclist):
    try:
      self.logger.debug("SubscriberMW::register")
      self.topiclist = topiclist
      for topic in topiclist: self.got_hist[topic] = False
      # first check to see if discovery is in zookeeper
      while not self.zkc.exists("/discovery"): time.sleep(1)
      # now join zookeeper once discovery has joined
      sub = f"{self.name}:{self.addr}:{self.port}"
      if not self.zkc.exists(f'/discovery/subs/{sub}'):
        self.zkc.ensure_path(f'/discovery/subs')
        self.zkc.create(f'/discovery/subs/{sub}', b'subscriber-node', ephemeral=True)
      self.logger.info("Registered with zookeeper.")
      # now register with the lead discovery service
      self.listen_for_new_discovery()
    except Exception as e: handle_exception(e)

  """listen to zookeeper for alerts about new publishers joining"""
  def listen_for_new_discovery(self):
    try:
      self.logger.debug("SubscriberMW::listen_for_new_discovery")
      DataWatch(self.zkc, '/discovery/leader', self.handle_discovery_change)
    except Exception as e: handle_exception(e)
  
  """Handles the event where there are changes to the pubs in zookeeper"""
  def handle_discovery_change(self, data, stat, event=None):
    try:
      if (data):
        self.logger.debug(f"SubscriberMW::handle_discovery_change - data: {data}")
        self.logger.info("Connecting to the lead discovery service.")
        if self.discovery: self.req.disconnect(self.discovery)
        self.discovery = "tcp://" + data.decode()
        self.req.connect(self.discovery)
        self.logger.info(f"Connected to: {self.discovery}")
        time.sleep(0.1)
        # now build a register req message
        register_req = discovery_pb2.RegisterReq()
        register(self.logger, register_req.SUBSCRIBER, self.name, 
               self.addr, self.port, self.req, topiclist=self.topiclist)
        self.event_loop()
        self.logger.info("Subscriber app registered.")
    except Exception as e: handle_exception(e)

  """locate all of the publishers that we care about"""
  def locate_pubs(self, topiclist):
    try:
      self.logger.debug("SubscriberMW::locate_pubs")
      self.topiclist = topiclist
      # build the request message
      disc_req = discovery_pb2.DiscoveryReq()
      getpubs_msg = discovery_pb2.LookupPubByTopicReq()
      getpubs_msg.topiclist.extend(topiclist)
      disc_req.msg_type = discovery_pb2.LOOKUP_PUB_BY_TOPIC
      disc_req.topics.CopyFrom(getpubs_msg)
      # send the message
      send_message(self.logger, self.req, disc_req)
      # now go to our event loop to receive a response to this request
      self.logger.debug("SubscriberMW::locate_pubs - now wait for reply")
      publishers = self.event_loop()
      return publishers
    except Exception as e: handle_exception(e)

  """listen to zookeeper for alerts about new publishers joining"""
  def listen_for_new_pubs(self):
    try:
      self.logger.debug("SubscriberMW::listen_for_new_pubs")
      self.zkc.ensure_path('/discovery/pubs')
      ChildrenWatch(self.zkc, '/discovery/pubs', self.handle_pubs_change)
    except Exception as e: handle_exception(e)
  
  """Handles the event where there are changes to the pubs in zookeeper"""
  def handle_pubs_change(self, children):
    try:
      self.logger.debug(f"SubscriberMW::handle_pubs_change - children: {children}")
      time.sleep(0.5) # sleep for a moment to not overwhelm
      pubs = self.locate_pubs(self.topiclist)
      if (len(pubs) < len(self.pubs)): self.logger.info("Publisher left. Removing from list.")
      if (len(pubs) == 0): self.logger.info("No publishers present. Waiting...")
      else:
        included = False
        for pub1 in pubs:
          p1 = json.loads(pub1)
          for pub2 in self.pubs:
            p2 = json.loads(pub2)
            if p1['name'] == p2['name']: included = True
          if not included:
            pub_addr = f"tcp://{p1['ip']}:{p1['port']}"
            self.sub.connect(pub_addr)
            self.logger.info(f"Subscribed to new publisher: {pub_addr}")
          included = False
      self.pubs = pubs
    except Exception as e: handle_exception(e)

  """subscribe to the publishers that we care about"""
  def sub_to_pubs(self, pubs, topiclist):
    try:
      self.logger.debug("SubscriberMW::sub_to_pubs")
      # First, set up the ZMQ socket filters for our desired topics
      for topic in topiclist:
        self.logger.debug(f"SubscriberMW::sub_to_pubs - topic: {topic}")
        self.sub.setsockopt(zmq.SUBSCRIBE, topic.encode('utf-8'))
      # Then subscribe to each publisher we care about
      for pub in pubs:
        p = json.loads(pub)
        pub_addr = f"tcp://{p['ip']}:{p['port']}"
        self.sub.connect(pub_addr)
        self.logger.info(f"Subscribed to publisher: {pub_addr}")
        self.pubs.append(pub)
    except Exception as e: handle_exception(e)

  """listen to all of our subscribed publishers"""
  def listen_to_pubs(self):
    try:
      self.logger.debug("SubscriberMW::listen_to_pubs")
      while True:
        # receive messages from the publishers
        message_bytes = self.sub.recv_multipart()
        message = str(message_bytes[0], 'UTF-8')
        # determine if a message is a history message
        if "pi-" in message and "-hs-" in message and "-hw-" in message:
          # if it is a history message, determine if it matches our requirement
          history_size = int(message.split("-hs-")[1].split("-hw-")[0])
          topic = message.split(":")[0]
          if history_size >= self.min_hist and not self.got_hist[topic]:
            # if it does match, print out the history (we have received it successfully)
            self.logger.info(f"History received from publisher for topic: {topic}")
            history = eval(message.split("-hw-")[1])
            for hist_msg in history: self.logger.info(f"Historic message from publisher: {hist_msg}")
            self.got_hist[topic] = True
          # if it does not match, disconnect from the publisher
          elif history_size < self.min_hist:
            pub_info = message.split("pi-")[1].split("-hs-")[0]
            self.logger.info(f"Publisher doesnt meet minimum history. Unsubscribing.")
            self.sub.disconnect(f"tcp://{pub_info}")
            self.logger.info(f"Unsubscribed from publisher: {pub_info}")
        # if it is not a history message, simply print the message
        else: self.logger.info(f"Message from publisher: {message}")
    except Exception as e: handle_exception(e)

  """run event loop where we expect to receive replies to sent requests"""
  def event_loop(self):
    try:
      self.logger.debug("SubscriberMW::event_loop - run the event loop")
      while True:
        # poll for events. We give it an infinite timeout.
        # The return value is a socket to event mask mapping
        events = dict(self.poller.poll())
        if self.req in events: return self.handle_reply()
    except Exception as e: handle_exception(e)
            
  """handle an incoming reply"""
  def handle_reply(self):
    try:
      self.logger.debug("SubscriberMW::handle_reply")
      # let us first receive all the bytes
      bytesRcvd = self.req.recv()
      # now use protobuf to deserialize the bytes
      disc_resp = discovery_pb2.DiscoveryResp()
      disc_resp.ParseFromString(bytesRcvd)
      # Depending on the message type, the contents of the msg will differ
      if(disc_resp.msg_type == discovery_pb2.REGISTER):
        if disc_resp.register_resp.result == discovery_pb2.RegisterResp().Result.FAILURE:
          raise Exception(disc_resp.register_resp.fail_reason) # return register error
        else: return disc_resp.register_resp.result # return response to register
      elif(disc_resp.msg_type == discovery_pb2.LOOKUP_PUB_BY_TOPIC):
        return disc_resp.resp.publishers # response to lookup_pub... message
      else: raise Exception("Unrecognized response message.")
    except Exception as e: handle_exception(e)
