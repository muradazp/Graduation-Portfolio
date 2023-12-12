###############################################
# Author: Aniruddha Gokhale
# Updated by: Patrick Muradaz
# Vanderbilt University
# Purpose: Publisher middleware for PAs
# Semester: Spring 2023
###############################################
#
# Here is what this middleware should do
# (1) it must maintain the ZMQ sockets, one in the REQ role to talk to the 
#     Discovery service and one in the PUB role to disseminate topics
# (2) It must, on behalf of the application logic, register the publisher 
#     application with the discovery service. To that end, it must use the 
#     protobuf-generated serialization code to send the appropriate message 
#     with the contents to the discovery service.
# (3) On behalf of the publisher appln, it must also query the discovery 
#     service (when instructed) to see if it is fine to start dissemination
# (4) It must do the actual dissemination activity of the topic data when 
#     instructed by the 
#
# Import statements
import sys, os, zmq, time, json
sys.path.append(os.getcwd())
from Apps.Common.common import handle_exception, \
  disseminate, register, deregister
from Apps.Common import discovery_pb2
from Apps.Common.topic_selector import TopicSelector
from kazoo.client import KazooClient
from kazoo.recipe.watchers import ChildrenWatch
from kazoo.recipe.watchers import DataWatch

"""Publisher Middleware class"""
class PublisherMW():

  """constructor"""
  def __init__(self, logger):
    self.logger = logger    # internal logger for print statements
    self.pub = None         # will be a ZMQ PUB socket for dissemination
    self.req = None         # will be a ZMQ REQ socket to talk to Discov service
    self.poller = None      # used to wait on incoming replies
    self.addr = None        # our advertised IP address
    self.port = None        # port num where we are going to publish our topics
    self.name = None        # name of this publisher application
    self.zkc = None         # kazoo client instance used to interact with zookeeper
    self.topiclist = None   # the list of topics we care about
    self.discovery = None   # the current connect string for discovery
    self.history = None     # the maximum history window we keep of prior publications
    self.history_windows = None     # dictionary of sliding windows of prior publications (per topic)
    self.topics_strengths = None    # dictionary of the strength of each of our topics
    self.pre_existing_pubs = None   # the pubs that existed in zookeeper before we joined

  """configure/initialize"""
  def configure(self, args):
    try:
      self.logger.debug("PublisherMW::configure")
      # First retrieve our advertised IP addr and the publication port num
      self.port = args.port
      self.addr = args.addr
      self.name = args.name
      self.history = int(args.history)
      self.history_windows = {}
      self.topics_strengths = {}
      self.pre_existing_pubs = []
      # Next setup ZMQ
      context = zmq.Context()  # returns a singleton object
      self.poller = zmq.Poller()
      # Now setup the sockets
      self.req = context.socket(zmq.REQ)
      self.pub = context.socket(zmq.PUB)
      self.poller.register(self.req, zmq.POLLIN)
      bind_string = f"tcp://{self.addr}:{self.port}"
      self.pub.bind(bind_string)
      self.logger.debug(f"PublisherMW::configure - bound to socket: {bind_string}")
      # Now setup the zookeeper kazoo client
      self.zkc = KazooClient(hosts='10.0.0.1:2181')
      self.zkc.start()
    except Exception as e: handle_exception(e)

  """register with kzookeeper and discovery using the common function"""
  def register(self, topiclist):
    try:
      self.logger.debug("PublisherMW::register")
      self.topiclist = topiclist
      for topic in topiclist: 
        self.topics_strengths[topic] = 0
        self.history_windows[topic] = []
      # first check to see if discovery is in zookeeper
      while not self.zkc.exists("/discovery"): time.sleep(1)
      # now join zookeeper once discovery has joined
      pub = f"{self.name}:{self.addr}:{self.port}"
      self.zkc.ensure_path(f'/discovery/pubs')
      self.pre_existing_pubs = self.zkc.get_children('/discovery/pubs')
      if len(self.pre_existing_pubs) == 0: time.sleep(10)
      self.logger.debug(f"PublisherMW::register - pre_existing_pubs: {self.pre_existing_pubs}")
      if not self.zkc.exists(f'/discovery/pubs/{pub}'):
        self.zkc.create(f'/discovery/pubs/{pub}', str(self.topiclist).encode(), ephemeral=True)
      self.logger.info("Registered with zookeeper.")
      # now register with the lead discovery service
      self.listen_for_new_discovery()
    except Exception as e: handle_exception(e)

  """listen to zookeeper for alerts about new publishers joining"""
  def listen_for_new_discovery(self):
    try:
      self.logger.debug("PublisherMW::listen_for_new_discovery")
      DataWatch(self.zkc, '/discovery/leader', self.handle_discovery_change)
    except Exception as e: handle_exception(e)
  
  """Handles the event where there are changes to the pubs in zookeeper"""
  def handle_discovery_change(self, data, stat):
    try:
      if (data):
        self.logger.debug(f"PublisherMW::handle_discovery_change - data: {data}")
        self.logger.info("Connecting to the lead discovery service.")
        if self.discovery: self.req.disconnect(self.discovery)
        self.discovery = "tcp://" + data.decode()
        self.req.connect(self.discovery)
        self.logger.info(f"Connected to: {self.discovery}")
        # now build a register req message
        time.sleep(.1)
        register_req = discovery_pb2.RegisterReq()
        register(self.logger, register_req.PUBLISHER, self.name, 
               self.addr, self.port, self.req, topiclist=self.topiclist)
        self.event_loop()
        self.logger.info("Publisher app registered.")
    except Exception as e: handle_exception(e)

  """deregister with kzookeeper and discovery using the common function"""
  def deregister(self, name, topiclist):
    try:
      self.logger.debug("PublisherMW::deregister")
      time.sleep(0.5) # sleep for a sec to not overwhelm
      # leave zookeeper if our node path still exists
      pub = f"{self.name}:{self.addr}:{self.port}" 
      if self.zkc.exists(f'/discovery/pubs/{pub}'): 
        self.zkc.delete(f'/discovery/pubs/{pub}')
      # now build a deregister req message
      deregister_req = discovery_pb2.DeregisterReq()
      deregister(self.logger, deregister_req.PUBLISHER, name, 
               self.addr, self.port, self.req, topiclist=topiclist)
      # now go to our event loop to receive a response to this request
      return self.event_loop()
    except Exception as e: handle_exception(e)

  """run the event loop where we expect to receive a reply to a sent request"""
  def event_loop(self):
    try:
      self.logger.debug("PublisherMW::event_loop - run the event loop")
      while True:
        # poll for events. We give it an infinite timeout.
        # The return value is a socket to event mask mapping
        events = dict(self.poller.poll())
        if self.req in events: return self.handle_reply()
    except Exception as e: handle_exception(e)

  """handle an incoming reply"""
  def handle_reply(self):
    try:
      self.logger.debug("PublisherMW::handle_reply")
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
      elif(disc_resp.msg_type == discovery_pb2.DEREGISTER):
        if disc_resp.deregister_resp.result == discovery_pb2.DeregisterResp().Result.FAILURE:
          raise Exception(disc_resp.deregister_resp.fail_reason) # return deregister error
        else: return disc_resp.deregister_resp.result # return response to deregister
      else: raise Exception("Unrecognized response message")
    except Exception as e: handle_exception(e)
            
  """disseminate the data on our pub socket using the common function"""
  def disseminate(self, iters):
      try:
        self.logger.debug("PublisherMW::disseminate")
        self.evaluate_ownership_strength() # evaluate our ownership strength ASAP
        self.listen_for_pubs_leaving() # listen for pubs leaving (to re-evaluate ownership strength)
        ts = TopicSelector()
        for i in range(iters):
          # Here, we choose to disseminate on all topics that we publish.  
          # Also, we don't care about their values. But in future assignments, this can change.
          for topic in self.topiclist:
            time.sleep(.01)
            owner_strength = self.topics_strengths[topic]
            if owner_strength == 0:
              data = topic + ":" + ts.gen_publication(topic)
              disseminate(self.logger, self.pub, data)
              self.update_history(topic, data)
              topic_hist = topic + ":" + "hs-" + str(self.history) + "-hw-" + str(self.history_windows[topic])
              disseminate(self.logger, self.pub, topic_hist)
            else: self.logger.debug(f"PublisherMW::disseminate - Skipping topic. Current strength: {owner_strength}")
        self.logger.info("Dissemination finished. Exiting.")
      except Exception as e: handle_exception(e)

  """listen to zookeeper for alerts about publishers leaving"""
  def listen_for_pubs_leaving(self):
    try:
      self.logger.debug("PublisherMW::listen_for_pubs_leaving")
      self.zkc.ensure_path('/discovery/pubs')
      ChildrenWatch(self.zkc, '/discovery/pubs', self.handle_pubs_change)
    except Exception as e: handle_exception(e)
  
  """Handles the event where there are changes to the pubs in zookeeper"""
  def handle_pubs_change(self, children):
    try:
      self.logger.debug(f"PublisherMW::handle_pubs_change - children: {children}")
      # go through our list of pre-existing pubs
      i = 0
      for pub in self.pre_existing_pubs:
        # check to see if it still exists in the children
        still_exists = False
        for child in children:
          # mark True if it does still exist
          if pub == child: still_exists = True
        # if it doesn't exist any more, remove it and re-evaluate ownership strength
        if still_exists == False:
          self.logger.info("Publisher left. Re-evaluating ownership strength.")
          # remove the dead pub using it's index in our list
          self.pre_existing_pubs.pop(i)
          self.evaluate_ownership_strength()
        i += 1
    except Exception as e: handle_exception(e)
  
  """Determines the ownership strength of our topics"""
  def evaluate_ownership_strength(self):
    try:
      self.logger.debug("PublisherMW::evaluate_ownership_strength")
      # evaluate the ownerhsip strength of each topic in our list
      i = 0
      for topic in self.topiclist:
        # evaluate the strength by looking at whether this topic is published
        # by any of the pubs that were here before we were
        for pub in self.pre_existing_pubs:
          self.logger.debug(f"PublisherMW::evaluate_ownership_strength - topic: {topic}")
          self.logger.debug(f"PublisherMW::evaluate_ownership_strength - pub: {pub}")
          # Get data of the node
          node_path = f'/discovery/pubs/{pub}'
          data, _ = self.zkc.get(node_path)

          # Decode the data as a string (assuming it's stored as bytes)
          data = data.decode('utf-8')
          self.logger.debug(f"PublisherMW::evaluate_ownership_strength - data: {data}")
          pub_topics = eval(data)
          if topic in pub_topics: self.topics_strengths[topic] += 1
        i += 1
    except Exception as e: handle_exception(e)

  """Updates our sliding history window with the current publication"""
  def update_history(self, topic, publication):
    try:
      self.logger.debug("PublisherMW::update_history")
      if len(self.history_windows[topic]) == self.history:
        self.logger.debug(f"PublisherMW::update_history - history: {self.history_windows}")
        self.history_windows[topic].pop(0)
      self.history_windows[topic].append(publication)
    except Exception as e: handle_exception(e)
