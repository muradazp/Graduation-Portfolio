###############################################
# Author: Aniruddha Gokhale
# Updated by: Patrick Muradaz
# Vanderbilt University
# Purpose: Broker middleware for PAs
# Semester: Spring 2023
###############################################
#
# The broker serves as a proxy and hence has both publisher and subscriber roles. 
# So in addition to the REQ socket to talk to the Discovery service, it will have 
# both PUB and SUB sockets as it must work on behalf of the real publishers and 
# subscribers. So this will have the logic of both publisher and subscriber middleware.
#
# Import statements
import sys, os, zmq, json, time, random
sys.path.append(os.getcwd())
from Apps.Common.common import handle_exception, \
  send_message, disseminate, register
from Apps.Common import discovery_pb2
from kazoo.client import KazooClient
from kazoo.recipe.watchers import ChildrenWatch
from kazoo.recipe.watchers import DataWatch

"""Broker Middleware class"""
class BrokerMW():

  """constructor"""
  def __init__(self, logger):
    self.logger = logger  # internal logger for print statements
    self.pub = None       # will be a ZMQ PUB socket for dissemination
    self.sub = None       # will be a ZMQ SUB socket for listening to pubs
    self.req = None       # will be a ZMQ REQ socket to talk to Discov service
    self.poller = None    # used to wait on incoming replies
    self.name = None      # our name (some unique name)
    self.addr = None      # our advertised IP address
    self.port = None      # port num where we are going to publish our topics
    self.zkc = None       # kazoo client instance used to interact with zookeeper
    self.pubs = None      # the array of our current publishers
    self.discovery = None # the current connect string for discovery
    self.is_lead = False  # used to tell if we are the current leader
    self.index = None     # our current co-lead index
    self.pub_listen = False # used to tell if we are listening for new pubs
    self.watch_lead = False # used to tell if we are watching the leaders

  """configure/initialize"""
  def configure(self, args):
    try:
      self.logger.debug("BrokerMW::configure")
      # First retrieve our advertised IP addr and the publication port num
      self.name = args.name
      self.port = args.port
      self.addr = args.addr
      self.pubs = []
      # Now setup ZMQ
      context = zmq.Context()  # returns a singleton object
      self.poller = zmq.Poller()
      # Now setup the sockets
      self.req = context.socket(zmq.REQ)
      self.pub = context.socket(zmq.PUB)
      self.sub = context.socket(zmq.SUB)
      self.poller.register(self.req, zmq.POLLIN)
      bind_string = f"tcp://{self.addr}:{self.port}"
      self.logger.debug(f"BrokerMW::configure - bound to: {bind_string}")
      self.pub.bind(bind_string)
      # Finally, subscribe to any/all topics
      self.sub.subscribe("")
      # Now setup the zookeeper kazoo client
      self.zkc = KazooClient(hosts='10.0.0.1:2181')
      self.zkc.start()
      return self.join_zookeeper()
    except Exception as e: handle_exception(e)

  """handles configuring this nodes place in zookeeper"""
  def join_zookeeper(self):
      try:
          self.logger.debug("BrokerMW::join_zookeeper")
          self.zkc.ensure_path('/broker/leaders')
          self.zkc.ensure_path('/broker/backups')
          if not self.zkc.exists('/broker/leaders/lead-0'):
              self.zkc.create('/broker/leaders/lead-0', f'{self.addr}:{self.port}'.encode(), ephemeral=True)
              self.is_lead = True
              self.index = 0
          else:
              self.zkc.create(f'/broker/backups/backup-{self.addr}:{self.port}', b'broker-backup', ephemeral=True)
              self.is_lead = False
          return self.is_lead
      except Exception as e: handle_exception(e)

  """handles configuring this nodes place in zookeeper as a co-leader"""
  def join_zookeeper_as_colead(self, index):
      try:
          self.logger.debug("BrokerMW::join_zookeeper_as_colead")
          self.zkc.ensure_path('/broker/leaders')
          if self.zkc.exists(f'/broker/backups/backup-{self.addr}:{self.port}'):
            self.zkc.delete(f'/broker/backups/backup-{self.addr}:{self.port}')
          self.zkc.create('/broker/leaders/lead-' + index, f'{self.addr}:{self.port}'.encode(), ephemeral=True)
          self.is_lead = True
          self.index = int(index)
      except Exception as e: handle_exception(e)

  """handles configuring this nodes place in zookeeper as a backup"""
  def return_to_backup_pool(self):
      try:
          self.logger.debug("BrokerMW::return_to_backup_pool")
          self.zkc.ensure_path('/broker/backups')
          if self.index and self.zkc.exists(f'/broker/leaders/lead-{str(self.index)}'):
            self.zkc.delete(f'/broker/leaders/lead-{str(self.index)}')
          self.zkc.create(f'/broker/backups/backup-{self.addr}:{self.port}', b'broker-backup', ephemeral=True)
          self.is_lead = False
          if not self.pub_listen: self.listen_for_new_pubs()
          if not self.watch_lead: self.watch_leaders()
      except Exception as e: handle_exception(e)
  
  """watches the lead broker nodes to take over if needed"""
  def watch_leaders(self):
      try:
          self.logger.debug("BrokerMW::watch_leaders")
          self.logger.info("Watching current leaders to take over if needed...")
          DataWatch(self.zkc, '/broker/leaders/lead-0', self.leader_left)
          self.watch_lead = True
          while not self.is_lead: time.sleep(1) # check if we are a lead every second
      except Exception as e: handle_exception(e)

  """called when the leader broker node dies or leaves"""
  def leader_left(self, data, stat):
      try:
          self.logger.debug("BrokerMW::leader_left")
          # only continue if a lead has died and we are not a lead
          if data == None and stat == None and self.is_lead == False:
              self.logger.info("A lead broker node has left.")
              time.sleep(random.uniform(0, 1)) # random wait so there is no leader overlap
              if not self.zkc.exists('/broker/leaders/lead-0'):
                  self.logger.info("Setting self as a new lead node.")
                  self.zkc.delete(f'/broker/backups/backup-{self.addr}:{self.port}')
                  self.zkc.create('/broker/leaders/lead-0', f'{self.addr}:{self.port}'.encode(), ephemeral=True)
                  self.is_lead = True
                  self.register_and_listen()
              else: self.logger.info("Another node has replaced the dead lead.")
      except Exception as e: handle_exception(e)

  """register the broker and start listening to pubs"""
  def register_and_listen(self):
    # Use middleware to register us with the discovery service
    self.logger.info("Registering app with discovery service.")
    result = self.register()
    self.logger.debug(f"BrokerAppln::driver - result: {str(result)}")
    self.logger.info("Broker app registered.")
    # Now, find all publishers that are registered with discovery
    self.logger.info("Locating all registered publishers.")
    pubs = self.locate_pubs()
    # Then, subscribe and listen to the publishers
    if len(pubs) > 0: 
      self.logger.info("Subscribing to all registered publishers.")
      self.sub_to_pubs(pubs)
    if not self.pub_listen: self.listen_for_new_pubs()
    self.listen_to_pubs()

  """register with the discovery service using the common function"""
  def register(self):
    try:
      self.logger.debug("BrokerMW::register")
      # register with the lead discovery service
      self.listen_for_new_discovery()
    except Exception as e: handle_exception(e)

  """listen to zookeeper for alerts about new publishers joining"""
  def listen_for_new_discovery(self):
    try:
      self.logger.debug("BrokerMW::listen_for_new_discovery")
      DataWatch(self.zkc, '/discovery/leader', self.handle_discovery_change)
    except Exception as e: handle_exception(e)
  
  """Handles the event where there are changes to discovery leader in zookeeper"""
  def handle_discovery_change(self, data, stat):
    try:
      if (data):
        self.logger.debug(f"BrokerMW::handle_discovery_change - data: {data}")
        self.logger.info("Connecting to the lead discovery service.")
        if self.discovery: self.req.disconnect(self.discovery)
        self.discovery = "tcp://" + data.decode()
        self.req.connect(self.discovery)
        self.logger.info(f"Connected to: {self.discovery}")
        time.sleep(0.1)
        # now build a register req message
        register_req = discovery_pb2.RegisterReq()
        register(self.logger, register_req.BROKER, self.name, 
               self.addr, self.port, self.req)
        self.event_loop()
        self.logger.info("Subscriber app registered.")
    except Exception as e: handle_exception(e)

  """locate all of the registered publishers"""
  def locate_pubs(self):
    try:
      self.logger.debug("BrokerMW::locate_pubs")
      # build the request message
      disc_req = discovery_pb2.DiscoveryReq()
      getpubs_msg = discovery_pb2.LookupAllPubsReq()
      disc_req.msg_type = discovery_pb2.LOOKUP_ALL_PUBS
      disc_req.pubs_req.CopyFrom(getpubs_msg)
      # send the message
      send_message(self.logger, self.req, disc_req)
      # now go to our event loop to receive a response to this request
      publishers = self.event_loop()
      return publishers
    except Exception as e: handle_exception(e)

  """listen to zookeeper for alerts about new publishers joining"""
  def listen_for_new_pubs(self):
    try:
      self.logger.debug("BrokerMW::listen_for_new_pubs")
      self.logger.info("Watching current pubs load to balance if needed...")
      self.zkc.ensure_path('/discovery/pubs')
      ChildrenWatch(self.zkc, '/discovery/pubs', self.handle_pubs_change)
      self.pub_listen = True
    except Exception as e: handle_exception(e)
  
  """Handles the event where there are changes to the pubs in zookeeper"""
  def handle_pubs_change(self, children):
    try:
      self.logger.debug(f"BrokerMW::handle_pubs_change - children: {children}")
      leaders = self.zkc.get_children('/broker/leaders')
      index = len(leaders)
      self.logger.debug(f"BrokerMW::handle_pubs_change - index: {index}")
      if self.is_lead and len(self.pubs) == 0:
        time.sleep(0.5) # sleep for a moment to not overwhelm
        pubs = self.locate_pubs()
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
      elif len(self.pubs) == 0 and len(children) > index:
        time.sleep(random.uniform(0, 1)) # random wait so there is no co-leader overlap
        node_exists = self.zkc.exists('/broker/leaders/lead-' + str(index))
        self.logger.debug(f"BrokerMW::handle_pubs_change - node_exists: {node_exists}")
        if not node_exists:
          self.logger.info(f"Load increased. Joining as co-lead to balance new load.")
          # join zookeeper as a co-lead broker
          self.join_zookeeper_as_colead(str(index))
          # register with discovery to get paired with a pub
          self.register_and_listen()
      elif len(self.pubs) > 0:
        # check to see if our paired publisher is gone
        pub_gone = True; self_pub = self.pubs[0]
        for child in children:
          pub_id = json.loads(self_pub)
          child_id = child.split(':')
          print(pub_id); print(child_id)
          if pub_id['name'] == child_id[0] and pub_id['ip'] == child_id[1] and \
            pub_id['port'] == child_id[2]: pub_gone = False
        # if our paired publisher has left we should return to the backup pool
        if pub_gone and self.index != 0:
          self.logger.info(f"Load decreased. Returning to backup pool.")
          self.return_to_backup_pool()
    except Exception as e: handle_exception(e)

  """subscribe to all of the registered publishers"""
  def sub_to_pubs(self, pubs):
    try:
      self.logger.debug("BrokerMW::sub_to_pubs")
      # Subscribe to each publisher
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
      self.logger.debug("BrokerMW::listen_to_pubs")
      while True:
        # receive and disseminate messages from the publishers
        message_bytes = self.sub.recv_multipart()
        message = str(message_bytes[0], 'UTF-8')
        self.logger.debug(f"BrokerMW::listen_to_pubs - Passing on message from publisher: {message}")
        # determine if a message is a history message
        if "hs-" in message and "-hw-" in message:
          # if it is a history message, add our info to the message
          msg_parts = message.split("hs-")
          new_message = msg_parts[0] + f"pi-{self.addr}:{self.port}-hs-" + msg_parts[1]
          disseminate(self.logger, self.pub, new_message)
        else: disseminate(self.logger, self.pub, message)
    except Exception as e: handle_exception(e)

  """run event loop where we expect to receive replies to sent requests"""
  def event_loop(self):
    try:
      self.logger.debug("BrokerMW::event_loop - run the event loop")
      while True:
        # poll for events. We give it an infinite timeout.
        # The return value is a socket to event mask mapping
        events = dict(self.poller.poll())
        if self.req in events: return self.handle_reply()
    except Exception as e: handle_exception(e)
             
  """handle an incoming reply"""
  def handle_reply(self):
    try:
      self.logger.debug("BrokerMW::handle_reply")
      # let us first receive all the bytes
      bytesRcvd = self.req.recv()
      # now use protobuf to deserialize the bytes
      disc_resp = discovery_pb2.DiscoveryResp()
      disc_resp.ParseFromString(bytesRcvd)
      # Depending on the message type, the contents of the msg will differ
      if disc_resp.msg_type == discovery_pb2.REGISTER:
        if disc_resp.register_resp.result == discovery_pb2.RegisterResp().Result.FAILURE:
          raise Exception(disc_resp.register_resp.fail_reason) # return register error
        else: return disc_resp.register_resp.result # return response to register
      elif disc_resp.msg_type == discovery_pb2.LOOKUP_ALL_PUBS:
        return disc_resp.pubs_resp.publishers # return response to lookup_all_pubs request
      else: raise Exception("Unrecognized response message.")
    except Exception as e: handle_exception(e)
