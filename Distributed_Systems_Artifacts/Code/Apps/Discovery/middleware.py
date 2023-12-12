###############################################
# Author: Patrick Muradaz
# Vanderbilt University
# Purpose: Centralized discovery middleware for PAs
# Semester: Spring 2023
###############################################
#
# Import statements
import zmq, json, sys, os, time, random, configparser
sys.path.append(os.getcwd())
from Apps.Common.common import \
  handle_exception, format_pubs, send_message
from Apps.Common import discovery_pb2
from kazoo.client import KazooClient
from kazoo.recipe.watchers import DataWatch
from kazoo.recipe.watchers import ChildrenWatch

"""Discovery Middleware class"""
class DiscoveryMW():

    """constructor"""
    def __init__(self, logger):
        self.dissemination = None # direct or via broker
        self.logger = logger      # internal logger for print statements
        self.rep = None           # will be a ZMQ REP socket for discovery
        self.poller = None        # used to wait on incoming replies
        self.addr = None          # our advertised IP address
        self.port = None          # port num where we listen for pubs/subs
        self.name = None          # the name of this discovery node
        self.pubs = None          # the array of publishers that are registering
        self.paired_pubs = None   # the array of publisher that are paired to brokers
        self.subs = None          # the array of subscribers that are registering
        self.brokers = None       # the brokers to use if we are using that approach
        self.ready_sent = 0       # number of ready replys sent (will match pubs/subs)
        self.zkc = None           # kazoo client instance used to interact with zookeeper

    """configure/initialize"""
    def configure(self, args):
        try:
            self.logger.debug("DiscoveryMW::configure")
            # get the configuration object
            config = configparser.ConfigParser()
            config.read(args.config)
            self.dissemination = config["Dissemination"]["Strategy"]
            # Here we initialize any internal variables
            self.port = args.port
            self.addr = args.addr
            self.name = args.name
            self.brokers = []
            self.paired_pubs = []
            # now set up ZMQ
            context = zmq.Context()  # Next get the ZMQ context (singleton object)
            self.poller = zmq.Poller()  # get the ZMQ poller object
            # set up the REP socket
            self.rep = context.socket(zmq.REP)
            self.poller.register(self.rep, zmq.POLLIN)
            bind_string = f"tcp://{self.addr}:{self.port}"
            self.logger.debug(f"DiscoveryMW::configure - bound to: {bind_string}")
            self.rep.bind(bind_string)  # bind to the REP socket
            # Now setup the zookeeper kazoo client
            self.zkc = KazooClient(hosts='10.0.0.1:2181')
            self.zkc.start()
            return self.join_zookeeper()
        except Exception as e: handle_exception(e)

    """handles configuring this nodes place in zookeeper"""
    def join_zookeeper(self):
        try:
            self.logger.debug("DiscoveryMW::join_zookeeper")
            self.zkc.ensure_path('/discovery')
            if not self.zkc.exists('/discovery/leader'):
                self.zkc.create('/discovery/leader', f'{self.addr}:{self.port}'.encode(), ephemeral=True)
                return True
            else:
                self.zkc.create(f'/discovery/backup-{self.addr}:{self.port}', b'discovery-backup', ephemeral=True)
                return False
        except Exception as e: handle_exception(e)
    
    """watches the lead discovery node to take over if needed"""
    def watch_leader(self, pubs, subs):
        try:
            self.logger.debug("DiscoveryMW::watch_leader")
            DataWatch(self.zkc, '/discovery/leader', self.leader_left)
            self.listen(pubs, subs)
        except Exception as e: handle_exception(e)

    """called when the leader discovery node dies or leaves"""
    def leader_left(self, data, stat):
        try:
            self.logger.debug("DiscoveryMW::leader_left")
            if data == None and stat == None:
                self.logger.info("The lead discovery node has left.")
                time.sleep(random.uniform(0, 1)) # random wait so there is no leader overlap
                if not self.zkc.exists('/discovery/leader'):
                    self.logger.info("Setting self as the new lead node.")
                    self.zkc.delete(f'/discovery/backup-{self.addr}:{self.port}')
                    self.zkc.create('/discovery/leader', f'{self.addr}:{self.port}'.encode(), ephemeral=True)
                    self.logger.info("Listening for registration requests...")
                else: self.logger.info("Another node has been elected the new lead.")
        except Exception as e: handle_exception(e)

    """register with the discovery service"""
    def listen(self, pubs, subs):
        try:
            self.logger.debug("DiscoveryMW::listen")
            self.pubs = pubs; self.subs = subs
            self.listen_for_broker_failures()
            self.listen_for_pub_sub_failures()
            
            while True:
                # poll for events. We give it an infinite timeout.
                # The return value is a socket to event mask mapping
                events = dict(self.poller.poll())
                if self.rep in events: self.handle_message()
        except Exception as e: handle_exception(e)

    """Watches the lead broker nodes to handle if any die"""
    def listen_for_broker_failures(self):
        try:
            self.logger.debug("DiscoveryMW::listen_for_broker_failures")
            DataWatch(self.zkc, '/broker/leaders/lead-0', self.handle_broker_change)
        except Exception as e: handle_exception(e)

    """Handles event when a lead broker node dies or leaves"""
    def handle_broker_change(self, data, stat):
        try:
            self.logger.debug("DiscoveryMW::handle_broker_change")
            # only continue if a lead has died
            if data == None and stat == None:
                self.logger.info("A lead broker node has failed.")
                if len(self.paired_pubs) > 0: 
                    self.pubs.append(self.paired_pubs.pop(0))
        except Exception as e: handle_exception(e)

    """listen to zookeeper for alerts about publishers/subscribers dying"""
    def listen_for_pub_sub_failures(self):
        try:
            self.logger.debug("DiscoveryMW::listen_for_pub_sub_failures")
            self.zkc.ensure_path('/discovery/pubs')
            ChildrenWatch(self.zkc, '/discovery/pubs', self.handle_pubs_change)
            self.zkc.ensure_path('/discovery/subs')
            ChildrenWatch(self.zkc, '/discovery/subs', self.handle_subs_change)
        except Exception as e: handle_exception(e)
  
    """Handles the event where there are changes to the pubs in zookeeper"""
    def handle_pubs_change(self, children):
        try:
            self.logger.debug(f"DiscoveryMW::handle_pubs_change - children: {children}")
            if (len(children) < len(self.pubs)): 
                self.logger.info("Publisher failed. Removing from list.")
                pub_index = 0
                for pub1 in self.pubs:
                    remove = True
                    for pub2 in children:
                        pub_id = pub2.split(':')
                        if pub1.id.name == pub_id[0] and pub1.id.ip == pub_id[1] and \
                            pub1.id.port == pub_id[2]: remove = False
                    if remove: self.pubs.pop(pub_index)
                    pub_index += 1
            if (len(children) == 0): 
                self.logger.info("No publishers present.")
                self.pubs = []
        except Exception as e: handle_exception(e)

    """Handles the event where there are changes to the subs in zookeeper"""
    def handle_subs_change(self, children):
        try:
            self.logger.debug(f"DiscoveryMW::handle_subs_change - children: {children}")
            if (len(children) < len(self.subs)): 
                self.logger.info("Subscriber failed. Removing from list.")
                sub_index = 0
                for sub1 in self.subs:
                    remove = True
                    for sub2 in children:
                        sub_id = sub2.split(':')
                        if sub1.id.name == sub_id[0] and sub1.id.ip == sub_id[1] and \
                            sub1.id.port == sub_id[2]: remove = False
                    if remove: self.subs.pop(sub_index)
                    sub_index += 1
            if (len(children) == 0): 
                self.logger.info("No Subscribers present.")
                self.subs = []
        except Exception as e: handle_exception(e)

    """handle an incoming message"""
    def handle_message(self):
        try:
            self.logger.debug("DiscoveryMW::handle_message")
            # let us first receive all the bytes
            bytesRcvd = self.rep.recv()
            # now use protobuf to deserialize the bytes
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.ParseFromString(bytesRcvd)
            # Depending on the message type, the contents of the msg will differ
            if (disc_req.msg_type == discovery_pb2.REGISTER): self.handle_register(disc_req.register_req)
            elif (disc_req.msg_type == discovery_pb2.DEREGISTER): self.handle_deregister(disc_req.deregister_req)
            elif (disc_req.msg_type == discovery_pb2.LOOKUP_ALL_PUBS): self.handle_pub_lookup(return_all_pubs=True)
            elif (disc_req.msg_type == discovery_pb2.LOOKUP_PUB_BY_TOPIC): self.handle_pub_lookup(return_all_pubs=False)
            else: raise Exception("Unrecognized response message")
        except Exception as e: handle_exception(e)

    """handle a registration with the discovery service"""
    def handle_register(self, register_req):
        try:
            self.logger.debug("DiscoveryMW::handle_register")
            id = register_req.id; req_id = f"{id.name} - {id.ip}:{id.port}"
            self.logger.info(f"New registration request from: {req_id}")

            if (register_req.role == discovery_pb2.RegisterReq().Role.PUBLISHER):
                self.logger.debug("DiscoveryMW::handle_message - handle pub register")
                self.pubs.append(register_req)
            elif (register_req.role == discovery_pb2.RegisterReq().Role.SUBSCRIBER):
                self.logger.debug("DiscoveryMW::handle_message - handle sub register")
                self.subs.append(register_req)
            elif (register_req.role == discovery_pb2.RegisterReq().Role.BROKER):
                self.logger.debug("DiscoveryMW::handle_message - handle broker register")
                self.brokers.append(register_req)
            else: raise Exception("Unrecognized result message")

            # build the response message
            disc_resp = discovery_pb2.DiscoveryResp()
            register_resp = discovery_pb2.RegisterResp()
            register_resp.result = register_resp.Result.SUCCESS
            disc_resp.msg_type = discovery_pb2.REGISTER
            disc_resp.register_resp.CopyFrom(register_resp)
            # send the message
            send_message(self.logger, self.rep, disc_resp)
            self.logger.info(f"Registration request handled successfully.")
        except Exception as e: handle_exception(e)
    
    """handle a deregistration with the discovery service"""
    def handle_deregister(self, deregister_req):
        try:
            self.logger.debug("DiscoveryMW::handle_deregister")
            id = deregister_req.id; req_id = f"{id.name} - {id.ip}:{id.port}"
            self.logger.info(f"New deregistration request from: {req_id}")

            if (deregister_req.role == discovery_pb2.RegisterReq().Role.PUBLISHER):
                self.logger.debug("DiscoveryMW::handle_message - handle pub deregister")
                self.del_from_arr(deregister_req, self.pubs)
            elif (deregister_req.role == discovery_pb2.RegisterReq().Role.SUBSCRIBER):
                self.logger.debug("DiscoveryMW::handle_message - handle sub deregister")
                self.del_from_arr(deregister_req, self.subs)
            elif (deregister_req.role == discovery_pb2.RegisterReq().Role.BROKER):
                self.logger.debug("DiscoveryMW::handle_message - handle broker deregister")
                self.del_from_arr(deregister_req, self.brokers)
            else: raise Exception("Unrecognized result message")

            # build the response message
            disc_resp = discovery_pb2.DiscoveryResp()
            deregister_resp = discovery_pb2.DeregisterResp()
            deregister_resp.result = deregister_resp.Result.SUCCESS
            disc_resp.msg_type = discovery_pb2.DEREGISTER
            disc_resp.deregister_resp.CopyFrom(deregister_resp)
            # send the message
            send_message(self.logger, self.rep, disc_resp)
            self.logger.info(f"Deregistration request handled successfully.")
        except Exception as e: handle_exception(e)

    """responds with all of the requested pubs"""
    def handle_pub_lookup(self, return_all_pubs):
        try:
            self.logger.debug("DiscoveryMW::handle_pub_lookup")
            # build the response message
            disc_resp = discovery_pb2.DiscoveryResp()
            if return_all_pubs:
                # we should pair the broker to the pub and make sure no other broker gets paired to this pub
                pubs_msg = discovery_pb2.LookupAllPubsResp()
                pubs_msg.publishers.extend(format_pubs(self.pubs))
                if len(self.pubs) > 0: self.paired_pubs.append(self.pubs.pop())
                disc_resp.msg_type = discovery_pb2.LOOKUP_ALL_PUBS
                disc_resp.pubs_resp.CopyFrom(pubs_msg)
            else:
                matching_pubs_msg = discovery_pb2.LookupPubByTopicResp()
                matching_pubs_msg.publishers.extend(format_pubs(self.brokers))
                disc_resp.msg_type = discovery_pb2.LOOKUP_PUB_BY_TOPIC
                disc_resp.resp.CopyFrom(matching_pubs_msg)
            # send the message
            send_message(self.logger, self.rep, disc_resp)
        except Exception as e: handle_exception(e)

    """Removes the given object from the given array"""
    def del_from_arr(self, obj, arr):
        # Find the index of the object to remove
        i_to_del = None
        for i in range(len(arr)): 
            if arr[i].id.name == obj.id.name: 
                i_to_del = i; break
        # Remove the object from the array
        if i_to_del is not None and arr[i_to_del] is not None: 
            arr.pop(i_to_del)
