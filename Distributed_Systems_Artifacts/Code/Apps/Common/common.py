###############################################
# Author: Aniruddha Gokhale
# Updated by: Patrick Muradaz
# Vanderbilt University
# Purpose: Python file for common functions
# Semester: Spring 2023
###############################################
# 
# This file contains any declarations that are common to all middleware entities
#
# import statements
import json
from Apps.Common import discovery_pb2

"""handle the given exception"""
def handle_exception(e):
    exc_traceback = e.__traceback__
    raise e.with_traceback(exc_traceback)

"""dump the contents of the object"""
def dump(logger, app, ip, port, name=None, iters=None,
         topiclist=None, numpubs=None, numsubs=None):
  try:
    logger.debug("**********************************")
    logger.debug(f"     {app}::dump")
    logger.debug("------------------------------")
    if name: logger.debug(f"     Name: {name}")
    logger.debug(f"     IP: {ip}")
    logger.debug(f"     Port: {port}")
    if topiclist: 
      logger.debug(f"     TopicList: {topiclist}")
      logger.info(f"App: {name} - {ip}:{port}; Topiclist: {topiclist}")
    if iters: logger.debug(f"     Iterations: {iters}")
    if numpubs: logger.debug(f"     Pubs Expected: {numpubs}")
    if numsubs: logger.debug(f"     Subs Expected: {numsubs}")
    logger.debug("**********************************")
  except Exception as e: handle_exception(e)

"""format and return the given array of publishers"""
def format_pubs(pubs):
    try:
      formatted_pubs = []; pub_names = []
      for pub in pubs:
          try: publisher = {"name": pub.id.name, "ip": pub.id.ip, "port": pub.id.port}
          except: publisher = {"name": pub.name, "ip": pub.ip, "port": pub.port}
          if publisher["name"] not in pub_names:
              formatted_pubs.append(json.dumps(publisher))
              pub_names.append(publisher["name"])
      return formatted_pubs
    except Exception as e: handle_exception(e)

"""send the given message on the given socket"""
def send_message(logger, socket, message):
    try:
      logger.debug("Common::send_message")
      buf2send = message.SerializeToString()
      socket.send(buf2send)
    except Exception as e: handle_exception(e)

"""register with the discovery service"""
def register(logger, role, name, addr, port, req, topiclist=None):
  try:
    logger.debug("Common::register")
    # build the request message
    disc_req = discovery_pb2.DiscoveryReq()
    register_req = discovery_pb2.RegisterReq() 
    register_req.role = role
    if topiclist: register_req.topiclist.extend(topiclist)
    register_req.id.name = name
    register_req.id.ip = addr
    register_req.id.port = port
    disc_req.msg_type = discovery_pb2.REGISTER
    disc_req.register_req.CopyFrom(register_req)
    # send the message
    send_message(logger, req, disc_req)
  except Exception as e: handle_exception(e)

"""deregister with the discovery service"""
def deregister(logger, role, name, addr, port, req, topiclist=None):
  try:
    logger.debug("Common::register")
    # build the request message
    disc_req = discovery_pb2.DiscoveryReq()
    deregister_req = discovery_pb2.DeregisterReq() 
    deregister_req.role = role
    if topiclist: deregister_req.topiclist.extend(topiclist)
    deregister_req.id.name = name
    deregister_req.id.ip = addr
    deregister_req.id.port = port
    disc_req.msg_type = discovery_pb2.DEREGISTER
    disc_req.deregister_req.CopyFrom(deregister_req)
    # send the message
    send_message(logger, req, disc_req)
  except Exception as e: handle_exception(e)

"""disseminate the data on our pub socket"""
def disseminate(logger, pub, data):
    logger.debug(f"Common::disseminate - {data}")
    try: pub.send_string(data)
    except Exception as e: handle_exception(e)
