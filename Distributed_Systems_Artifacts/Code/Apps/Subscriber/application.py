###############################################
# Author: Aniruddha Gokhale
# Updated by: Patrick Muradaz
# Vanderbilt University
# Purpose: Subscriber application for PAs
# Semester: Spring 2023
###############################################
#
# The key steps for the subscriber application are
# (1) parse command line and configure application level parameters
# (2) obtain the subscriber middleware object and configure it.
# (3) As in the publisher, register ourselves with the discovery service
# (4) since we are a subscriber, we need to ask the discovery service to
#     let us know of each publisher that publishes the topic of interest to 
#     us. Then our middleware object will connect its SUB socket to all these 
#     publishers for the Direct strategy else connect just to the broker.
# (5) Subscriber will always be in an event loop waiting for some matching
#     publication to show up. We also compute the latency for dissemination and
#     store all these time series data in some database for later analytics.
#
# Import statements
import sys, os, time, argparse, configparser, logging, random
sys.path.append(os.getcwd())
from Apps.Common.common import handle_exception, dump
from Apps.Common.topic_selector import TopicSelector
from Apps.Subscriber.middleware import SubscriberMW

"""SubscriberAppln class"""
class SubscriberAppln():

  """constructor"""
  def __init__(self, logger):
    self.logger = logger      # internal logger for print statements
    self.name = None          # our name (some unique name)
    self.addr = None          # our advertised IP address
    self.port = None          # port num where we are going to publish our topics
    self.topiclist = None     # the different topics that we subscribe to
    self.lookup = None        # one of the diff ways we do lookup
    self.dissemination = None # direct or via broker
    self.mw_obj = None        # handle to the underlying Middleware object

  """configure/initialize"""
  def configure(self, args):
    try:
      self.logger.debug("SubscriberAppln::configure")
      # initialize our variables
      self.name = args.name
      self.port = args.port
      self.addr = args.addr
      # Now, get the configuration object
      config = configparser.ConfigParser()
      config.read(args.config)
      self.lookup = config["Discovery"]["Strategy"]
      self.dissemination = config["Dissemination"]["Strategy"]
      # Now get our topic list of interest
      ts = TopicSelector()
      self.topiclist = ts.interest()
      # Now setup up our underlying middleware object
      self.mw_obj = SubscriberMW(self.logger)
      self.mw_obj.configure(args)      
      self.logger.info("Subscriber app configured.")
      dump(self.logger, "SubscriberAppln", self.addr, self.port, 
           name=self.name, topiclist=self.topiclist)
    except Exception as e: handle_exception(e)

  """driver program"""
  def driver(self):
    try:
      self.logger.debug("SubscriberAppln::driver")
      # Use middleware to register us with zookeeper and discovery
      self.logger.info("Registering app with zookeeper and discovery.")
      self.mw_obj.register(self.topiclist)
      # Now, find the publishers that match our topics
      self.logger.info("Locating publishers for our topics.")
      pubs = self.mw_obj.locate_pubs(self.topiclist)
      # Finally, subscribe and listen to the publishers
      if len(pubs) > 0: self.logger.info("Subscribing to relevant publishers.")
      self.mw_obj.sub_to_pubs(pubs, self.topiclist)
      self.mw_obj.listen_for_new_pubs()
      self.logger.info("Listening for new publishers.")
      self.mw_obj.listen_to_pubs()
    except Exception as e: handle_exception(e)

"""Parse command line arguments"""
def parseCmdLineArgs():
  # instantiate a ArgumentParser object
  parser = argparse.ArgumentParser(description="Subscriber Application")
  # Now specify all the optional arguments we support  
  parser.add_argument(
    "-n", "--name", default="sub", 
    help="Some name assigned to us. Keep it unique per subscriber."
  )
  parser.add_argument(
    "-a", "--addr", default="127.0.0.1", 
    help="IP addr of this subscriber to advertise (default: localhost)"
  )
  parser.add_argument(
    "-p", "--port", default="5566", 
    help="Port number on which our underlying subscriber ZMQ service runs, " + 
      "default=5566"
  )
  parser.add_argument(
    "-hs", "--history", default="10", 
    help="The minimum publication history we requre from our pubs."
  )
  parser.add_argument(
    "-c", "--config", default="Apps/Common/config.ini", 
    help="configuration file (default: Apps/Common/config.ini)"
  )
  parser.add_argument(
    "-l", "--loglevel", type=int, default=logging.INFO, 
    choices=[
      logging.DEBUG,logging.INFO,logging.WARNING,
      logging.ERROR,logging.CRITICAL
    ], 
    help="logging level, choices 10,20,30,40,50: default 20=logging.INFO"
  )
  return parser.parse_args()

"""Main program"""
def main():
  try:
    # obtain a system wide logger and initialize it to debug to begin with
    logging.debug("Main - acquire a child logger to log messages in")
    logger = logging.getLogger("SubAppln")
    # first parse the arguments
    logger.debug("Main: parse command line arguments")
    args = parseCmdLineArgs()
    # reset the log level to as specified
    logger.debug("Main: resetting log level to {}".format(args.loglevel))
    logger.setLevel(args.loglevel)
    logger.debug("Main: effective log level is {}".format(logger.getEffectiveLevel()))
    # Obtain a subscriber application
    logger.debug("Main: obtain the object")
    sub_app = SubscriberAppln(logger) # get the object
    sub_app.configure(args)           # configure the object
    sub_app.driver()                  # invoke the object driver
  except Exception as e: handle_exception(e)

"""Main entry point"""
if __name__ == "__main__":
  # set underlying default logging capabilities
  logging.basicConfig(level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  main()
