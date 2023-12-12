###############################################
# Author: Aniruddha Gokhale
# Updated by: Patrick Muradaz
# Vanderbilt University
# Purpose: Publisher application for PAs
# Semester: Spring 2023
###############################################
#
# The core logic of the publisher application will be as follows:
# (1) The publisher app decides which topics it is going to publish.
#     For this assignment, we don't care about the values for these topics.
# (2) The application reaches out to the discovery service using
#     the CS6381_MW middleware APIs so that it can register itself with the
#     service. Essentially, it simply delegates this activity to the 
#     underlying middleware publisher object.
# (3) Register with the lookup service letting it know all the topics it is 
#     publishing and any other details needed for the underlying middleware to 
#     make the communication happen via ZMQ.
# (4) Keep periodically checking with the discovery service if the entire 
#     system is initialized so that the publisher knows that it can proceed 
#     with its periodic publishing.
# (5) Start a loop on the publisher for sending of topics.
#     In each iteration, the application decides (randomly) which
#     topics it is going to publish, and then accordingly invokes
#     the publish method of the middleware passing the topic
#     and its value. 
#     Note that additional info like timestamp etc may also need to
#     be sent and the whole thing serialized under the hood before
#     actually sending it out. Use Protobuf for this purpose
# (6) When the loop terminates, possibly after a certain number of iterations 
#     of publishing are over, proceed to clean up the objects and exit
#
# Import statements
import sys, os, time, argparse, configparser, logging, random
sys.path.append(os.getcwd())
from Apps.Common.common import handle_exception, dump
from Apps.Common.topic_selector import TopicSelector
from Apps.Publisher.middleware import PublisherMW

"""PublisherAppln class"""
class PublisherAppln():

  """constructor"""
  def __init__(self, logger):
    self.logger = logger      # internal logger for print statements
    self.name = None          # our name (some unique name)
    self.addr = None          # our advertised IP address
    self.port = None          # port num where we are going to publish our topics
    self.iters = None         # number of iterations of publication
    self.topiclist = None     # the different topics that we publish on
    self.lookup = None        # one of the diff ways we do lookup
    self.dissemination = None # direct or via broker
    self.mw_obj = None        # handle to the underlying Middleware object

  """configure/initialize"""
  def configure(self, args):
    try:
      self.logger.debug("PublisherAppln::configure")
      # Initialize our variables
      self.name = args.name
      self.iters = args.iters
      self.port = args.port
      self.addr = args.addr
      # Get the configuration object
      config = configparser.ConfigParser()
      config.read(args.config)
      self.lookup = config["Discovery"]["Strategy"]
      self.dissemination = config["Dissemination"]["Strategy"]
      # Get our topic list of interest
      ts = TopicSelector()
      self.topiclist = ts.interest()
      # Setup up our underlying middleware object
      self.mw_obj = PublisherMW(self.logger)
      self.mw_obj.configure(args) # pass remainder of args to middleware
      self.logger.info("Publisher app configured.")
      dump(self.logger, "PublisherAppln", self.addr, self.port, 
           name=self.name, topiclist=self.topiclist, iters=self.iters)
    except Exception as e: handle_exception(e)

  """driver program"""
  def driver(self):
    try:
      self.logger.debug("PublisherAppln::driver")
      # Use middleware to register us with zookeeper and discovery
      self.logger.info("Registering app with zookeeper and discovery.")
      self.mw_obj.register(self.topiclist)
      # Now disseminate on our topics
      self.logger.info("Disseminating info on our topics.")
      self.mw_obj.disseminate(self.iters)
      # Now deregister from zookeeper and discovery since dissemination is done
      self.logger.info("Deregistering app from zookeeper and discovery.")
      self.mw_obj.deregister(self.name, self.topiclist)
    except Exception as e: handle_exception(e)

"""Parse command line arguments"""
def parseCmdLineArgs():
  # instantiate a ArgumentParser object
  parser = argparse.ArgumentParser(description="Publisher Application")
  # Now specify all the optional arguments we support
  parser.add_argument(
    "-n", "--name", default="pub", 
    help="Some name assigned to us. Keep it unique per publisher"
  )
  parser.add_argument(
    "-a", "--addr", default="127.0.0.1", 
    help="IP addr of this publisher to advertise (default: 127.0.0.1)"
  )
  parser.add_argument(
    "-p", "--port", default="5577", 
    help="Port number on which our underlying publisher ZMQ service runs, " + 
      "default=5577"
  )
  parser.add_argument(
    "-hs", "--history", default="10", 
    help="The maximum publication history window we keep."
  )
  parser.add_argument(
    "-d", "--discovery", default="127.0.0.1:5555",
     help="IP Addr:Port combo for the discovery service, " + 
      "default 127.0.0.1:5555"
  )
  parser.add_argument(
    "-c", "--config", default="Apps/Common/config.ini", 
    help="configuration file (default: Apps/Common/config.ini)"
  )
  parser.add_argument(
    "-i", "--iters", type=int, default=1000, 
    help="number of publication iterations (default: 1000)"
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
    logger = logging.getLogger("PubAppln")
    # first parse the arguments
    logger.debug("Main: parse command line arguments")
    args = parseCmdLineArgs()
    # reset the log level to as specified
    logger.debug("Main: resetting log level to {}".format(args.loglevel))
    logger.setLevel(args.loglevel)
    logger.debug("Main: effective log level is {}".format(logger.getEffectiveLevel()))
    # Obtain a publisher application
    logger.debug("Main: obtain the object")
    pub_app = PublisherAppln(logger) # get the object
    pub_app.configure(args)          # configure the object
    pub_app.driver()                 # invoke the object driver
  except Exception as e: handle_exception(e)

"""Main entry point"""
if __name__ == "__main__":
  # set underlying default logging capabilities
  logging.basicConfig(level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  main()
