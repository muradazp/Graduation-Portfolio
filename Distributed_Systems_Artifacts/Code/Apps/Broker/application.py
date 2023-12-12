###############################################
# Author: Aniruddha Gokhale
# Updated by: Patrick Muradaz
# Vanderbilt University
# Purpose: Broker application for PAs
# Semester: Spring 2023
###############################################
#
# The Broker is involved only when the dissemination strategy is via the broker.
#
# A broker is an intermediary; thus it plays both the publisher and subscriber roles
# but in the form of a proxy. For instance, it serves as the single subscriber to
# all publishers. On the other hand, it serves as the single publisher to all the subscribers. 
#
# Import statements
import sys, os, time, argparse, logging, random
sys.path.append(os.getcwd())
from Apps.Common.common import handle_exception, dump
from Apps.Broker.middleware import BrokerMW

"""BrokerAppln class"""
class BrokerAppln():

  """constructor"""
  def __init__(self, logger):
    self.logger = logger  # internal logger for print statements
    self.name = None      # our name (some unique name)
    self.addr = None      # our advertised IP address
    self.port = None      # port num where we are going to publish our topics
    self.mw_obj = None    # handle to the underlying Middleware object
    self.is_lead = None   # used to determine if this is the lead discovery node

  """configure/initialize"""
  def configure(self, args):
    try:
      self.logger.debug("BrokerAppln::configure")
      # Initialize our variables
      self.name = args.name
      self.port = args.port
      self.addr = args.addr
      # Setup up our underlying middleware object
      self.mw_obj = BrokerMW(self.logger)
      self.is_lead = self.mw_obj.configure(args) # pass remainder of args to middleware
      self.logger.info("Broker app configured.")
      dump(self.logger, "BrokerAppln", self.addr, self.port, name=self.name)
    except Exception as e: handle_exception(e)

  """driver program"""
  def driver(self):
    try:
      self.logger.debug("BrokerAppln::driver")
      if self.is_lead: self.mw_obj.register_and_listen()
      else: 
        self.mw_obj.listen_for_new_pubs()
        self.mw_obj.watch_leaders()
    except Exception as e: handle_exception(e)

"""Parse command line arguments"""
def parseCmdLineArgs():
  # instantiate a ArgumentParser object
  parser = argparse.ArgumentParser(description="Broker Application")
  # Now specify all the optional arguments we support
  parser.add_argument(
    "-n", "--name", default="broker", 
    help="The name of this broker. We can probably keep the default: 'broker'"
  )
  parser.add_argument(
    "-a", "--addr", default="127.0.0.1", 
    help="IP addr of this broker to advertise (default: 127.0.0.1)"
  )
  parser.add_argument(
    "-p", "--port", default="5588", 
    help="Port number on which our underlying broker ZMQ service runs, default=5577"
  )
  parser.add_argument(
    "-c", "--config", default="Apps/Common/config.ini", 
    help="configuration file (default: Apps/Common/config.ini)"
  )
  parser.add_argument(
    "-l", "--loglevel", type=int, default=logging.INFO, 
    choices=[logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR,logging.CRITICAL], 
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
    pub_app = BrokerAppln(logger)    # get the object
    pub_app.configure(args)          # configure the object
    pub_app.driver()                 # invoke the object driver
  except Exception as e: handle_exception(e)

"""Main entry point"""
if __name__ == "__main__":
  # set underlying default logging capabilities
  logging.basicConfig(level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  main()
