###############################################
# Author: Patrick Muradaz
# Vanderbilt University
# Purpose: File for clearing zookeeper paths
#          after each mininet test runs.
# Semester: Spring 2023
###############################################
#
# import statements
import sys, os; sys.path.append(os.getcwd())
from datetime import datetime
from kazoo.client import KazooClient

ZKC = KazooClient(hosts='10.0.0.1:2181')
    
def clear_zookeeper():
    ZKC.start() # Connect to ZooKeeper
    print("Clearing at: " + datetime.now().strftime("%H:%M:%S"))
    print("Starting root: " + ', '.join(ZKC.get_children('/')))
    print("Starting /discovery: " + ', '.join(ZKC.get_children('/discovery')))
    print("Starting /discovery/pubs: " + ', '.join(ZKC.get_children('/discovery/pubs')))
    print("Starting /discovery/subs: " + ', '.join(ZKC.get_children('/discovery/subs')))
    try: print("Starting /broker: " + ', '.join(ZKC.get_children('/broker')))
    except: print("No /broker to clear")
    # Recursively delete discovery node and all children
    ZKC.delete('/discovery', recursive=True)
    try: ZKC.delete('/broker', recursive=True)
    except: print("No /broker to clear")
    print("\nCleared at: " + datetime.now().strftime("%H:%M:%S"))
    print("Cleared root: " + ', '.join(ZKC.get_children('/')))
    ZKC.stop() # Close the connection to ZooKeeper

if __name__ == '__main__':
    clear_zookeeper()
