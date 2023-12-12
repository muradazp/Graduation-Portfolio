###############################################
# Author: Patrick Muradaz
# Vanderbilt University
# Purpose: File for testing PAs in mininet
# Semester: Spring 2023
###############################################
#
# import statements
import sys, os; sys.path.append(os.getcwd())
import time, subprocess
from mininet.net import Mininet
from mininet.log import setLogLevel
from mininet.topo import SingleSwitchTopo

# constants
RUN_PUBLISHER = 'python3 Apps/Publisher/application.py'
RUN_SUBSCRIBER = 'python3 Apps/Subscriber/application.py'
RUN_DISCOVERY = 'python3 Apps/Discovery/application.py'
RUN_BROKER = 'python3 Apps/Broker/application.py'
PIPE_OUTPUT = '2>&1 | tee Logs/'

def run_broker_experiment():
    topo = 13 # <- Number of machines to provision in the topo
    net = Mininet(topo=SingleSwitchTopo(topo))
    net.start()
    host_index = 1; stop_broker = net.hosts[2]
    for host in net.hosts:
        if host_index == 1: host.sendCmd(f'sudo /opt/zookeeper/bin/zkServer.sh start-foreground {PIPE_OUTPUT}zookeeper.txt'); time.sleep(1)
        if host_index == 2: host.sendCmd(f'{RUN_DISCOVERY} -n disc1 -a 10.0.0.2 -p 5551 -l 20 {PIPE_OUTPUT}disc_1.txt'); time.sleep(1)
        elif host_index == 3: host.sendCmd(f'{RUN_BROKER} -n broker1 -a 10.0.0.3 -p 5581 -l 20 {PIPE_OUTPUT}broker_1.txt'); time.sleep(1)
        elif host_index == 4: host.sendCmd(f'{RUN_BROKER} -n broker2 -a 10.0.0.4 -p 5582 -l 20 {PIPE_OUTPUT}broker_2.txt'); time.sleep(1)
        elif host_index == 5: host.sendCmd(f'{RUN_BROKER} -n broker3 -a 10.0.0.5 -p 5583 -l 20 {PIPE_OUTPUT}broker_3.txt'); time.sleep(1)
        elif host_index == 6: host.sendCmd(f'{RUN_BROKER} -n broker4 -a 10.0.0.6 -p 5584 -l 20 {PIPE_OUTPUT}broker_4.txt'); time.sleep(1)
        elif host_index == 7: 
            host.sendCmd(f'{RUN_PUBLISHER} -n pub1 -a 10.0.0.7 -p 5571 -i 500 -l 20 {PIPE_OUTPUT}pub_1.txt'); time.sleep(1)
            print("stopped broker"); stop_broker.stop(); time.sleep(15)
        elif host_index == 8: host.sendCmd(f'{RUN_PUBLISHER} -n pub2 -a 10.0.0.8 -p 5572 -i 500 -l 20 {PIPE_OUTPUT}pub_2.txt'); time.sleep(1)
        elif host_index == 9: host.sendCmd(f'{RUN_PUBLISHER} -n pub3 -a 10.0.0.9 -p 5573 -i 500 -hs 20 -l 20 {PIPE_OUTPUT}pub_3.txt'); time.sleep(1)
        elif host_index == 10: host.sendCmd(f'{RUN_SUBSCRIBER} -n sub1 -a 10.0.0.10 -p 5561 -l 20 {PIPE_OUTPUT}sub_1.txt'); time.sleep(1)
        elif host_index == 11: host.sendCmd(f'{RUN_SUBSCRIBER} -n sub2 -a 10.0.0.11 -p 5562 -l 20 {PIPE_OUTPUT}sub_2.txt'); time.sleep(1)
        elif host_index == 12: host.sendCmd(f'{RUN_SUBSCRIBER} -n sub3 -a 10.0.0.12 -p 5563 -hs 20 -l 20 {PIPE_OUTPUT}sub_3.txt'); time.sleep(1)
        host_index += 1
    time.sleep(30)
    stop_hosts(net.hosts, net.hosts[12])

def stop_hosts(hosts, zk_cleaner):
    # Stop all of the given hosts
    host_index = 1; zk_server = hosts[0]
    for host in hosts: 
        if host_index != len(hosts) and host_index != 1:
            host.stop()
        host_index += 1
    zk_cleaner.sendCmd(f'python3 clear_zookeeper.py {PIPE_OUTPUT}clear_zk.txt'); time.sleep(1)
    zk_cleaner.stop()
    zk_server.stop()

def clear_mininet():
    cmds = ['sudo', 'mn', '-c'] # Commands to clear mininet
    cmn_process = subprocess.Popen(cmds) # Clear out mininet
    cmn_process.wait() # Wait for mininet to clear

if __name__ == '__main__':
    setLogLevel('info')
    run_broker_experiment()
    clear_mininet()
