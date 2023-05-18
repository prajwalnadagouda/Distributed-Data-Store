from kazoo.client import KazooClient
from kazoo.security import make_digest_acl_credential, make_acl

import time

import configparser

# reading the config file
config = configparser.ConfigParser()
config.read('config.ini')
ZooIPAddress=config['acrossZOOKEEPER']['IPAddress']
ZooPortNumber=config['acrossZOOKEEPER']['PortNumber']
IPAddress=config['PROXY']['IPAddress']
PortNumber=config['PROXY']['PortNumber']
from kazoo.client import KazooState

print(ZooIPAddress+":"+ZooPortNumber)
def my_listener(state):
    if state == KazooState.LOST:
        time.sleep(4)
        zooconnect()
    elif state == KazooState.SUSPENDED:
        pass
    else:
        pass

def zooconnect():
    global zk
    zk = KazooClient(hosts=ZooIPAddress+":"+ZooPortNumber)
    zk.start()
    while True:
        if(not zk.exists("/servers/"+IPAddress+":"+PortNumber)):
            break
    zk.create("/servers/"+IPAddress+":"+PortNumber,ephemeral=True)
    # zk.add_auth("digest","cmpe:275")
    zk.add_listener(my_listener)
    return zk

zk=zooconnect()

while True:
    print("here")
    time.sleep(100)