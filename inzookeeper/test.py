from kazoo.client import KazooClient
import os
import time


# zk = KazooClient(hosts='10.0.12.1:2191') #change it to the zookeeper address
zk = KazooClient(hosts='127.0.0.1:2191') #change it to the zookeeper address
zk.start()
children = zk.get_children("/available")
print(children)