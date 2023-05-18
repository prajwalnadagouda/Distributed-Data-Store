from kazoo.client import KazooClient
from kazoo.security import ACL, make_digest_acl

zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()
zk.create("/servers")