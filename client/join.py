from kazoo.client import KazooClient
from kazoo.security import make_digest_acl_credential, make_acl

IPAddr = "10.0.12.1"
import time


zk = KazooClient(hosts='127.0.0.1:2181',default_acl=([make_acl('world', 'anyone', read=True, delete=False)]))
zk.start()
zk.add_auth("digest","cmpe:275")

# zk.ensure_path("/servers/")
# children = zk.create("/servers/data",b"a value",ephemeral=True)
children = zk.create("/servers/"+IPAddr,b"1234",ephemeral=True)
# children = zk.exists("/servers", watch=my_func)
data, stat = zk.get("/servers/"+IPAddr)
print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))
print(children)

while True:
    time.sleep(200)
zk.stop()