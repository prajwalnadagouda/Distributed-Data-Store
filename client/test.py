from kazoo.client import KazooClient
from kazoo.security import make_digest_acl_credential, make_acl

IPAddr = "10.0.12.2"

zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()


# children = zk.create("/servers/data",b"a value",ephemeral=True)
# children = zk.create("/servers/"+IPAddr+"/data",b"porto",ephemeral=True)
# children = zk.get_children("/servers/"+IPAddr)
zk.add_auth("digest","cmpe:275")
data= zk.get_children("/")#+IPAddr+"/data")
print(data)
data= zk.get_children("/servers")#+IPAddr+"/data")
print(data)
# zk.add_auth("digest","rohan:mom")
children = zk.create("/servers/1234",b"1234",ephemeral=True)
print(children)
# data= zk.delete("/servers",recursive=True)#+IPAddr+"/data")
data= zk.delete("/servers/10.0.12.1")#+IPAddr+"/data")
data= zk.get_children("/servers")#+IPAddr+"/data")
# data, stat = zk.get("/servers/"+IPAddr+"/data")
# print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))
print(data)

zk.stop()