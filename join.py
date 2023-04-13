from kazoo.client import KazooClient
IPAddr = "10.0.12.2"
import time


zk = KazooClient(hosts='127.0.0.1:2182')
zk.start()
def my_func(event):
    # check to see what the children are now
        print("event: ", event)
        zk.get_children("/servers", watch=my_func)


# zk.delete("/servers", recursive=True)

# Call my_func when the children change
zk.ensure_path("/servers/")
# children = zk.create("/servers/data",b"a value",ephemeral=True)
children = zk.create("/servers/"+IPAddr,b"1234",ephemeral=True)
# children = zk.exists("/servers", watch=my_func)
data, stat = zk.get("/servers/"+IPAddr)
print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))
print(children)

while True:
    time.sleep(200)
zk.stop()