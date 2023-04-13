from kazoo.client import KazooClient
# IPAddr = "10.0.12.2"
import os


zk = KazooClient(hosts='127.0.0.1:2182')
zk.start()
def my_func(event):
    # check to see what the children are now
        print("event: ", event)
        # zk.get_children("/servers", watch=my_func)


# children = zk.create("/servers/data",b"a value",ephemeral=True)
# children = zk.create("/servers/"+IPAddr+"/data",b"porto",ephemeral=True)
children = zk.get_children("/servers")
print(children)
if(len(children)!=0):
    for child in children:
        data, stat = zk.get("/servers/"+str(child))
        print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))
        hostname = str(child) #example
        response = os.system("ping -c 1 " + hostname)
        #and then check the response...
        if response == 0:
            print (hostname, 'is up!')
        else:
          print (hostname, 'is down!')
else:
     print("No active server")


# data, stat = zk.get("/servers/data")
# print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))
# data, stat = zk.get("/servers/"+IPAddr+"/data")
# print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))

zk.stop()