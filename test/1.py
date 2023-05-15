import bisect
import hashlib

class ConsistentHash:
    def __init__(self, replication_factor):
        self.replication_factor = replication_factor
        self.ring = {}

    def add_server(self, server):
        for i in range(self.replication_factor):
            key = f"{server}_{i}"
            hash_code = self._hash(key)
            self.ring[hash_code] = server

    def remove_server(self, server):
        for i in range(self.replication_factor):
            key = f"{server}_{i}"
            hash_code = self._hash(key)
            if hash_code in self.ring:
                del self.ring[hash_code]

    def get_servers(self, data):
        hash_code = self._hash(data)
        keys = sorted(self.ring.keys())
        pos = bisect.bisect(keys, hash_code)
        if pos == len(keys):
            pos = 0
        servers = []
        while len(servers) < self.replication_factor:
            server = self.ring[keys[pos]]
            if server not in servers:
                servers.append(server)
            pos = (pos + 1) % len(keys)
            if(len((self.__dict__)['ring'])<self.replication_factor*self.replication_factor):
                # print("don")
                break
        return servers

    def _hash(self, key):
        key_with_year = f"{key}201420152016201720182019202020212022"
        hash_code = hashlib.sha256(key_with_year.encode("utf-8")).hexdigest()
        # hash_code = hashlib.sha256(key.encode("utf-8")).hexdigest()
        # print(int(hash_code, 16))
        return int(hash_code, 16)

replication_factor = 2
ch = ConsistentHash(replication_factor)

# Add servers
ch.add_server("server1")
ch.add_server("server2")
ch.add_server("server3")
ch.add_server("server4")

# Get servers for data
for data in range(2014,2023):
    data = str(data)
    servers = ch.get_servers(data)
    print(f"The data {data} is stored on servers {servers}")

# Remove a server
# print(ch.__dict__)
ch.remove_server("server2")
if(len((ch.__dict__)['ring'])<replication_factor*replication_factor):
    print("don")
    exit()

# Get servers for data again
for data in range(2014,2023):
    data = str(data)
    servers = ch.get_servers(data)
    print(f"The data {data} is now stored on servers {servers}")
