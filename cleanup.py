import datetime
import time
import concurrent.futures
import threading

import arvados
import arvados.util

arv_client = arvados.api()

# 1. Make a list of arvados-cwl-runner containers with nonempty requesting_container_uuid
# 2. Iterate over intermediate collections more than 1 week old, starting from oldest
# 3. Delete collection unless container_request is in the list from (1)

protected_requests = set()

for container_request in arvados.util.keyset_list_all(
        arv_client.container_requests().list,
        filters=[
            ["command", "like", "[\"arvados-cwl-runner%"],
            ["requesting_container_uuid", "!=", None]
        ],
        select=["uuid"]):

    protected_requests.add(container_request["uuid"])

before = datetime.datetime.utcnow() - datetime.timedelta(days=7)

before_timestamp = before.isoformat("T") + 'Z'

executor = concurrent.futures.ThreadPoolExecutor(4)

class AtomicCounter:
    def __init__(self, initial=0):
        """Initialize a new atomic counter to given initial value (default 0)."""
        self.value = initial
        self._lock = threading.Lock()

    def increment(self, num=1):
        """Atomically increment the counter by num (default 1) and return the
        new value.
        """
        with self._lock:
            self.value += num
            return self.value

count = AtomicCounter()

def delete_item(uuid):
    arv_client.collections().delete(uuid=uuid).execute()
    v = count.increment(1)
    if v % 100 == 0:
        print(v)

skip = 0
start = time.time()
print("start")
for col in arvados.util.keyset_list_all(
        arv_client.collections().list,
        filters=[
            ["properties.type", "in", ["intermediate", "log"]],
            ["properties", "exists", "container_request"],
            ["modified_at", "<", before_timestamp]
        ],
        select=["uuid", "properties"]):

    if col["properties"]["container_request"] not in protected_requests:
        executor.submit(delete_item, col["uuid"])
    else:
        skip += 1

executor.shutdown()
print("count", count.value, "skip", skip)
