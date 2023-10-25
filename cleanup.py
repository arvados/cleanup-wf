import datetime
import arvados
import arvados.util
import time

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

count = 0
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

    count += 1
    if count % 100 == 0:
        print(count, "period", time.time() - start, "seconds")
        start = time.time()

    if col["properties"]["container_request"] not in protected_requests:
        arv_client.collections().delete(uuid=col["uuid"]).execute()
    else:
        skip += 1

print("count", count, "skip", skip)
