"""
Requêtes d'analyse Airbnb dans Redis
Exécution :  python3 scripts/queries.py
"""

import redis, pprint
from collections import Counter

r = redis.Redis(host="airbnb-redis", port=6379, decode_responses=True)
assert r.ping(), "Redis non joignable"

pp = pprint.PrettyPrinter(width=120)

# -------------------------------------------------
print("\n  Listings par type de propriété")
props = Counter()
for k in r.scan_iter("movie:*"):
    props[r.hget(k, "genre") or "unknown"] += 1
pp.pprint(dict(props))

# -------------------------------------------------
print("\n  Listings du 12/06/2024 à Paris")
count_1206 = sum(
    1
    for k in r.scan_iter("movie:*")
    if r.hget(k, "release_year") == "2024-06-12" and r.hget(k, "neighbourhood") == "Paris"
)
print("Total :", count_1206)

# -------------------------------------------------
print("\n  Top-5 par nombre de reviews")
top = sorted(
    ((int(r.hget(k, "votes")), k) for k in r.scan_iter("movie:*")),
    reverse=True
)[:5]
for votes, key in top:
    print(key, "→", votes)

# -------------------------------------------------
print("\n  Nombre d’hôtes uniques")
host_ids = {r.hget(k, "host_id") for k in r.scan_iter("movie:*")}
print("Hosts uniques :", len(host_ids))

# -------------------------------------------------
print("\n  Instant-bookable")
total, instant = 0, 0
for k in r.scan_iter("movie:*"):
    total += 1
    if r.hget(k, "instant_bookable") == "True":
        instant += 1
print(f"{instant} sur {total} soit {instant/total:.2%}")

# -------------------------------------------------
print("\n  Hôtes avec >100 listings")
host_counter = Counter()
for k in r.scan_iter("movie:*"):
    host_counter[r.hget(k, "host_id")] += 1
big_hosts = {h:c for h,c in host_counter.items() if c > 100}
pp.pprint(big_hosts)
print("Ils représentent", f"{len(big_hosts)/len(host_counter):.2%}", "des hôtes")

# -------------------------------------------------
print("\n  Superhosts")
superhosts = {
    h.split(":")[1] for h in r.scan_iter("actor:*")
    if "superhost" in (r.hget(h, "about") or "").lower()
}
print("Superhosts :", len(superhosts),
      f"({len(superhosts)/len(host_counter):.2%} des hôtes)")
