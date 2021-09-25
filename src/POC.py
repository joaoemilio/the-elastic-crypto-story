import ScientistUtils as su
print("deploying my first docker container for a python job")

symbols = su.read_json("symbols.json")

total = len(symbols)
split = int(total/5)

count = 0
groups = {}
group = 1
for s in symbols:
    if group not in groups: groups[group] = []
    groups[group].append(s)
    count += 1
    if count > split:
        group += 1
        count = 0

for g in groups:
    su.write_json(groups[g], f"../config/symbols-group{g}.json")