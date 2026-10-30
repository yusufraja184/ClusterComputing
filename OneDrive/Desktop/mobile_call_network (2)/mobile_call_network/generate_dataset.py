"""
generate_dataset.py
Creates a fake (synthetic) mobile call records file: data/call_records.csv
Real telecom data is private, so we create our own with the same columns.
Only plain Python is used here (no Spark).
"""
import random   # to pick random numbers/users
import os       # to create the "data" folder

NUM_USERS = 1000       # total mobile users
NUM_CALLS = 50000      # number of calls among normal users

# 1) Make user ids: U00001, U00002, ... U01000
users = []
for i in range(1, NUM_USERS + 1):
    users.append("U" + str(i).zfill(5))      # zfill(5) -> 1 becomes 00001

# 2) Split users: first 900 are normal users, last 100 are "rare" users
normal_users = users[:900]
rare_users = users[900:]

# 3) Popularity: user 1 is the most popular, user 2 a bit less, and so on.
#    A bigger weight means that user is picked as receiver more often.
weights = []
for i in range(900):
    weights.append(1 / (i + 1))              # 1, 1/2, 1/3, 1/4 ...

# 4) Helper to make a random date-time text like "2026-02-14 09:35:07"
def random_time():
    month = random.randint(1, 3)
    day = random.randint(1, 28)
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return f"2026-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}"

# 5) Create the CSV file
os.makedirs("data", exist_ok=True)
file = open("data/call_records.csv", "w")
file.write("call_id,caller_id,receiver_id,call_start,duration_sec\n")   # header row

call_id = 1

# 5a) Calls among normal users
for _ in range(NUM_CALLS):
    caller = random.choice(normal_users)                       # anyone can call
    receiver = random.choices(normal_users, weights)[0]        # popular users get called more
    if caller == receiver:                                     # nobody calls themselves
        continue
    duration = random.randint(5, 1800)                         # 5 sec to 30 min
    file.write(f"{call_id},{caller},{receiver},{random_time()},{duration}\n")
    call_id += 1

# 5b) Rare users: each makes or receives only 1 call (so they are "isolated")
for rare in rare_users:
    other = random.choice(normal_users)
    if random.random() < 0.5:                                  # 50% rare user is the caller
        caller, receiver = rare, other
    else:                                                      # 50% rare user is the receiver
        caller, receiver = other, rare
    file.write(f"{call_id},{caller},{receiver},{random_time()},{random.randint(5, 600)}\n")
    call_id += 1

file.close()
print("Dataset created: data/call_records.csv with", call_id - 1, "calls")
