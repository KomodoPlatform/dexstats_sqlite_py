import sqlite3
from datetime import datetime, timedelta

timestamp1 = 1620680401
timestamp2 = 1620766799
timestamp_24h_ago = int((datetime.now() - timedelta(1)).strftime("%s"))
print(timestamp_24h_ago)
conn = sqlite3.connect("MM2.db")
sql_coursor = conn.cursor()
t = (timestamp1, timestamp2)
sql_coursor.execute("SELECT * FROM stats_swaps WHERE started_at > ? AND started_at < ? AND is_success=1;", t)
for row in sql_coursor.fetchall():
    print(row)
#swap_statuses = [dict(row) for row in sql_coursor.fetchall()]
#print(swap_statuses)
#with open(str(timestamp1)+"_"+str(timestamp2)+".json", 'w') as out_file:
#    for status in swap_statuses:
#        print(status)
#        out_file.write(status + "\n")
