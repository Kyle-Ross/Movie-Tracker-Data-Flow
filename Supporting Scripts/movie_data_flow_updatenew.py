import movie_data_flow
import time

print("Script to check for new movie ids and add data to database if found...")
print("Waiting 60 sec to let the database start up...")

for x in range(59,0,-1):
    time.sleep(1)
    print("%s seconds left until script start - movie_data_flow 'updatenew' method" % x)

time.sleep(1)
print("Script running...")

movie_data_flow.run_it("updatenew")

print("Script complete...")