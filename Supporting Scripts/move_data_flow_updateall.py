import movie_data_flow

from datetime import datetime
startTime = datetime.now()

movie_data_flow.run_it("updateall")

print(datetime.now() - startTime)