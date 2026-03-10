import csv

data = [
    ["delta",0.9,12.1,1.4,320],
]

with open("metrics/benchmark_results.csv","a") as f:
    writer = csv.writer(f)
    writer.writerows(data)