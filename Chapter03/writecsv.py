from faker import Faker
import csv

output = open("../output/data.csv", "w")
fake = Faker()
header = ["name", "age", "street", "city", "state", "zip", "lng", "lat"]
mywriter = csv.writer(output)
mywriter.writerow(header)
for r in range(1000):
    row = [fake.name(), fake.random_int(min=18, max=80, step=1),
           fake.street_address(), fake.city(), fake.state(),
           fake.zipcode(), fake.longitude(), fake.latitude()]
    mywriter.writerow(row)
output.close()
