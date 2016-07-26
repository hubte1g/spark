//Determine the airlines with the greatest number of flights.

val carrierRdd = sc.textfile("/kohls/eim/lab/flights.csv").map(x => x.split(",")).map(column => (column(5),1))

