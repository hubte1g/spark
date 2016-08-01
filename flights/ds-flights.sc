// meta: https://docs.google.com/spreadsheets/d/11qZV5wRRt625oKIlOYCMdTH4-YgQ4o4Te1m38YF33xc/edit#gid=0 

/**
 * 01 Determine the airlines with the greatest number of flights.
 */
 
val dir = s"/../.."

val carrierRdd_split = sc.textFile(dir + "/lab/flights.csv").map(x => x.split(",")).take(5)
// Array[Array[String]] = Array(Array(Year, Month, DayofMonth, DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime, UniqueCarrier, FlightNum, TailNum, ActualElapsedTime, CRSElapsedTime, AirTime, ArrDelay, DepDelay, Origin, Dest, Distance, TaxiIn, TaxiOut, Cancelled, CancellationCode, Diverted, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay, LateAircraftDelay), Array(2008, 1, 3, 4, 2003, 1955, 2211, 2225, WN, 335, N712SW, 128, 150, 116, -14, 8, IAD, TPA, 810, 4, 8, 0, "", 0, NA, NA, NA, NA, NA), Array(2008, 1, 3, 4, 754, 735, 1002, 1000, WN, 3231, N772SW, 128, 145, 113, 2, 19, IAD, TPA, 810, 5, 10, 0, "", 0, NA, NA, NA, NA, NA), Array(2008, 1, 3, 4, 628, 620, 804, 750, WN, 448, N428WN, 96, 90, 76, 14, 8, IND, BWI, 515, 3, 17, 0, "", 0, NA, NA, NA, NA, NA), Array(2008,...

val carrierRdd = sc.textfile(dir + "/lab/flights.csv").map(x => x.split(",")).map(column => (column(8),1))
// Array[(String, Int)] = Array((UniqueCarrier,1), (WN,1), (WN,1), (WN,1), (WN,1))

/* Perform a reduce and sort the results, then display the top three carrier codes by
 * number of flights based on this data.
 */
 
val carriersSorted_rbk = carrierRdd.reduceByKey((x,y) => x + y).take(9)
// carriersSorted_rbk: Array[(String, Int)] = Array((B6,196091), (UA,449515), (DL,451931), (9E,262208), (US,453589), (XE,374510), (F9,95762), (YV,254930), (OH,197607)) 
// Another way using associative function // val carriersSorted_rbk = carrierRdd.reduceByKey(_ + _).take(100)

val carriersSorted_rbk = carrierRdd.reduceByKey(_ + _).map{ case (a,b) => (b,a) }.sortByKey(ascending = false)
// Array[(Int, String)] = Array((1201754,WN), (604885,AA), (567159,OO), (490693,MQ), (453589,US), (451931,DL), (449515,UA), (374510,XE), (347652,NW), (298455,CO), (280575,EV), (262208,9E), (261684,FL), (254930,YV), (197607,OH), (196091,B6), (151102,AS), (95762,F9), (61826,HA), (7800,AQ), 

/**
 * 02 Determine the most common routes between two cities. Uses RDD above from flights and also new RDD with airports.
 */
 
 // airport code and city
 val cityRdd = sc.textFile(dir + "/lab/airports.csv").map(x => x.split(",")).map(column => (column(0), column(2)))
 
 // origin and destination  //  Array[(String, String)] = Array((Origin,Dest), (IAD,TPA), 
 val OrigDestRdd = sc.textFile(dir + "/lab/flights.csv").map(x => x.split(",")).map(column => (column(16), column(17)))
 
 //origin code as the key, with a value of (destination code, origin city)
 val origJoinRdd = OrigDestRdd.join(cityRdd) // Array[(String, (String, String))] = Array((RIC,(IAH,Richmond)), 
 
 // use values() to filter out the origin code
 // k: destination code, v: (origin city, destination city) //  Array[(String, (String, String))] = Array((RIC,(Boston,Richmond))
 val destOrigJoin = origJoinRdd.values.join(cityRdd)
 
 //  Array[(String, String)] = Array((Boston,Richmond),
 val citiesCleanedRdd = destOrigJoin.values
 
// Array[((String, String), Int)] = Array(((Boston,Richmond),1),
 val citiesKV = citiesCleanedRdd.map( cities => (cities, 1))
 
 // Array[(Int, (String, String))] = Array((19548,(New York,Boston)), 
 val citiesReducedSortedRdd = citiesKV.reduceByKey((x,y) => x + y).map{ case (x,y) => (y,x)}.sortByKey(ascending = false)

// ** The top three origin city / destination combinations are New York to Boston, Boston to New York, and Chicago to New York.

/////

// Find the longest departure delays for any airline that experienced a delay of 15 minutes or more.
// use depDelay and UniqueCarrierID
//--val delayRdd = sc.textFile("/kohls/eim/lab/flights.csv").map(x => x.split(",")).filter(delay => (delay(15).toInt) > 15).map(column => (column(8), column(15).toInt))
//--val delayRdd_no_hdr = sc.textFile("/kohls/eim/lab/flights.csv").map(x => x.split(",")).zipWithIndex().filter(_._2 > 0).filter(delay => (delay(15).toInt) > 15).map(column => (column(8), column(15).toInt))
val delayRdd = sc.textFile(dir + "/lab/flights.csv").mapPartitions(_.drop(1)).map(x => x.split(",")).filter(delay => (delay(15).toInt) > 15).map(column => (column(8), column(15).toInt))
