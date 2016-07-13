// Windows
scala> val datadir = "C:/data/RB-Scala"
datadir: String = C:/data/RB-Scala

scala> val autoData = sc.textFile(datadir + "/auto-data.csv")

// replace delimiter
scala> val datadir = "C:/data/RB-Scala"
datadir: String = C:/data/RB-Scala

scala> val autoData = sc.textFile(datadir + "/auto-data.csv")

// filter
scala> val toyotaData = autoData.filter(x => x.contains("toyota"))
