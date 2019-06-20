val colArr = df.columns

val rootColsSet = colArr.toSet.toSeq

rootColsSet.foreach(c => println(c + " update distinct count: \t" + df.distinct.count))
