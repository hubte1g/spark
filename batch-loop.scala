import scala.math.ceil
import scala.collection.mutable.ListBuffer
val batch = 99

// Not serializable in worksheet, runs in shell.

df.foreachPartition{
  partition=>
    val records=partition.toList
    val numRecords=records.size
    val iterations=ceil(numRecords.toDouble/batch).toInt
    var counter=0
    var start=0
    var remainingRecordsToPush=numRecords
    while(counter<iterations){
      val numRecordsInBatch=if(batch<remainingRecordsToPush) batch else remainingRecordsToPush
      val recordsInBatch=records.slice(start,start+numRecordsInBatch)
      //val putRecordsRequest = new PutRecordsRequest

      //val putRecordsRequestEntryList = new java.util.ArrayList[PutRecordsRequestEntry]
      recordsInBatch.foreach{
        record=>
          //val putRecordsRequestEntry = new PutRecordsRequestEntry
//          putRecordsRequestEntry.setData(ByteBuffer.wrap(
            println("HERE: " + String.valueOf(record))
//              .getBytes(StandardCharsets.UTF_8)))
//          val random = new scala.util.Random()
//          val partKey = String.format(BigInt(128, random).toString(10))
//          putRecordsRequestEntry.setPartitionKey(String.format(partKey))
//          putRecordsRequestEntryList.add(putRecordsRequestEntry)
      }
//      putRecordsRequest.setRecords(putRecordsRequestEntryList)
//      val putRecordsResult = kpClient.putRecords(putRecordsRequest)
//      val failedCount=putRecordsResult.getFailedRecordCount
//      if(failedCount>0)
//        println("Failed to put "+failedCount)
      counter+=1
      start=start+numRecordsInBatch
      remainingRecordsToPush-=numRecordsInBatch

    }
}
