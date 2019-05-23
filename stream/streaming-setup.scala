// https://blog.clairvoyantsoft.com/productionalizing-spark-streaming-applications-4d1c8711c7b0
val checkpointDirectory = "hdfs://..."   // define checkpoint directory

// Function to create and setup a new StreamingContext
def functionToCreateContext(): StreamingContext = {
  val ssc = new StreamingContext(...)   // new context
  val lines = ssc.socketTextStream(...) // create DStreams
  ...
  ssc.checkpoint(checkpointDirectory)   // set checkpoint directory
  ssc  // Return the StreamingContext
}

// Get StreamingContext from checkpoint data or create a new one
val ssc = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext)

ssc.start() // Start the context
