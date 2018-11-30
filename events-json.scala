import org.apache.spark.sql.types._ // https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/package-summary.html
import org.apache.spark.sql.functions._
// Our JSON Schema
val jsonSchema = new StructType()
  .add("sensor", StringType)
  .add("temperatureValue", StringType)
  .add("humidityValue", StringType)
  .add("createdAt", StringType)
// Convert our EventHub data, where the body contains our message and which we decode the JSON
val messages = eventHubs
  // Parse our columns from what EventHub gives us (which is the data we are sending, plus metadata such as offset, enqueueTime, ...)
  .withColumn("Offset", $"offset".cast(LongType))
  .withColumn("Time (readable)", $"enqueuedTime".cast(TimestampType))
  .withColumn("Timestamp", $"enqueuedTime".cast(LongType))
  .withColumn("Body", $"body".cast(StringType))
  // Select them so we can play with them
  .select("Offset", "Time (readable)", "Timestamp", "Body")
  // Parse the "Body" column as a JSON Schema which we defined above
  .select(from_json($"Body", jsonSchema) as "sensors")
  // Now select the values from our JSON Structure and cast them manually to avoid problems
  .select(
    $"sensors.sensor".cast("string"),
    $"sensors.createdAt".cast("timestamp"), 
    $"sensors.temperatureValue".cast("double") as "tempVal", 
    $"sensors.humidityValue".cast("double") as "humVal"
  )
// Print the schema to know what we are working with
messages.printSchema()
