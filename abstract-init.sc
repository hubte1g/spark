import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType

var df : DataFrame = _

def initializeDataFrame(query: String): DataFrame = {
  //cache the dataframe
  if (df == null) {
    df = hiveCtxt.sql(query).na.drop().cache()
  }
  return df
}

initializeDataFrame: (query: String)org.apache.spark.sql.DataFrame

