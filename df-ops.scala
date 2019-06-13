def dfColsToHashRangeList(df: DataFrame, hash_key: String, range_key: String): Array[String] = {
  
  import org.apache.spark.sql.functions._

  val stg_hash_range_key = df.select(
    concat_ws(",", col(hash_key), col(range_key))
      .as("ddb_hash_range_key")
  )

  val hash_range_key_list = stg_hash_range_key.select(
    collect_list("ddb_hash_range_key")
      .as("ddb_hash_range_key_list")
  )

  import scala.collection.mutable.WrappedArray
  val stg_lkpList = hash_range_key_list.first.getAs[WrappedArray[String]](0).toArray

  val lkpList = stg_lkpList.foldLeft(new StringBuilder())(_ append _ + ",").toString.split(",")

  lkpList

}
