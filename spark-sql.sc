// 'I' compare for scd
.select(

      when(col("event")===lit("someValue"),
      if(col("e_col") == col("m_col")) null else col("e_col")).otherwise(col("e_col")).as("col"),

hc.sql("select sum(metric_) as sales from schema.table where ky_dte='2015-01-01'").collect()

//cache df
var df: DataFrame = _

df = hc.sql("select sum(metric_) as sales from schema.table where ky_dte='2015-01-01'").na.drop().cache()

res24: org.apache.spark.sql.DataFrame = [sales: decimal(25,2)]

df.show()
