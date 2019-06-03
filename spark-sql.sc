//Unpack values

val df1 = sc.parallelize(Seq(1->2, 2->3, 3->7,5->4,1->3)).toDF("col1","col2")
val df2 = sc.parallelize(Seq(1->3,5->1)).toDF("col1","col2")

val z = df1.join(df2, cols.map(c => df1(c) === df2(c)).reduce(_ || _) )
z.select(cols.map(df1(_)) :_*).show

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
