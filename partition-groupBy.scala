val input = sc.parallelize(1 to 10000000, 42)
val definition = input.toDS.groupByKey(_ % 42).reduceGroups(_ + _)

// Spark Summit 2019 -- https://www.youtube.com/watch?v=daXEp4HmS-E
// Partitions - Right Sizing - Shuffle - Master Equation
// - Largest shuffle stage: Target Size <= 200MB/partition
// - Partition count = Stage Input Data / Target Size: Solve for Partition Count
// Default is 200.
spark.conf.set("spark.sql.shuffle.partitions", n) // :: relate to # cores
