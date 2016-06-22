val df = sc.parallelize(0 until 100).toDF("id").withColumn("rand1", rand(seed=10)).withColumn("rand2", rand(seed=27)).withColumn("rand3", rand(seed=33)
