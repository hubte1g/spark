////
# get data from hive
sqlContext.sql("SELECT * FROM lab_ent_anltc.user_recsys LIMIT 25").collect().foreach(println)

from pyspark.sql import SQLContext
sqlCtx = SQLContext(sc)

# load data from hive parquet (3 columns: mstr_prsna_key_id, ei_sku_id, rating)
ur = sqlCtx.parquetFile("hdfs://HADOOP/lab/user_recsys_parquet_nonnull")

ur = sqlCtx.parquetFile("hdfs://HADOOP/user_recsys_parquet_nonnull")

# count all records
ur.count()
////

%hdfs
hdfs dfs -put /home/localuser/edl-in/test/u.data hdfs://HADOOP/lab/

%pyspark
movielens = sc.textFile("hdfs://KOHLSEDLPROD1NNHA/kohls/eim/lab/u.data")
movielens.count()

movielens.first() #'u '

# isolate rating column (test) & count distinct values
rate = ur.map(lambda y: float(y[2]))
rate.distinct().count()

# import three function from mllib
from pyspark.mllib.recommendation import ALS,MatrixFactorizationModel,Rating

# create ratings object
ratings = ur.map(lambda z: Rating(int(z[0]), int(z[1]), int(z[2])))
#ratings = ur.map(lambda z: Rating(z[0]), float(z[1]), float(z[2])))

# create training and test set with random seed
train, test = ratings.randomSplit([0.71,0.29],6999)

# cache data to speed up traning
train.cache()
test.cache()

# set up paramaters for ALS and create model on training data
rank = 5 # latent factors to be made
numIterations = 10 # times to repeat process
model = ALS.train(train, rank, numIterations)
