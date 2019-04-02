def input(i: Int) = sc.parallelize(1 to i*100000)
def serial = (1 to 10).map(i => input(i).reduce(_ + _)).reduce(_ + _)
def parallel = (1 to 10).map(i => Future(input(i).reduce(_ + _))).map(Await.result(_, 10.minutes)).reduce(_ + _)


case class Test(a: Int = Random.nextInt(1000000),
                b: Double = Random.nextDouble,
                c: String = Random.nextString(1000),
                d: Seq[Int] = (1 to 100).map(_ => Random.nextInt(1000000))) extends Serializable

val input = sc.parallelize(1 to 1000000, 42).map(_ => Test()).persist(DISK_ONLY)
input.count() // Force initialization
val shuffled = input.repartition(43).count()
