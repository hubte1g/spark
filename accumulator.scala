accumulator[T](initialValue: T)(implicit param: AccumulatorParam[T]): Accumulator[T]
accumulator[T](initialValue: T, name: String)(implicit param: AccumulatorParam[T]): Accumulator[T]
accumulator methods create accumulators of type T with the initial value initialValue.

scala> val acc = sc.accumulator(0) // not needed for example
acc: org.apache.spark.Accumulator[Int] = 0

scala> val counter = sc.accumulator(0, "counter")
counter: org.apache.spark.Accumulator[Int] = 0

scala> counter.value
res2: Int = 0

scala> sc.parallelize(0 to 9).foreach(n => counter += n)

scala> counter.value
res4: Int = 45

//Scala do-while Loops

var count = 0

do {
  count += 1
  println(count)
} while (count < 4.99)
