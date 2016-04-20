transformation description

groupByKey([numTasks])
when called on a dataset of (K, V) pairs, returns a
dataset of (K, Seq[V]) pairs

reduceByKey(func,
[numTasks])
when called on a dataset of (K, V) pairs, returns
a dataset of (K, V) pairs where the values for each
key are aggregated using the given reduce function

sortByKey([ascending],
[numTasks])
when called on a dataset of (K, V) pairs where K
implements Ordered, returns a dataset of (K, V)
pairs sorted by keys in ascending or descending order,
as specified in the boolean ascending argument

join(otherDataset,
[numTasks])
when called on datasets of type (K, V) and (K, W),
returns a dataset of (K, (V, W)) pairs with all pairs
of elements for each key

cogroup(otherDataset,
[numTasks])
when called on datasets of type (K, V) and (K, W),
returns a dataset of (K, Seq[V], Seq[W]) tuples –
also called groupWith

cartesian(otherDataset) when called on datasets of types T and U, returns a
dataset of (T, U) pairs (all pairs of elements)
