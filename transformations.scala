transformation description

map(func)
return a new distributed dataset formed by passing
each element of the source through a function func

filter(func)
return a new dataset formed by selecting those
elements of the source on which func returns true

flatMap(func)
similar to map, but each input item can be mapped
to 0 or more output items (so func should return a
Seq rather than a single item)

sample(withReplacement,
fraction, seed)
sample a fraction fraction of the data, with or without
replacement, using a given random number generator
seed

union(otherDataset)
return a new dataset that contains the union of the
elements in the source dataset and the argument

distinct([numTasks]))
return a new dataset that contains the distinct elements
of the source dataset

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
