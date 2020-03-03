
A.apply() is the same as A() which is the same as A{}. 

DataFrame API is untyped Dataset.


From a category theory point of view you would say it "forms" a monad if the necessary functions exist.
From a class/object point of view you would say it "is" a monad if it has the correct functions as part of it's construction.
From a scala/cats perspective you'd say it "has" a monad if an instance of Monad exists that defines those function for the type.

Accumulator vs. count

rdd
Dataframe — Row of generic untyped jam objects, with named col’s
Dataset — strongly typed, with case class5

UDF, vectorization
Persist v. broadcast

Trait (no constructor parameters) v. Abstract class
Object v. Class
Class v. Abstract class

Review:
complexTypeExtractors.scala
jsonExpressions.scala — StructsToJson
Functions.scala, generators.scala

— collection accumulator for error-handling
whenThenCoalesce+MatchCase

-custom explode, join via uff


Spark PR's
https://spark-prs.appspot.com/open-prs -- participate / get involved with code reviews.

http://spark.apache.org/community.html

https://stackoverflow.com/questions/tagged/apache-spark


-----
https://towardsdatascience.com/recognising-handwritten-digits-with-scala-9829d035f7bc

https://www.mapr.com/blog/spark-streaming-and-twitter-sentiment-analysis
% spark

From quick-start to sci-ki-learn: https://databricks.com/blog/2016/05/18/spark-mllib-from-quick-start-to-scikit-learn.html

http://www.slideshare.net/jonathandinu/the-data-scientists-guide-to-apache-spark

Kafka notes:
https://daggubati-tech.blogspot.com/2018/12/steps-to-follow-to-pull-data-from.html


# def say_hello():
#     print('Hello, World')

# for i in range(5):
#     say_hello()
    

    
input = [(1,2),(2,3),(3,4),(4,5)]

# def new_func():
#     for i in input:
#         #print(i)
#         if i == (1,2):
#             print(i)

        

# new_func()

def findPath( root, path, k):
    if root is None:
        return False
    
    
    else:
        return True

findPath(0,0,0)
