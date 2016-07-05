// -*- coding: utf-8 -*-
/*
-----------------------------------------------------------------------------

                   Spark with Scala

             Copyright : V2 Maestros @2016
                    
Code Samples : Spark Machine Learning - Linear Regression

Problem Statement
*****************
The input data set contains data about details of various car 
models. Based on the information provided, the goal is to come up 
with a model to predict Miles-per-gallon of a given model.

Techniques Used:

1. Linear Regression ( multi-variate)
2. Data Imputation - replacing non-numeric data with numeric ones
3. Variable Reduction - picking up only relevant features

-----------------------------------------------------------------------------
*/
val datadir = "file:///home/tkb2171/edl-in/courses/"

//Create a SQL Context from Spark context
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

//Load the CSV file into a RDD
val autoData = sc.textFile(datadir + "/auto-miles-per-gallon.csv")
autoData.cache()

//Remove the first line (contains headers)
val dataLines = autoData.filter(x => !x.contains( "CYLINDERS"))
dataLines.count()

//Convert the RDD into a Dense Vector. As a part of this exercise
//   1. Remove unwanted columns
//   2. Change non-numeric ( values=? ) to numeric
//Use default for average HP

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint

val avgHP =sc.broadcast(80.0)

def transformToNumeric( inputStr : String) : Vector = {

    val attList=inputStr.split(",") 
    //Replace ? values with a normal value
    var hpValue = attList(3)
    if (hpValue.contains("?")) {
        hpValue=avgHP.value.toString
    }
    //Filter out columns not wanted at this stage
    val values= Vectors.dense(attList(0).toFloat, 
                     attList(1).toFloat,  
                     hpValue.toFloat,    
                     attList(5).toFloat,  
                     attList(6).toFloat
                     )
    return values
}
//Keep only MPG, CYLINDERS, HP,ACCELERATION and MODELYEAR
val autoVectors = dataLines.map(transformToNumeric)
autoVectors.collect()

//Keep only MPG, CYLINDERS, HP,ACCELERATION and MODELYEAR
val autoVectors = dataLines.map(transformToNumeric)
autoVectors.collect()

//Perform statistical Analysis
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
val autoStats=Statistics.colStats(autoVectors)
autoStats.mean
autoStats.variance
autoStats.min
autoStats.max
...
