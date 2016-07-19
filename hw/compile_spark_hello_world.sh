#!/bin/bash

# -----------------------------
# 
# -----------------------------

# compiles Spark_HelloWorld.scala by executing the java program scala.tools.nsc.Main, which compiles scala code into java bytecode, and puts the resultant class files into the target directory.  This is in lieu of using the scalac compiler, since it is not installed on the cluster yet.  

# One important note, spark-submit requires that your generated class files be rolled up into a jar, so the java archive tool is called at the end ("jar -cf"). 

# make sure that target directory is available
if [ ! -d target ]; then
	mkdir target	
fi

# -Dscala.usejavacp=true adds the java class path variables to the scala compiler class path.  spark-native-yarn/lib contains the jars necessary for compiling scala code into java bytecode.

java -Dscala.usejavacp=true -cp "/usr/hdp/2.3.2.0-2950/spark/lib/*" scala.tools.nsc.Main src/main/scala/com/Spark_HelloWorld.scala -d target/.

# the generated class files must now be placed into a jar for spark-submit submission.
cd target
jar -cf Spark_HelloWorld.jar *.class
cd ../
