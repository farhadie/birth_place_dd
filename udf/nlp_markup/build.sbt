name := "nlp_markup"

version := "1.0"

scalaVersion := "2.11.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.0" % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.11
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.0" % "provided"


resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

// https://mvnrepository.com/artifact/databricks/spark-corenlp
libraryDependencies += "databricks" % "spark-corenlp" % "0.2.0-s_2.11"

libraryDependencies ++= Seq(
  "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0" classifier "models"
)

unmanagedJars in Compile += file("/home/sam/local2/lib/scala/udf_wrapper/target/scala-2.11/udf_wrapper-assembly-1.0.jar")