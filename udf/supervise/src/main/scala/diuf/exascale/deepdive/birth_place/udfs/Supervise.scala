package diuf.exascale.deepdive.birth_place.udfs

/**
  * Created by Ehsan on 7/29/17.
  */
import diuf.exascale.deepdive.udf.wrapper.Deepdive
import org.apache.spark.sql.SparkSession
import scala.collection.mutable
import util.control.Breaks._
import math._

object Supervise extends Deepdive{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Corenlp")
      // compress parquet datasets with snappy
      .config("spark.sql.parquet.compression.codec", "snappy")
      .getOrCreate()

    val t0 = System.nanoTime()//time measurement

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    load_tables(args,spark)
    val inputDF = spark.sql(args(1))
    inputDF.show

    val BORN = Array("bear")
    val NEGATIVE = Array("graduate", "live", "play", "die", "attend", "move")
    val MAX_DIST = 30
    var born_in = Array.empty[(String, String, Int, String)]
    val labled = inputDF.flatMap{
      r => {
        val person_end_idx = min(r(2).asInstanceOf[Int], r(5).asInstanceOf[Int])
        val place_start_idx = max(r(1).asInstanceOf[Int], r(4).asInstanceOf[Int])
        val place_end_idx = max(r(2).asInstanceOf[Int],r(5).asInstanceOf[Int])
        val intermediate_lemmas = r(10).asInstanceOf[mutable.WrappedArray[String]].slice(person_end_idx+1, place_start_idx)
        val intermediate_ner_tags = r(12).asInstanceOf[mutable.WrappedArray[String]].slice(person_end_idx+1,place_start_idx)

        if(intermediate_lemmas.length > MAX_DIST) born_in = born_in :+ (r(0).asInstanceOf[String], r(3).asInstanceOf[String], -1, "neg:far_apart")
        if(intermediate_ner_tags.contains("LOCATION")) born_in = born_in :+ (r(0).asInstanceOf[String], r(3).asInstanceOf[String], -1, "neg:another_place_between")
        if(BORN.intersect(intermediate_lemmas).length > 0) born_in = born_in :+ (r(0).asInstanceOf[String], r(3).asInstanceOf[String], 2, "pos:born_between")
        if(NEGATIVE.intersect(intermediate_lemmas).length > 0) born_in = born_in :+ (r(0).asInstanceOf[String], r(3).asInstanceOf[String], -2, "neg:other_verbs_between")
        born_in
      }
    }.toDF("person_id", "place_id", "label", "rule_id").cache
    save(labled)

    val t1 = System.nanoTime() //time measurement
    println("Elapsed time: " + (t1 - t0) + "ns")
  }// --main
}
