package diuf.exascale.deepdive.birth_place.udfs

/**
  * Created by Ehsan on 7/29/17.
  */
import diuf.exascale.deepdive.udf.wrapper.Deepdive
import org.apache.spark.sql.SparkSession
import scala.collection.mutable

object NationalityMention extends Deepdive{
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
    val mentions = inputDF.flatMap{
      r => {
        val ner_tags = r(3).asInstanceOf[mutable.WrappedArray[String]]
        val num_tokens = ner_tags.length
        var first_indexes = Array.empty[Int]
        var p_men = Array.empty[(String, String, String, Int, Int, Int)]
        ner_tags.zipWithIndex.foreach{
          case (tag:String, i:Int) => {
            if(tag == "MISC" && (i == 0 || ner_tags(i-1) != "MISC")){
              first_indexes = first_indexes :+ i
            }
          }
        }
        first_indexes.foreach{
          case(first_index:Int) => {
            var end_index = first_index + 1
            //while(end_index<num_tokens && ner_tags(end_index) == "MISC") end_index += 1
            end_index -= 1
            val doc_id = r(0).asInstanceOf[String]
            val sentence_index = r(1).asInstanceOf[Int]
            val mention_id = "%s_%d_%d_%d".format(doc_id, sentence_index, first_index, end_index)
            val mention_text = r(2).asInstanceOf[mutable.WrappedArray[String]].slice(first_index, end_index+1).mkString(" ")
            p_men = p_men :+ (mention_id, mention_text, doc_id, sentence_index, first_index, end_index)
          }
        }
        p_men
      }
    }.toDF("mention_id", "mention_text", "doc_id", "sentence_index", "begin_index", "end_index").cache
    save(mentions)

    val t1 = System.nanoTime() //time measurement
    println("Elapsed time: " + (t1 - t0) + "ns")

  }// --main
}
