package diuf.exascale.deepdive.birth_place.udfs

/**
  * input is article(id, content)
  * output sentences("doc_id", "sentence_index", "sentence_text", "tokens", "lemmas", "pos_tags", "ner_tags", "doc_offsets", "dep_types", "dep_tokens")
  * Created by Ehsan on 7/30/17.
  */

import diuf.exascale.deepdive.udf.wrapper.Deepdive
import org.apache.spark.sql.SparkSession
import com.databricks.spark.corenlp.functions._
import org.apache.spark.sql._
import scala.collection.mutable

object NLPmarkup extends Deepdive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Corenlp")
      // compress parquet datasets with snappy
      .config("spark.sql.parquet.compression.codec", "snappy")
      .getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames

    val t0 = System.nanoTime()//time measurement

    import spark.implicits._
    load_tables(args, spark)
    val inputDF = spark.sql(args(1))

    // split article to sentence with index
    val sentences = inputDF.select('column_0.as('doc_id), ssplit('column_1)).flatMap {
      r =>
        r(1).asInstanceOf[mutable.WrappedArray[String]].zipWithIndex.map {
          sen => (r(0).asInstanceOf[String], sen._2, sen._1)
        }
    }.toDF("doc_id", "sentence_index", "sentence_text")
    val sen_tags = sentences.select('doc_id, 'sentence_index, 'sentence_text, tokenize('sentence_text),
      lemma('sentence_text), pos('sentence_text), ner('sentence_text), depparse('sentence_text)).map {
      r => {
        // extrace doc_offset
        var doc_offset = Array.empty[Int]
        var last_idx = 0
        r(3).asInstanceOf[mutable.WrappedArray[String]].foreach {
          token => {
            if (token == "-LRB-") {
              doc_offset = doc_offset :+ last_idx + 1
              last_idx = last_idx + 1
            }else if (token == "-RRB-"){
				doc_offset = doc_offset :+ last_idx
				last_idx = last_idx + 1
            }else {
              val offset = r(2).asInstanceOf[String].indexOf(token, last_idx)
              if (offset >= 0) {
                doc_offset = doc_offset :+ offset
                last_idx = offset + token.length + 1
              }
            }
          }
        }
        // extract depparse
        var dep_types = Array.empty[String]
        var dep_tokens = Array.empty[Int]
        r(7).asInstanceOf[mutable.WrappedArray[Row]].foreach {
          dep => {
			while(dep(4).asInstanceOf[Int] > dep_tokens.length + 1){
				dep_types = dep_types :+ ""
				dep_tokens = dep_tokens :+ 0

			}
			dep_types = dep_types :+ dep(2).asInstanceOf[String]
			dep_tokens = dep_tokens :+ dep(1).asInstanceOf[Int]
          }
        }
        (r(0).asInstanceOf[String], r(1).asInstanceOf[Int], r(2).asInstanceOf[String],
          r(3).asInstanceOf[mutable.WrappedArray[String]], r(4).asInstanceOf[mutable.WrappedArray[String]],
          r(5).asInstanceOf[mutable.WrappedArray[String]],
          r(6).asInstanceOf[mutable.WrappedArray[String]], doc_offset, dep_types, dep_tokens)
      }
    }.toDF("doc_id", "sentence_index", "sentence_text", "tokens", "lemmas", "pos_tags", "ner_tags", "doc_offsets", "dep_types", "dep_tokens")
    
    val filtered = sen_tags.rdd.filter{
		 r => if(r(3).asInstanceOf[mutable.WrappedArray[String]].length != r(8).asInstanceOf[mutable.WrappedArray[String]].length) true else false
	 }.toDF("doc_id", "sentence_index", "sentence_text", "tokens", "lemmas", "pos_tags", "ner_tags", "doc_offsets", "dep_types", "dep_tokens")
    
    save(filtered)

    val t1 = System.nanoTime() //time measurement
    println("Elapsed time: " + (t1 - t0) + "ns")

  } // -- main
}
