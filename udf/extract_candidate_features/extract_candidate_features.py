import sys
from pyspark.sql import SparkSession
import ddlib
from util import Deepdive


def func(s):
    sent = []
    for i,t in enumerate(s[8]):
        sent.append(ddlib.Word(
            begin_char_offset=None,
            end_char_offset=None,
            word=t.encode('utf-8'),
            lemma=s[9][i].encode('utf-8'),
            pos=s[10][i],
            ner=s[11][i],
            dep_par=s[13][i] - 1,  # Note that as stored from CoreNLP 0 is ROOT, but for DDLIB -1 is ROOT
            dep_label=s[12][i]
        ))
    p1_span = ddlib.Span(begin_word_id=s[2], length=(s[3] - s[2]+1))
    p2_span = ddlib.Span(begin_word_id=s[4], length=(s[5] - s[4]+1))
    features = []
    for feature in ddlib.get_generic_features_relation(sent, p1_span, p2_span):
        features.append([s[0], s[1], feature])
    return features


def main():
    spark = SparkSession \
        .builder \
        .appName("feature extraction") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()
    args = sys.argv[1:]
    deepdive = Deepdive(spark)
    deepdive.load_tables(args=args)
    inputDF = spark.sql(args[1])
    t = inputDF.rdd.flatMap(lambda row: func(row)).toDF(["person_id", "place_id", "feature"])

    deepdive.save(result=t)

if __name__ == "__main__":
    main()
