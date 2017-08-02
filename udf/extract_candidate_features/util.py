import re


class Deepdive():
    saved_args = ""
    spark = None

    def __init__(self, spark):
        self.spark = spark

    def load_tables(self, args):
        self.saved_args = args
        hadoop_dir = args[0]
        input_sql = args[1]
        relations = []
        from_phrases = re.findall(r"(?s)(?<=FROM )[^\\(].*?(?=GROUP|WHERE|$|\n$)", input_sql)
        for from_phrase in from_phrases:
            for one_table in from_phrase.split(","):
                relations.append(re.sub(r'\n| +$|^ +', '', one_table).split(" ")[0])
        for relation in set(relations):
            df = self.spark.read.load("/" + hadoop_dir + "/" + relation)
            df.createOrReplaceTempView(relation)

    def save(self, result):
        output_directory = self.saved_args[0]
        table_name = self.saved_args[2]
        print("saving output to: /" + output_directory + "/" + table_name + "_tmp")
        result.write.mode("append").format("parquet").save("/" + output_directory + "/" + table_name + "_tmp")
        print("saving done.")
