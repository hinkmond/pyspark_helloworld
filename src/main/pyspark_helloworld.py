# Header imports

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("pyspark_helloworld") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sc = spark.sparkContext

#####
# Your Code Below
#
DEBUG_MODE = False


def print_map(x):
    print(x)


text_file = sc.textFile("/opt/spark/README.md")
text_tokens = text_file.flatMap(lambda x: x.split(' '))

print("text_file line count: ", text_file.count())
print("text_file word count: ", text_tokens.count())

data = [["java, 3000"], ["scala, 1000"], ["python, 5000"]]
columns = ["value"]

df = spark.createDataFrame(data, columns)
df.printSchema()
df.show()

processed_table_df = df.selectExpr("split(value, ',')[0] as PROG_LANG",
                                   "split(value, ',')[1] as NUM_DEV")
processed_table_df.printSchema()
processed_table_df.show()

if DEBUG_MODE == "WARN":
    text_tokens.foreach(print_map)
