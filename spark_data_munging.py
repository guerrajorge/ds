
from pyspark.sql import SparkSession
import re
from pyspark.sql import Row

def main():
    
    # creating spark object
    spark = SparkSession.builder.getOrCreate()
    # supressing INFO logs
    spark.sparkContext.setLogLevel("ERROR")
    
    print('\n\nRunning Spark Data Munging\n\n')
    
    print('reading and merging files')
    # reading files
    # dataset description
    #	samples.json = user_id, features_[1|8]
    #	samples.customs = user_id, features_[9|10]
    #	samples.tsv = user_id, labels
    
    json_filename = "dataset/samples/samples.json"
    df = spark.read.load(json_filename, format='json')
    
    # create sparkContext object
    sc = spark.sparkContext
    
    # Processing samples.custom file
    # read the file
    rdd = sc.textFile("dataset/samples/samples.custom")
    # split each line and obtain 3 columns: user_id, feature_9 and feature_10
    rdd = rdd.map(lambda line: re.split("user_id=|feature_9=|feature_10=", line)[1:])
    # convert rdd to dataframe
    feat_9_10_df = rdd.map(lambda line: Row(user_id=line[0], feature_9=line[1], feature_10=line[2])).toDF()
    
    # Processing samples.tsv file
    # read the file
    rdd = sc.textFile("dataset/samples/samples.tsv")
    # split each line and obtain user_id and labels
    label_rdd = rdd.map(lambda line: line.split("\t"))
    # convert label_rdd to df
    label_df = label_rdd.map(lambda line: Row(user_name=line[0], label=line[1])).toDF()
    
    # merging all the dataframes
    print('df schema')
    df.printSchema()
    print('number of cols = {0}, rows={1}'.format(len(df.columns), df.count()))
    # merge df with the rest of the features df based on user_id
    df = df.join(feat_9_10_df,['user_id'],'inner')
    print('df schema after merge')
    df.printSchema()
    # sanity check
    print('number of cols = {0}, rows={1}'.format(len(df.columns), df.count()))

    print('first 5 observations')
    df.show(5, truncate=True)

    print('datatype columns')
    df.printSchema()

    print('first 5 observations')
    df.show(5, truncate=True)
    


    spark.stop()


# TO DO:
# 1. check for types of variables:
    # - Useless = unique, discrete data with no potential relationship with the outcome variable. A useless feature has high cardinality.
    # - Ratio (equal spaces between values and a meaningful zero value — mean makes sense)
    # - Interval (equal spaces between values, but no meaningful zero value — mean makes sense)
    # - Ordinal (first, second, third values, but not equal space between first and second and second and third — median makes sense)
    # - Nominal (no numerical relationship between the different categories — mean and median are meaningless). You can one-hot-encode or hash nominal features. Do not ordinal encode them because the relationship between the groups cannot be reduced to a monotonic function. The assigning of values would be random.
    
if __name__ == '__main__':
    
    main()