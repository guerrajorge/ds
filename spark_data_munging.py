
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
    
    json_filename = 'samples/samples.json'
    df = spark.read.load(json_filename, format='json')
    
    # create sparkContext object
    sc = spark.sparkContext
    
    # Processing samples.custom file
    # read the file
    rdd = sc.textFile('samples/samples.custom')
    # split each line and obtain 3 columns: user_id, feature_9 and feature_10
    rdd = rdd.map(lambda line: re.split('user_id=|feature_9=|feature_10=', line)[1:])
    # convert rdd to dataframe
    feat_9_10_df = rdd.map(lambda line: Row(user_id=line[0], feature_9=line[1], feature_10=line[2])).toDF()
    
    # Processing samples.tsv file
    # read the file
    rdd = sc.textFile('samples/samples.tsv')
    # split each line and obtain user_id and labels
    label_rdd = rdd.map(lambda line: line.split('\t'))
    # convert label_rdd to df
    label_df = label_rdd.map(lambda line: Row(user_id=line[0], label=line[1])).toDF()
    
    print('merging json and custom')
    df_n_cols, df_n_rows = len(df.columns), df.count()
    print('number of cols = {0}, rows={1}'.format(df_n_cols, df_n_rows))
    # merge df with the rest of the features df based on user_id
    df_2 = df.join(feat_9_10_df,['user_id'],'inner')
    df_2_n_cols, df_2_n_rows = len(df_2.columns), df_2.count()
    # sanity check
    print('number of cols = {0}, rows={1}'.format(df_2_n_cols, df_2_n_rows))
    
    if df_2_n_rows == df_n_rows:
        print('same user_id found in both files')
    else:
        print('diff user_id found in both files')
    
    print('df schema after merge')
    df_2.printSchema()
    print('first 5 observations')
    df_2.show(5, truncate=True)
    
    print('merging json + custom and tsv')
    # merge new df with the labels df based on user_id
    dataset = df_2.join(label_df,['user_id'],'inner')
    dataset_n_cols, dataset_n_rows = len(dataset.columns), dataset.count()
    # sanity check
    print('number of cols = {0}, rows={1}'.format(dataset_n_cols, dataset_n_rows))
    
    if dataset_n_rows == df_n_rows:
        print('same user_id found in both files')
    else:
        print('diff user_id found in both files')
    
    print('df schema after merge')
    dataset.printSchema()
    print('first 5 observations')
    dataset.show(5, truncate=True)
    

    spark.stop()
    
if __name__ == '__main__':
    
    main()