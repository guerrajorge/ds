
from pyspark.sql import SparkSession
import re
from pyspark.sql import Row
from pyspark.sql.types import StringType, BinaryType, DoubleType


def data_merger():
    """
    combine all the files
    :return: a agregated dataframe
    """
    
    print('loading files')
    # reading files
    # dataset description
    #	samples.json = user_id, features_[1|8]
    #	samples.customs = user_id, features_[9|10]
    #	samples.tsv = user_id, labels
    
    # creating spark object
    spark = SparkSession.builder.getOrCreate()
    # supressing INFO logs
    spark.sparkContext.setLogLevel("ERROR")
    
    # create sparkContext object
    sc = spark.sparkContext
    
    json_filename = 'samples/samples.json'
    df = spark.read.load(json_filename, format='json')
    
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
    print('\tnumber of cols = {0}, rows={1}'.format(df_n_cols, df_n_rows))
    # merge df with the rest of the features df based on user_id
    df_2 = df.join(feat_9_10_df,['user_id'],'inner')
    df_2_n_cols, df_2_n_rows = len(df_2.columns), df_2.count()
    # sanity check
    print('\tnumber of cols = {0}, rows={1}'.format(df_2_n_cols, df_2_n_rows))
    
    if df_2_n_rows == df_n_rows:
        print('same \"user_id\"s found in both files')
    else:
        print('diff \"user_id\"s found in files')
    
    print('merging json + custom and tsv')
    # merge new df with the labels df based on user_id
    n_dataset = df_2.join(label_df,['user_id'],'inner')
    dataset_n_cols, dataset_n_rows = len(n_dataset.columns), n_dataset.count()
    # sanity check
    print('\tnumber of cols = {0}, rows={1}'.format(dataset_n_cols, dataset_n_rows))
    
    if dataset_n_rows == df_n_rows:
        print('same \"user_id\"s found in both files')
    else:
        print('diff \"user_id\"s found in files')
    
    print('df schema after merger')
    n_dataset.printSchema()
    
    return n_dataset


def convertColumn(df, names, new_type):
    """
    :param df: complete dataset df
    :param names: column of interest
    :param newType: type to change it to
    :return: dataframe with the column of intered with a modified dtype
    """
    for name in names: 
        df = df.withColumn(name, df[name].cast(new_type))
    return df 


def data_processing(df):
    """
    process the dataset by modifying dtype
    :return: a processed dataframe
    """
    # analyse all the variables
    for col in df.columns:
        if col not in ['user_id', 'feature_2', 'label']:
            df.select(col).describe().show()
    
    # since its eather 0 or 1
    df = convertColumn(df, ['label'], BinaryType())
    # since its A, B, C
    df = convertColumn(df, ['feature_2'], StringType())
    # analyse all the variables
    for col in df.columns:
        if col not in ['user_id', 'feature_2', 'label']:
            df = convertColumn(dataset, ['feature_9'], DoubleType())
    
    return df
        
def main():
    
    print('\n\nRunning Spark Data Munging\n\n')
    
    # function to merge files
    dataset = data_merger()
    
    # function to process the dataset
    dataset = data_processing(df=dataset)

    spark.stop()
    
if __name__ == '__main__':
    
    main()