# python functions
import re
import random

# spark processing tools
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline

# ML models
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

random.seed(1)


def data_merger(spark):
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
    
    # # dataset.select('label').distinct().rdd.map(lambda r: r[0]).collect()
    # # possible values = 0, 1
    df = convertColumn(dataset, ['label'], IntegerType())
    # modify the rest of the variable to DoubleType 
    for col in dataset.columns:
        if col not in ['user_id', 'feature_2', 'feature_6', 'label']:
            df = convertColumn(df, [col], DoubleType())

    df = df.select('label', 'feature_1','feature_2', 'feature_3','feature_4','feature_5', 'feature_6', 'feature_7','feature_8','feature_9', 'feature_10')
    cols = df.columns

    categoricalColumns = ['feature_2']
    stages = list()
    for categoricalCol in categoricalColumns:
        stringIndexer = StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + 'Index')
        encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
        stages += [stringIndexer, encoder]

    numericCols = ['feature_1', 'feature_3','feature_4','feature_5', 'feature_6', 'feature_7','feature_8','feature_9', 'feature_10']
    assemblerInputs = [c + "classVec" for c in categoricalColumns] + numericCols
    assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
    stages += [assembler]

    pipeline = Pipeline(stages = stages)
    pipelineModel = pipeline.fit(df)
    df = pipelineModel.transform(df)
    selectedCols = ['features'] + cols
    df = df.select(selectedCols)
    
    return df


def build_model(df):
    """
    this function implements three models: logistic regression, decision trees and random forest
    :df: processed dataframe
    :result: None
    """
    
    # Split the data into train and test sets
    train_data, test_data = df.randomSplit([.8,.2],seed=7)
    
    print("Training Dataset Count: {0}".format(train_data.count()))
    print("Test Dataset Count: {0}".format(test_data.count()))

    lr = LogisticRegression(featuresCol = 'features', labelCol = 'label', maxIter=10)
    lr_model = lr.fit(train_data)

    training_summary = lr_model.summary

    lr_predictions = lr_model.transform(test_data)

    evaluator = BinaryClassificationEvaluator()
    print('Test Area Under ROC', evaluator.evaluate(lr_predictions))

    dt = DecisionTreeClassifier(featuresCol = 'features', labelCol = 'label', maxDepth = 3)
    df_model = dt.fit(train_data)
    df_predictions = df_model.transform(test_data)

    evaluator = BinaryClassificationEvaluator()
    print('Test Area Under ROC', evaluator.evaluate(df_predictions))

    rf = RandomForestClassifier(featuresCol = 'features', labelCol = 'label')
    rf_model = rf.fit(train_data)
    rf_predictions = rf_model.transform(test_data)

    evaluator = BinaryClassificationEvaluator()
    print('Test Area Under ROC', evaluator.evaluate(rf_predictions))
    
    gbt = GBTClassifier(maxIter=10)
    gbt_model = gbt.fit(train_data)
    gbt_predictions = gbt_model.transform(test_data)

    evaluator = BinaryClassificationEvaluator()
    print('Test Area Under ROC', evaluator.evaluate(gbt_predictions))

    # cross-validation
    paramGrid = (ParamGridBuilder()
                 .addGrid(gbt.maxDepth, [2, 4, 6])
                 .addGrid(gbt.maxBins, [20, 60])
                 .addGrid(gbt.maxIter, [10, 20])
                 .build())
    cv = CrossValidator(estimator=gbt, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=5)
    # Run cross validations.  This can take about 6 minutes since it is training over 20 trees!
    cv_gbt_odel = cv.fit(train_data)
    cv_gbt_predictions = cv_gbt_odel.transform(test_data)
    evaluator.evaluate(cv_gbt_predictions)
        
def main():
    
    # creating spark object
    spark_object = SparkSession.builder.getOrCreate()
    # supressing INFO logs
    spark_object.sparkContext.setLogLevel("ERROR")
    
    print('\n\nRunning Spark Data Munging\n\n')
    
    # function to merge files
    dataset = data_merger(spark=spark_object)
    
    # function to process the dataset
    dataset = data_processing(df=dataset)
    
    build_model(df=dataset)

    spark_object.stop()
    
if __name__ == '__main__':
    
    main()