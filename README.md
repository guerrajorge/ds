# ds

This repository contains the following files:  

- spark_install = bash script to install [py]spark in a CentOS 7 system in addition to pip, py4j, numpy and panda in order to run other scripts  
- main.py = python script which does the following:
    - reads the following files : samples.json, samples.customs and samples.tvs, from a samples/ folder
    - merges them
    - preprocess them: check for class balance, missiness, variables dtype
    - build ML models, logistic regression, decission tree, and random forest, for a binary classification task
    - evaluates the results using the following metrics: AUC, precission, recall, F1

