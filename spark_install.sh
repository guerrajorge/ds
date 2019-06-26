#!/bin/bash

echo "Downloading Spark"

# shell config file used to store aliases
shell_profile="$HOME/.bashrc"

# location to download and install spark config files
spark_install_loc="$HOME/spark"
if [ -d $spark_install_loc ]
then 
	echo "cleaning installation folder"
	$(rm -rf $spark_install_loc) 
fi
# create directory
mkdir $spark_install_loc
echo "spark will be downloaded and installed in $spark_install_loc"

spark_installed_sucessfully=0

# url and version
spark_version=spark-2.4.3-bin-hadoop2.7.tgz
spark_url=http://apache.mirrors.tds.net/spark/spark-2.4.3/$spark_version
spark_folder=$spark_install_loc/$spark_version
# sumcheck md5 - verify the files were downloaded correctly 
md5_checksum=5d79926ad35cb3b214ed4f7878063aa5

# store the zip file in a folder with named by the version
curl $spark_url > $spark_folder
# make sure md5 checksum mataches
if [[ $(md5sum $spark_folder | sed -e "s/ .*$//") == "$md5_checksum" ]]
then
    # Unzip
    tar -xzf $spark_folder -C $spark_install_loc
    # Remove the compressed file
    rm $spark_folder
fi

# make sure pip is installed and if not isntall it
pip -version 2> /dev/null
if [ ! $? -eq 0 ]
then
	wget https://bootstrap.pypa.io/get-pip.py && python get-pip.py --user
fi

# install py4j
cd ~/.local/bin
# pyspark 2.4.3 has requirement py4j==0.10.7
./pip install py4j==0.10.7 --user
# needed to run models in pyspark
./pip install numpy --user
./pip install pandas --user

# remove extension in order to have the right name
spark_folder=$(echo $spark_folder | sed -e "s/.tgz$//")

cd $spark_folder
echo "
# Spark variables
export SPARK_HOME=\"$spark_folder\"
export PYTHONPATH=\"$spark_folder/python/:$PYTHONPATH\"

# Spark 2
export PYSPARK_DRIVER_PYTHON=python
export PATH=\$SPARK_HOME/bin:\$PATH
alias pyspark=\"$spark_folder/bin/pyspark \
    --conf spark.sql.warehouse.dir='file:///tmp/spark-warehouse' \
    --packages com.databricks:spark-csv_2.11:1.5.0 \"

export 
" >> $shell_profile

echo "install complete"

