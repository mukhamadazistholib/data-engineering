# Description
1. Creating a dashboard of [yellow taxi trips records 2019-2020](https://www.kaggle.com/datasets/microize/newyork-yellow-taxi-trip-data-2020-2019?resource=download).

2. Pyspark guide installation.


## Documentations

#### Public Yellow Taxi Trips 19/20 [Dashboard](https://lookerstudio.google.com/s/mLQK20FcwGA)

#### Pyspark Installation Guide
<strong>What is PySpark?</strong><br>
<strong>PySpark</strong> is the Python API for Apache Spark, which is a distributed data processing framework designed to handle large-scale data processing tasks. PySpark allows developers to use the Python programming language to interact with Spark, enabling them to write distributed data processing applications using familiar Python syntax.

PySpark provides an intuitive and easy-to-use programming interface for processing large volumes of data in parallel across a cluster of machines. It offers a high-level abstraction layer for distributed data processing, making it easy to develop and deploy complex data processing applications without needing to write low-level code for handling parallelism, fault-tolerance, and data distribution.

Some of the key features of PySpark include support for distributed data processing, in-memory processing, batch processing, and real-time streaming processing. PySpark also provides support for a wide range of data sources and formats, including Hadoop Distributed File System (HDFS), Apache Parquet, JSON, CSV, and more. Additionally, PySpark provides a range of built-in libraries and APIs for machine learning, graph processing, and data visualization, making it a powerful tool for building end-to-end data processing pipelines.

<strong>How to install PySpark?</strong><br>
To install PySpark, you need to follow these steps:
1. Install Java: PySpark requires Java 8 or later versions to be installed on our machine. We can download and install Java from the official [website](https://www.java.com/en/download/) of Oracle or OpenJDK.

2. Download and install Apache Spark: We can download the latest version of Apache Spark from the official website of Apache Spark. Choose the package type that is compatible with your operating system.

3. Set up environment variables: After installing Apache Spark, we need to set up environment variables to point to the Spark installation directory. We can do this by adding the following lines to our shell profile file (.bashrc or .bash_profile on Linux or macOS):
```bash
export SPARK_HOME=/path/to/spark
export PATH=$PATH:$SPARK_HOME/bin
export PYSPARK_PYTHON=python3
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH
```
Replace /path/to/spark with the path to the directory where you installed Spark.
4. Install PySpark: Once we have to set up the environment variables, we can install PySpark using pip, which is the package installer for Python. Open a terminal and run the following command:
```bash
pip install pyspark
```
This will install the latest version of PySpark and its dependencies.
5. Verify the installation: To verify that PySpark is installed correctly, open a Python shell and run the following code:
```bash
from pyspark import SparkContext
sc = SparkContext("local", "First App")
rdd = sc.parallelize([1, 2, 3, 4, 5])
print(rdd.count())
```
This code creates a SparkContext object, initializes it with a local mode, creates an RDD (Resilient Distributed Dataset) of numbers from 1 to 5, and prints its count. If the installation is successful, we should see the output 5 on the console.