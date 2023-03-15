#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#


"""
This script initializes a SparkSession and SparkContext, and creates a SparkConf object. 
The SparkSession is created using either an active SparkSession or a new SparkSession builder.
The SparkContext is retrieved using the getOrCreate method.
Finally, the SparkConf object is created using the getConf method on the SparkContext object.

The idea is that this is generally available and we avoid a more verbose import/initialization
"""

# Import required modules
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


# Initialize SparkSession and SparkContext
config = SparkConf().setAll([("spark.executor.allowSparkContext", "true")])

spark: SparkSession = (
    SparkSession.builder.enableHiveSupport().config(conf=config).getOrCreate()
)


sc: SparkContext = SparkContext.getOrCreate()

# Create SparkConf object
conf: SparkConf = sc.getConf()
