#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#

import logging

import pyspark.sql.functions as F

from pyspark.ml.feature import CountVectorizer, MinHashLSH, NGram
from pyspark.mllib.linalg import SparseVector
from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType, BooleanType, StringType


logger = logging.getLogger(__name__)


def lsh_match_text(
    data: DataFrame,
    text_col: str = "Text",
    data_partitions: int = 640,
    include_word_delimiter: bool = True,
    ngram_length: int = 4,
    num_hash_tables: int = 3,
    min_df: float = 0.0001,
    max_df: float = 0.1,
    min_similarity: float = 0.8,
) -> DataFrame:
    """
    Perform text fuzzy matching on a given text column of a dataframe by calculating jaccard similarity on ngrams using MinHash

    Params:
        data: Spark dataframe
            Dataframe with a column containing text to be matched against each other
        text_col: str, default = 'Text'
            name of a column in the dataframe containing text values to be fuzzy matched
        data_partitions:
            number of partitions to repartition the input dataframe
        include_word_delimiter: bool, default = True
            True if we want to add leading and trailing word delimiters (i.e. whitespaces for English) to the text
        ngram_length: int, default = 4
            Length of substrings on which we will calculate jaccard similarity (default = 4)
        num_hash_tables:int, default = 3
            Number of hash tables used by LSH. Increasing has table numbers will increase the accuracy but will also increase compute time.
        min_df: float, default = 0.0001
            min number of different text values an ngram must appear in to be included in the vocabulary
        max_df: float, default = 0.1
            max fraction of different text values an ngram must appear in to be included in the vocabulary
        min_similarity: float, default = 0.8
            Remove all text matching pairs with jacard similarity below this threshold

    Returns:
        matches: Spark dataframe
            Dataframe with schema [Source, Target, Similarity]. Note: output data is not mirrored (Source < Target)
    """
    # create char array from input text
    text_data = data.select(text_col).dropDuplicates()
    text_data = text_data.withColumn(
        "TextChars", __to_char_array_udf(text_col, F.lit(include_word_delimiter))
    )
    text_data = text_data.filter(F.size(F.col("TextChars")) >= ngram_length).cache()
    logger.info(f"text data count for : {text_data.count()}")

    # create ngrams
    ngram_featurizer = NGram(n=ngram_length, inputCol="TextChars", outputCol="Ngrams")
    text_data = ngram_featurizer.transform(text_data)

    # compute count vectorizers
    count_vectorizer = CountVectorizer(
        inputCol="Ngrams",
        outputCol="NgramCountVectors",
        minDF=min_df,
        maxDF=max_df,
    ).fit(text_data)
    text_data = count_vectorizer.transform(text_data)
    text_data = text_data.filter(__check_zero_vector_udf("NgramCountVectors"))

    # minhash
    lsh_model = MinHashLSH(
        inputCol="NgramCountVectors",
        outputCol="HashValues",
        numHashTables=num_hash_tables,
    ).fit(text_data)
    text_data = lsh_model.transform(text_data).repartition(data_partitions).cache()
    logger.info(f"Finished hashing - data count: {text_data.count()}")

    # compute fuzzy-matched text
    matches = lsh_model.approxSimilarityJoin(
        text_data, text_data, 1 - min_similarity
    ).filter(F.col("distCol") != 0)
    matches = matches.withColumnRenamed("distCol", "JaccardDistance")
    matches = matches.withColumn("Source", F.col("datasetA")[text_col])
    matches = matches.withColumn("Target", F.col("datasetB")[text_col])
    matches = matches.filter(F.col("Source") < F.col("Target"))
    matches = matches.drop(*["datasetA", "datasetB"])
    matches = matches.withColumn("Similarity", 1 - F.col("JaccardDistance")).drop(
        "JaccardDistance"
    )
    return matches


def __convert_to_char_array(
    text: str, include_word_delimiter: bool = True, word_delimiter: str = " "
) -> list:
    """
    Convert string to char arrays.

    Params:
        text: str
            Text to be converted to character array
        include_word_delimiter: bool, default = True
            True if we want to add leading and trailing word delimiters to reduce the effect of word ordering (default = True)
        word_delimiter: str, default = ' '
            Character to separate words (default = white space for English)

    Returns:
        array of characters
    """
    if include_word_delimiter:
        chars = [word_delimiter]
    else:
        chars = []
    words = text.split(word_delimiter)
    words = [word for word in words if word.strip() != ""]
    for word in words:
        chars.extend(list(word))
        chars.append(word_delimiter)
    if include_word_delimiter:
        return chars
    else:
        return chars[: -len(word_delimiter)]


def __check_zero_vector(num_vector: SparseVector):
    """
    Returns True if a vector has any non-zero element

    Params:
        num_vector: SparseVector
            Vector of numeric elements

    Returns:
        bool True if the vector has any non-zero element
    """
    return num_vector.numNonzeros() > 0


# UDFS
__to_char_array_udf = F.udf(__convert_to_char_array, ArrayType(StringType()))
__check_zero_vector_udf = F.udf(__check_zero_vector, BooleanType())
