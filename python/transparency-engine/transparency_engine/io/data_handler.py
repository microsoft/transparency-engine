#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project.
#


import logging
import os

from enum import Enum
from typing import Callable, Dict, Union

from dataclasses import dataclass
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from transparency_engine.spark.utils import spark


logger = logging.getLogger(__name__)


class DataHandlerModes(str, Enum):
    """
    Enum for different I/O modes supported by Transparency Engine.
    """

    REDIS = "redis"
    HIVE = "hive"
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"

    @staticmethod
    def from_string(mode: str):
        """
        Converts a string to a DataHandlerModes enum.

        Parameters
        ----------
        mode : str
            The string to convert.

        Returns
        -------
        DataHandlerModes
            The DataHandlerModes enum.
        """
        if mode == "redis":
            return DataHandlerModes.REDIS
        elif mode == "hive":
            return DataHandlerModes.HIVE
        elif mode == "csv":
            return DataHandlerModes.CSV
        elif mode == "json":
            return DataHandlerModes.JSON
        elif mode == "parquet":
            return DataHandlerModes.PARQUET
        else:
            raise ValueError(f"Invalid mode: {mode}")


@dataclass
class DataHandlerConfig:
    delimiter: str = ","
    encoding: str = "UTF-8"

    @staticmethod
    def from_dict(config: dict) -> "DataHandlerConfig":
        """
        Converts a dict to a DataHandlerConfig object.

        Parameters
        ----------
        config : dict
            The dict to convert.

        Returns
        -------
        DataHandlerConfig
            The DataHandlerConfig object.
        """
        return DataHandlerConfig(**config)

    @staticmethod
    def empty() -> "DataHandlerConfig":
        """
        Returns an empty DataHandlerConfig object.

        Returns
        -------
        DataHandlerConfig
            The empty DataHandlerConfig object.
        """
        return DataHandlerConfig()


class DataHandler:
    """
    Data Handler class for I/O operations.
    Works as a Factory Method for different I/O modes.Supports Redis and Hive modes.
    """

    mode: DataHandlerModes
    storage_root: str
    config: DataHandlerConfig

    def __init__(
        self,
        mode: DataHandlerModes = DataHandlerModes.HIVE,
        storage_root: str = "",
        config: DataHandlerConfig = DataHandlerConfig.empty(),
    ):
        self.mode = mode
        self.storage_root = storage_root
        self.config = config

        self._init_storage()

    def _init_storage(self) -> None:
        """
        Initializes the storage for the given mode.

        Parameters
        ----------
        mode : DataHandlerModes
            The mode to initialize the storage for.
        storage_root : str
            The root path or db to use for the storage.
        """

        if self.storage_root:
            if self.mode in (DataHandlerModes.HIVE, DataHandlerModes.REDIS):
                spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.storage_root}")
            elif self.mode in (
                DataHandlerModes.CSV,
                DataHandlerModes.JSON,
                DataHandlerModes.PARQUET,
            ):
                if not os.path.exists(self.storage_root):
                    os.makedirs(self.storage_root)
            else:
                logger.warning(f"Invalid mode: {self.mode}")

    def get_mode(self) -> DataHandlerModes:
        """
        Gets the DataHandlerModes enum.

        Returns
        -------
        DataHandlerModes
            The DataHandlerModes enum.
        """
        return self.mode

    def load_data(
        self,
        input_name: str,
        mode: Union[DataHandlerModes, None] = None,
        schema: Union[StructType, None] = None,
    ) -> DataFrame:
        """
        Loads data from the given path and mode.

        Parameters
        ----------
        input_name : str
            The input file or table name to load the data from. Gets concatenated with the Handler's storage root
            eg: "database."+"input_name", "file_path."+"input_name"
        mode : DataHandlerModes, optional
            The mode to use for loading the data, by default DataHandlerModes.HIVE

        Returns
        -------
        DataFrame
            The loaded data.
        """
        data_loader = get_data_loader(mode or self.mode)
        if self.mode in (DataHandlerModes.HIVE, DataHandlerModes.REDIS):
            return data_loader(f"{self.storage_root}.{input_name}", self.config, schema)
        else:
            return data_loader(f"{self.storage_root}{input_name}", self.config, schema)

    def write_data(
        self,
        data: DataFrame,
        output_name: str,
        mode: Union[DataHandlerModes, None] = None,
    ) -> None:
        """
        Writes data to the given path and mode.

        Parameters
        ----------
        data : DataFrame
            The data to write.
        output_name : str
            The file or table name to write the data to. Gets concatenated with the Handler's storage root
            eg: "database."+"input_name", "file_path."+"input_name"
        mode : DataHandlerModes, optional
            The mode to use for writing the data, by default DataHandlerModes.HIVE
        """
        data_writer = get_data_writer(mode or self.mode)
        if self.mode in (DataHandlerModes.HIVE, DataHandlerModes.REDIS):
            return data_writer(data, f"{self.storage_root}.{output_name}", self.config)
        else:
            return data_writer(data, f"{self.storage_root}{output_name}", self.config)


def get_data_loader(
    mode: DataHandlerModes = DataHandlerModes.HIVE,
) -> Callable[[str, DataHandlerConfig, Union[StructType, None]], DataFrame]:
    """
    Gets the data loader function for the given mode.

    Parameters
    ----------
    mode : DataHandlerModes, optional
        The mode to use for loading the data, by default DataHandlerModes.HIVE

    Returns
    -------
    Callable[[str], DataFrame]
        The data loader function.
    """
    if mode == DataHandlerModes.REDIS:
        return _load_redis_data
    elif mode == DataHandlerModes.HIVE:
        return _load_hive_data
    elif mode == DataHandlerModes.CSV:
        return _load_csv_data
    elif mode == DataHandlerModes.JSON:
        return _load_json_data
    elif mode == DataHandlerModes.PARQUET:
        return _load_parquet_data
    else:
        raise ValueError("Invalid mode")


def get_data_writer(
    mode: DataHandlerModes = DataHandlerModes.HIVE,
) -> Callable[[DataFrame, str, DataHandlerConfig], None]:
    """
    Gets the data writer function for the given mode.

    Parameters
    ----------
    mode : DataHandlerModes, optional
        The mode to use for writing data, by default DataHandlerModes.HIVE

    Returns
    -------
    Callable[[DataFrame, str], None]
        The data writer function.
    """
    if mode == DataHandlerModes.REDIS:
        return _write_redis_data
    elif mode == DataHandlerModes.HIVE:
        return _write_hive_data
    elif mode == DataHandlerModes.CSV:
        return _write_csv_data
    elif mode == DataHandlerModes.JSON:
        return _write_json_data
    elif mode == DataHandlerModes.PARQUET:
        return _write_parquet_data
    else:
        raise ValueError("Invalid mode")


def _load_redis_data(
    data_path: str, config: DataHandlerConfig, schema: Union[StructType, None]
) -> DataFrame:
    # TODO: Do we still need Redis? Check with Monica once the backend is in place
    raise NotImplementedError


def _load_hive_data(
    table_name: str, _: DataHandlerConfig, schema: Union[StructType, None]
) -> DataFrame:
    """
    Load a Spark DataFrame from a given table name in hive

    Parameters
    ----------
    table_name : str
        The name of the table to load the data from.

    schema: StructType, optional
        The schema to use for the DataFrame

    Returns
    -------
    DataFrame
        The loaded DataFrame.
    """
    # Use read instead of select to avoid code injection
    if schema is not None:
        return spark.read.schema(schema).table(table_name)
    else:
        return spark.read.table(table_name)


def _load_csv_data(
    data_path: str, config: DataHandlerConfig, schema: Union[StructType, None]
) -> DataFrame:
    """
    Load a Spark DataFrame from a given csv file. Schema is infered if None is provided

    Parameters
    ----------
    data_path : str
        The path to the csv file load the data from.

    schema: StructType, optional
        The schema to use for the DataFrame

    Returns
    -------
    DataFrame
        The loaded DataFrame.
    """

    options: Dict[str, str] = {
        "delimiter": config.delimiter,
        "encoding": config.encoding,
        "header": "true",
        "multiline": "true",
    }
    if schema is not None:
        return spark.read.options(**options).csv(data_path, schema=schema)
    else:
        return spark.read.options(**options).csv(data_path)


def _load_json_data(
    data_path: str, config: DataHandlerConfig, schema: Union[StructType, None]
) -> DataFrame:
    """
    Load a Spark DataFrame from a given json file. Schema is infered if None is provided

    Parameters
    ----------
    data_path : str
        The path to the json file to load the data from.

    schema: StructType, optional
        The schema to use for the DataFrame

    Returns
    -------
    DataFrame
        The loaded DataFrame.
    """

    options: Dict[str, str] = {"encoding": config.encoding}
    if schema is not None:
        return spark.read.options(**options).schema(schema).json(data_path)
    else:
        return spark.read.options(**options).json(data_path)


def _load_parquet_data(
    data_path: str, config: DataHandlerConfig, schema: Union[StructType, None]
) -> DataFrame:
    """
    Load a Spark DataFrame from a given json file. Schema is infered if None is provided

    Parameters
    ----------
    data_path : str
        The path to the json file to load the data from.

    schema: StructType, optional
        The schema to use for the DataFrame

    Returns
    -------
    DataFrame
        The loaded DataFrame.
    """

    options: Dict[str, str] = {"encoding": config.encoding}
    if schema is not None:
        return spark.read.options(**options).schema(schema).parquet(data_path)
    else:
        return spark.read.options(**options).parquet(data_path)


def _write_redis_data(
    data: DataFrame, output_path: str, config: DataHandlerConfig
) -> None:
    # TODO: Do we still need Redis? Check with Monica once the backend is in place
    raise NotImplementedError


def _write_hive_data(data: DataFrame, table_name: str, _: DataHandlerConfig) -> None:
    """
    Writes a DataFrame to Hive storage

    Parameters
    ----------
    data : DataFrame
        The DataFrame to write.

    table_name : str
        The name of the table to write the data to.

    Returns
    -------
    None
    """
    data.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        table_name
    )


def _write_csv_data(
    data: DataFrame, output_path: str, config: DataHandlerConfig
) -> None:
    """
    Writes a DataFrame to a csv in the give path

    Parameters
    ----------
    data : DataFrame
        The DataFrame to write.

    output_path: str
        The path to write the data to.

    Returns
    -------
    None
    """
    data.write.mode("Overwrite").option("delimiter", config.delimiter).option(
        "encoding", config.encoding
    ).csv(output_path)


def _write_json_data(
    data: DataFrame, output_path: str, config: DataHandlerConfig
) -> None:
    """
    Writes a DataFrame to a json in the give path

    Parameters
    ----------
    data : DataFrame
        The DataFrame to write.

    output_path: str
        The path to write the data to.

    Returns
    -------
    None
    """
    data.write.mode("Overwrite").option("encoding", config.encoding).json(output_path)


def _write_parquet_data(
    data: DataFrame, output_path: str, config: DataHandlerConfig
) -> None:
    """
    Writes a DataFrame to a parquet file in the give path

    Parameters
    ----------
    data : DataFrame
        The DataFrame to write.

    output_path: str
        The path to write the data to.

    Returns
    -------
    None
    """
    data.write.mode("Overwrite").option("encoding", config.encoding).parquet(
        output_path
    )
