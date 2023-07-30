import configparser
import re
from typing import List, Optional, Tuple

import psycopg2
import sqlalchemy
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class PostgreSQLConnector:
    def __init__(self, user: str, password: str, host="localhost", port="5432"):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection = None
        self.cursor = None
        self.connect()

    def connect(self, db_name="postgres") -> None:
        """
        Establishes a connection to the PostgreSQL database and creates a cursor.

        The method sets autocommit to True to avoid transactions for specific operations.
        """
        try:
            self.connection = psycopg2.connect(
                dbname=db_name,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
            )
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
            print(f"Connected to the {db_name} database.")
        except psycopg2.Error as e:
            print(f"Error connecting to the {db_name} database: {e}")

    def execute_query(self, query: str, fetch: bool = False) -> Optional[List[Tuple]]:
        """
        Executes an SQL query on the PostgreSQL database.

        Args:
            query (str): The SQL query to be executed.
            fetch (bool, optional): Whether to fetch the results of the query. Defaults to False.
            is_create_database (bool, optional): Whether the query is for creating a new database.
                Defaults to False.

        Returns:
            Optional[List[Tuple]]: The fetched results if `fetch` is True; otherwise, None.
        """
        try:
            self.cursor.execute(query)
            if fetch:
                return self.cursor.fetchall()
        except psycopg2.Error as e:
            print(f"Error executing the query: {e}")
            return None

    def check_database_exists(self, db_name: str) -> bool:
        """Check if the specified database exists.

        Args:
            db_name (str): The name of the database to check.

        Returns:
            bool: True if the database exists, False otherwise.
        """

        check_query = f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'"
        result = self.execute_query(check_query, fetch=True)
        return bool(result)

    def switch_database(self, new_dbname: str) -> None:
        """
        Switch the active database.

        Args:
            new_dbname (str): The name of the database to switch to.
        """
        self.connect(db_name=new_dbname)

    def create_table(self, table_name: str, columns: List[Tuple[str, str]]) -> None:
        """
        Create a table within the database.

        Args:
            table_name (str): The name of the table to be created.
            columns (List[Tuple[str, str]]): A list of tuples containing column names and data types.

        """
        if not columns:
            print("No columns specified for table creation.")
            return

        column_defs = ",\n".join(
            [f"{col_name} {data_type}" for col_name, data_type in columns]
        )

        self.execute_query(
            f"CREATE TABLE IF NOT EXISTS {table_name} (\n{column_defs}\n);"
        )

    def close(self) -> None:
        """
        Closes the connection to the PostgreSQL database and the associated cursor.
        """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("Connection to the PostgreSQL database closed.")


class WealthDataProcessor:
    def __init__(
        self,
        spark: SparkSession,
        db_user: str,
        db_password: str,
        db_host: str,
        db_port: str,
        db_name: str,
        options: dict = None,
        data_file: str = None,
        series_file: str = None,
        country_file: str = None,
    ):
        self.spark = spark
        self.options = options if options else {"header": "true", "inferSchema": "true"}
        self.data_file = (
            data_file if data_file else "../datasets/Wealth-AccountData.csv"
        )
        self.series_file = (
            series_file if series_file else "../datasets/Wealth-AccountSeries.csv"
        )
        self.country_file = (
            country_file if country_file else "../datasets/Wealth-AccountsCountry.csv"
        )

        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name

    @staticmethod
    def clean_cols(df: DataFrame) -> DataFrame:
        """
        Clean column names of a DataFrame.

        Args:
            df (DataFrame): The DataFrame whose columns need to be cleaned.

        Returns:
            DataFrame: The DataFrame with cleaned column names.
        """
        # Fixing the year columns
        df = df.select(
            *[
                F.col(col).alias(re.sub(r"^(\d{4}) \[YR\1\]$", r"year_\1", col))
                for col in df.columns
            ]
        )
        # Removing spaces
        df = df.select(
            [
                F.col(column).alias(
                    re.sub(r"^[^a-zA-Z]+", "", "_".join(column.split(" ")).lower())
                    .replace("-", "_")
                    .strip()
                )
                for column in df.columns
            ]
        )

        return df

    def read_tables(self) -> Tuple[DataFrame, DataFrame, DataFrame]:
        """
        Read data from CSV files into DataFrames.

        Returns:
            Tuple[DataFrame, DataFrame, DataFrame]: A tuple containing DataFrames for account data, account series,
            and account country data.
        """
        df_account_data = (
            self.spark.read.options(**self.options).format("csv").load(self.data_file)
        )
        df_account_series = (
            self.spark.read.options(**self.options).format("csv").load(self.series_file)
        )
        df_account_country = (
            self.spark.read.options(**self.options)
            .format("csv")
            .load(self.country_file)
        )

        return df_account_data, df_account_series, df_account_country

    def process_data(self) -> Tuple[DataFrame, DataFrame, DataFrame]:
        """
        Process the loaded data.

        Returns:
            Tuple[DataFrame, DataFrame, DataFrame]: A tuple containing processed DataFrames for account data,
            account series, and account country data.
        """
        df_account_data, df_account_series, df_account_country = self.read_tables()

        df_account_data = (
            self.clean_cols(df_account_data)
            .withColumn("id", F.monotonically_increasing_id())
            .withColumn("series_code", F.trim(F.col("series_code")))
            .withColumn("country_code", F.trim(F.col("country_code")))
        )

        df_account_series = self.clean_cols(df_account_series).withColumn(
            "code", F.trim(F.col("code"))
        )

        df_account_country = (
            self.clean_cols(df_account_country)
            .withColumn("code", F.trim(F.col("code")))
            .dropna(thresh=5)
        )

        return df_account_data, df_account_series, df_account_country

    def write_to_postgres(self, df: DataFrame, table_name: str) -> None:
        """
        Write a DataFrame to a PostgreSQL table.

        Args:
            df (DataFrame): The DataFrame to be written to the table.
            table_name (str): The name of the table to write the DataFrame.

        """

        url = f"jdbc:postgresql://{self.db_host}:{self.db_port}/{self.db_name}"

        (
            df.write.format("jdbc")
            .option("url", url)
            .option("dbtable", table_name)
            .option("user", self.db_user)
            .option("password", self.db_password)
            .mode("append")
            .save()
        )


if __name__ == "__main__":
    # Get the db credentials
    config = configparser.ConfigParser()
    config.read("config.ini")

    user = config["postgresql"]["user"]
    password = config["postgresql"]["password"]
    dbname = config["postgresql"]["dbname"]
    port = 5432

    # Pass in the credentials and create a connection to the default db
    connector = PostgreSQLConnector(user=user, password=password)

    # Create new databases
    for db_name in ["moeindb"]:
        if not connector.check_database_exists(db_name=db_name):
            connector.execute_query(query=f"CREATE DATABASE {db_name}")

    # Switch the connection to a desired database
    connector.switch_database(new_dbname="moeindb")

    # Drop tables
    connector.execute_query(f"DROP TABLE IF EXISTS account_country CASCADE")
    connector.execute_query(f"DROP TABLE IF EXISTS account_data CASCADE")
    connector.execute_query(f"DROP TABLE IF EXISTS account_series CASCADE")

    # Creating the account country table
    account_country_columns = [
        ("code", "VARCHAR PRIMARY KEY"),
        ("long_name", "VARCHAR"),
        ("income_group", "VARCHAR"),
        ("region", "VARCHAR"),
        ("lending_category", "VARCHAR"),
        ("other_groups", "VARCHAR"),
        ("currency_unit", "VARCHAR"),
        ("latest_population_census", "VARCHAR"),
        ("latest_household_survey", "VARCHAR"),
        ("special_notes", "VARCHAR"),
        ("national_accounts_base_year", "VARCHAR"),
        ("national_accounts_reference_year", "VARCHAR"),
        ("system_of_national_accounts", "VARCHAR"),
        ("sna_price_valuation", "VARCHAR"),
        ("alternative_conversion_factor", "VARCHAR"),
        ("ppp_survey_years", "VARCHAR"),
        ("balance_of_payments_manual_in_use", "VARCHAR"),
        ("external_debt_reporting_status", "VARCHAR"),
        ("system_of_trade", "VARCHAR"),
        ("government_accounting_concept", "VARCHAR"),
        ("imf_data_dissemination_standard", "VARCHAR"),
        ("source_of_most_recent_income_and_expenditure_data", "VARCHAR"),
        ("vital_registration_complete", "VARCHAR"),
        ("latest_agricultural_census", "VARCHAR"),
        ("latest_industrial_data", "VARCHAR"),
        ("latest_trade_data", "VARCHAR"),
        ("latest_water_withdrawal_data", "VARCHAR"),
        ("alpha_code", "VARCHAR"),
        ("wb_2_code", "VARCHAR"),
        ("table_name", "VARCHAR"),
        ("short_name", "VARCHAR"),
    ]

    connector.create_table("account_country", account_country_columns)

    # Creating the account series table
    account_series_columns = [
        ("code", "VARCHAR PRIMARY KEY"),
        ("indicator_name", "VARCHAR"),
        ("long_definition", "VARCHAR"),
        ("source", "VARCHAR"),
        ("topic", "VARCHAR"),
        ("unit_of_measure", "VARCHAR"),
        ("periodicity", "VARCHAR"),
        ("reference_period", "VARCHAR"),
        ("statistical_concept_and_methodology", "VARCHAR"),
        ("previous_indicator_code", "VARCHAR"),
        ("previous_indicator_name", "VARCHAR"),
    ]

    connector.create_table("account_series", account_series_columns)

    # Create the account data table
    account_data_columns = [
        ("id", "SERIAL PRIMARY KEY"),
        ("country_name", "VARCHAR"),
        ("country_code", "VARCHAR REFERENCES account_country(code)"),
        ("series_name", "VARCHAR"),
        ("series_code", "VARCHAR REFERENCES account_series(code)"),
        ("year_1995", "VARCHAR"),
        ("year_1996", "VARCHAR"),
        ("year_1997", "VARCHAR"),
        ("year_1998", "VARCHAR"),
        ("year_1999", "VARCHAR"),
        ("year_2000", "VARCHAR"),
        ("year_2001", "VARCHAR"),
        ("year_2002", "VARCHAR"),
        ("year_2003", "VARCHAR"),
        ("year_2004", "VARCHAR"),
        ("year_2005", "VARCHAR"),
        ("year_2006", "VARCHAR"),
        ("year_2007", "VARCHAR"),
        ("year_2008", "VARCHAR"),
        ("year_2009", "VARCHAR"),
        ("year_2010", "VARCHAR"),
        ("year_2011", "VARCHAR"),
        ("year_2012", "VARCHAR"),
        ("year_2013", "VARCHAR"),
        ("year_2014", "VARCHAR"),
        ("year_2015", "VARCHAR"),
        ("year_2016", "VARCHAR"),
        ("year_2017", "VARCHAR"),
        ("year_2018", "VARCHAR"),
    ]

    # Create the PostgreSQL table using the specified columns and data types
    connector.create_table("account_data", account_data_columns)

    # Populate the tables
    spark = SparkSession.builder.appName("data-modelling").getOrCreate()

    obj = WealthDataProcessor(
        spark=spark,
        db_user=config["postgresql"]["user"],
        db_password=config["postgresql"]["password"],
        db_host="localhost",
        db_port="5432",
        db_name="moeindb",
    )

    # Getting the enriched data
    df_account_data, df_account_series, df_account_country = obj.process_data()

    # Writing the enriched data to Postresql
    obj.write_to_postgres(df=df_account_series, table_name="account_series")
    obj.write_to_postgres(df=df_account_country, table_name="account_country")
    obj.write_to_postgres(df=df_account_data, table_name="account_data")

    # Close the connection
    connector.close()
