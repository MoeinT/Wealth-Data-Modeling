import psycopg2
import configparser
from typing import Optional, List, Tuple

class PostgreSQLConnector:
    def __init__(self, user: str, password: str, host='localhost', port='5432'):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection = None
        self.cursor = None
        self.connect()

    def connect(self, db_name = "postgres") -> None:
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
                port=self.port
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
            

if __name__ == "__main__":

    config = configparser.ConfigParser()
    config.read("config.ini")

    connector = PostgreSQLConnector(
        user=config["postgresql"]["user"], 
        password=config["postgresql"]["password"]
    )

    for db_name in ["moeindb"]:
        if not connector.check_database_exists(db_name=db_name):
            connector.execute_query(query=f"CREATE DATABASE {db_name}")
    
    connector.switch_database(new_dbname="moeindb")