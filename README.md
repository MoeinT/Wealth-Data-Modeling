# Data Modeling for Wealth Data using PySpark and PostgreSQL
Welcome to the repository for data modeling using Python and PySpark! This documentation will guide you through the components of this project and how to set up your environment to work with the codebase.

## Introduction
Data modeling is an essential process in the field of data analysis and database design. It involves structuring and organizing data in a way that facilitates efficient storage, retrieval, and analysis. By creating a blueprint for data representation, data modeling enables us to understand the relationships between different data elements and uncover valuable insights from complex datasets.

## Our Dataset - Wealth Data
For this learning journey, we will be working with wealth data, represented by three datasets: **Wealth-AccountData.csv**, **Wealth-AccountsCountry.csv**, and **Wealth-AccountSeries.csv**. These datasets contain information about wealth distribution across countries and various economic indicators.

## Understanding the Components
In data modeling, we deal with two fundamental components: **fact tables** and **dimension tables**. The fact table represents the core data, often containing numerical values (facts) and references to other tables. Dimension tables, on the other hand, provide context to the data in the fact table by storing descriptive attributes.

Our fact table, **Wealth-AccountData.csv**, will be complemented by two dimension tables: **Wealth-AccountsCountry.csv** and **Wealth-AccountSeries.csv**. We will establish relationships between these tables, forming the foundation of our data model. Here we can see a schematic representation of the data model:

<p align="center">
  <img width="550" height="300" src=./assets/DataModel.PNG>
</p>

## Technology Stack
To accomplish our data modeling journey, we will leverage the power of Python, PySpark, and PostgreSQL. Python provides us with a versatile and intuitive programming language, while PySpark empowers us to process large-scale data efficiently. PostgreSQL, a robust relational database management system, will be our storage solution to persist our modeled data.

## Database and Environment Setup
Before we dive into the data modeling process, let's ensure we have everything set up correctly to run the code smoothly.

1. Make sure you have Python 3.11 installed on your system.

2. Install Poetry (a Python package manager) version 1.5.1.

3. Clone the repository to your local machine.

4. Create a virtual environment (optional but recommended).

5. Install the required Python packages using Poetry. This will install all the necessary dependencies for the project.

6. Set up a PostgreSQL database on your local machine with appropriate credentials (username, password).

7. Configure the database connection details in the config.ini file.

## Code Overview
This section will provide an overview of the main components of the codebase.

### PostgreSQLConnector
The PostgreSQLConnector class handles the connection to the PostgreSQL database. It allows you to execute SQL queries and provides methods for creating, switching, and checking databases.

**Methods**
- connect: Establishes a connection to the PostgreSQL database and creates a cursor. The method sets autocommit to True to avoid transactions for specific operations.

- execute_query: Executes an SQL query on the PostgreSQL database.

- check_database_exists: Checks if the specified database exists.

- switch_database: Switches the connection to any other database

- create_table: Creates table within the connected database

- close: Closes the connection to the PostgreSQL database and the associated cursor.

### WealthDataProcessor
The WealthDataProcessor class is responsible for processing the wealth data and writing it to the PostgreSQL database. It uses PySpark to read, transform, and write the data.

**Methods:**
- clean_cols: Cleans the column names of a DataFrame to make them database-friendly.

- read_tables: Reads data from CSV files into DataFrames.

- process_data: Processes the loaded data, including cleaning column names, dropping null values, and adding a unique identifier to the fact table.

- write_to_postgres: Writes a DataFrame to a PostgreSQL table.

Note: The methods are documented with their input parameters and return types for better understanding and usage.

## CI/CD Pipeline
The repository is equipped with a CI/CD pipeline using GitHub Actions. The pipeline runs whenever a pull request is created to the main branch. The workflow performs the following checks:

Python Linting: It checks the code format using the black and isort tools to ensure consistent and properly formatted code.

## Workflow Details
The CI/CD pipeline is defined in the .github/workflows/main.yml file. It consists of the following jobs:

python-lint
This job runs on an **ubuntu-latest** runner and is responsible for checking the Python code formatting.

Steps:
1. checkout source repo: Checks out the source repository to the runner.

2. Setting up Python: Sets up the specified Python version.

3. Upgrading poetry: Upgrades the Poetry package manager to version 1.5.1.

4. Setting up poetry: Sets up Poetry for the project using the specified version.

5. Installing poetry: Installs the project dependencies using Poetry.

6. Checking formatting: Runs the black tool to check the code formatting without making changes.

7. Checking imports: Runs the isort tool to check the import order without making changes.

## Conclusion
With this documentation, you should have a clear understanding of the project's goals, data modeling process, and the CI/CD pipeline that ensures code quality before merging pull requests. Feel free to explore the code, run it on your local environment, and contribute to the project! If you have any questions or need assistance, don't hesitate to reach out to the repository maintainers. Happy coding!

## Contact 
- moin.torabi@gmail.com
- [LinkedIn](https://www.linkedin.com/in/moeintorabi/)