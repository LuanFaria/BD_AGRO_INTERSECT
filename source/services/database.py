import warnings

import psycopg2
from sqlalchemy import create_engine, exc

import pandas as pd

warnings.filterwarnings("ignore")


class DataBase:
    def __init__(
            self,
            host: str,
            port: str,
            user: str,
            database: str,
            password: str) -> None:
        """
        Inicializa uma instância da classe DataBase.

        Parâmetros:
        - host (str): O endereço do host do banco de dados.
        - port (str): A porta para a conexão com o banco de dados.
        - user (str): O nome de usuário para autenticação.
        - database (str): O nome do banco de dados.
        - password (str): A senha para autenticação.
        """
        self.host = host
        self.port = port
        self.user = user
        self.database = database
        self.password = password

        self.connection = self.__connection()

    def __connection(self):
        """
        Método privado para estabelecer uma conexão com o banco de dados.

        Retorna:
        - psycopg2.connect: Objeto de conexão psycopg2.
        """
        try:
            return psycopg2.connect(
                database=self.database,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
        except psycopg2.Error as e:
            print(f"Error connecting to PostgreSQL: {e}")

    @staticmethod
    def close_connection(connection: psycopg2.connect) -> None:
        """
        Fecha a conexão com o banco de dados.

        Parâmetros:
        - connection (psycopg2.connect): Objeto de conexão a ser fechado.
        """
        if connection is not None:
            connection.close()

    def get_data_from_table(
            self,
            query: str,
            close_connection: bool = False) -> pd.DataFrame:
        """
        Obtém dados de uma tabela no banco de dados.

        Parâmetros:
        - table (str): Nome da tabela.
        - query (str): Consulta SQL personalizada (opcional).
        - schema (str): Esquema da tabela (padrão é 'public').
        - close_connection (bool): Indica se a conexão deve ser fechada após a
        consulta (padrão é False).

        Retorna:
        - pd.DataFrame: DataFrame contendo os dados da tabela.
        """
        try:
            data = pd.read_sql_query(query, con=self.connection)

            if close_connection:
                self.close_connection(self.connection)
            return data
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            self.close_connection(self.connection)

    def delete_rows(
            self,
            sql_delete_query: str,
            close_connection: bool = False) -> None:
        """
        Deleta linhas do banco de dados usando uma consulta SQL.

        Parâmetros:
        - sql_delete_query (str): Consulta SQL de exclusão.
        - delete_params (tuple): Parâmetros para a consulta SQL.
        - close_connection (bool): Indica se a conexão deve ser fechada após
        a exclusão (padrão é False).
        """
        cursor = self.connection.cursor()
        try:
            # Execute the query with parameters
            cursor.execute(sql_delete_query)

            deleted_rows = cursor.rowcount

            # Commit the changes to the database
            self.connection.commit()

            print(f"{deleted_rows} row(s) deleted successfully.")

            if close_connection:
                self.close_connection(self.connection)

        except psycopg2.Error as e:
            print(f"\nError deleting row: {e}")
            self.connection.rollback()
        finally:
            # Close the cursor
            cursor.close()

    def insert_dataframe_into_postgres(
            self,
            schema: str,
            table_name: str,
            dataframe: pd.DataFrame,
            close_connection: bool = False) -> bool:
        """
        Insere um DataFrame em uma tabela PostgreSQL.

        Parâmetros:
        - schema (str): Esquema da tabela.
        - table_name (str): Nome da tabela.
        - dataframe (pd.DataFrame): DataFrame a ser inserido.

        Retorna:
        - bool: True se a inserção for bem-sucedida, False caso contrário.
        """
        engine = self.__create_engine()
        select_query = f"SELECT COUNT(*) FROM {schema}.{table_name}"

        try:
            # Obtém o número de linhas antes da inserção
            rows_before_insert = pd.read_sql(select_query, engine).iloc[0, 0]

            # Insere o DataFrame na tabela PostgreSQL
            dataframe.to_sql(
                table_name,
                engine,
                if_exists='append',
                index=False,
                schema=schema)

            # Calcula o número de linhas
            rows_after_insert = pd.read_sql(select_query, engine).iloc[0, 0]

            # Calcula o número de linhas inseridas
            rows_inserted = rows_after_insert - rows_before_insert

            print(
                f"{rows_inserted} rows inserted successfully into "
                f"{table_name}.")

            if close_connection:
                # Fecha a conexão com o banco de dados
                engine.dispose()
            return True

        except exc.SQLAlchemyError as e:
            print(f"\nError inserting data into {table_name}: {e}")

    def __create_engine(self):
        """
        Cria e retorna um objeto de conexão SQLAlchemy.

        Retorna:
        - Engine: Objeto de conexão SQLAlchemy.
        """
        return create_engine(f"postgresql://"
                             f"{self.user}:"
                             f"{self.password}@"
                             f"{self.host}:"
                             f"{self.port}/"
                             f"{self.database}")

    def sql_query(
            self,
            query: str,
            close_connection: bool = False) -> pd.DataFrame:
        try:
            data = pd.read_sql_query(query, con=self.connection)

            if close_connection:
                self.close_connection(self.connection)

            return data
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            self.close_connection(self.connection)
            return pd.DataFrame()

    def execute_sql(
            self,
            sql_query: str,
            close_connection: bool = False) -> None:
        try:
            cursor = self.connection.cursor()

            # Executing the query
            cursor.execute(sql_query)

            # Commit your changes in the database
            self.connection.commit()

            if close_connection:
                self.close_connection(self.connection)

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            self.close_connection(self.connection)

    def insert(
            self,
            table: str,
            schema: str,
            columns: str,
            values: str,
            message: str = '') -> bool:
        """EXAMPLE: - INSERT INTO Customers (CustomerName, ContactName,
        Address, City, PostalCode, Country) VALUES ('Cardinal', 'Tom B.
        Erichsen', 'Skagen 21', 'Stavanger', '4006', 'Norway');
        """
        try:
            cursor = self.connection.cursor()
            # Insert single record now

            sql_update_query = (f"INSERT INTO {schema}.{table} "
                                f"{columns} VALUES {values};")
            # Executing the query
            cursor.execute(sql_update_query)

            # Commit your changes in the database
            self.connection.commit()

            if message:
                print(message)
            return True
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            self.close_connection(self.connection)
            return False
