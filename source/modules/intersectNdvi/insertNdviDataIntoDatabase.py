import os
from pathlib import Path
from datetime import datetime

import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv

from source.helpers.gisFunctions import Shapefile
from source.services.database import DataBase
from .intersectNdviData import IntersectNdvi

load_dotenv(os.path.join(os.getcwd(), '.env'))


class InsertIntersectNdviIntoDatabase(DataBase):
    """
    Esta classe é responsável por inserir dados do NDVI Intersect no banco de dados.

    Ela herda funcionalidades básicas de um banco de dados da classe DataBase.
    """
    def __init__(
            self,
            safra_list: list[int],
            janela_list: list[str],
            clients_folder: str,
            clients_to_remove: list[int] = None,
            clients_id: list[int] | None = None,
            schema: str = 'powerbi',
            table: str = 'intersect_ndvi') -> None:
        """
        Inicializa a classe InsertIntersectNdviIntoDatabase.

        Args:
            safra_list (list[int]): Lista de anos de safra.
            janela_list (list[str]): Lista de janelas de tempo.
            clients_folder (str): Caminho para a pasta de clientes.
            clients_to_remove (list[int], opcional): Lista de IDs de clientes a serem removidos. Padrão é None.
            clients_id (list[int], opcional): Lista de IDs de clientes. Padrão é None.
            schema (str, opcional): Esquema do banco de dados. Padrão é 'powerbi'.
            table (str, opcional): Tabela onde os dados serão inseridos. Padrão é 'intersect_ndvi'.
        """
        super().__init__(
            host=os.getenv("HOST_RDS"),
            port=os.getenv("PORT_RDS"),
            user=os.getenv("USER_RDS"),
            database=os.getenv("DATABASE_RDS"),
            password=os.getenv("PASSWORD_RDS")
        )
        self.safra_list = safra_list
        self.janela_list = janela_list
        self.clients_folder = clients_folder

        self.schema = schema
        self.table = table
        self.clients_to_remove = clients_to_remove

        self.clients_data = self.get_data_from_table(
            query='SELECT * FROM public.clientes')

        self.clients_id = self.__get_clients_id_list(clients_id)

    def main(self) -> None:
        """
        Função principal para inserir dados do NDVI Intersect no banco de dados.
        """
        for client_id in self.clients_id:
            if client_id not in self.clients_to_remove:
                for safra in self.safra_list:
                    for janela in self.janela_list:
                        client_ndvi_data = (
                            self.__get_client_intersect_ndvi_summarized(
                                client_id, janela, safra))

                        if not client_ndvi_data.empty:
                            self.__insert_ndvi_data_into_database(
                                client_id=client_id,
                                janela=janela,
                                safra=safra,
                                client_ndvi_data=client_ndvi_data)

        self.close_connection(self.connection)

    def __get_clients_id_list(self, clients_id: list[int] | None) -> list[int]:
        """
        Obtém a lista de IDs de clientes.

        Args:
            clients_id (list[int]): Lista de IDs de clientes.

        Returns:
            list[int]: Lista de IDs de clientes.
        """
        if clients_id:
            return clients_id

        clients_data = self.clients_data.sort_values(by="id")
        return clients_data['id'].drop_duplicates().to_list()

    def __insert_ndvi_data_into_database(
            self,
            client_id: int,
            janela: str,
            client_ndvi_data: pd.DataFrame,
            safra: int) -> None:
        """
        Insere dados do NDVI Intersect no banco de dados.

        Args:
            client_id (int): ID do cliente.
            janela (str): Janela de tempo.
            client_ndvi_data (pd.DataFrame): DataFrame contendo os dados do NDVI Intersect do cliente.
            safra (int): Ano da safra.
        """
        client_complete_name = self.__get_client_data_interest(
            column='cliente', client_id=client_id)

        client_ndvi_data['client_id'] = client_id
        client_ndvi_data['client_name'] = client_complete_name

        intersect_ndvi = f'INTERSECT_NDVI_{janela}_{safra}'

        print(f'\nINSERINDO NO BANCO ----> {intersect_ndvi} '
              f'DO CLIENTE: "{client_complete_name}"')
        client_ndvi_data['data_img'] = pd.to_datetime(client_ndvi_data['data_img'], dayfirst=True)
        data_img = client_ndvi_data['data_img'].iloc[0]

        self.__delete_old_rows_from_database(
            data_img, client_id, janela, safra)

        self.insert_dataframe_into_postgres(
            schema=self.schema,
            table_name=self.table,
            dataframe=client_ndvi_data)

        print(f'\n------- {intersect_ndvi} DO CLIENTE: '
              f'"{client_complete_name}" INSERIDO NO BANCO -------')

    def __delete_old_rows_from_database(
            self,
            data_img: datetime,
            client_id: int,
            janela: str,
            safra: int) -> None:
        """
        Exclui linhas antigas do banco de dados.

        Args:
            data_img (datetime): Data da imagem.
            client_id (int): ID do cliente.
            janela (str): Janela de tempo.
            safra (int): Ano da safra.
        """
        sql_delete_query = (f"DELETE FROM {self.schema}.{self.table} "
                            f"WHERE client_id = {client_id} "
                            f"AND safra = {safra} "
                            f"AND janela = '{janela}' ")
        self.delete_rows(sql_delete_query=sql_delete_query)

    def __get_client_intersect_ndvi(
            self, client_id: int, janela: str, safra: int) -> gpd.GeoDataFrame:
        """
        Obtém os dados do NDVI Intersect para um cliente específico.

        Args:
            client_id (int): ID do cliente.
            janela (str): Janela de tempo.
            safra (int): Ano da safra.

        Returns:
            gpd.GeoDataFrame: DataFrame contendo os dados do NDVI Intersect do cliente.
        """
        client_complete_name = self.__get_client_data_interest(
            column='cliente', client_id=client_id)

        client_intersect_ndvi_folder = (
            self.__get_client_folder_intersect_ndvi_folder(client_id))

        if client_intersect_ndvi_folder:
            return GetClientIntersectNdviGeodataframe(
                safra=safra,
                janela=janela,
                client_id=client_id,
                client_name=client_complete_name,
                client_intersect_ndvi_folder=client_intersect_ndvi_folder
            ).ndvi_geodataframe()

        return gpd.GeoDataFrame()

    def __get_client_intersect_ndvi_summarized(
            self, client_id: int, janela: str, safra: int) -> pd.DataFrame:
        """
        Obtém os dados sumarizados do NDVI Intersect para um cliente específico.

        Args:
            client_id (int): O ID do cliente.
            janela (str): A janela de tempo dos dados do NDVI.
            safra (int): A safra dos dados do NDVI.

        Returns:
            pd.DataFrame: DataFrame sumarizado dos dados do NDVI Intersect para o cliente, se encontrado; caso contrário, um DataFrame vazio.
        """
        client_ndvi_data = self.__get_client_intersect_ndvi(
            client_id, janela, safra)

        if not client_ndvi_data.empty:
            return IntersectNdviSummarized(
                safra=safra,
                client_id=client_id,
                janela=janela,
                intersect_ndvi_data=client_ndvi_data
            ).ndvi_data_summarized()

        return client_ndvi_data

    def __get_client_data_interest(
            self, column: str, client_id: int) -> str | int:
        """
        Obtém informações específicas do cliente.

        Args:
            column (str): A coluna da qual deseja-se obter informações.
            client_id (int): O ID do cliente.

        Returns:
            Union[str, int]: O valor correspondente à coluna especificada para o cliente.
        """

        client_data = self.clients_data.loc[
            self.clients_data['id'] == client_id]

        if not client_data.empty:
            return client_data[column].iloc[0]

    def __get_client_folder_intersect_ndvi_folder(self, client_id: int) -> str:
        """
        Obtém a pasta do cliente com o NDVI intersect.

        Args:
            client_id (int): ID do cliente.

        Returns:
            str: Pasta do cliente com o NDVI intersect.
        """
        client_folder = [
            str(os.path.join(self.clients_folder, client_folder))
            for client_folder in os.listdir(self.clients_folder)
            if client_folder.split('_')[0] == f'{client_id}']

        if client_folder:
            return str(
                os.path.join(client_folder[0], '3_intersect_ndvi'))


class GetClientIntersectNdviGeodataframe:
    """
    Esta classe obtém o DataFrame Geoespacial do NDVI Intersect específico de um cliente.

    Ela permite acessar e processar os dados do NDVI Intersect para um cliente específico.
    """
    def __init__(
            self,
            safra: int,
            janela: str,
            client_id: int,
            client_name: str,
            client_intersect_ndvi_folder: str) -> None:
        """
        Inicializa a classe GetClientIntersectNdviGeodataframe.

        Args:
            safra (int): O ano da safra dos dados do NDVI.
            janela (str): A janela de tempo dos dados do NDVI.
            client_id (int): O ID do cliente.
            client_name (str): O nome do cliente.
            client_intersect_ndvi_folder (str): O caminho para a pasta contendo os dados do NDVI Intersect do cliente.
        """
        self.safra = safra
        self.janela = janela
        self.client_id = client_id
        self.client_name = client_name
        self.client_intersect_ndvi_folder = client_intersect_ndvi_folder

    def ndvi_geodataframe(self) -> gpd.GeoDataFrame:
        """
        Obtém o DataFrame Geoespacial do NDVI Intersect do cliente.

        Returns:
            gpd.GeoDataFrame: O DataFrame Geoespacial do NDVI Intersect do cliente, se encontrado; caso contrário, um GeoDataFrame vazio.
        """
        client_intersect_ndvi_folder = (
            self.__get_client_intersect_ndvi_shapefile())

        if client_intersect_ndvi_folder:

            print(f'Abrindo shapefile "{client_intersect_ndvi_folder}"')
            intersect_geodataframe = Shapefile(
                file=client_intersect_ndvi_folder).open()

            return IntersectNdvi(
                self.safra, intersect_geodataframe).ndvi_data()

        return gpd.GeoDataFrame()

    def __get_client_intersect_ndvi_shapefile(self) -> str:
        """
        Obtém o caminho do shapefile do NDVI Intersect do cliente.

        Returns:
            str: O caminho do shapefile do NDVI Intersect do cliente, se encontrado; caso contrário, uma string vazia.
        """
        print(f'\nProcurando INTERSECT_NDVI_{self.janela}_{self.safra} '
              f'do cliente: "{self.client_name}"')

        shapefile_intersect_ndvi = [
            str(os.path.join(self.client_intersect_ndvi_folder, shapefile))
            for shapefile in os.listdir(self.client_intersect_ndvi_folder)
            if shapefile.endswith('.shp')
            and shapefile[:14] == 'INTERSECT_NDVI'
            and str(Path(shapefile).stem)[-7:] == f'{self.janela}_{self.safra}'
        ]

        if shapefile_intersect_ndvi:
            return shapefile_intersect_ndvi[0]

        print(
            f'Arquivo INTERSECT_NDVI_{self.janela}_{self.safra} '
            f'do cliente: {self.client_name} não foi encontrado na pasta: '
            f'{self.client_intersect_ndvi_folder}')

        return ''


class IntersectNdviSummarized:
    """
    Classe para sumarizar os dados do NDVI Intersect.

    Attributes:
        safra (int): A safra dos dados do NDVI.
        janela (str): A janela de tempo dos dados do NDVI.
        client_id (int): O ID do cliente associado aos dados.
        intersect_ndvi_data (gpd.GeoDataFrame): O DataFrame Geoespacial dos dados do NDVI Intersect.
    """
    def __init__(
            self,
            safra: int,
            janela: str,
            client_id: int,
            intersect_ndvi_data: gpd.GeoDataFrame) -> None:
        """
        Inicializa a classe IntersectNdviSummarized.

        Args:
            safra (int): A safra dos dados do NDVI.
            janela (str): A janela de tempo dos dados do NDVI.
            client_id (int): O ID do cliente associado aos dados.
            intersect_ndvi_data (gpd.GeoDataFrame): O DataFrame Geoespacial dos dados do NDVI Intersect.
        """
        self.safra = safra
        self.janela = janela
        self.client_id = str(client_id)
        self.intersect_ndvi_data = intersect_ndvi_data

    def ndvi_data_summarized(self) -> pd.DataFrame:
        """
        Sumariza os dados do NDVI Intersect.

        Returns:
            pd.DataFrame: DataFrame sumarizado dos dados do NDVI Intersect.
        """
        self.__format_columns()

        self.__create_chave_column()
        self.__groupby_intersect_ndvi_data_by_chave()

        self.intersect_ndvi_data.reset_index(drop=True)

        self.__groupby_data_final()

        return self.intersect_ndvi_data

    def __format_columns(self) -> None:
        """
        Formata as colunas do DataFrame.

        Esta função renomeia e ajusta as colunas do DataFrame dos dados do NDVI Intersect
        para melhorar a consistência e legibilidade dos dados.
        """
        self.intersect_ndvi_data['SAFRA'] = self.safra
        self.intersect_ndvi_data['JANELA'] = self.janela

        self.intersect_ndvi_data['SAFRA'] = self.intersect_ndvi_data[
            'SAFRA'].astype(str)

        self.intersect_ndvi_data['ESTAGIO'] = self.intersect_ndvi_data[
            'ESTAGIO_D']

        self.intersect_ndvi_data.columns = [
            column.lower() for column in self.intersect_ndvi_data.columns]

    def __create_chave_column(self) -> None:
        """
        Cria a coluna 'chave'.

        Esta função cria uma nova coluna no DataFrame dos dados do NDVI Intersect
        que serve como uma chave única para identificar e agrupar os dados durante
        o processo de sumarização.
        """
        print('\nCriando coluna "chave"...')
        self.intersect_ndvi_data['chave'] = (
                self.intersect_ndvi_data['fazenda'].astype(str) + '_' +
                self.intersect_ndvi_data['gridcode'].astype(str) + '_' +
                self.intersect_ndvi_data['estagio'].astype(str) + '_' +
                self.intersect_ndvi_data['tp_prop'].astype(str) + '_' +
                self.intersect_ndvi_data['variedade'].astype(str) + '_' +
                self.intersect_ndvi_data['jan_col'].astype(str) + '_' +
                self.intersect_ndvi_data['safra'].astype(str) + '_' +
                self.intersect_ndvi_data['janela'].astype(str))

    def __groupby_intersect_ndvi_data_by_chave(self) -> None:
        """
        Agrupa os dados do NDVI Intersect pela chave.

        Esta função agrupa os dados do NDVI Intersect pelo valor da chave
        e calcula a soma da área do NDVI para cada chave.
        """
        print('Agrupando por chave e somando area_ndvi...')

        self.intersect_ndvi_data = self.intersect_ndvi_data.groupby(
            ['chave']).agg(
            {
                'safra': 'first',
                'fazenda': 'first',
                'gridcode': 'first',
                'estagio': 'first',
                'tp_prop': 'first',
                'variedade': 'first',
                'jan_col': 'first',
                'janela': 'first',
                'area_ndvi': 'sum',
                'data_img': 'first'
            }
        ).reset_index()

    def __groupby_data_final(self) -> None:
        """
        Agrupa os dados finais.

        Esta função agrupa os dados finais do NDVI Intersect e calcula a soma
        da área do NDVI para cada conjunto de características relevantes.
        """
        self.__create_chave_final_column()

        list_columns = ['chave', 'safra', 'janela', 'estagio',
                        'jan_col', 'gridcode', 'tp_prop', 'data_img']

        self.intersect_ndvi_data = self.intersect_ndvi_data.groupby(
            list_columns, as_index=False).agg({'area_ndvi': 'sum'})

    def __create_chave_final_column(self) -> None:
        """
        Cria a coluna 'chave' final.

        Esta função cria uma chave final que inclui informações adicionais
        como o ID do cliente, a safra e a janela de tempo.
        """
        print('Criando chave final...')
        self.intersect_ndvi_data['safra'] = self.intersect_ndvi_data[
            'safra'].astype(int)

        self.intersect_ndvi_data['chave'] = (
                self.client_id + '_' +
                self.intersect_ndvi_data['safra'].astype(str) + '_' +
                self.intersect_ndvi_data['janela'].astype(str) + '_' +
                self.intersect_ndvi_data['estagio'].astype(str) + '_' +
                self.intersect_ndvi_data['jan_col'].astype(str) + '_' +
                self.intersect_ndvi_data['gridcode'].astype(str))
