import warnings

import numpy as np
import pandas as pd
import geopandas as gpd

from source.helpers.gisFunctions import ReprojectGeometries

warnings.filterwarnings("ignore")


class IntersectNdvi:
    """
    Classe para manipulação e formatação de dados do NDVI Intersect.
    """
    def __init__(
            self,
            safra: int,
            intersect_ndvi: gpd.GeoDataFrame) -> None:
        """
        Inicializa a classe IntersectNdvi.

        Args:
            safra (int): Ano da safra.
            intersect_ndvi (gpd.GeoDataFrame): DataFrame contendo os dados do NDVI Intersect.
        """
        self.safra = safra
        self.intersect_ndvi = intersect_ndvi

    def ndvi_data(self) -> gpd.GeoDataFrame:
        """
        Formata e classifica os dados do NDVI Intersect.

        Returns:
            gpd.GeoDataFrame: DataFrame contendo os dados do NDVI Intersect formatados e classificados.
        """
        self.__format_columns()

        improved_contracts = ImproveContratos(
            self.intersect_ndvi).format_contratos()

        improved_estagios = ImproveEstagios(
            improved_contracts).format_estagios()

        ndvi_data_classified = JanelaDeColheita(
            improved_estagios).classify_janela_colheita()

        ndvi_data_classified = self.__classify_date_image(ndvi_data_classified)

        NdviDataValidation(ndvi_data_classified)

        return ndvi_data_classified

    def __check_columns(self) -> None:
        """
        Verifica se as colunas necessárias estão presentes no DataFrame do NDVI Intersect.

        Raises:
            ValueError: Se alguma coluna necessária estiver ausente.
        """
        columns_list = ['DESC_CANA', 'GRIDCODE', 'IDADE_IMG', 'DATA_IMG']

        for column in columns_list:
            if column not in self.intersect_ndvi.columns:
                raise ValueError(f"Coluna {column} não está no shapefile!")

    @staticmethod
    def __classify_date_image(ndvi_data: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Classifica a data da imagem com a maior área de NDVI.

        Args:
            ndvi_data (gpd.GeoDataFrame): DataFrame contendo os dados do NDVI Intersect.

        Returns:
            gpd.GeoDataFrame: DataFrame contendo os dados do NDVI Intersect com a data da imagem classificada.
        """
        grouped = ndvi_data.groupby('DATA_IMG')['AREA_NDVI'].sum()

        date_with_highest_sum_area = grouped.idxmax()

        ndvi_data['DATA_IMG'] = date_with_highest_sum_area

        return ndvi_data

    def __format_columns(self) -> None:
        """
        Formata as colunas do DataFrame do NDVI Intersect.
        """

        # Converte as colunas do tipo 'object' para 'string'
        object_columns = self.intersect_ndvi.select_dtypes(
            include='object').columns

        self.intersect_ndvi[object_columns] = (
            self.intersect_ndvi[object_columns].astype('string'))

        # Converte os nomes das colunas para maiúsculas e define a coluna 'geometry'
        self.intersect_ndvi.columns = [
            column.upper() for column in self.intersect_ndvi.columns]

        self.intersect_ndvi['geometry'] = self.intersect_ndvi['GEOMETRY']

        # Cria as colunas 'ESTAGIO', 'DATA_IMG' e 'DT_ULT_COR'
        self.__create_estagio_column()
        self.__create_dt_imagem_column()
        self.__create_dt_ult_corte_column()

        self.__check_columns()

    def __create_dt_imagem_column(self) -> None:
        """
        Cria a coluna 'DATA_IMG' se ela não existir e formata seus valores.
        """
        if 'DATA_IMG' not in self.intersect_ndvi.columns:
            list_name_columns = ['DT_IMAGEM', 'DT_IMG']

            column = [dt_image_column for dt_image_column in list_name_columns
                      if dt_image_column in self.intersect_ndvi.columns]

            if not column:
                raise ValueError(
                    "INTERSECT_NDVI não possui coluna de DATA_IMG")
            
            self.intersect_ndvi['DATA_IMG'] = self.intersect_ndvi[column[0]]


        self.intersect_ndvi = self.__format_date_column(
            self.intersect_ndvi, date_column='DATA_IMG')

    def __create_estagio_column(self) -> None:
        """
        Cria a coluna 'ESTAGIO' se ela não existir e formata seus valores.
        """
        estagio_ano_column = f'ESTAGIO_{str(self.safra)[-2:]}'

        if estagio_ano_column in self.intersect_ndvi.columns:
            self.intersect_ndvi['ESTAGIO'] = self.intersect_ndvi[
                estagio_ano_column]

        if 'ESTAGIO' not in self.intersect_ndvi.columns:
            raise ValueError("INTERSECT_NDVI não possui coluna de ESTAGIO")

        # Sim, algum corno manso colocou esse Â em
        # alguns arquivos de INTERSECT_NDVI
        self.intersect_ndvi['ESTAGIO'] = self.intersect_ndvi[
            'ESTAGIO'].str.replace('Â', '')

        self.intersect_ndvi['ESTAGIO'] = self.intersect_ndvi[
            'ESTAGIO'].str.replace(' ', '')

    def __create_dt_ult_corte_column(self) -> None:
        """
        Cria a coluna 'DT_ULT_COR' se ela não existir e formata seus valores.
        """
        if 'DT_ULT_COR' not in self.intersect_ndvi.columns:
            last_year = int(str(self.safra)[-2:]) - 1

            columns_dt_ult_corte = ['ULT_CORTE', f'CORTE_{str(last_year)}']

            dt_ult_corte = [dt_ult_corte for dt_ult_corte
                            in columns_dt_ult_corte
                            if dt_ult_corte in self.intersect_ndvi.columns]

            if not dt_ult_corte:
                raise ValueError(
                    "INTERSECT_NDVI não possui coluna de DT_ULT_COR")

            self.intersect_ndvi['DT_ULT_COR'] = self.intersect_ndvi[
                dt_ult_corte[0]]

        self.intersect_ndvi = self.__format_date_column(
            self.intersect_ndvi, date_column='DT_ULT_COR')
        
    #ALTEREI, ESTAVA TRANSFORMANDO A COLUNA GEOMETRY EM FORMATO STRING 

    # @staticmethod
    # def __format_date_column(
    #         geodataframe: gpd.GeoDataFrame,
    #         date_column: str) -> gpd.GeoDataFrame:
    #     """
    #     Formata a coluna de data.

    #     Args:
    #         geodataframe (gpd.GeoDataFrame): O DataFrame que contém a coluna de data a ser formatada.
    #         date_column (str): O nome da coluna de data.

    #     Returns:
    #         gpd.GeoDataFrame: O DataFrame com a coluna de data formatada.
    #     """
    #     geodataframe[date_column] = geodataframe[date_column].fillna(
    #         '1900-01-01')

    #     geodataframe.loc[
    #         (geodataframe[date_column] == '') |
    #         (geodataframe[date_column].isna()) |
    #         (geodataframe[date_column].isnull()) |
    #         (geodataframe[date_column] == '<NA>')] = '1900-01-01'

    #     geodataframe[date_column] = geodataframe[
    #         date_column].apply(
    #         lambda x: '1900-01-01'
    #         if pd.isnull(x)
    #         else pd.to_datetime(x).strftime('%Y-%m-%d'))

    #     return geodataframe

    @staticmethod
    def __format_date_column(
            geodataframe: gpd.GeoDataFrame,
            date_column: str) -> gpd.GeoDataFrame:
        """
        Formata a coluna de data.

        Args:
            geodataframe (gpd.GeoDataFrame): O DataFrame que contém a coluna de data a ser formatada.
            date_column (str): O nome da coluna de data.

        Returns:
            gpd.GeoDataFrame: O DataFrame com a coluna de data formatada.
        """
        # Preenche valores ausentes com '1900-01-01'
        geodataframe[date_column] = geodataframe[date_column].fillna('1900-01-01')

        # Corrige apenas a coluna de data, sem afetar outras colunas
        mask = (geodataframe[date_column] == '') | \
            (geodataframe[date_column].isna()) | \
            (geodataframe[date_column].isnull()) | \
            (geodataframe[date_column] == '<NA>')

        geodataframe.loc[mask, date_column] = '1900-01-01'

        # Converter a coluna para formato de data
        geodataframe[date_column] = pd.to_datetime(geodataframe[date_column], errors='coerce').dt.strftime('%Y-%m-%d')

        return geodataframe

class ImproveContratos:
    """
    Classe para melhorar os dados dos contratos.

    Attributes:
        geodataframe (gpd.GeoDataFrame): O GeoDataFrame contendo os dados dos contratos.
        proprias_values (list[str]): Lista de valores para identificar contratos próprios.
        fornecedores_values (list[str]): Lista de valores para identificar contratos de fornecedores.
    """
    def __init__(self, geodataframe: gpd.GeoDataFrame) -> None:
        """
        Construtor da classe ImproveContratos.

        Args:
            geodataframe (gpd.GeoDataFrame): O GeoDataFrame contendo os dados dos contratos.
        """
        self.geodataframe = geodataframe

        self.proprias_values: list[str] = [
            '1-SETOR A PROPRIA', 'Próprio', 'AGRICOLA CASE',
            '2-PARCERIA', 'PROPRIA', 'SUBPARCERIA', 'ARRENDAMENTO',
            'COMPRA DE CANA', 'ARRE'
        ]

        self.fornecedores_values: list[str] = [
            'Fornecedores', '3-FORNECEDOR',
            '6-FRANQUIA', 'FORNEC.C', 'FORNEC.S', 'SPOT']

    def format_contratos(self) -> gpd.GeoDataFrame:
        """
        Formata os contratos no GeoDataFrame.

        Returns:
            gpd.GeoDataFrame: O GeoDataFrame com os contratos formatados.
        """
        print('\nFormatando Contratos...')
        self.geodataframe.loc[self.geodataframe['TP_PROP'].str[0].astype(
            str) == 'P', 'TP_PROP'] = 'PRÓPRIAS'

        self.geodataframe.loc[self.geodataframe['TP_PROP'].isin(
            self.proprias_values), 'TP_PROP'] = 'PRÓPRIAS'

        self.geodataframe.loc[self.geodataframe['TP_PROP'].str[0].astype(
            str) == 'F', 'TP_PROP'] = 'FORNECEDORES'
        self.geodataframe.loc[self.geodataframe['TP_PROP'].isin(
            self.fornecedores_values), 'TP_PROP'] = 'FORNECEDORES'

        empty_rows_value = self.__get_empty_rows_value()

        self.geodataframe.loc[
            (self.geodataframe['TP_PROP'] == '') |
            (self.geodataframe['TP_PROP'].isna()) |
            (self.geodataframe[
                 'TP_PROP'].isnull()), 'TP_PROP'] = empty_rows_value

        return self.geodataframe

    def __get_empty_rows_value(self) -> str:
        """
        Obtém o valor das linhas vazias.

        Returns:
            str: O valor para preencher as linhas vazias.
        """
        options_contracts = self.geodataframe[
            'TP_PROP'].dropna().drop_duplicates().to_list()

        match len(options_contracts):
            case 1:
                return options_contracts[0]
            case _:
                return 'ADEF'


class ImproveEstagios:
    """
    Classe para classificar e formatar os estágios dos dados.

    Esta classe fornece métodos para classificar e formatar os estágios dos dados
    contidos em um GeoDataFrame.

    Attributes:
        geodataframe (gpd.GeoDataFrame): O GeoDataFrame contendo os dados dos estágios.
        rules_1c (list[str]): Lista de regras para classificação do estágio 1ºC.
        rules_2c (list[str]): Lista de regras para classificação do estágio 2ºC.
        rules_3c (list[str]): Lista de regras para classificação do estágio 3ºC.
        rules_4c (list[str]): Lista de regras para classificação do estágio 4ºC.
        rules_5c (list[str]): Lista de regras para classificação do estágio 5ºC.
        rules_plantio (list[str]): Lista de regras para classificação do estágio de plantio.
        rules_adef (list[str]): Lista de regras para classificação do estágio ADEF.
        rules_bis (list[str]): Lista de regras para classificação do estágio BIS.
    """
    def __init__(self, geodataframe: gpd.GeoDataFrame) -> None:
        """
        Inicializa a classe ImproveEstagios.

        Args:
            geodataframe (gpd.GeoDataFrame): O GeoDataFrame contendo os dados dos estágios.
        """
        self.geodataframe = geodataframe

        self.rules_1c = ['9M', '10M',
                         '11M', '12M',
                         '13M', '14M',
                         '15M', '16M',
                         '17M', '18M', '2VER']
        self.rules_2c = ['2ºC', '2°C', '2º', '2°', '2º CORTE', '2° CORTE']
        self.rules_3c = ['3ºC', '3°C', '3º', '3°', '3º CORTE', '3° CORTE']
        self.rules_4c = ['4ºC', '4°C', '4º', '4°', '4º CORTE', '4° CORTE']
        self.rules_5c = self.__create_5c_list_values()

        self.rules_plantio = ['12MF', '15MF', '18MF', '9MF']
        self.rules_adef = ['', 'EXP', 'POU', 'POUSIO', 'ADEF']
        self.rules_bis = ['BIS', 'BISADA', 'BISANOEMEIO', 'SOQUEIRA', 'SOQ']

    def format_estagios(self) -> gpd.GeoDataFrame:
        """
        Formata os estágios no GeoDataFrame.

        Este método classifica e formata os estágios presentes no GeoDataFrame,
        preenchendo a coluna 'ESTAGIO_D' com a classificação adequada.

        Returns:
            gpd.GeoDataFrame: O GeoDataFrame com os estágios formatados.
        """
        print('Classificando Estágios...')
        self.geodataframe['ESTAGIO_D'] = ''
        self.__classify_adef_ref_plantio()
        self.__classify_bis()
        self.__classify_cortes()

        return self.geodataframe

    @staticmethod
    def __create_5c_list_values() -> list[str]:
        """
        Cria a lista de valores para o estágio 5ºC+.

        Returns:
            list[str]: Lista de valores para o estágio 5ºC+.
        """
        estagios_5c = []

        [estagios_5c.append(f'{number}ºC') for number in range(5, 31)]
        [estagios_5c.append(f'{number}°C') for number in range(5, 31)]

        [estagios_5c.append(f'{number}º') for number in range(5, 31)]
        [estagios_5c.append(f'{number}°') for number in range(5, 31)]

        [estagios_5c.append(f'{number}ºCORTE') for number in range(5, 31)]
        [estagios_5c.append(f'{number}°CORTE') for number in range(5, 31)]

        [estagios_5c.append(f'{number}ºC+') for number in range(5, 31)]
        [estagios_5c.append(f'{number}°C+') for number in range(5, 31)]

        return estagios_5c

    def __classify_cortes(self) -> None:
        """
        Classifica os estágios de corte.

        Este método classifica os estágios de corte de acordo com as regras definidas
        e preenche a coluna 'ESTAGIO_D' com a classificação correspondente.
        """
        print('ESTAGIO: 1ºC')
        self.geodataframe.loc[self.geodataframe['ESTAGIO'].isin(
            self.rules_1c), 'ESTAGIO_D'] = '1ºC'

        print('ESTAGIO: 2ºC')
        self.geodataframe.loc[self.geodataframe['ESTAGIO'].isin(
            self.rules_2c), 'ESTAGIO_D'] = '2ºC'

        print('ESTAGIO: 3ºC')
        self.geodataframe.loc[self.geodataframe['ESTAGIO'].isin(
            self.rules_3c), 'ESTAGIO_D'] = '3ºC'

        print('ESTAGIO: 4ºC')
        self.geodataframe.loc[self.geodataframe['ESTAGIO'].isin(
            self.rules_4c), 'ESTAGIO_D'] = '4ºC'

        print('ESTAGIO: 5ºC')
        self.geodataframe.loc[
            (self.geodataframe['ESTAGIO'].isin(self.rules_5c)) |
            (self.geodataframe['ESTAGIO'].str[0].astype(str) == '>'),
            'ESTAGIO_D'] = '5ºC+'

    def __classify_adef_ref_plantio(self) -> None:
        """
        Classifica os estágios ADEF, REF e de plantio.

        Este método classifica os estágios ADEF, REF e de plantio de acordo com as regras
        definidas e preenche a coluna 'ESTAGIO_D' com a classificação correspondente.
        """
        print('ESTAGIO: ADEF')
        self.geodataframe.loc[
            (self.geodataframe['ESTAGIO'].isin(self.rules_adef)) |
            (self.geodataframe['ESTAGIO'].isna()) |
            (self.geodataframe['ESTAGIO'].isnull()), 'ESTAGIO_D'] = 'ADEF'

        print('ESTAGIO: PLANTIO')
        self.geodataframe.loc[self.geodataframe['ESTAGIO'].isin(
            self.rules_plantio), 'ESTAGIO_D'] = 'PLANTIO'

        print('ESTAGIO: REF')
        self.geodataframe.loc[
            self.geodataframe['ESTAGIO'] == 'REF', 'ESTAGIO_D'] = 'REF'

    def __classify_bis(self) -> None:
        """
        Classifica os estágios bis.

        Este método classifica os estágios bis de acordo com as regras definidas
        e preenche a coluna 'ESTAGIO_D' com a classificação correspondente.
        """
        print('ESTAGIO: BIS')
        self.geodataframe.loc[
            (self.geodataframe['ESTAGIO'].str[-1:].astype(str) == 'B') |
            (self.geodataframe['ESTAGIO'].str[-3:].astype(str) == 'BIS') |
            (self.geodataframe['ESTAGIO'].isin(self.rules_bis)), 'ESTAGIO_D'
        ] = 'BIS'


class JanelaDeColheita:
    """
    Classe para classificar a janela de colheita com base nos dados do NDVI.

    Esta classe fornece métodos para classificar a janela de colheita com base
    nos dados do NDVI contidos em um GeoDataFrame.

    Attributes:
        ndvi_geodataframe (gpd.GeoDataFrame): O GeoDataFrame contendo os dados do NDVI.
    """
    def __init__(self, ndvi_geodataframe: gpd.GeoDataFrame) -> None:
        """
        Inicializa a classe JanelaDeColheita.

        Args:
            ndvi_geodataframe (gpd.GeoDataFrame): O GeoDataFrame contendo os dados do NDVI.
        """
        self.ndvi_geodataframe = ndvi_geodataframe

    def classify_janela_colheita(self) -> gpd.GeoDataFrame:
        """
        Classifica a janela de colheita.

        Este método classifica a janela de colheita com base nos dados do NDVI e retorna
        um GeoDataFrame com os resultados.

        Returns:
            gpd.GeoDataFrame: O GeoDataFrame com os dados da janela de colheita classificados.
        """
        if 'JAN_COL' in self.ndvi_geodataframe.columns.tolist():
            print('Mantendo coluna JAN_COL existente')
        else:
            print('Gerando coluna JAN_COL a partir dos dados encontrados')
            self.__classificar_estagio()
            self.__classificar_cana_bis()
            self.__classificar_mes_corte()
            self.__classificar_idade_img()

        self.__format_gridcode()

        return ReprojectGeometries(self.ndvi_geodataframe).reproject()

    def __format_gridcode(self) -> None:
        """
        Formata os valores da coluna 'GRIDCODE'.

        Este método formata os valores da coluna 'GRIDCODE' no GeoDataFrame do NDVI
        para garantir que sejam inteiros de 64 bits.
        """
        self.ndvi_geodataframe['GRIDCODE'] = self.ndvi_geodataframe[
            'GRIDCODE'].astype(int)
        self.ndvi_geodataframe['GRIDCODE'] = self.ndvi_geodataframe[
            'GRIDCODE'].astype(str)
        self.ndvi_geodataframe['GRIDCODE'] = self.ndvi_geodataframe[
            'GRIDCODE'].astype(np.int64)

    def __classificar_estagio(self) -> None:
        """
        Classifica o estágio da janela de colheita.

        Este método classifica o estágio da janela de colheita com base nos dados
        do estágio presentes no GeoDataFrame do NDVI.
        """
        print('\nClassificando Janela de Colheita por ESTAGIO...')
        self.ndvi_geodataframe['JAN_COL'] = ''

        self.ndvi_geodataframe.loc[
            self.ndvi_geodataframe['ESTAGIO'] == '12M', 'JAN_COL'] = 'TARDIA'
        self.ndvi_geodataframe.loc[
            self.ndvi_geodataframe['ESTAGIO'] == '15M', 'JAN_COL'] = 'MÉDIA'
        self.ndvi_geodataframe.loc[
            self.ndvi_geodataframe['ESTAGIO'] == '18M', 'JAN_COL'] = 'INÍCIO'
        self.ndvi_geodataframe.loc[
            self.ndvi_geodataframe['ESTAGIO'] == '2VER', 'JAN_COL'] = 'INÍCIO'

    def __classificar_cana_bis(self) -> None:
        """
        Classifica o estágio BIS da janela de colheita.

        Este método classifica o estágio BIS da janela de colheita com base nos dados
        presentes no GeoDataFrame do NDVI.
        """
        print('\nClassificando BIS...')
        self.ndvi_geodataframe.loc[
            (self.ndvi_geodataframe['DESC_CANA'] == 'BIS') |
            (self.ndvi_geodataframe[
                 'DESC_CANA'] == 'BISADA'), 'JAN_COL'] = 'INÍCIO'

    def __classificar_mes_corte(self) -> None:
        """
        Classifica o estágio da janela de colheita com base no mês de corte.

        Este método classifica o estágio da janela de colheita com base no mês de corte
        das áreas presentes no GeoDataFrame do NDVI. Ele determina se o mês de corte
        representa o início, a média ou o final da janela de colheita e atribui essa
        classificação à coluna 'JAN_COL'.

        O método utiliza a coluna 'MES_CORTE' criada pelo método '__create_mes_corte_column'
        para determinar o mês de corte de cada área. Se a coluna 'JAN_COL' já estiver preenchida,
        a área será ignorada. Caso contrário, o mês de corte é verificado e a classificação é
        atribuída com base nos seguintes critérios:
        - Se o mês de corte estiver entre janeiro e junho, a janela de colheita é classificada
          como 'INÍCIO'.
        - Se o mês de corte estiver entre julho e agosto, a janela de colheita é classificada
          como 'MÉDIA'.
        - Se o mês de corte estiver entre setembro e dezembro, a janela de colheita é classificada
          como 'TARDIA'.
        """
        self.__create_mes_corte_column()

        print('Classificando Janela de Colheita por "MES_CORTE"..')
        self.ndvi_geodataframe.loc[
            (
                    (self.ndvi_geodataframe['JAN_COL'] == '') |
                    (self.ndvi_geodataframe['JAN_COL'].isnull()) |
                    (self.ndvi_geodataframe['JAN_COL'].str.len() == 0)
            ) & (self.ndvi_geodataframe['MES_CORTE'].isin(
                [1, 2, 3, 4, 5, 6])), 'JAN_COL'] = 'INÍCIO'

        self.ndvi_geodataframe.loc[
            (
                    (self.ndvi_geodataframe['JAN_COL'] == '') |
                    (self.ndvi_geodataframe['JAN_COL'].isnull()) |
                    (self.ndvi_geodataframe['JAN_COL'].str.len() == 0)
            ) & (self.ndvi_geodataframe['MES_CORTE'].isin(
                [7, 8])), 'JAN_COL'] = 'MÉDIA'

        self.ndvi_geodataframe.loc[
            (
                    (self.ndvi_geodataframe['JAN_COL'] == '') |
                    (self.ndvi_geodataframe['JAN_COL'].isnull()) |
                    (self.ndvi_geodataframe['JAN_COL'].str.len() == 0)
            ) & (self.ndvi_geodataframe['MES_CORTE'].isin(
                [9, 10, 11, 12])), 'JAN_COL'] = 'TARDIA'

    def __create_mes_corte_column(self) -> None:
        """
        Cria a coluna MES_CORTE para armazenar o mês de corte.

        Este método cria a coluna MES_CORTE no GeoDataFrame do NDVI para armazenar
        o mês de corte das áreas.
        """
        print('\nCriando coluna "MES_CORTE"...')
        self.ndvi_geodataframe['MES_CORTE'] = pd.to_datetime(
            self.ndvi_geodataframe['DT_ULT_COR']).dt.month.fillna(0)

        self.ndvi_geodataframe['MES_CORTE'] = self.ndvi_geodataframe[
            'MES_CORTE'].astype(int)

        self.ndvi_geodataframe.loc[
            self.ndvi_geodataframe['JAN_COL'].str.len() > 0, 'MES_CORTE'] = 0
        
    def __classificar_idade_img(self) -> None:
        """
        Classifica a idade da imagem da janela de colheita.

        Este método classifica a idade da imagem da janela de colheita com base nos dados
        presentes no GeoDataFrame do NDVI. Ele determina a idade da imagem e classifica a
        janela de colheita como início, média ou tardia com base nessa idade.

        O método verifica se a coluna 'JAN_COL' já está preenchida para evitar duplicações.
        Em seguida, verifica se o mês de corte é igual a zero, o que indica que a área ainda
        não foi cortada. Depois, avalia a idade da imagem e atribui a classificação de acordo
        com os seguintes critérios:
        - Se a idade da imagem for maior que 10 dias, a janela de colheita é classificada como
          'INÍCIO'.
        - Se a idade da imagem estiver entre 8 e 10 dias, a janela de colheita é classificada como
          'MÉDIA'.
        - Se a idade da imagem for menor ou igual a 7 dias, a janela de colheita é classificada como
          'TARDIA'.
        """
        print('\nClassificando Janela de Colheita por "IDADE_IMG"...')
        self.ndvi_geodataframe['IDADE_IMG'] = self.ndvi_geodataframe[
            'IDADE_IMG'].astype(int)

        self.ndvi_geodataframe.loc[
            (
                    (self.ndvi_geodataframe['JAN_COL'] == '') |
                    (self.ndvi_geodataframe['JAN_COL'].isnull()) |
                    (self.ndvi_geodataframe['JAN_COL'].str.len() == 0)
            ) &
            (self.ndvi_geodataframe['MES_CORTE'] == 0) &
            (self.ndvi_geodataframe['IDADE_IMG'] > 10), 'JAN_COL'] = 'INÍCIO'

        self.ndvi_geodataframe.loc[
            (
                    (self.ndvi_geodataframe['JAN_COL'] == '') |
                    (self.ndvi_geodataframe['JAN_COL'].isnull()) |
                    (self.ndvi_geodataframe['JAN_COL'].str.len() == 0)
            ) &
            (self.ndvi_geodataframe['MES_CORTE'] == 0) &
            (self.ndvi_geodataframe['IDADE_IMG'].isin(
                [8, 9, 10])), 'JAN_COL'] = 'MÉDIA'

        self.ndvi_geodataframe.loc[
            (
                    (self.ndvi_geodataframe['JAN_COL'] == '') |
                    (self.ndvi_geodataframe['JAN_COL'].isnull()) |
                    (self.ndvi_geodataframe['JAN_COL'].str.len() == 0)
            ) &
            (self.ndvi_geodataframe['MES_CORTE'] == 0) &
            (self.ndvi_geodataframe['IDADE_IMG'] <= 7), 'JAN_COL'] = 'TARDIA'


class NdviDataValidation:
    """
    Classe responsável pela validação dos campos de ESTAGIO_D, TP_PROP e GRIDCODE.
    """
    def __init__(self, ndvi_data: gpd.GeoDataFrame) -> None:
        """
        Inicializa a validação dos dados NDVI.

        Args:
            ndvi_data (gpd.GeoDataFrame): O GeoDataFrame contendo os dados NDVI.
        """
        self.ndvi_data = ndvi_data

        self.gridcode_values: list[int] = [1, 2, 3, 4, 5, 6]
        self.contratos_values: list[str] = ['PRÓPRIAS', 'FORNECEDORES', 'ADEF']

        self.estagios_values: list[str] = [
            '1ºC', '2ºC', '3ºC',
            '4ºC', '5ºC+', 'BIS',
            'ADEF', 'REF', 'PLANTIO'
        ]

        self.__validation()

    def __validation(self) -> None:
        """
        Realiza a validação dos dados NDVI.

        Verifica se os valores de "ESTAGIOS", "CONTRATOS" e "GRIDCODE" no GeoDataFrame
        NDVI estão corretos.
        """
        print('\nValidando "ESTAGIOS", "CONTRATOS" e "GRIDCODE"...')

        self.__estagios_validation()
        self.__gridcode_validation()
        self.__contratos_validation()

        print("Dados validados!")

        print(self.ndvi_data['TP_PROP'].drop_duplicates().to_list())
        print(self.ndvi_data['ESTAGIO_D'].drop_duplicates().to_list())

    def __contratos_validation(self) -> None:
        """
        Valida os valores da coluna "TP_PROP" no GeoDataFrame NDVI.

        Lança um ValueError se os valores de "TP_PROP" estiverem incorretos.
        """
        contratos_values = gpd.GeoDataFrame(self.ndvi_data.loc[~self.ndvi_data[
            'TP_PROP'].isin(self.contratos_values)])

        wrong_values = contratos_values[
            'TP_PROP'].drop_duplicates().to_list()

        if not contratos_values.empty:
            raise ValueError(
                f'\n\nArquivo INTERSECT_NDVI CONTÉM VALORES DE TP_PROP_D '
                f'INCORRETOS!\nValores Incorretos: {wrong_values}')

    def __estagios_validation(self) -> None:
        """
        Valida os valores da coluna "ESTAGIO_D" no GeoDataFrame NDVI.

        Lança um ValueError se os valores de "ESTAGIO_D" estiverem incorretos.
        """
        list_estagios = self.ndvi_data['ESTAGIO_D'].drop_duplicates().to_list()

        estagios_values = gpd.GeoDataFrame(self.ndvi_data.loc[~self.ndvi_data[
            'ESTAGIO_D'].isin(self.estagios_values)])

        wrong_values = estagios_values['ESTAGIO_D'].drop_duplicates().to_list()

        if not estagios_values.empty:
            raise ValueError(
                f'\n\nArquivo INTERSECT_NDVI CONTÉM VALORES DE ESTAGIO_D '
                f'INCORRETOS!\nValores Incorretos: {wrong_values}\n'
                f'TODOS OS ESTÁGIOS: {list_estagios}')

    def __gridcode_validation(self) -> None:
        """
        Valida os valores da coluna "GRIDCODE" no GeoDataFrame NDVI.

        Lança um ValueError se os valores de "GRIDCODE" estiverem incorretos.
        """
        gridcode_values = gpd.GeoDataFrame(self.ndvi_data.loc[~self.ndvi_data[
            'GRIDCODE'].isin(self.gridcode_values)])
        wrong_values = gridcode_values['GRIDCODE'].drop_duplicates().to_list()

        if not gridcode_values.empty:
            raise ValueError(
                f'\n\nArquivo INTERSECT_NDVI CONTÉM VALORES DE "GRIDCODE" '
                f'INCORRETOS!\nValores Incorretos: {wrong_values}')
