import utm
import warnings

import geopandas as gpd

from shapely.wkt import loads
from shapely.validation import make_valid

warnings.filterwarnings("ignore")


class Shapefile:
    def __init__(self, file: str, dissolve: bool = False) -> None:
        """Classe Shapefile realiza operações de manipulação de um arquivo shp.
        Params:
            - file(str): Arquivo .shp de interesse.
            - dissolve(bool): Se True, realiza o dissolve do shapefile
            (default False).
        """
        self.file = file
        self.dissolve = dissolve

    def open(self) -> gpd.GeoDataFrame:
        """Função responsável por abrir o shapefile e realizar a operação
        desejada.
        Returns:
        - GeoDataFrame
        """
        geodataframe = self.__open_shapefile()
        if self.dissolve:
            geodataframe = Dissolve(geodataframe).dissolve_geodataframe()

        return geodataframe

    def __open_shapefile(self) -> gpd.GeoDataFrame:
        """Realiza a operação de abrir um shapefile.
        Returns:
                GeoDataFrame
        """
        return gpd.read_file(self.file)

    def __open_and_dissolve_shapefile(self) -> gpd.GeoDataFrame:
        """Realiza a operação de abrir e dissolver um shapefile.

        Returns:
            GeoDataFrame
        """
        geodataframe = self.__open_shapefile()

        return Dissolve(geodataframe).dissolve_geodataframe()


class ReprojectGeometries:
    def __init__(
            self, geodataframe: gpd.GeoDataFrame, to: str = '4326') -> None:
        """Classe de Reprojected de geometrias

        Params: - geodataframe(gpd. Geodataframe): geodataframe com
        geometrias a serem reprojetadas. - to(str): Parâmetro para
        selecionar a projeção
        """
        self.to = to
        self.geodataframe = geodataframe

    def reproject(self) -> gpd.GeoDataFrame:
        """Reprojeta a geometria a projeção escolhida no parâmetro 'to'.
        Return:
            - GeoDataFrame
        """

        match self.to:
            case 'utm':
                return self.__reproject_to_utm()

            case _:
                return self.__reproject()

    def __reproject_to_utm(self) -> gpd.GeoDataFrame:
        """Reprojeta a geometria para EPSG UTM.

        Returns:
            - GeoDataFrame.
        """
        for idx, row in self.geodataframe.iterrows():
            c = row.geometry.centroid
            utm_x, utm_y, band, zone = utm.from_latlon(c.y, c.x)

            if c.y < 0:  # Northern zone
                epsg = 32700 + band
                try:
                    self.geodataframe = self.geodataframe.to_crs(epsg=epsg)
                except Exception as e:
                    print(f"Erro ao reprojetar, setando projeção: {e}")
                    self.geodataframe.crs = f"EPSG:{epsg}"

        return gpd.GeoDataFrame(self.geodataframe)

    def __reproject(self) -> gpd.GeoDataFrame:
        """Reprojeta a geometria para EPSG 4326.
        Returns:
            - GeoDataFrame
        """
        try:
            self.geodataframe = self.geodataframe.to_crs(epsg=int(self.to))
        except Exception as e:
            print(f"Erro ao reprojetar, setando projeção: {e}")
            self.geodataframe.crs = f"EPSG:{self.to}"

        return gpd.GeoDataFrame(self.geodataframe)


class MakeValidGeometries:
    def __init__(self, geodataframe: gpd.GeoDataFrame) -> None:
        """Essa função realiza as operações de repair geometries e retorna
        uma geometria válida.

        Params:
            - geodataframe (gpd. GeoDataFrame): GeoDataFrame de interesse.
        """
        self.geoM_type_list: list = ['Polygon',
                                     'MultiPolygon',
                                     'GeometryCollection']

        self.geodataframe = gpd.GeoDataFrame(
            geodataframe.loc[
                (~geodataframe.is_empty) &
                (geodataframe['geometry'].geom_type.isin(self.geoM_type_list))
                ]
        )

    def improve_geometry(self) -> gpd.GeoDataFrame:
        """Essa função retorna o geodataframe com geometrias válidas
        Returns:
            GeoDataFrame
        """
        new_geodataframe = self.__improve_geometry_collection()

        return self.__make_valid(new_geodataframe)

    def __improve_geometry_collection(self) -> gpd.GeoDataFrame:
        """Essa função percorre o geodataframe, e se houver geometry
        collection é gerado um novo GeoDataFrame apenas com geometrias dos
        tipos Polygon, MultiPolygon, GeometryCollection Returns: GeoDataFrame.
        """
        # Looping over all polygons
        for index, row in self.geodataframe.iterrows():
            if row['geometry'].geom_type == 'GeometryCollection':
                new_geometry = ImproveGeometryCollections(
                    row_geometry=row[
                        'geometry']).geometry_collection_to_multipolygon()

                self.geodataframe.loc[[index], 'geometry'] = new_geometry

        return self.geodataframe

    @staticmethod
    def __make_valid(geodataframe: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Essa função percorre o geodataframe, verifica se a geometria da
        row é válida, se não for, é aplicado o método make_valid do shapely
        para gerar uma geometria válida."""

        # Looping over all polygons
        for index, row in geodataframe.iterrows():
            if not row['geometry'].is_valid:
                # Repair broken geometries

                fix = make_valid(row['geometry'])
                try:
                    geodataframe.loc[[index], 'geometry'] = fix
                except Exception as _:
                    geodataframe.loc[[index], 'geometry'] = geodataframe.loc[
                        [index], 'geometry'
                    ].buffer(0)
        geodataframe['geometry'] = geodataframe.buffer(0)

        return ReprojectGeometries(geodataframe=geodataframe,
                                   to='4326').reproject()


class ImproveGeometryCollections:
    def __init__(self, row_geometry: gpd.GeoSeries) -> None:
        self.row_geometry = row_geometry

    def geometry_collection_to_multipolygon(
            self,
            new_gdf: gpd.GeoDataFrame = gpd.GeoDataFrame()
    ) -> gpd.GeoDataFrame:
        for geometry in self.row_geometry.geoms:
            if geometry.geom_type != 'LineString':
                new_gdf = new_gdf.append(
                    self.__multipolygon_to_polygons(geometry))

        new_multipolygon = gpd.GeoDataFrame(new_gdf.dissolve())

        return self.__get_new_geometry(new_multipolygon['geometry'])

    @staticmethod
    def __multipolygon_to_polygons(
            row_multipolygon_geometry: gpd.GeoSeries) -> gpd.GeoDataFrame:
        d = {'geometry': [row_multipolygon_geometry]}
        gdf = gpd.GeoDataFrame(d, crs="EPSG:4326")

        return gpd.GeoDataFrame(gdf.explode())

    @staticmethod
    def __get_new_geometry(new_multipolygon: gpd.GeoSeries):
        geom_string = str(gpd.GeoSeries(new_multipolygon).geometry.values[0])
        return loads(geom_string).buffer(0)


class Intersect:
    def __init__(
            self,
            geodataframe1: gpd.GeoDataFrame,
            geodataframe2: gpd.GeoDataFrame,
            calc_area: bool = False) -> None:
        """Classe para realizar a intersecção de geodataframes.

        Params:
            - geodataframe1 (gpd.GeoDataFrame)
            - geodataframe2 (gpd.GeoDataFrame)
        """
        self.calc_area = calc_area
        self.geodataframe1 = MakeValidGeometries(
            geodataframe=geodataframe1).improve_geometry()
        self.geodataframe2 = MakeValidGeometries(
            geodataframe=geodataframe2).improve_geometry()

    def intersection(self) -> gpd.GeoDataFrame:
        """Retorna a intersecção dos dois geodataframes.

        Return:
            - geometrias válidas interseccionadas.
        """
        intersect = gpd.overlay(
            self.geodataframe1, self.geodataframe2, how='intersection')

        return MakeValidGeometries(geodataframe=intersect).improve_geometry()


class Area:
    def __init__(
            self,
            geodataframe: gpd.GeoDataFrame,
            column_name: str = 'AREA_CALC') -> None:

        self.column_name = column_name
        self.geodataframe = ReprojectGeometries(
            geodataframe, to='utm').reproject()

    def calculate_area(self) -> gpd.GeoDataFrame:
        self.geodataframe[f'{self.column_name}'] = round(
            self.geodataframe['geometry'].area / 10000, 7)

        return MakeValidGeometries(
            geodataframe=self.geodataframe).improve_geometry()


class Dissolve:
    def __init__(
            self,
            geodataframe: gpd.GeoDataFrame,
            dissolve_atributes: list[str] | None = None,
            calc_area: bool = False,
            column_area: str = 'AREA_CALC') -> None:
        self.calc_area = calc_area
        self.column_area = column_area
        self.geodataframe = geodataframe
        self.dissolve_atributes = dissolve_atributes

    def dissolve_geodataframe(self) -> gpd.GeoDataFrame:
        if self.dissolve_atributes is None:
            geodataframe_dissolve = self.__simple_dissolve()
        else:
            try:
                geodataframe_dissolve = self.geodataframe.dissolve(
                    by=self.dissolve_atributes,
                    as_index=False)
            except:
                geodataframe_dissolve = MakeValidGeometries(
                    geodataframe=self.geodataframe
                ).improve_geometry()

                geodataframe_dissolve = geodataframe_dissolve.dissolve(
                    by=self.dissolve_atributes,
                    as_index=False)

            if geodataframe_dissolve.empty:
                raise ValueError("GEOMETRIA DISSOLVIDA ESTA VAZIA!!!")

        if self.calc_area:
            return Area(
                geodataframe=geodataframe_dissolve,
                column_name=self.column_area).calculate_area()

        return ReprojectGeometries(geodataframe=geodataframe_dissolve,
                                   to='4326').reproject()

    def __simple_dissolve(self) -> gpd.GeoDataFrame:
        try:
            geodataframe_dissolve = self.geodataframe.dissolve()
        except:
            geodataframe_dissolve = MakeValidGeometries(
                geodataframe=self.geodataframe
            ).improve_geometry()

            geodataframe_dissolve = geodataframe_dissolve.dissolve()

        return geodataframe_dissolve


class Buffer:
    def __init__(self, geodataframe: gpd.GeoDataFrame,
                 area_buffer: float) -> None:
        self.area_buffer = area_buffer
        self.geodataframe = geodataframe

    def buffer_area(self) -> gpd.GeoDataFrame:
        self.geodataframe['geometry'] = self.geodataframe.geometry.buffer(
            self.area_buffer)

        return ReprojectGeometries(self.geodataframe).reproject()


class SymmetricDifference:
    def __init__(
            self,
            geodataframe1: gpd.GeoDataFrame,
            geodataframe2: gpd.GeoDataFrame,
            calc_area: bool = False,
            area_column: str = 'AREA_DIFF') -> None:
        self.geodataframe1 = geodataframe1
        self.geodataframe2 = geodataframe2

        self.calc_area = calc_area
        self.area_column = area_column

    def symmetric_difference(self) -> gpd.GeoDataFrame:
        symmetric_difference = gpd.overlay(self.geodataframe1,
                                           self.geodataframe2,
                                           how='symmetric_difference')
        if self.calc_area:
            print('Calculando área...')
            symmetric_difference = Area(geodataframe=symmetric_difference,
                                        column_name=self.area_column
                                        ).calculate_area()

        return symmetric_difference
