import os
import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine, text
import re

# Configurações do banco
DB_CONFIG = {
    "host": "localhost",
    "dbname": "postgis_34_sample",
    "user": "postgres",
    "password": "postgres",
    "port": "5432"
}

# Caminho base
BASE_DIR = r"X:\Sigmagis\Projetos\TOMOGRAFIA\Tomo4Lite"
TABELA_DESTINO = "base_intersect_ndvi"

# Criar engine SQLAlchemy
engine = create_engine(
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
)

def extrair_cliente_id(nome_pasta):
    """Extrai o número do início do nome da pasta (ex: '12_USINA' -> 12)."""
    match = re.match(r"(\d+)", nome_pasta)
    if match:
        return int(match.group(1))
    return None

def tabela_possui_registros(engine, cliente_id, janela, safra):
    """Verifica se já existem registros com o mesmo cliente, janela e safra."""
    query = text(f"""
        SELECT COUNT(*) 
        FROM {TABELA_DESTINO}
        WHERE clientes_id = :cliente_id AND janela = :janela AND safra = :safra
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"cliente_id": cliente_id, "janela": janela, "safra": safra})
        return result.scalar() > 0

def corrigir_tipos_numericos(gdf):
    """Converte colunas numéricas que estão como string."""
    for col in gdf.columns:
        if gdf[col].dtype == "object":
            try:
                gdf[col] = pd.to_numeric(gdf[col].astype(str).str.replace(",", "."), errors="ignore")
            except Exception:
                pass
    return gdf

def inserir_shapefile(engine, shp_path, cliente_id, janela, safra):
    print(f"🔍 Processando {os.path.basename(shp_path)}...")
    gdf = gpd.read_file(shp_path)
    gdf = corrigir_tipos_numericos(gdf)

    gdf["clientes_id"] = cliente_id
    gdf["janela"] = janela
    gdf["safra"] = safra

    gdf.to_postgis(TABELA_DESTINO, engine, if_exists="append", index=False)
    print(f"✅ Inserido: cliente={cliente_id}, janela={janela}, safra={safra}")

def processar_pastas():
    for cliente_dir in os.listdir(BASE_DIR):
        cliente_path = os.path.join(BASE_DIR, cliente_dir)
        if not os.path.isdir(cliente_path):
            continue

        cliente_id = extrair_cliente_id(cliente_dir)
        if cliente_id is None:
            print(f"⚠️ Ignorando pasta sem ID numérico: {cliente_dir}")
            continue

        for safra in os.listdir(cliente_path):
            safra_path = os.path.join(cliente_path, safra)
            if not os.path.isdir(safra_path):
                continue

            for janela in os.listdir(safra_path):
                janela_path = os.path.join(safra_path, janela)
                if not os.path.isdir(janela_path):
                    continue

                shp_files = [f for f in os.listdir(janela_path) if f.endswith(".shp")]
                for shp_file in shp_files:
                    shp_path = os.path.join(janela_path, shp_file)

                    if tabela_possui_registros(engine, cliente_id, janela, safra):
                        print(f"⚠️ Já existe no banco: cliente={cliente_id}, janela={janela}, safra={safra}. Pulando...")
                        continue

                    inserir_shapefile(engine, shp_path, cliente_id, janela, safra)

if __name__ == "__main__":
    processar_pastas()
