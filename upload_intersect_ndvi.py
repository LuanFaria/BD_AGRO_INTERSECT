import os
import re
import geopandas as gpd
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from tkinter import messagebox
from datetime import datetime

# Função utilitária para converter número em string para float
def parse_number(val):
    if val is None:
        return None
    # se já for numérico
    if isinstance(val, (int, float)):
        return float(val)
    # strings vazias ou apenas espaços
    s = str(val).strip()
    if s == "" or s.lower() in ["nan", "n/a", "null", "none", "-"]:
        return None
    # remover pontos de milhar e transformar vírgula decimal em ponto
    # exemplos: "1.234,56" -> "1234.56"; "1,234.56" -> "1234.56"
    # estratégia: se houver vírgula e ponto, inferir formato:
    if "," in s and "." in s:
        # se ponto aparece antes da vírgula -> ponto é milhar
        if s.find(".") < s.find(","):
            s = s.replace(".", "")
            s = s.replace(",", ".")
        else:
            # se vírgula antes do ponto (ex: "1,234.56"), remover vírgula (milhar)
            s = s.replace(",", "")
    else:
        # se só há vírgula, então é separador decimal
        if "," in s:
            s = s.replace(",", ".")
        # se só há ponto, assume padrão en
    # remover quaisquer caracteres não numéricos exceto ponto e minus
    s = re.sub(r"[^\d\.\-eE]", "", s)
    if s == "":
        return None
    try:
        return float(s)
    except Exception:
        return None

# Função utilitária para parsear datas em formatos comuns
def parse_date(val):
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    s = str(val).strip()
    if s == "" or s.lower() in ["nan", "n/a", "null", "none", "-"]:
        return None
    # tentar alguns formatos comuns
    fmts = ["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d", "%m/%d/%Y", "%d.%m.%Y"]
    for f in fmts:
        try:
            return datetime.strptime(s, f).date()
        except Exception:
            continue
    # tentar parse mais flexível com pandas (pode aceitar mais formatos)
    try:
        ts = pd.to_datetime(s, dayfirst=True, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.date()
    except Exception:
        return None

def upload_intersect_ndvi(clientes_id, janela, ano, db_config):
    try:
        clients_folder = "X:/Sigmagis/Projetos/TOMOGRAFIA/Tomo4Lite"

        # Localizar a pasta do cliente (ex: "2_FERRARI")
        cliente_pasta = next((p for p in os.listdir(clients_folder) if p.startswith(f"{clientes_id}_")), None)
        if not cliente_pasta:
            raise FileNotFoundError(f"Pasta do cliente com ID {clientes_id} não encontrada.")

        intersect_folder = os.path.join(clients_folder, cliente_pasta, "3_intersect_ndvi")
        if not os.path.exists(intersect_folder):
            raise FileNotFoundError(f"Pasta 3_intersect_ndvi não encontrada dentro de {cliente_pasta}")

        # Buscar shapefile correspondente (busca padrão "INTERSECT_NDVI_<CLIENTE>_<JX>_<ANO>.shp" ou apenas <JX>_<ANO>)
        shp_file = None
        for file in os.listdir(intersect_folder):
            if file.endswith(".shp") and f"{janela}_{ano}" in file:
                shp_file = os.path.join(intersect_folder, file)
                break

        if not shp_file:
            raise FileNotFoundError(f"Nenhum shapefile encontrado para {janela}_{ano} em {intersect_folder}.")

        # Ler shapefile com geopandas
        gdf = gpd.read_file(shp_file)

        # Normalizar nomes de colunas (tudo minúsculo)
        gdf.columns = [col.lower() for col in gdf.columns]

        # Garantir colunas essenciais
        if "gridcode" not in gdf.columns:
            gdf["gridcode"] = None

        # Adicionar metadados
        gdf["clientes_id"] = clientes_id
        gdf["janela"] = janela
        gdf["safra"] = int(ano)

        # Reprojetar para SIRGAS 2000 (ajuste EPSG se necessário)
        try:
            gdf = gdf.to_crs(epsg=4674)
        except Exception:
            # se já estiver no mesmo CRS, ignora
            pass

        # Preparar coluna geom em WKT
        gdf["geom"] = gdf["geometry"].apply(lambda g: g.wkt if g else None)

        # Definir colunas esperadas (sem o 'id' primário)
        expected_columns = [
            'gridcode','setor','fazenda','bloco','talhao','safra','tp_prop','chave','objetivo',
            'secao','pivo','desc_faz','variedade','maturacao','ambiente','estagio','grupo_dash',
            'grupo_ndvi','nmro_corte','desc_cana','area_bd','a_est_moag','a_colhida','a_est_muda',
            'a_muda','tch_est','tc_est','tch_rest','tc_rest','tch_real','tc_real','dt_corte',
            'dt_ult_cor','dt_plantio','idade_cort','atr','atr_est','irrigacao','tah','tph',
            'cliente','tch_v0','tc_v0','area_gis','obs_img','data_img','idade_img','classe',
            'area_ndvi','clientes_id','janela','geom'
        ]

        # Criar colunas faltantes
        for col in expected_columns:
            if col not in gdf.columns:
                gdf[col] = None

        # LISTA: colunas que devem ser numéricas (converter)
        numeric_cols = [
            'area_bd','a_est_moag','a_colhida','a_est_muda','a_muda',
            'tch_est','tc_est','tch_rest','tc_rest','tch_real','tc_real',
            'idade_cort','atr','atr_est','tah','tph','tch_v0','tc_v0',
            'area_gis','idade_img','area_ndvi'
        ]

        # LISTA: colunas que devem ser data
        date_cols = ['dt_corte','dt_ult_cor','dt_plantio','data_img']

        # Relatórios de conversão
        conversion_failures = {col: 0 for col in numeric_cols + date_cols}
        conversion_success = {col: 0 for col in numeric_cols + date_cols}

        # Converter coluna a coluna usando parse_number / parse_date
        for col in numeric_cols:
            if col in gdf.columns:
                # aplicar parse_number em toda a coluna
                converted = gdf[col].apply(parse_number)
                # contar falhas (None quando original não era None)
                for orig, conv in zip(gdf[col], converted):
                    if orig is None or (isinstance(orig, float) and pd.isna(orig)):
                        # original vazio: não conta como falha
                        continue
                    if conv is None:
                        conversion_failures[col] += 1
                    else:
                        conversion_success[col] += 1
                gdf[col] = converted

        for col in date_cols:
            if col in gdf.columns:
                converted = gdf[col].apply(parse_date)
                for orig, conv in zip(gdf[col], converted):
                    if orig is None or (isinstance(orig, float) and pd.isna(orig)):
                        continue
                    if conv is None:
                        conversion_failures[col] += 1
                    else:
                        conversion_success[col] += 1
                gdf[col] = converted

        # Garantir que o DataFrame tenha ordem das expected_columns
        gdf = gdf[expected_columns]

        # Transformar NaN/NaT em None para psycopg2
        gdf = gdf.where(pd.notnull(gdf), None)

        # Conectar ao banco
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        # Verificar se já existe registro para cliente/safra/janela
        cur.execute("""
            SELECT COUNT(*) FROM public.base_intersect_ndvi
            WHERE clientes_id = %s AND safra = %s AND janela = %s
        """, (clientes_id, ano, janela))
        count = cur.fetchone()[0]

        if count > 0:
            resposta = messagebox.askyesno(
                "Dados já existentes",
                f"Essa janela ({janela}) do ano {ano} já possui dados no banco.\nDeseja substituir os atuais?"
            )
            if resposta:
                cur.execute("""
                    DELETE FROM public.base_intersect_ndvi
                    WHERE clientes_id = %s AND safra = %s AND janela = %s
                """, (clientes_id, ano, janela))
                conn.commit()
            else:
                messagebox.showinfo("Cancelado", "Upload cancelado pelo usuário.")
                cur.close()
                conn.close()
                return

        # Inserção em batch
        insert_query = f"""
            INSERT INTO public.base_intersect_ndvi ({', '.join(expected_columns)})
            VALUES %s
        """
        values = [tuple(row) for _, row in gdf.iterrows()]
        if values:
            execute_values(cur, insert_query, values)
            conn.commit()

        cur.close()
        conn.close()

        # Preparar relatório resumido para o usuário
        total_rows = len(gdf)
        failed_total = sum(conversion_failures[col] for col in conversion_failures)
        success_total = sum(conversion_success[col] for col in conversion_success)
        msg_lines = [
            f"Arquivo {os.path.basename(shp_file)} importado com sucesso!",
            f"Registros inseridos: {total_rows}",
            f"Conversões numéricas/datas bem-sucedidas (total): {success_total}",
            f"Conversões falhadas (total): {failed_total}"
        ]
        # adicionar detalhes por coluna se houver falhas
        details = []
        for col in conversion_failures:
            if conversion_failures[col] > 0:
                details.append(f"{col}: {conversion_failures[col]} valores inválidos")
        if details:
            msg_lines.append("Detalhes de conversão:\n" + "\n".join(details))

        messagebox.showinfo("Sucesso", "\n".join(msg_lines))

    except Exception as e:
        messagebox.showerror("Erro", f"Ocorreu um erro ao subir o shapefile:\n\n{e}")
