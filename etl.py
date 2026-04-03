import asyncio
import httpx
import pandas as pd
from tqdm import tqdm
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import numpy as np

load_dotenv()
API_KEY = os.getenv("API_KEY")
DB_SENHA= os.getenv("DB_SENHA")
base_url = "https://api.themoviedb.org/3"
db = create_engine(f'postgresql://eduar:{DB_SENHA}@localhost:5432/DW_Filmes')
# Para limitar o número de conexões em 20
sem = asyncio.Semaphore(20)
lista_ids_datas = dict()
valores_com_erro = []
def transforma_dim_data(df:pd.DataFrame):
   
    df_limpo = df.copy()
    df_limpo["data"] = pd.to_datetime(df_limpo["data"], errors='coerce')
    
    df_limpo = df_limpo.dropna(subset=['data'])
    
    dim_data = pd.DataFrame()
    dim_data["data_original"] = df_limpo["data"]

    dim_data = dim_data.assign(
        ano= (dim_data["data_original"].dt.year).astype(int),
        mes=(dim_data["data_original"].dt.month).astype(int),
        dia=( dim_data["data_original"].dt.day).astype(int),
        semestre= np.where(dim_data["data_original"].dt.quarter>2,2,1),
        dia_da_semana= (dim_data["data_original"].dt.day_of_week).astype(int)
    )
    dim_data["sk_data"] = df_limpo["data"].dt.strftime('%Y%m%d').astype(int)

    dim_data = dim_data.drop(columns=["data_original"])
    

    return dim_data

async def busca_data_completa(client, id_imdb):
    async with sem: # Ocupa um "guichê"
        full_url = f"{base_url}/find/{id_imdb}?api_key={API_KEY}&external_source=imdb_id"
        try:
            # Timeout serve para caso de ruim ele vá para outra tentativa, não morra a conexão
            resp = await client.get(full_url, timeout=10.0)
            if resp.status_code == 200:
                dados = resp.json()
                results = dados.get("movie_results")
                if results:
                    lista_ids_datas[id_imdb]= results[0].get("release_date")
                    return {"data": results[0].get("release_date")}
            return None
        except Exception:
            return None
def exclui_tabelas_aux(*tabelas,db):

    with db.connect() as conexao:
        for tabela in tabelas:
                conexao.execute(text(f"DROP TABLE IF EXISTS {tabela}"))
                conexao.commit()
                print(f"Tabela {tabela} excluída!")

async def main():
    # Converta para lista para garantir estabilidade na iteração
    ids = dim_filme['id_imdb'].unique().tolist()
    final_results = []

    # Configuração robusta para o cliente
    limits = httpx.Limits(max_connections=50, max_keepalive_connections=20)
    
    # O bloco "async with" deve envolver TODO o processo de espera
    async with httpx.AsyncClient(limits=limits, timeout=None) as client:
        tasks = [busca_data_completa(client, i) for i in ids]
        
        with tqdm(total=len(tasks), desc=f"Extraindo Datas ({len(ids)})", unit="filme") as pbar:
            # as_completed garante que a barra atualize conforme as respostas chegam
            for coro in asyncio.as_completed(tasks):
                try:
                    res = await coro
                    if res:
                        final_results.append(res)
                except Exception as e:
                    pass # Evita que um erro de conexão feche o programa
                finally:
                    pbar.update(1)

    # Finalização
    if final_results:
        df_resultados = pd.DataFrame(final_results)
        print(f"\nSucesso! {len(df_resultados)} filmes encontrados.")
        dim_data= transforma_dim_data(df_resultados)
        
        dim_data = dim_data.drop_duplicates()
        
        dim_data.to_sql('dim_data', db, if_exists='delete_rows', index=False)


        df_aux =pd.DataFrame(lista_ids_datas.items(),columns=(["id_imdb","sk_data"]))

        df_aux['sk_data'] = pd.to_datetime(df_aux['sk_data'], errors='coerce')
        df_aux = df_aux.dropna(subset=['sk_data'])
        df_aux['sk_data'] = df_aux['sk_data'].dt.strftime('%Y%m%d').astype(int)

        df_aux.to_sql("tabela_aux_datas_ids",
                      con=db,
                      if_exists="replace",
                      index=False)
        query = "select dim_filme.sk_filme, tabelaId.sk_data, tabela_aux.nota_media, tabela_aux.numero_votos, tabela_aux.tempo_minutos from tabela_aux INNER join dim_filme on tabela_aux.id_imdb = dim_filme.id_imdb INNER JOIN tabela_aux_datas_ids tabelaId on tabela_aux.id_imdb = tabelaId.id_imdb"
        df_fato_filme = pd.read_sql(query,db)
        df_fato_filme.to_sql("fato_filme",
                             con=db,
                             if_exists="replace",
                             index=False)
        exclui_tabelas_aux("tabela_aux_datas_ids","tabela_aux",db=db)

    else:
        print("\nNenhum dado foi coletado.")

if __name__ == "__main__":
    # Garanta que a leitura do SQL está aqui ou carregada globalmente
    dim_filme = pd.read_sql("SELECT id_imdb FROM dim_filme", db)
    asyncio.run(main())