import asyncio
import httpx
import pandas as pd
from tqdm import tqdm
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import numpy as np

load_dotenv()
API_KEY = os.getenv("API_KEY")
DB_SENHA= os.getenv("DB_SENHA")
base_url = "https://api.themoviedb.org/3"
db = create_engine(f'postgresql://eduar:{DB_SENHA}@localhost:5432/DW_Filmes')
# Para limitar o número de conexões em 20
sem = asyncio.Semaphore(20)


def transforma_dim_data(df:pd.DataFrame):
   
    dim_data = pd.to_datetime(df["data"])
    dim_data = pd.DataFrame(dim_data)

    dim_data = dim_data.assign(
        ano= (dim_data["data"].dt.year).astype("str"),
        mes=(dim_data["data"].dt.month).astype("str"),
        dia=( dim_data["data"].dt.day).astype("str"),
        semestre= np.where(dim_data["data"].dt.quarter>2,"1","2"),
        dia_da_semana= (dim_data["data"].dt.day_of_week).astype("str")
    )



    
    dim_data["data"]= dim_data["data"].astype("str")
    dim_data = dim_data.rename(columns={"data":"sk_data"})

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
                    return {"data": results[0].get("release_date")}
            return None
        except Exception:
            return None

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
        dim_data["sk_data"] = dim_data["sk_data"].str.replace("-","").astype("int")
        dim_data.to_sql('dim_data', db, if_exists='delete_rows', index=False)

    else:
        print("\nNenhum dado foi coletado.")

if __name__ == "__main__":
    # Garanta que a leitura do SQL está aqui ou carregada globalmente
    dim_filme = pd.read_sql("SELECT id_imdb FROM dim_filme LIMIT 10", db)
    asyncio.run(main())