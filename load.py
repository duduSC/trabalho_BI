load_dotenv()
DB_SENHA=os.getenv("DB_SENHA")
db = create_engine('postgresql://postgres:{DB_SENHA}@localhost:5432/DataWarehouse_Filmes')

query = "SELECT * FROM fato_filme"
consulta = pd.read_sql(query,db)
consulta


dim_genero.to_sql(
    name="dim_genero",
    con=db,
    if_exists="append",
    index=False
)


pd.read_sql("SELECT * FROM dim_genero",db)

dim_filme.to_sql(
    name="dim_filme",
    con=db,
    if_exists="append",
    index=False)


pd.read_sql("SELECT * FROM dim_filme",db)