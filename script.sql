-- 1. Tabela de Dimensão: Filme
drop table if exists dim_filme cascade;
CREATE TABLE dim_filme (
    sk_filme SERIAL PRIMARY KEY, -- Chave surrogate (gerada automaticamente)
    idImdb VARCHAR(20) NOT NULL UNIQUE, -- ID original (ex: tt0000009)
    tituloPrincipal VARCHAR(255) NOT NULL,
    tituloOriginal VARCHAR(255),
    tempoMinutos INTEGER
);

-- 2. Tabela de Dimensão: Gênero
drop table if exists dim_genero cascade;
CREATE TABLE dim_genero (
    sk_genero SERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL UNIQUE
);

-- 3. Tabela de Dimensão: Data (Simplificada para o seu caso)
drop table if exists dim_data cascade;
CREATE TABLE dim_data (
    sk_data INTEGER PRIMARY KEY, -- Formato YYYYMMDD
    ano INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    dia INTEGER NOT NULL
);

-- 4. Tabela de Fatos: Filme
-- Esta tabela guarda as métricas e chaves para as dimensões
drop table if exists fato_filme cascade;
CREATE TABLE fato_filme (
    sk_filme INTEGER NOT NULL,
    sk_data_lancamento INTEGER,
    notaMedia FLOAT DEFAULT 0,
    numeroVotos INTEGER DEFAULT 0,
    
    CONSTRAINT fk_fato_filme FOREIGN KEY (sk_filme) REFERENCES dim_filme (sk_filme),
    CONSTRAINT fk_fato_data FOREIGN KEY (sk_data_lancamento) REFERENCES dim_data (sk_data)
);

-- 5. Tabela Bridge: Filme e Gênero (Muitos para Muitos)
-- É aqui que o seu "explode" do Pandas vai morar!
drop table if exists bridge_filme_genero cascade;
CREATE TABLE bridge_filme_genero (
    sk_filme INTEGER NOT NULL,
    sk_genero INTEGER NOT NULL,
    
    PRIMARY KEY (sk_filme, sk_genero),
    CONSTRAINT fk_bridge_filme FOREIGN KEY (sk_filme) REFERENCES dim_filme (sk_filme),
    CONSTRAINT fk_bridge_genero FOREIGN KEY (sk_genero) REFERENCES dim_genero (sk_genero)
);
