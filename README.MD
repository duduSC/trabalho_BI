## 1. Entre os gêneros mais lucrativos, qual é o preferido do público?
(Aqui eu filtro por lucro total e ordeno por nota média)

```SQL
SELECT g.nome, AVG(f.notaMedia) as media_nota
FROM fato_filme f
JOIN bridge_filme_genero b ON f.sk_filme = b.sk_filme
JOIN dim_genero g ON b.sk_genero = g.sk_genero
GROUP BY g.nome
ORDER BY AVG(f.notaMedia) DESC;
```

## 2. A popularidade dos gêneros mudou ao longo do tempo?
Contamos quantos filmes foram lançados por gênero e por ano.

```SQL
SELECT d.ano, g.nome, COUNT(f.sk_filme) as total_filmes
FROM fato_filme f
JOIN dim_data d ON f.sk_data_lancamento = d.sk_data
JOIN bridge_filme_genero b ON f.sk_filme = b.sk_filme
JOIN dim_genero g ON b.sk_genero = g.sk_genero
GROUP BY d.ano, g.nome
ORDER BY d.ano DESC, total_filmes DESC;
```

## 3. Os clássicos são mais bem avaliados do que filmes mais recentes?
Usando a coluna ano como agrupador.

```SQL
SELECT 
    CASE WHEN d.ano < 1980 THEN 'Clássico' ELSE 'Moderno' END as epoca,
    AVG(f.notaMedia) as nota_media
FROM fato_filme f
JOIN dim_data d ON f.sk_data_lancamento = d.sk_data
GROUP BY epoca;
```

## 4. Qual gênero é mais lucrativo?

```SQL
SELECT g.nome, SUM(f.receita - f.orcamento) as lucro_total
FROM fato_filme f
JOIN bridge_filme_genero b ON f.sk_filme = b.sk_filme
JOIN dim_genero g ON b.sk_genero = g.sk_genero
GROUP BY g.nome
ORDER BY lucro_total DESC;
```

## 5. Alto investimento resulta em boas críticas?
Aqui comparo o orçamento com a nota média.

```SQL
SELECT 
    CASE 
        WHEN orcamento > 100000000 THEN 'Blockbuster (100M+)'
        WHEN orcamento > 10000000 THEN 'Médio (10M-100M)'
        ELSE 'Baixo Orçamento'
    END as faixa_investimento,
    AVG(notaMedia) as media_critica
FROM fato_filme
GROUP BY faixa_investimento;
```

## 6. A crítica importa para a bilheteria?
Verifico a correlação entre a nota Media e a receita.

```SQL
SELECT notaMedia, AVG(receita) as media_faturamento
FROM fato_filme
GROUP BY notaMedia
ORDER BY notaMedia DESC;
