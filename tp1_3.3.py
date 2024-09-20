import psycopg2
from typing import Iterable, Any
from typing import List, Tuple

def lista_5(product_id: Any, config) -> Iterable[Any]:
    # Consulta SQL para selecionar os 5 comentários mais úteis com maior e menor avaliação
    query = """
        (
        SELECT produtos.product_id, produtos.asin, avaliacoes.customer_id, avaliacoes.review_date, avaliacoes.rating, avaliacoes.helpful 
        FROM produtos, avaliacoes 
        WHERE produtos.product_id=%s
        ORDER BY rating DESC, helpful DESC 
        LIMIT 5
        )
        UNION ALL 
        (
        SELECT produtos.product_id, produtos.asin, avaliacoes.customer_id, avaliacoes.review_date, avaliacoes.rating, avaliacoes.helpful 
        FROM produtos, avaliacoes 
        WHERE produtos.product_id=%s
        ORDER BY rating ASC, helpful DESC
        LIMIT 5
        );
    """
    
    # Inicializa o gerenciador de banco de dados
    
    try:
        # Obter as configurações de conexão

        # Conectar ao banco de dados
        conn = psycopg2.connect(**config)
        cur = conn.cursor()

        # Executar a consulta
        cur.execute(query, (product_id, product_id))
        result = cur.fetchall()

        # Fechar a conexão
        cur.close()
        conn.close()

        # Retornar o resultado
        return result
    except Exception as error:
        print(f"Erro ao executar a consulta: {error}")
        return None

def listar_mais_vendidos(config) -> List[Tuple[str, int, str]]:
    # Consulta SQL para listar os 10 produtos mais vendidos por grupo
    query = """
    SELECT title, salesrank, product_group 
    FROM (
        SELECT title, salesrank, product_group, 
               RANK() OVER (PARTITION BY product_group ORDER BY salesrank ASC) AS Rank 
        FROM produtos 
        WHERE salesrank > 0
    ) rs 
    WHERE Rank <= 10;
    """
    
    try:
        # Conectar ao banco de dados
        conn = psycopg2.connect(**config)
        cur = conn.cursor()

        # Executar a consulta
        cur.execute(query)
        result = cur.fetchall()

        # Fechar a conexão
        cur.close()
        conn.close()

        # Retornar o resultado
        return result

    except Exception as e:
        print(f"Erro ao executar a consulta: {e}")
        return []
    
def listar_similares_maiores_vendas(product_id,config) -> List[Tuple[str, int]]:
    # Consulta SQL para listar produtos similares com melhores vendas
    query = """
    SELECT ps.similar_asin, p2.title, p2.salesrank
    FROM produtos p1
    JOIN produtos_similares ps ON p1.asin = ps.product_asin
    JOIN produtos p2 ON ps.similar_asin = p2.asin
    WHERE p1.asin = %s 
    AND p2.salesrank < p1.salesrank  
    ORDER BY p2.salesrank ASC;       
    """
    try:
        # Obter as configurações de conexão

        # Conectar ao banco de dados
        conn = psycopg2.connect(**config)
        cur = conn.cursor()

        # Executar a consulta
        cur.execute(query, (product_id,))
        result = cur.fetchall()

        # Fechar a conexão
        cur.close()
        conn.close()

        # Retornar o resultado
        return result
    except Exception as e:
        print(f"Erro ao executar a consulta: {e}")
        return []

def evolucao_medias_avaliacao(product_id, config):
    # Consulta SQL para calcular a média das avaliações por data
    query = """
    SELECT p.title, r.review_date, round(avg(r.rating), 2)
    FROM produtos p
    INNER JOIN avaliacoes r ON p.product_id = r.product_id
    WHERE p.product_id = %s
    GROUP BY p.title, r.review_date
    ORDER BY r.review_date ASC;
    """
    
    try:
        # Conectar ao banco de dados
        conn = psycopg2.connect(**config)
        cur = conn.cursor()

        # Executar a consulta
        cur.execute(query, (product_id,))
        result = cur.fetchall()

        # Fechar a conexão
        cur.close()
        conn.close()

        # Exibir o resultado
        for row in result:
            print(f"Produto: {row[0]}, Data: {row[1]}, Média de Avaliação: {row[2]}")

    except Exception as e:
        print(f"Erro ao executar a consulta: {e}")

def listar_clientes(config) -> List[Tuple[str, int, int, str]]:
    # Consulta SQL para listar os 10 principais clientes
    query = """
        SELECT customer_id, n_reviews, review_rank, product_group
        FROM (
        SELECT customer_id, n_reviews, product_group,
               ROW_NUMBER() OVER (PARTITION BY t1.product_group ORDER BY t1.n_reviews DESC) AS review_rank
        FROM (
            SELECT r.customer_id, COUNT(r.customer_id) AS n_reviews, p.product_group
            FROM produtos p
            INNER JOIN avaliacoes r ON p.product_id = r.product_id
            GROUP BY p.product_group, r.customer_id
        ) AS t1
        ORDER BY t1.product_group ASC, t1.n_reviews DESC
    ) AS t2
    WHERE review_rank <= 10;
    """

    try:
        # Conectar ao banco de dados
        conn = psycopg2.connect(**config)
        cur = conn.cursor()

        # Executar a consulta
        cur.execute(query)
        result = cur.fetchall()

        # Fechar a conexão
        cur.close()
        conn.close()

        # Retornar o resultado
        return result

    except Exception as e:
        print(f"Erro ao executar a consulta: {e}")
        return []

def listar_categorias(config) -> List[Tuple[str, float]]:
    # Consulta SQL para listar as 5 principais categorias
    query = """
    SELECT c.name, ROUND(t_avg.avg, 2)
    FROM categoria c
    INNER JOIN (
        SELECT pc.category_id, AVG(qtd_pos.count) AS avg
        FROM produto_categoria pc
        INNER JOIN (
            SELECT r.product_id, COUNT(*) AS count
            FROM avaliacoes r
            WHERE r.helpful > 0
            GROUP BY r.product_id
        ) qtd_pos ON qtd_pos.product_id = pc.product_id
        GROUP BY pc.category_id
        HAVING AVG(qtd_pos.count) > 0
        ORDER BY avg DESC
        LIMIT 5
    ) t_avg ON c.category_id = t_avg.category_id;
    """
    
    try:
        # Conectar ao banco de dados
        conn = psycopg2.connect(**config)
        cur = conn.cursor()

        # Executar a consulta
        cur.execute(query)
        result = cur.fetchall()

        # Fechar a conexão
        cur.close()
        conn.close()

        # Retornar o resultado
        return result

    except Exception as e:
        print(f"Erro ao executar a consulta: {e}")
        return []

def listar_produtos() -> List[Tuple[int, str, str, float, int]]:
    # Consulta SQL para listar os produtos
    query = """
    SELECT t2.product_id, t2.title, t2.product_group, t2.avg_helpful, t2.n_rank
    FROM (
        SELECT p.product_id, p.title, p.product_group, t1.avg_helpful, 
               ROW_NUMBER() OVER (PARTITION BY p.product_group ORDER BY t1.avg_helpful DESC) AS n_rank
        FROM produtos p
        JOIN (
            SELECT r.product_id, ROUND(AVG(r.helpful), 2) AS avg_helpful
            FROM avaliacoes r
            WHERE r.helpful > 0
            GROUP BY r.product_id
        ) t1 ON t1.product_id = p.product_id
    ) AS t2
    WHERE t2.n_rank <= 10;
    """
    
    try:
        # Conectar ao banco de dados
        conn = psycopg2.connect(**config)
        cur = conn.cursor()

        # Executar a consulta
        cur.execute(query)
        result = cur.fetchall()

        # Fechar a conexão
        cur.close()
        conn.close()

        # Retornar o resultado
        return result

    except Exception as e:
        print(f"Erro ao executar a consulta: {e}")
        return []
    


config = {
            'dbname': 'amazon3',
            'user': 'postgres',
            'password': 'thiago2014',
            'host': 'localhost',
            'port': '5432'
        }


print("Selecione as seguintes opções:\na)para listar os comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação\nb)listar os produtos similares com maiores vendas que ele\nc)Para mostrar a evolução diária das médias de avaliação ao longo do intervalo de tempo\nd)Para listar os 10 produtos lideres de venda em cada grupo de produtos\ne)Para listar os 10 produtos com a maior média de avaliações úteis positivas por produto\nf)Para listar 5 categorias de produtos com maior média de avaliações úteis positivas por produto\ng)Para listar os 10 clientes que mais fizeram comentários por grupo de produto ")
escolha = (input(("Digite uma letra:")))

if escolha == "d":
    x = listar_mais_vendidos(config)

    print(f"{'Title'.ljust(60)} {'Salesrank'.ljust(10)} {'Product Group'}")
    print("="*80)

    # Itera sobre os dados e exibe-os formatados
    for item in x:
        title = item[0].ljust(60)  # Ajusta a largura da coluna 'Title'
        salesrank = str(item[1]).ljust(10)  # Ajusta a largura da coluna 'Salesrank'
        product_group = item[2]  # 'Product Group' não precisa de ajuste
        print(f"{title} {salesrank} {product_group}")
if escolha == "a":
    aux = int(input(("Digite o ID:")))
    x = lista_5(aux,config)
    # Cabeçalhos da tabela
    # Cabeçalhos da tabela
    print(f"{'Product ID':<10} {'ASIN':<12} {'Customer ID':<20} {'Review Date':<15} {'Rating':<6}    {'Helpful':<15}")
    print("=" * 80)

    # Exibindo os dados de forma organizada
    for row in x:
        product_id, asin, customer_id, review_date, rating, helpful = row  
        print(f"{product_id:<10} {asin:<12} {customer_id:<20} {review_date}           {rating:<6} {helpful:<15}")

if escolha == "b":
    aux = input(("Digite o ASIN:"))
    x = listar_similares_maiores_vendas(aux,config)
    print(f"{'Similar ASIN':<12} {'Title':<50} {'Salesrank':<10}")
    print("=" * 75)

    for row in x:
        similar_asin, title, salesrank = row
        print(f"{similar_asin:<12} {title:<50} {salesrank:<10}")
    
if escolha == "c":
    aux = int(input(("Digite o ID:")))
    evolucao_medias_avaliacao(aux,config)

if escolha == "g":
    x = listar_clientes(config)
    print(f"{'Cliente':<15} {'Sales':<6} {'Rank':<5} {'Type':<10}")
    print("=" * 40)

    # Impressão dos dados
    for item in x:
        cliente, sales, rank, type_ = item
        print(f"{cliente:<15} {sales:<6} {rank:<5} {type_:<10}")

if escolha == "f":
    x = listar_categorias(config)
    print(f"{'Categoria':<20} {'Média':<10}")
    print("=" * 30)

    # Impressão dos dados
    for categoria, media in x:
        print(f"{categoria:<20} {media:<10}")

if escolha == "e":
    x = listar_produtos()
    print(f"{'ID':<5} {'Título':<100} {'Grupo':<10} {'Média Helpful':<15} {'Rank':<5}")
    print("=" * 130)

    # Impressão dos dados
    for product_id, title, group, media_helpful, rank in x:
        print(f"{product_id:<5} {title:<100} {group:<10} {media_helpful:<15} {rank:<5}")