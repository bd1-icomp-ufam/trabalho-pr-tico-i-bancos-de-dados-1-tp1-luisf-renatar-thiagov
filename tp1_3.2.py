import psycopg2
from tqdm import tqdm
import re


def criar_tabelas(config):
    commands = (
        """
        CREATE TABLE IF NOT EXISTS produtos (
            product_id INTEGER NOT NULL PRIMARY KEY,
            asin VARCHAR(10) NOT NULL UNIQUE,
            title VARCHAR(500),
            product_group VARCHAR(50),
            salesrank INTEGER,
            review_total INTEGER DEFAULT 0,
            review_downloaded INTEGER DEFAULT 0,
            review_avg FLOAT DEFAULT 0.0
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS produtos_similares (
            product_asin VARCHAR(10) NOT NULL,
            similar_asin VARCHAR(10) NOT NULL,
            PRIMARY KEY (product_asin, similar_asin),
            FOREIGN KEY (product_asin) REFERENCES produtos(asin)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS categoria (
            category_id INTEGER NOT NULL PRIMARY KEY,
            name VARCHAR(220),
            parent_id INTEGER,
            FOREIGN KEY (parent_id) REFERENCES categoria(category_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS produto_categoria (
            product_id INTEGER NOT NULL,
            category_id INTEGER NOT NULL,
            PRIMARY KEY (product_id, category_id),
            FOREIGN KEY (product_id) REFERENCES produtos(product_id),
            FOREIGN KEY (category_id) REFERENCES categoria(category_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS avaliacoes (
            product_id INTEGER NOT NULL,
            customer_id VARCHAR(16) NOT NULL,
            review_date DATE NOT NULL,
            rating INTEGER DEFAULT 0,
            votes INTEGER DEFAULT 0,
            helpful INTEGER DEFAULT 0,
            PRIMARY KEY (product_id, customer_id, review_date),
            FOREIGN KEY (product_id) REFERENCES produtos(product_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS cliente (
            customer_id VARCHAR(16) NOT NULL PRIMARY KEY
        );
        """
    )

    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                # Executar cada comando SQL para criar as tabelas
                for command in commands:
                    cur.execute(command)
                # Confirmar as alterações
                conn.commit()
                print("Tabelas criadas com sucesso!")

    except (psycopg2.DatabaseError, Exception) as error:
        print(f"Erro ao criar tabelas: {error}")

def extract_category_id(category_str):
    # Procura por números entre colchetes
    match = re.search(r'\[(\d+)\]', category_str)
    if match:
        return match.group(1)  # Retorna apenas o número encontrado
    return None  # Retorna None se nenhum número for encontrado
def products(produto,config):
        data_dict = produto
        itens_nao_vazios = {k: v for k, v in produto.items() if v != []}
        quantidade_itens = len(itens_nao_vazios)
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:  
                if quantidade_itens == 2:
                        products_data = list(zip(data_dict['id'], data_dict['asin']))
                        insert_query = """
                        INSERT INTO produtos (product_id, asin, title, product_group, salesrank)
                        VALUES (%s, %s, NULL, NULL, NULL)
                        ON CONFLICT (product_id) DO NOTHING;
                        """
                else:
                    total_reviews, downloaded_reviews, average_rating = data_dict['reviews']
                    products_data = list(zip(
                            data_dict['id'],
                            data_dict['asin'],
                            data_dict['title'],
                            data_dict['group'],
                            data_dict['salesrank'],
                            [total_reviews],     
                            [downloaded_reviews], 
                            [average_rating]      
                        ))
                    insert_query = """
                        INSERT INTO produtos (product_id, asin, title, product_group, salesrank,review_total,review_downloaded,review_avg)
                        VALUES (%s, %s, %s, %s, %s, %s,%s,%s)
                        ON CONFLICT (product_id) DO NOTHING;
                        """
                cur.executemany(insert_query, products_data)
                conn.commit()

def reviews(produto, config):
    with psycopg2.connect(**config) as conn:
        with conn.cursor() as cur:  
            data_dict = produto
            review_details_data = []
            customer_ids = set()  # Usar um conjunto para evitar duplicatas
            product_id = data_dict['id'][0]  # Supondo que 'id' seja uma lista

            for review in data_dict['reviews_details']:
                if len(review) == 5:  # Certifique-se de que há exatamente 5 elementos na avaliação
                    review_date, user_id, rating, helpfulness, total_votes = review
                    review_details_data.append((
                        product_id, 
                        review_date, 
                        user_id, 
                        rating, 
                        helpfulness, 
                        total_votes
                    ))
                    customer_ids.add(user_id)  # Adiciona o ID do cliente ao conjunto
            
            # Inserir os detalhes dos reviews
            insert_review_query = """
                INSERT INTO avaliacoes (product_id, review_date, customer_id, rating, votes, helpful)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (product_id, customer_id, review_date) DO NOTHING;
            """
            cur.executemany(insert_review_query, review_details_data)

            # Inserir dados dos clientes
            insert_cliente_query = """
                INSERT INTO cliente (customer_id)
                VALUES (%s)
                ON CONFLICT (customer_id) DO NOTHING;
            """
            cur.executemany(insert_cliente_query, [(customer_id,) for customer_id in customer_ids])

def similar(produto,config):
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:  
                data_dict = produto
                similar_products_data = []
                for i, similar_asins in enumerate(data_dict['similar']):
                    if len(similar_asins) > 1 and i < len(data_dict['asin']):
                        for similar_asin in similar_asins:
                             similar_products_data.append((data_dict['asin'][i], similar_asin))


                insert_query = """
                    INSERT INTO produtos_similares (product_asin, similar_asin)
                    VALUES (%s, %s)
                    ON CONFLICT (product_asin, similar_asin) DO NOTHING;
                                """
                cur.executemany(insert_query, similar_products_data)


def category(produto,config):
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:  
                data_dict = produto
                category_data = []
                for category_list in data_dict['categories']:
                    for category in category_list:
                                # Tenta extrair o ID numérico da categoria
                        category_id = extract_category_id(category)
                        if category_id:
                                    # Extrai o nome da categoria removendo tudo após o colchete
                            category_name = category.split('[')[0].strip()  # Remove espaços em branco
                            category_data.append((int(category_id), category_name, None))


                        # Inserir os dados na tabela 'category'
                insert_query = """
                        INSERT INTO categoria (category_id, name, parent_id)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (category_id) DO NOTHING;
                        """
                cur.executemany(insert_query, category_data)

def prodcategory(produto,config):
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:  
                data_dict = produto
                product_category_data = []

                        # Iterar sobre cada produto e suas categorias
                for product_id in data_dict['id']:
                    for category_list in data_dict['categories']:
                        for category_str in category_list:
                                                            # Extrair o número da categoria
                            category_id = extract_category_id(category_str)
                            product_category_data.append((product_id, category_id))
                                #print(product_category_data)
                    insert_query = """
                            INSERT INTO produto_categoria (product_id, category_id)
                            VALUES (%s, %s)
                            ON CONFLICT (product_id, category_id) DO NOTHING;
                            """
                    cur.executemany(insert_query, product_category_data)
                    product_category_data = []
def extrair_itens(arquivo):
    conteudo = arquivo.readlines()
    id = []
    asin = []
    title = []
    group = []
    salesrank = []
    similar = []
    categories = []
    reviews = []
    reviews_details = []
    produto = {}
    produtos = []
    for i in tqdm(range(3, len(conteudo)), desc="Extraindo itens"):
        if conteudo[i].startswith("Id:"):
            id.append(conteudo[i].split("Id:")[1].strip())
        elif conteudo[i].startswith("ASIN:"):
            asin.append(conteudo[i].split("ASIN:")[1].strip())
        elif conteudo[i].strip().startswith("title:"):
            title.append(conteudo[i].split("title:")[1].strip())
        elif conteudo[i].strip().startswith("group:"):
            group.append(conteudo[i].split("group:")[1].strip())
        elif conteudo[i].strip().startswith("salesrank:"):
            salesrank.append(conteudo[i].split("salesrank:")[1].strip())
        elif conteudo[i].strip().startswith("similar:"):
            similar.append(conteudo[i].split("similar:")[1].strip().split()[1:]) # n ta retornando a quantidade ver depois
        elif conteudo[i].strip().startswith("categories:"):
            categories1 = []
        elif conteudo[i].strip().startswith("|"):
                lista = conteudo[i].strip()
                lista = lista.split("|")
                lista = [item.strip() for item in lista if item]  # Remove espaços em branco e itens vazios
                categories.append(lista)
        elif conteudo[i].strip().startswith("reviews:"): 
            parts = conteudo[i].split()
            #print(parts)
            total_index = parts.index('total:') + 1
            downloaded_index = parts.index('downloaded:') + 1
            avg_rating_index = parts.index('avg') + 2  # 'avg' está na frente de 'rating:' e o valor está na frente de 'rating:'

            total = int(parts[total_index])
            downloaded = int(parts[downloaded_index])
            avg_rating = float(parts[avg_rating_index])
            reviews.append(total)
            reviews.append(downloaded)
            reviews.append(avg_rating)
        elif conteudo[i].lstrip()[:4].isdigit():
            reviews_details1 = []
            parts = conteudo[i].split()# Extrair valores
            date = parts[0]
            customer_code = parts[2]
            rating = int(parts[4])
            votes = int(parts[6])
            helpful = int(parts[8])
            reviews_details1.append(date)
            reviews_details1.append(customer_code)
            reviews_details1.append(rating)
            reviews_details1.append(votes)
            reviews_details1.append(helpful)
            reviews_details.append(reviews_details1)

        
        if conteudo[i].strip() == "" or i == len(conteudo)-1:
            produto = {
                'id': id,
                'asin': asin,
                'title': title,
                'group': group,
                'salesrank': salesrank,
                'similar': similar,
                'categories': categories,
                'reviews': reviews,
                'reviews_details': reviews_details,
            }
            produtos.append(produto)
            # Limpe as variáveis após armazenar no dicionário
            id = []
            asin = []
            title = []
            group = []
            salesrank = []
            similar = []
            categories = []
            reviews = []
            reviews_details = []
    return produtos


def inserir_bd(produtos,config):
    for produto in tqdm(produtos, desc="Processando produtos"):
        products(produto,config)
        reviews(produto,config)
        similar(produto,config)
        category(produto,config)
        prodcategory(produto,config)


config = {
    'dbname': 'xxxxx',
    'user': 'xxxxx',
    'password': 'xxxxxx',
    'host': 'xxxxx',
    'port': '54xxxxx32'
        }

arquivo = open("amazon-meta2.txt", "r", encoding="utf8")

produtos = extrair_itens(arquivo)
criar_tabelas(config)
inserir_bd(produtos,config)



