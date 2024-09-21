import psycopg2
from tqdm import tqdm
import re
import time

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
                
                for command in commands:
                    cur.execute(command)
                
                conn.commit()
                print("Tabelas criadas com sucesso!")

    except (psycopg2.DatabaseError, Exception) as error:
        print(f"Erro ao criar tabelas: {error}")

def extract_category_id(category_str):
    
    match = re.search(r'\[(\d+)\]', category_str)
    if match:
        return match.group(1) 
    return None 
def products(produto, config):
    products_data = []

    data_dict = produto
    itens_nao_vazios = {k: v for k, v in data_dict.items() if v != []}
    quantidade_itens = len(itens_nao_vazios)

    
    if quantidade_itens == 2:
        products_data = list(zip(data_dict['id'], data_dict['asin']))
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

 
    for i in range(len(products_data)):
        if len(products_data[i]) < 8:
            products_data[i] += (None,) * (8 - len(products_data[i]))

    return products_data


def reviews(produto, config):
    review_details_data = []
    customer_ids = set()
    
    data_dict = produto
    product_id = data_dict['id'][0]  

  
    for review in data_dict['reviews_details']:
        if len(review) == 5:
            review_date, user_id, rating, helpfulness, total_votes = review
            review_details_data.append((
                product_id, 
                review_date, 
                user_id, 
                rating, 
                helpfulness, 
                total_votes
            ))
            customer_ids.add(user_id)

    return review_details_data, customer_ids




def similar(produto, config):
    similar_products_data = []
    
    data_dict = produto

  
    for i, similar_asins in enumerate(data_dict['similar']):
        if len(similar_asins) > 1 and i < len(data_dict['asin']):
            for similar_asin in similar_asins:
                similar_products_data.append((data_dict['asin'][i], similar_asin))

    return similar_products_data



def category(produto, config):
    category_data = []
    
    data_dict = produto

    
    for category_list in data_dict['categories']:
        for category in category_list:
            category_id = extract_category_id(category)
            if category_id:
                category_name = category.split('[')[0].strip()
                category_data.append((int(category_id), category_name, None))

    return category_data


def prodcategory(produto, config):
    product_category_data = []
    
    data_dict = produto

    
    for product_id in data_dict['id']:
        for category_list in data_dict['categories']:
            for category_str in category_list:
                category_id = extract_category_id(category_str)
                if category_id:  
                    product_category_data.append((product_id, category_id))

    return product_category_data

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
            similar.append(conteudo[i].split("similar:")[1].strip().split()[1:])
        elif conteudo[i].strip().startswith("categories:"):
            categories1 = []
        elif conteudo[i].strip().startswith("|"):
                lista = conteudo[i].strip()
                lista = lista.split("|")
                lista = [item.strip() for item in lista if item]  
                categories.append(lista)
        elif conteudo[i].strip().startswith("reviews:"): 
            parts = conteudo[i].split()

            total_index = parts.index('total:') + 1
            downloaded_index = parts.index('downloaded:') + 1
            avg_rating_index = parts.index('avg') + 2 

            total = int(parts[total_index])
            downloaded = int(parts[downloaded_index])
            avg_rating = float(parts[avg_rating_index])
            reviews.append(total)
            reviews.append(downloaded)
            reviews.append(avg_rating)
        elif conteudo[i].lstrip()[:4].isdigit():
            reviews_details1 = []
            parts = conteudo[i].split()
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


def inserir_bd(produtos, config):
    all_products_data = []
    all_similar_products_data = []
    all_review_details_data = []
    all_customer_ids = set()
    all_category_data = []
    all_product_category_data = []

    for produto in tqdm(produtos, desc="Processando produtos"):
        product_data = products(produto, config)
        all_products_data.extend(product_data)

        # Coletar produtos similares
        similar_data = similar(produto, config)
        all_similar_products_data.extend(similar_data)

        # Coletar detalhes das avaliações e IDs dos clientes
        review_data, customer_ids = reviews(produto, config)
        all_review_details_data.extend(review_data)
        all_customer_ids.update(customer_ids)

        # Coletar categorias
        category_data = category(produto, config)
        all_category_data.extend(category_data)

        # Coletar associações entre produtos e categorias
        product_category_data = prodcategory(produto, config)
        all_product_category_data.extend(product_category_data)

    
    insert_query = """
        INSERT INTO produtos (product_id, asin, title, product_group, salesrank, review_total, review_downloaded, review_avg)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (product_id) DO NOTHING;
    """

    insert_similar_query = """
        INSERT INTO produtos_similares (product_asin, similar_asin)
        VALUES (%s, %s)
        ON CONFLICT (product_asin, similar_asin) DO NOTHING;
    """

    insert_review_query = """
        INSERT INTO avaliacoes (product_id, review_date, customer_id, rating, votes, helpful)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (product_id, customer_id, review_date) DO NOTHING;
    """

    insert_cliente_query = """
        INSERT INTO cliente (customer_id)
        VALUES (%s)
        ON CONFLICT (customer_id) DO NOTHING;
    """

    insert_category_query = """
        INSERT INTO categoria (category_id, name, parent_id)
        VALUES (%s, %s, %s)
        ON CONFLICT (category_id) DO NOTHING;
    """

    insert_product_category_query = """
        INSERT INTO produto_categoria (product_id, category_id)
        VALUES (%s, %s)
        ON CONFLICT (product_id, category_id) DO NOTHING;
    """
    print("INSERINDO OS VALORES NAS TABELAS")
    with psycopg2.connect(**config) as conn:
        with conn.cursor() as cur:
           
            if all_products_data:
                start_time = time.time()
                try:
                    cur.executemany(insert_query, all_products_data)
                    elapsed_time = time.time() - start_time
                    print(f"Inserção de produtos concluída com sucesso em {elapsed_time:.2f} segundos.")
                except Exception as e:
                    print(f"Erro ao inserir produtos: {e}")

            
            if all_similar_products_data:
                start_time = time.time()
                try:
                    cur.executemany(insert_similar_query, all_similar_products_data)
                    elapsed_time = time.time() - start_time
                    print(f"Inserção de produtos similares concluída com sucesso em {elapsed_time:.2f} segundos.")
                except Exception as e:
                    print(f"Erro ao inserir produtos similares: {e}")

            
            if all_review_details_data:
                start_time = time.time()
                try:
                    cur.executemany(insert_review_query, all_review_details_data)
                    elapsed_time = time.time() - start_time
                    print(f"Inserção de detalhes dos reviews concluída com sucesso em {elapsed_time:.2f} segundos.")
                except Exception as e:
                    print(f"Erro ao inserir detalhes dos reviews: {e}")

            
            if all_customer_ids:
                start_time = time.time()
                try:
                    cur.executemany(insert_cliente_query, [(customer_id,) for customer_id in all_customer_ids])
                    elapsed_time = time.time() - start_time
                    print(f"Inserção de dados dos clientes concluída com sucesso em {elapsed_time:.2f} segundos.")
                except Exception as e:
                    print(f"Erro ao inserir dados dos clientes: {e}")

            
            if all_category_data:
                start_time = time.time()
                try:
                    cur.executemany(insert_category_query, all_category_data)
                    elapsed_time = time.time() - start_time
                    print(f"Inserção de categorias concluída com sucesso em {elapsed_time:.2f} segundos.")
                except Exception as e:
                    print(f"Erro ao inserir categorias: {e}")

            
            if all_product_category_data:
                start_time = time.time()
                try:
                    cur.executemany(insert_product_category_query, all_product_category_data)
                    elapsed_time = time.time() - start_time
                    print(f"Inserção de associações entre produtos e categorias concluída com sucesso em {elapsed_time:.2f} segundos.")
                except Exception as e:
                    print(f"Erro ao inserir associações entre produtos e categorias: {e}")

        conn.commit()


 




config = {
    'dbname': 'xxxxx',
    'user': 'xxxxx',
    'password': 'xxxxx',
    'host': 'xxxxx',
    'port': 'xxxxx'
        }

arquivo = open("amazon-meta.txt", "r", encoding="utf8")

produtos = extrair_itens(arquivo)
criar_tabelas(config)
inserir_bd(produtos,config)
print("FINALIZADO")
