[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/zixaop7v)



# Configuração do Ambiente

Para garantir que o projeto funcione corretamente, siga os passos abaixo para ajustar as configurações no seu computador.

## 1. Configuração do Banco de Dados

No arquivo `tp1_3.2.py` (e possivelmente no `tp1_3.3.py`), localize o dicionário `config`:

```python
config = { 
    'dbname': 'xxxxx', 
    'user': 'xxxxx', 
    'password': 'xxxxx', 
    'host': 'xxxxx', 
    'port': 'xxxxx' 
}
```
Você deve substituir 'xxxxx' com as informações corretas do banco de dados do seu computador, como nome do banco de dados (dbname), nome de usuário (user), senha (password), endereço (host) e porta (port).

## 2. Configuração do arquivo de entrada amazon-meta.txt

Alteração do nome do arquivo: No  `tp1_3.2.py`, existe a linha:

```python
arquivo = open("amazon-meta2.txt", "r", encoding="utf8")
```
Substitua "amazon-meta2.txt" pelo nome do arquivo .txt que você deseja processar para extrair os itens.
