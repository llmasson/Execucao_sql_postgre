import psycopg2
import pandas as pd
import csv
import os


def ler_arquivo_csv(nome_arquivo):
    """
    Lê o arquivo CSV contendo informações das conexões PostgreSQL.
    Retorna uma lista de dicionários com as configurações de cada banco.
    """
    conexoes = []
    try:
        with open(nome_arquivo, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file, delimiter=";")
            for row in reader:
                conexoes.append(row)
    except Exception as e:
        print(f"Erro ao ler o arquivo CSV: {e}")
    return conexoes


def ler_query_sql(diretorio_sql):
    """
    Lê todos os arquivos .sql no diretório especificado e retorna um dicionário com o conteúdo.
    """
    sql_queries = {}
    try:
        for arquivo in os.listdir(diretorio_sql):
            if arquivo.endswith(".sql"):
                caminho_arquivo = os.path.join(diretorio_sql, arquivo)
                with open(caminho_arquivo, "r", encoding="utf-8") as f:
                    sql_queries[arquivo] = f.read()
    except Exception as e:
        print(f"Erro ao ler os arquivos SQL: {e}")
    return sql_queries


def executar_query(conexao, query):
    """
    Executa a query no banco PostgreSQL e retorna os resultados.
    """
    try:
        with psycopg2.connect(
                host=conexao["ip"],
                database=conexao["schema"],
                user=conexao["usuario"],
                password=conexao["senha"],
                port=conexao["porta"]
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                colunas = [desc[0] for desc in cur.description]  # Obtém os nomes das colunas
                dados = cur.fetchall()
                return [dict(zip(colunas, linha)) for linha in dados]
    except Exception as e:
        print(f"Erro ao conectar/executar query na base {conexao['schema']}: {e}")
        return None


def salvar_resultado_csv(dados, nome_arquivo_saida):
    """
    Salva os dados extraídos no formato CSV.
    """
    try:
        df = pd.DataFrame(dados)
        df.to_csv(nome_arquivo_saida, index=False, sep=";", encoding="utf-8")
        print(f"Arquivo {nome_arquivo_saida} gerado com sucesso!")
    except Exception as e:
        print(f"Erro ao salvar o arquivo CSV: {e}")


def main():
    """
    Função principal que executa o fluxo de leitura, conexão e extração.
    """
    arquivo_conexao = "lista_de_bases.csv"  # Nome do arquivo com conexões
    diretorio_sql = "sql/"  # Pasta onde estão os arquivos .sql
    nome_arquivo_saida = "resultado_extracao.csv"  # Nome do arquivo de saída

    conexoes = ler_arquivo_csv(arquivo_conexao)
    queries = ler_query_sql(diretorio_sql)

    resultados = []

    for conexao in conexoes:

        for nome_sql, query in queries.items():
            print(f"Executando {nome_sql} na base {conexao['schema']}...")
            resultado = executar_query(conexao, query)
            if resultado:
                for linha in resultado:
                    #linha["base"] = conexao["schema"]  # Adiciona a origem do dado
                    #linha["arquivo_sql"] = nome_sql  # Adiciona a query usada
                    resultados.append(linha)

    if resultados:
        salvar_resultado_csv(resultados, nome_arquivo_saida)
    else:
        print("Nenhum dado extraído.")


if __name__ == "__main__":
    main()

"""conexao = ler_query_sql('sql/')
print(conexao)"""