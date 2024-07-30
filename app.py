from flask import Flask, jsonify, request
import psycopg2
from psycopg2 import sql
import json
from datetime import datetime, date, time
from credenciais import credencial

app = Flask(__name__)

# Função para estabelecer uma conexão com o banco de dados PostgreSQL
def conectar():
    try:
        conexao = psycopg2.connect(**credencial)
        return conexao
    except psycopg2.Error as e:
        print(f"Erro: Não foi possível conectar ao banco de dados\n{e}")
        return None

#inserir teste
@app.route('/teste', methods=['POST'])
def inserir_teste():
    conexao = conectar()
    if conexao:
        try:
            # parametros
            tabela = 'conexao_internet'
            data = request.json['data']
            hora = request.json['hora']
            ping = request.json['ping']
            download = request.json['download']
            upload = request.json['upload']

            cursor = conexao.cursor()

            # verificar se o registro já existe
            cursor.execute(f"SELECT EXISTS (SELECT 1 FROM {tabela} WHERE data = %s AND hora = %s)", (data, hora))
            exists = cursor.fetchone()[0]
            if exists:
                return {"message": "não gravado. registro já existe"}, 200
            
            # faz a inserção dos dados
            query_inserir_dados = sql.SQL(f'''
                INSERT INTO {tabela} (data, hora, ping, download, upload)
                VALUES (%s, %s, %s, %s, %s) RETURNING id;
            ''')
            cursor.execute(query_inserir_dados, (data, hora, ping, download, upload))
            id_teste = cursor.fetchone()[0]
            conexao.commit()
            return {"id": id_teste}, 200
        except psycopg2.Error as e:
            return {"message": "nao foi possivel incluir"}, 500
        finally:
            cursor.close()
            conexao.close()

#buscar dados
@app.route('/dados', methods=['GET'])
def get_dados():
    conexao = conectar()
    if conexao:
        try:
            # parametros
            tabela = 'conexao_internet'

            cursor = conexao.cursor()
            consulta = f'''
                SELECT data, hora, ping, download, upload 
                FROM {tabela};
            '''
            cursor.execute(consulta)
            # Obtém todos os resultados
            results = cursor.fetchall()

            # Se não houver resultados, retorna uma lista vazia
            if not results:
                return jsonify([]), 200

            # Obtém os nomes das colunas
            columns = [desc[0] for desc in cursor.description]

            # Converte os resultados para uma lista de dicionários
            result_dicts = []
            for row in results:
                row_dict = {}
                for col, val in zip(columns, row):
                    # Converte tipos não serializáveis para strings
                    if isinstance(val, (date, time)):
                        row_dict[col] = val.isoformat()
                    else:
                        row_dict[col] = val
                result_dicts.append(row_dict)

            return jsonify(result_dicts), 200
        except psycopg2.Error as e:
            return jsonify({"message": "Erro no servidor"}), 500
        finally:
            cursor.close()
            conexao.close()

#contagem de testes na tabela
@app.route('/contagem-testes', methods=['GET'])
def contar_testes():
    conexao = conectar()
    if conexao:
        try:
            # parametros
            tabela = 'conexao_internet'
            
            cursor = conexao.cursor()
            consulta = f'''
                SELECT COUNT(*) AS contagem FROM {tabela};
            '''
            cursor.execute(consulta)
            # Obtém o resultado
            result = cursor.fetchone()

            # Se não houver resultado, retorna None
            if result is None:
                return None

            # Obtém os nomes das colunas
            columns = [desc[0] for desc in cursor.description]

            # Cria um dicionário para a linha e converte para JSON
            row_dict = dict(zip(columns, result))

            return json.dumps(row_dict, indent=2, ensure_ascii=False), 200
            
        except psycopg2.Error as e:
            {"message": "nao foi possivel efetuar a contagem"}, 500
        finally:
            cursor.close()
            conexao.close()

#executar app
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)