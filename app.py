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
            servidor = request.json['server']
            ping = request.json['ping']
            download = request.json['download']
            upload = request.json['upload']
            tipo = request.json['tipo']

            cursor = conexao.cursor()

            # verificar se o registro já existe
            cursor.execute(f"SELECT EXISTS (SELECT 1 FROM {tabela} WHERE data = %s AND hora = %s)", (data, hora))
            exists = cursor.fetchone()[0]
            if exists:
                return {"message": "não gravado. registro já existe"}, 200
            
            # faz a inserção dos dados
            query_inserir_dados = sql.SQL(f'''
                INSERT INTO {tabela} (data, hora, servidor, ping, download, upload, tipo)
                VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id;
            ''')
            cursor.execute(query_inserir_dados, (data, hora, servidor, ping, download, upload, tipo))
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
                SELECT data, hora, servidor, ping, download, upload, tipo
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
            consulta = f'SELECT COUNT(*) AS contagem FROM {tabela};'
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

# maior data
@app.route('/max', methods=['GET'])
def get_max_data():
    conexao = conectar()
    if conexao:
        try:
            # parametros
            tabela = 'conexao_internet'
            
            cursor = conexao.cursor()
            consulta = f'SELECT cast(max(data) as text) AS data FROM {tabela};'
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

# Média mensal
@app.route('/media-mes', methods=['GET'])
def media_mes():
    conexao = conectar()
    if conexao:
        try:
            # parametros
            tabela = 'conexao_internet'

            cursor = conexao.cursor()
            consulta = f'''
                SELECT to_char(data
                    , 'yyyy-mm') AS ano_mes
                    , to_char(data, 'yyyy') AS ano
                    , to_char(data, 'mm') AS mes
                    , servidor
                    , avg(ping) AS ping
                    , avg(download) AS download
                    , avg(upload) AS upload
                    , count(data) AS qtd_testes
                FROM {tabela}
                GROUP BY to_char(data, 'yyyy-mm'), to_char(data, 'yyyy'), to_char(data, 'mm'), servidor;
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

# testes no mes atual
@app.route('/testes-mes-atual', methods=['GET'])
def testes_mes_atual():
    conexao = conectar()
    if conexao:
        try:
            # parametros
            tabela = 'conexao_internet'

            cursor = conexao.cursor()
            consulta = f'''
                SELECT data
                    , to_char(data, 'yyyy') AS ano
                    , to_char(data, 'mm') AS mes
                    , servidor
                    , ping
                    , download
                    , upload
                FROM {tabela}
                WHERE to_char(data, 'yyyy-mm') = (select to_char(max(data), 'yyyy-mm') FROM {tabela});
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

# agregação máximo e mínimo
@app.route('/agg', methods=['GET']) #/agg?t=download&a=max&p=atual
def get_agg():
    conexao = conectar()

    if conexao:
        try:
            # parametros
            tabela = 'conexao_internet'

            teste = request.args.get('t').lower()
            agg = request.args.get('a').lower()

            try:
                periodo = request.args.get('p').lower()
                periodo = f"AND to_char(data, 'yyyy-mm') = (select to_char(max(data), 'yyyy-mm') FROM {tabela})"
            except:
                periodo = f""

            cursor = conexao.cursor()
            consulta = f'''
                SELECT data
                    , hora
                    , to_char(data, 'yyyy') AS ano
                    , to_char(data, 'mm') AS mes
                    , servidor
                    , ping
                    , download
                    , upload
                FROM {tabela}
                WHERE download = (select {agg}({teste}) FROM {tabela})
                {periodo}
                LIMIT 1;
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

# executar app
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)