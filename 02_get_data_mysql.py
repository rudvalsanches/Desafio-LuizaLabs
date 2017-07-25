#coding: utf-8
import os, sys
import MySQLdb
import sqlite3

#Conectando na base de origem Mysql
try:
    db = MySQLdb.connect(
        host='big-data-analytics-test.cd2vfjltihkr.us-east-1.rds.amazonaws.com',
        user='desafio',
        passwd='1i450U',
        db='bigdata_desafio'
    )
except Exception as e:
    sys.exit("Não foi possivel conectar com o banco de dados")

#Coletando informações da base de origem Mysql
cursor = db.cursor()
cursor.execute('''select id as id_sale, o.order_id, p.category, p.sub_category, p.brand, cast(oi.selling_price as CHAR) as selling_price, o.order_date, o.status
                    from orderitem oi
                    inner join products p on p.product_id = oi.product_id
                    inner join orders o on o.order_id = oi.order_id'''
               )

result = cursor.fetchall()

#Connectando no banco de dados local
conn = sqlite3.connect('desafio-luizalabs.db')

# definindo um cursor
c = conn.cursor()
#Abrindo a transação
c.execute('begin')
#Inserindo dados necessarios para alinse, retirado da base de origem Mysql
c.executemany('INSERT INTO analysis_request VALUES (?,?,?,?,?,?,?,?)', result)
#Commit do insert
c.execute('commit')

#Conferindo quandos dados foram inseridos.
c1 = conn.cursor()
sql = "SELECT count(*) FROM analysis_request"
c1.execute(sql)
print c1.fetchall()