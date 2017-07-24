# 01_create_db_tables.py
import sqlite3

# conectando...
conn = sqlite3.connect('desafio-luizalabs.db')
# definindo um cursor
cursor = conn.cursor()

# criando as tabelas
cursor.execute("""
CREATE TABLE analysis_request (
        sale_id INTEGER NOT NULL PRIMARY KEY,
        order_id TEXT NOT NULL,
        category TEXT NOT NULL,
        sub_category TEXT NOT NULL,
        brand TEXT NOT NULL,
        selling_price DECIMAL(10.5) NOT NULL,
        order_date DATETIME NOT NULL
);
""")

cursor.execute("""
CREATE TABLE analysis_email (
        event_id TEXT NOT NULL PRIMARY KEY,
        event_type TEXT NOT NULL
);
""")

print('Tabelas criadas com sucesso.')
# desconectando...
conn.close()