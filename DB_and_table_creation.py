import sqllite3

def sqllite3_create_db():
    conn = sqlite3.connect('pkd_code_db')
    db_cursor = conn.cursor()
    
    db_cursor.execute('''CREATE TABLE IF NOT EXISTS file_recieved()''')

def sqllite3_connection_db(db_name):
    conn = sqlite3.connect(db_name)
    return conn.cursor()


c.execute('''
          CREATE TABLE IF NOT EXISTS products
          ([product_id] INTEGER PRIMARY KEY, [product_name] TEXT)
          ''')
          
c.execute('''
          CREATE TABLE IF NOT EXISTS prices
          ([product_id] INTEGER PRIMARY KEY, [price] INTEGER)
          ''')
                     
conn.commit()
