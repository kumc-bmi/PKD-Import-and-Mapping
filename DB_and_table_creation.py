import sqllite3
from os import path,join

class UserTableRequired:
    def __init__(self, table_name: str, columns: dict(), primary_col):
        self.tb_name = table_name
        # The columns recieve the column name as the key and
        # a list of ['type', 'constraints'] for the column
        # as value        
        self.columns= columns   
    
    def extract_tb_name(self):
        return tb_name.encoding('utf8')
    
    def extract_tb_columns(self):
        return columns
        
class Sqlite3Db:
    def __init__(self, db_name: str) -> None:
        self.db_name = db_name                  

    def sqllite3_insert_table(self, table: UserTableRequired):
        connection = sqlite3.connect(db_name)
        db_cursor = connection.cursor()
        
        SQL_statement_insert_1 = "CREATE TABLE IF NOT EXISTS {} (".format(table.extract_tb_name())  
        SQL_statement_insert_2 = ""
        
        for col in table.extract_tb_columns().keys():
            constraints_collection = ""
            for constraints in table.extract_tb_columns()[col]
                constraints_collection = constraints_collection + " " + constraints
            SQL_statement_insert_2 = SQL_statement_insert_2 + col + " " + constraints_collection + ","
            
        SQL_statement_insert_3 = ");"
        
        SQL_final_insert_statement = SQL_statement_insert_1 + SQL_statement_insert_2 +SQL_statement_insert_3
        db_cursor.execute(SQL_final_insert_statement)
        cursor.close()
        
        except sqlite3.Error as error:
            print("Error while creating a sqlite3 table: {} while creating table {}".format(error, table.extract_tb_name()))
        finally:
            if connection:
                connection.close()
        print("sqlite connection is closed")
    
    return 


c.execute('''
          CREATE TABLE IF NOT EXISTS products
          ([product_id] INTEGER PRIMARY KEY, [product_name] TEXT)
          ''')
          
c.execute('''
          CREATE TABLE IF NOT EXISTS prices
          ([product_id] INTEGER PRIMARY KEY, [price] INTEGER)
          ''')
                     
conn.commit()
