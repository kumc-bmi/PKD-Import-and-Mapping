import sqllite3
from os import path,join
from logging_module import startlogging

class UserTableRequired:
    def __init__(self, table_name: str, columns: dict(), primary_col, startlogging):
        '''
        Constructor for userTableRequirement used for 
        SQLlite3 DB in class Sqlite3Db
        '''
        self.tb_name = table_name        
        self.columns= columns
        startlogging(level=debug)   
    
    def extract_tb_name(self):
        '''
        for Extraction of table name of a table
        created with UserTableRequired Class
        '''
        logging.info("extract table name called")
        return tb_name.encoding('utf8')
    
    def extract_tb_columns(self):
        '''
        for Extraction of table columns of a table
        created with UserTableRequired Class with
        '''
        logging.info("extract table columns details called")
        return columns
        
class Sqlite3Db:
    def __init__(self, db_name: str, startlogging) -> None:
        self.db_name = db_name
        startlogging(level="debug")

    def sqllite3_insert_table(self, table: UserTableRequired):
        logging.info("Triggerd the insert table for table {}".format(table.extract_tb_name()))
        
        connection = sqlite3.connect(db_name)
        db_cursor = connection.cursor()
        logging.info("Connection to {} SQLLite Database is done with cursor initiation".format(db_name))
        
        SQL_statement_insert_1 = "CREATE TABLE IF NOT EXISTS {} (".format(table.extract_tb_name())  
        SQL_statement_insert_2 = ""
        
        for col in table.extract_tb_columns().keys():
            constraints_collection = ""
            for constraints in table.extract_tb_columns()[col]
                constraints_collection = constraints_collection + " " + constraints
            SQL_statement_insert_2 = SQL_statement_insert_2 + col + " " + constraints_collection + ","
        logging.info("The colums string has been made")
                    
        SQL_statement_insert_3 = ");"
        
        SQL_final_insert_statement = SQL_statement_insert_1 + SQL_statement_insert_2 +SQL_statement_insert_3
        logging.info("SQL statement for the insert table {} creared into {} \n".format(table.extract_tb_name(),db_name))
        logging.info("The SQL statement is {}".format(SQL_final_insert_statement))
        
        db_cursor.execute(SQL_final_insert_statement)
        connection.commit()
        cursor.close()
        logging.info("INSERT of table {} done to {} and cursor closed".format(table.extract_tb_name(),db_name))
        
        except sqlite3.Error as error:
            logging.error("Error while creating a sqlite3 table: {} while creating table {}".format(error, table.extract_tb_name()))
        finally:
            if connection:
                connection.close()
        logging.info("sqlite connection to db {} is closed".format(db_name))
    
    return

    def sqllite3_insert_data(self):
        '''
        Here you have to insert data into aspecific table
        by sending the data in form of a list or list of lists
        where theelements need to be in the same order as the 
        column dict used to create the table in sqllite3_insert_table
        class method
        '''
        
        
c.execute('''
          CREATE TABLE IF NOT EXISTS products
          ([product_id] INTEGER PRIMARY KEY, [product_name] TEXT)
          ''')
          
c.execute('''
          CREATE TABLE IF NOT EXISTS prices
          ([product_id] INTEGER PRIMARY KEY, [price] INTEGER)
          ''')
                     
conn.commit()
