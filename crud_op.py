'''

    @Author: Sudhanshu Kumar
    @Date: 19-11-2024 
    @Last Modified by: Sudhanshu Kumar
    @Last Modified time: 19-11-2024
    @Title : Crud Operation in Mysql

    
'''

import mysql.connector
import logger

log = logger.logger_init('crud_op')

class Crud:
    def __init__(self):
        try:
            self.conn = mysql.connector.connect(
                host = 'localhost',
                user = 'root',
                password = 'root'
            )
            self.my_cursor = self.conn.cursor()
        except Exception as e:
            log.info('MySQL exception while accessing local database')


    def create_db(self, db_name):
        """
        Description:
            Function to create database
        Parameters:
            db_name: Name of database to be created
        Returns: 
            None
        """
        list_db = self.show_db()
        if db_name in list_db:
            log.info(f"{db_name} database already present!!")
        else:
            self.my_cursor.execute(f'CREATE DATABASE {db_name}')
            log.info(f'Database {db_name} created !!!')

    def show_db(self):
        """
        Description:
            Function to show all databases
        Parameters:
            None
        Returns: 
            list of databases
        """
        self.my_cursor.execute('SHOW DATABASES')
        list_db = self.my_cursor.fetchall()
        
        for db in list_db:
            log.info(db)
        
        return list_db

    def use_db(self, db_name):
        """
        Description:
            Function to decide which database to be used 
        Parameters:
            db_name: name of database to be used
        Returns: 
            None
        """
        self.my_cursor.execute(f'USE {db_name}')
        log.info(f'Using {db_name} database')

    def drop_db(self,db_name):
        """
        Description:
            Function to decide which database to be dropped
        Parameters:
            db_name: name of database to be dropped
        Returns: 
            None
        """
        self.my_cursor.execute('SHOW DATABASES')
        list_db = self.my_cursor.fetchall()
        if db_name in list_db:
            self.my_cursor.execute(f'DROP DATBASE {db_name}')
            log.info(f'{db_name} dropped successfully!!')
        else:
            log.info(f'{db_name} database not found!!')

    def create_table(self):
        """
        Description:
            This function is used to create a table
        Parameter:
            None
        Return:
            None
        """
        self.my_cursor.execute('CREATE TABLE interns (id int auto_increment primary key, name varchar(50), role varchar(50), age int not null, doj date)')

    def show_tables(self):
        """
        Description:
            This function is used to show the list of tables present in the database in use 
        Parameter:
            None
        Return:
            list of tables present in a database
        """
        self.my_cursor.execute('SHOW TABLES')
        list_tables = self.my_cursor.fetchall()
        log.info(list_tables)
        return list_tables

    def alter_column(self):
        """
        Description:
            This function is used to alter table student 
        Parameter:
            None
        Return:
            None
        """
        self.my_cursor.execute('ALTER TABLE interns MODIFY age INT(6)')
        

if __name__=="__main__":

    crud_obj = Crud()

    try:
        print("Perform Crud Operations !!")
        print("1. Create database")
        print("2. Show Databases")
        value = int(input("Enter Value: "))
        if value==1:
                crud_obj.create_db(input('Database Name: '))
        elif value==2:
                crud_obj.show_db()
        else:
                print("None")

    except Exception as e:
        log.info(f'Exception: {e}')

