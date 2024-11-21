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
            This function is used to alter table intern
        Parameter:
            None
        Return:
            None
        """
        self.my_cursor.execute('ALTER TABLE intern MODIFY age INT(6)')
        
    def drop_table(self):
        """
        Description:
            This function is used to drop table intern
        Parameter:
            None
        Return:
            None
        """
        self.my_cursor.execute('DROP TABLE intern')

    def insert_into_table(self,values):
        """
        Description:
            Function to insert data into table intern
        Parameter:
            values: The values to be inserted into the table intern
        Return:
            None
        """
        try:
            insert_query = "INSERT INTO intern(name,role,age,doj)VALUES(%s,%s,%s,%s)"
            self.my_cursor.execute(insert_query,values)
            self.conn.commit()
            print("Record Successfully Inserted")
        except Exception as e:
            print(f"Raised Exception : {e}")

    def select_from_table(self):
        """
        Description:
            Function to display intern's table data
        Parameter:
            None
        Return:
            None
        """
        try:
            self.my_cursor.execute('select * from intern')
            rows = self.my_cursor.fetchall()
            for row in rows:
                print(row)
        except Exception as e:
            print(f"Raised Exception : {e}")

    
    def update_table(self,id):
        """
        Description:
            Function is used to update intern's table 
        Parameter:
            id: The id of intern to be updated
        Return:
            None
        """
        sql_query = "select * from intern where id=%s"
        val = (id,)
        try:
            self.my_cursor.execute(sql_query,val)
            row = self.my_cursor.fetchall()
            for col in row:
                name = col[1]
                role = col[2]
                age = col[3]
                doj = col[4]
            choice = int(input(f"1.Update your name\n2.Update your role\n3.Update your age\nEnter your choice : "))
            if choice == 1:
                name = input("Enter name : ")
            elif choice ==2:
                role = input("Enter role : ")
            elif choice == 3:
                age = input("Enter age: ")
            else:
                print("Invalid Input")

            update_query = "Update intern set name=%s,role=%s,age=%s where id=%s"
            value = (name,role,age,id)
            try:
                self.my_cursor.execute(update_query,value)
                self.conn.commit()
                log.info("Records Updated")
            except Exception as e:
                log.info(f"Raised Exception : {e}")
        except Exception as e:
            log.info(f"Raised Exception : {e}")

    def delete_table_row(self,name):
        """
        Description:
            Function to delete an existing table data
        Parameter:
            name: The name of intern based on which the data is to be deleted
        Return:
            None
        """
        delete_query = "delete from intern where name=%s"
        try:
            self.my_cursor.execute(query,name)
            self.conn.commit()
            log.info(f"Successfully deleted")
        except Exception as e:
            log.info(f"Raised Exception : {e}")

if __name__=="__main__":

    crud_obj = Crud()

    while True:
        try:
            print("Perform Crud Operations !!")
            print("1. Create database")
            print("2. Show Databases")
            print("3. Use Database")
            print("4. Drop Database")
            print("5. Create Table")
            print("6. Show Tables")
            print("7. Alter Interns table column Age")
            print("8. Drop table interns")
            print("9. Insert values in interns table")
            print("10. Display interns table")
            print("11. Update interns table by id")
            print("12. Delete interns table row")
            print("13. Exit")

            value = int(input("Enter Value: "))
            if value==1:
                crud_obj.create_db(input('Database Name: '))
            elif value==2:
                crud_obj.show_db()
            elif value==3:
                db_name = input("Enter DB name: ")
                crud_obj.use_db(db_name)
            elif value==4:
                db_name = input("Enter DB name: ")
                crud_obj.drop_db(db_name)
            elif value==5:
                crud_obj.create_table()
            elif value==6:
                crud_obj.show_tables()
            elif value==7:
                crud_obj.alter_column()
            elif value==8:
                crud_obj.drop_table()
            elif value==9:
                name = input("Enter name: ")
                role = input("Enter role: ")
                age = int(input("Enter age: "))
                doj = input("ENter DOJ: ")
                values = (name,role,age,doj)

                crud_obj.insert_into_table(values)
            elif value==10:
                crud_obj.select_from_table()

            elif value==11:
                id = int(input("Enter id to modify data: "))
                crud_obj.update_table(id)
            elif value ==12:
                name = input("Enter name of intern: ")
                crud_obj.delete_table_row(name)
            elif value ==13:
                break
            else:
                    print("None")

        except Exception as e:
            log.info(f'Exception: {e}')

