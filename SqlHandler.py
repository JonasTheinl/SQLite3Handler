import sqlite3
import os.path
import logging

class Sqlite3Handler:

    def __init__(self, path):
        self.__path = path
        self.__con = None
        #helper Strings
        __a = ','
        __b = 'SELECT {} FROM {}'
        __c = 'ALTER TABLE '

    #Method to connect to a database. If the database doesn't exists, it will create one.
    def connect(self):
        try:
            self.__con = sqlite3.connect(self.__path)
        except:
            logging.error("Not possible to connect to database")
        return self.__con

    #Method to disconnect from the database
    def disconnect(self):
        self.__con.close()

        #Method to create a table
    #Example: createTable("Infos", {
    #    "date":"text", 
    #    "name":"text", 
    #    "email":"text",
    #    "address":"varchar(500)"})
    #-------------Result String which gets executed-----------
    #CREATE TABLE Infos (date text,name text,email text,address varchar(500))
    def createTable(self, tablename, data):
        A=[]
        for B in data:
            A.append(B+' '+data[B])
        C=self.__a.join(A)
        D='CREATE TABLE '+tablename+' ({})'
        E=D.format(C)
        F=self.__con.cursor()
        F.execute(E)


    #Method to drop a table in the database
    def dropTable(self, tablename):
        A=self.__con.cursor()
        B='DROP TABLE {}'
        A.execute(B.format(tablename))
        self.__con.commit()

    
    #Method to execute query
    def query(self, query):
        A = self.__con.cursor()
        A.execute(query)
        self.__con.commit()


    #Method to select items from table
    #Example: selectWhere("details",{"name","email"},("county","india"))
    #-------------Result String which gets executed-----------
    #SELECT name, email FROM details WHERE country='india'
    def select(self, tablename, data):
        B=[]
        for D in data:
            B.append(D)
        E=self.__a.join(B)
        C=self.__con.cursor()
        A=self.__b
        A=A.format(E,tablename)
        C.execute(A)
        return C.fetchall()


    #Method to select complete table
    def selectAll(self, tablename):
        B=self.__con.cursor()
        A=self.__b
        A=A.format('*', tablename)
        B.execute(A)
        return B.fetchall()


    #Method to select item with conditions
    def selectWhere(self, tableName,y,z):
        B=[]
        for D in y:
            B.append(D)
        E=self.__a.join(B)
        C=self.__con.cursor()
        if type(z[1]) == str:
            A="SELECT {} FROM {} WHERE {}='{}'"
        else:
            A='SELECT {} FROM {} WHERE {}={}'
        A=A.format(E,tableName,z[0],z[1])
        C.execute(A)
        return C.fetchall()


    #Method to delete table from database
    def dropTable(self, tableName):
        A=self.__con.cursor()
        B='DROP TABLE {}'
        A.execute(B.format(tableName))
        self.__con.commit()


    #Method to add a column to a table
    def addColumn(self, tableName, data):
        A=[]
        for B in data:
            A.append(B)
            A.append(data[B])
        C=self.__c+tableName+' ADD {} {}'
        D=C.format(A[0],A[1])
        E=self.__con.cursor()
        E.execute(D)
        self.__con.commit()


    #Method to modify a table 
    #modifyColumn([con,"table_name"],{
    #    "column_name":"new_data_type"
    #})
    def modifyColumn(self, tableName, data):
        A=[]
        for B in data:
            A.append(B)
            A.append(data[B])
        C=self.__c+tableName+' MODIFY {} {}'
        D=C.format(A[0],A[1])
        E=self.__con.cursor()
        E.execute(D)
        self.__con.commit()


    #Method to drop a column of a table
    #dropColumn([con,"table_name"],"column_name")
    def dropColumn(self, tableName, data):
        A='ALTER TABLE {} DROP COLUMN {}'
        A=A.format(tableName,data)
        B=self.__con.cursor()
        B.execute(A)
        self.__con.commit()


    #Method to insert data in a table
    #data =  {
    #    "db_column":"Data for Insert",
    #    "db_column":"Data for Insert"
    #}
    #insert([con,"table_name"],data)
    def insert(self, tableName, data):
        J='"'
        A=[]
        B=[]
        C=[]
        for D in data:
            A.append(D)
            B.append(str(J+data[D]+J))
            C.append(str('%s'))
        E=self.__a.join(A)
        F=self.__a.join(B)
        K=self.__a.join(C)
        G='INSERT INTO '+tableName+' ({}) VALUES ({})'
        H=G.format(E,F)
        I=self.__con.cursor()
        I.execute(H)
        self.__con.commit()


    #Method to update a table
    #data = ("column","data to update")
    #updateAll([con,"table_name"],data)
    def updateAll(self, tableName, d):
        B=self.__con.cursor()
        if type(d[1])==str:
            A="UPDATE {} SET {}='{}'"
        else:
            A='UPDATE {} SET {}={}'
        B.execute(A.format(tableName, d[0],d[1]))
        self.__con.commit()


    #Method to update table with conditions
    #data = ("column","data to update")
    #where = ("column","data")
    #update([con,"table_name"],data,where)
    def update(self, tableName, d, c):
        B=self.__con.cursor()
        if type(d[1])==str:
            if type(c[1])==str:
                A="UPDATE {} SET {}='{}' WHERE {}='{}'"
            else:
                A="UPDATE {} SET {}='{}' WHERE {}={}"
        elif type(c[1])==str:
            A="UPDATE {} SET {}={} WHERE {}='{}'"
        else:
            A='UPDATE {} SET {}={} WHERE {}={}'
        B.execute(A.format(tableName,d[0],d[1],c[0],c[1]))
        self.__con.commit()


    def delete(self, tableName, d):
        B=self.__con.cursor()
        if type(d[1])==str:
            A="DELETE FROM {} WHERE {}='{}'"
        else:
            A='DELETE FROM {} WHERE {}={}'
        B.execute(A.format(tableName,d[0],d[1]))
        self.__con.commit()


    def deleteAll(self, tableName):
        A=self.__con.cursor()
        B='DELETE FROM {}'
        A.execute(B.format(tableName))
        self.__con.commit()


    def CheckIfDatabaseExists(self):
        return os.path.exists(self.path)


    def DeleteDb(self):
        os.remove(self.path)


    def CheckIfTableExists(self, tableName):
        cursor = self.__con.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        myresult = cursor.fetchall()
        tabelExists = False
        for index in range(0,len(myresult),1):    
            if myresult[index][0] == tableName:
                tabelExists = True
        return tabelExists

    
    def CheckIfColumnExists(self, tableName, columnname):
        cursor = self.__con.cursor()
        cursor.execute("SELECT name FROM PRAGMA_TABLE_INFO('{}')".format(tableName))
        myresult = cursor.fetchall()
        columnExists = False
        for index in range(0, len(myresult),1):
            if myresult[index][0] == columnname:
                columnExists = True
        return columnExists


    def AllTables(self):
        cursor = self.__con.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        myresult =  cursor.fetchall()
        return myresult
