import random
import pandas as pd
import mysql.connector
import csv, ast
# import buildDatabase

def multiple_table_insertion(mydb,mycursor,data2,tableName):
    cols = "`,`".join([str(i) for i in data2.columns.tolist()])
    for i,row in data2.iterrows():
        sql = "INSERT INTO " +tableName+ "(`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
        mycursor.execute(sql, tuple(row))
        mydb.commit() 

def createTable(mydb, mycursor, data, multiTable, myTabName):    
    if(multiTable == True):
        tableName = myTabName
        statement = "create table "+tableName+" ("
        for i in data.columns:
            statement = (statement + '\n{} varchar(255),').format(i.lower())
        # statement = statement + '\n' + "Annotation " + 'varchar(255),'
        statement = statement[:-1] + ");"

        print(statement)
        mycursor.execute(statement)
        mydb.commit()

        multiple_table_insertion(mydb,mycursor,data,tableName)

    else:    
        for itr in range(5):
            tableName = myTabName+str(itr)
            statement = "create table "+tableName+"("
            for i in data.columns:
                statement = (statement + '\n{} varchar(255),').format(i.lower())
            statement = statement + '\n' + "Annotation " + 'varchar(255),'
            statement = statement[:-1] + ");"

            # print(statement)
            # mycursor.execute(statement)
            # mydb.commit()

            # bag semantics 
            if(itr == 0):
                data3=data.groupby(data.columns.tolist(),as_index=False).size().reset_index(name='Annotation')
                print(data3)
                # cols = "`,`".join([str(i) for i in data3.columns.tolist()])
                # for i,row in data3.iterrows():
                #     sql = "INSERT INTO " +tableName+" (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
                #     mycursor.execute(sql, tuple(row))
                #     mydb.commit()

            duplicate_remov_dataFrame = pd.DataFrame(data.drop_duplicates()) 
            semantaincs_arr = []
            
            if(itr == 1):# provenence semantics
                for x in range(duplicate_remov_dataFrame.shape[0]):
                    semantaincs_arr.append("t"+str(x))
                duplicate_remov_dataFrame['Annotation'] = semantaincs_arr
                print(duplicate_remov_dataFrame)    
            
            if(itr == 2):# probabilty semantics
                for x in range(duplicate_remov_dataFrame.shape[0]):
                    semantaincs_arr.append(round(random.uniform(0.0,1.0), 2))
                duplicate_remov_dataFrame['Annotation'] = semantaincs_arr
                print(duplicate_remov_dataFrame)
            
            if(itr == 3):# certainity semantics
                for x in range(duplicate_remov_dataFrame.shape[0]):
                    semantaincs_arr.append(round(random.uniform(0.0,1.0), 2))
                duplicate_remov_dataFrame['Annotation'] = semantaincs_arr
                print(duplicate_remov_dataFrame)

            if(itr == 4):# standard semantics
                for x in range(duplicate_remov_dataFrame.shape[0]):
                    semantaincs_arr.append(1)
                duplicate_remov_dataFrame['Annotation'] = semantaincs_arr
                print(duplicate_remov_dataFrame)

            if(itr > 0):   
                # data2 = data.drop_duplicates()
                
                multiple_table_insertion(mydb,mycursor,data,tableName)
                
                # cols = "`,`".join([str(i) for i in duplicate_remov_dataFrame.columns.tolist()])
                # for i,row in duplicate_remov_dataFrame.iterrows():
                #     sql = "INSERT INTO " +tableName+ "(`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
                #     mycursor.execute(sql, tuple(row))
                #     mydb.commit()             
                # print(data2)

    
