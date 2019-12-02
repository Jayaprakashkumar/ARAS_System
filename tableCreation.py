import random
import pandas as pd
import mysql.connector
import csv, ast


def multiple_table_insertion(engine,mydb,mycursor,data2,tableName,LastInLoop):
    cols = "`,`".join([str(i) for i in data2.columns.tolist()])
    if(LastInLoop==True):
        data2.to_csv('test'+tableName+'.csv', encoding='utf-8', index=False)
    else:    
        data2.to_sql(tableName, con = engine, if_exists = 'append', index=False)
   
def createTable(engine,mydb, mycursor, data, multiTable, myTabName,LastInLoop):    
    tableName = myTabName
    if(multiTable == True):
        if(LastInLoop==False):            
            statement = "create table "+tableName+" ("
            for i in data.columns:
                statement = (statement + '\n{} varchar(65535),').format(i.lower())
            statement = statement[:-1] + ");"

            # print(statement)
            mycursor.execute(statement)
            mydb.commit()

        multiple_table_insertion(engine,mydb,mycursor,data,tableName,LastInLoop)

    else:    
        for itr in range(5):
            tableName = myTabName+str(itr)
            statement = "create table "+tableName+"("
            for i in data.columns:
                statement = (statement + '\n{} varchar(255),').format(i.lower())
            statement = statement + '\n' + "annotation " + 'varchar(255),'
            statement = statement[:-1] + ");"

            # print(statement)
            mycursor.execute(statement)
            mydb.commit()
            
            duplicate_remov_dataFrame = pd.DataFrame(data.drop_duplicates()) 
            semantaincs_arr = []            

            # bag semantics 
            if(itr == 0):
                for x in range(duplicate_remov_dataFrame.shape[0]):
                    semantaincs_arr.append(str(int(random.uniform(1,10))))
                duplicate_remov_dataFrame['annotation'] = semantaincs_arr
            
            if(itr == 1):# provenence semantics
                for x in range(duplicate_remov_dataFrame.shape[0]):
                    semantaincs_arr.append("t"+str(x))
                duplicate_remov_dataFrame['annotation'] = semantaincs_arr
            
            if(itr == 2):# probabilty semantics
                for x in range(duplicate_remov_dataFrame.shape[0]):
                    semantaincs_arr.append(str(round(random.uniform(0.0,1.0), 2)))
                duplicate_remov_dataFrame['annotation'] = semantaincs_arr
            
            if(itr == 3):# certainity semantics
                for x in range(duplicate_remov_dataFrame.shape[0]):
                    semantaincs_arr.append(str(round(random.uniform(0.0,1.0), 2)))
                duplicate_remov_dataFrame['annotation'] = semantaincs_arr

            if(itr == 4):# standard semantics
                for x in range(duplicate_remov_dataFrame.shape[0]):
                    semantaincs_arr.append(str(1))
                duplicate_remov_dataFrame['annotation'] = semantaincs_arr

            multiple_table_insertion(engine,mydb,mycursor,duplicate_remov_dataFrame,tableName,LastInLoop)
                

    
