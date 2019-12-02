import random
import pandas as pd
import mysql.connector
import csv, ast


def multiple_table_insertion(engine,mydb,mycursor,data2,tableName):
    cols = "`,`".join([str(i) for i in data2.columns.tolist()])
    datadb=data2.values.tolist()
    data2.to_sql(tableName, con = engine, if_exists = 'append', index=False)
    # for i,row in data2.iterrows():
    #     sql = "INSERT INTO " +tableName+ "(`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
    #     mycursor.execute(sql, tuple(row))
    #     mydb.commit() 

def createTable(engine,mydb, mycursor, data, multiTable, myTabName):    
    if(multiTable == True):
        tableName = myTabName
        statement = "create table "+tableName+" ("
        for i in data.columns:
            statement = (statement + '\n{} varchar(255),').format(i.lower())
        # statement = statement + '\n' + "Annotation " + 'varchar(255),'
        statement = statement[:-1] + ");"

        # print(statement)
        mycursor.execute(statement)
        mydb.commit()

        multiple_table_insertion(engine,mydb,mycursor,data,tableName)

    else:    
        for itr in range(5):
            tableName = myTabName+str(itr)
            statement = "create table "+tableName+"("
            for i in data.columns:
                statement = (statement + '\n{} varchar(255),').format(i.lower())
            statement = statement + '\n' + "annotation " + 'varchar(255),'
            statement = statement[:-1] + ");"

            # prin
            # mycursor.execute(statement)
            # mydb.commit()t(statement)
            
            duplicate_remov_dataFrame = pd.DataFrame(data.drop_duplicates()) 
            semantaincs_arr = []            

            # bag semantics 
            if(itr == 0):
                for x in range(duplicate_remov_dataFrame.shape[0]):
                    semantaincs_arr.append(str(int(random.uniform(1,10))))
                duplicate_remov_dataFrame['annotation'] = semantaincs_arr
                # print(duplicate_remov_dataFrame)


                # data3=data.groupby(data.columns.tolist(),as_index=False).size().reset_index(name='Annotation')
                # print(data3)
                # cols = "`,`".join([str(i) for i in data3.columns.tolist()])
                # for i,row in data3.iterrows():
                #     sql = "INSERT INTO " +tableName+" (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
                #     mycursor.execute(sql, tuple(row))
                #     mydb.commit()

            
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

            # if(itr >= 0):  
            #     multiple_table_insertion(engine,mydb,mycursor,duplicate_remov_dataFrame,tableName)
                

    
