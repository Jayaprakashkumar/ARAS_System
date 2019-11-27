import random
import pandas as pd
import mysql.connector
import csv, ast
import numpy as np
import tableCreation


def queryExecution(read_table, check_loop, check_union_or_prod, semantic_choice):
    # print(read_table)    
    datas = []
    new_annotation = []
    if(semantic_choice == str(0)):   #bag semantics
        if (check_loop == True or check_union_or_prod == "union"):
            header= read_table.columns.tolist()            
            header.remove('annotation')
            datas=read_table.groupby(header)['annotation'].agg([('annotation', ', '.join)]).reset_index()
            print(datas)    
            for i in datas['annotation']:
                results = list(map(int, i.split(",")))
                new_annotation.append(sum(results))
            datas['annotation'] =new_annotation
        
        elif check_union_or_prod == "product":  
            read_table['Annotation'] = read_table.apply(lambda row: (str(int(row['Annotation'])*int(row['Annotation1']))),axis=1)
            read_table.drop('Annotation1', axis=1, inplace=True)
            header= read_table.columns.tolist()
            header.remove('Annotation')
            datas=read_table.groupby(header)['Annotation'].agg([('Annotation', ', '.join)]).reset_index()
            for i in datas['Annotation']:
                results = list(map(int, i.split(",")))
                new_annotation.append(sum(results))
            datas['Annotation'] =new_annotation
                  
                                        
    if(semantic_choice == str(1)): #provenence semantics     
        if(check_union_or_prod == "product"):
            read_table['Annotation']=read_table['Annotation'] +" X "+ read_table['Annotation1']
            read_table.drop('Annotation1', axis=1, inplace=True)
            # new_annotation.append(i.replace(",", " X "))
            datas = read_table
        else: 
            header= read_table.columns.tolist()
            header.remove('annotation')
            datas=read_table.groupby(header)['annotation'].agg([('annotation', ' + '.join)]).reset_index()            

    if(semantic_choice == str(2)): #probability semantics
        if (check_loop == True):
            header= read_table.columns.tolist()
            header.remove('annotation')
            datas=read_table.groupby(header)['annotation'].agg([('annotation', ', '.join)]).reset_index()
            print(datas)         
            for i in datas['annotation']:
                str_empty = i.split(",")
                summ = 1
                for j in str_empty:
                    summ = summ * (1 - float(j))
                new_annotation.append(round((1 - summ) , 2))    
            datas['annotation'] = new_annotation

        else:
            if check_union_or_prod == "union":
                header= read_table.columns.tolist()
                header.remove('annotation')
                datas=read_table.groupby(header)['annotation'].agg([('annotation', ', '.join)]).reset_index()
                for i in datas['annotation']:
                    results = list(map(float, i.split(",")))
                    total = sum(results)        # a+b+c 
                    product = np.prod(results)  # abc
                    sumVal = results[0] * results[len(results) - 1]
                    
                    for j in range(len(results)):    # ab + bc + ac
                        if (j < len(results)-1 ):
                            sumVal = sumVal + (results[j] * results[j+1])
                    
                    new_annotation.append(round((total + product - sumVal),4))
                datas['annotation'] = new_annotation
            
            elif check_union_or_prod == "product": 
                read_table['Annotation'] = read_table.apply(lambda row: (str(round(float(row['Annotation'])*float(row['Annotation1']),4))),axis=1)
                read_table.drop('Annotation1', axis=1, inplace=True)
                header= read_table.columns.tolist()
                header.remove('Annotation')
                datas=read_table.groupby(header)['Annotation'].agg([('Annotation', ', '.join)]).reset_index()
                print(datas)
                for i in datas['Annotation']:
                    str_empty = i.split(",")
                    summ = 1;
                    for j in str_empty:
                        summ = summ * (1 - float(j))
                    new_annotation.append(round((1 - summ) , 2))    
                datas['Annotation'] = new_annotation
                # read_table['Annotation'] = read_table.apply(lambda row: (float(row['Annotation'])*float(row['Annotation1'])),axis=1)  
                        
    if(semantic_choice == str(3)):  #uncertainity semantics
        if (check_loop == True or check_union_or_prod == "union"):
            header= read_table.columns.tolist()            
            header.remove('annotation')
            datas=read_table.groupby(header)['annotation'].agg([('annotation', ', '.join)]).reset_index()
            print(datas)            
            for i in datas['annotation']:               
                str_empty = i.split(",")
                new_annotation.append(max(list(map(float, str_empty))))    
            datas['annotation'] = new_annotation

        elif check_union_or_prod == "product": 
            # read_table['Annotation'] = joinTuples(read_table)
            read_table['Annotation'] = read_table.apply(lambda row: (str(float(row['Annotation'])*float(row['Annotation1']))),axis=1)
            read_table.drop('Annotation1', axis=1, inplace=True)
            header= read_table.columns.tolist()
            header.remove('Annotation')
            datas=read_table.groupby(header)['Annotation'].agg([('Annotation', ', '.join)]).reset_index()
            for i in datas['Annotation']:               
                str_empty = i.split(",")
                new_annotation.append(max(list(map(float, str_empty))))    
            datas['Annotation'] = new_annotation            

    if(semantic_choice == str(4)):  #standard semantics
        if (check_loop == True or check_union_or_prod == "union"):
            header= read_table.columns.tolist()
            header.remove('annotation')
            datas=read_table.groupby(header)['annotation'].agg([('annotation', ', '.join)]).reset_index()
            print(datas)            
            for i in datas['annotation']:
                results = list(map(int, i.split(",")))
                new_annotation.append(max(results))
            datas['annotation'] =new_annotation

        elif(check_union_or_prod == "product"):
            read_table['Annotation'] = read_table.apply(lambda row: (str(int(row['Annotation'])*int(row['Annotation1']))),axis=1)
            read_table.drop('Annotation1', axis=1, inplace=True)
            header= read_table.columns.tolist()
            header.remove('Annotation')
            datas=read_table.groupby(header)['Annotation'].agg([('Annotation', ', '.join)]).reset_index()
            for i in datas['Annotation']:
                results = list(map(int, i.split(",")))
                new_annotation.append(np.prod(results))
            datas['Annotation'] =new_annotation
             
    return (datas)

# def joinTuples(table):
#     new_annotation = []    
#     for i in table['Annotation']:
#         str_empty = ""
#         str_empty = i.split(",")       
#         prod = 1
#         for j in str_empty:
#             prod = prod * float(j)
#         new_annotation.append(prod)

# def productTuples(table):
#     new_annotation = []       
#     for i,j in table['Annotation'],table['Annotation1']:
#         prod = 1        
#         prod = prod * float(j) * float(i)
#         new_annotation.append(prod)
#     print(new_annotation)
#     return(new_annotation)



        
def readTable(mydb, mycursor):
    check_loop = True
    check_union_or_prod = ""
    viewName = ""
    k = 0
    while True:
        input_query=input("Enter the relational algebra query: ")
        semantic_choice = input("choose the semantincs:\n 0 - Bag semantics\n 1 - Provenance semantics\n 2 - Probability semantics\n 3 - Uncertainity semantics\n 4 - Standard semantics\n " )
        
        if("#" in input_query):
            queryList = input_query.split("#")

            
            for query in queryList:
                if len(query) > 0:
                    queue = pd.read_sql_query(query, mydb)
                        
                    if("union" in query or "join" in query):
                        check_loop = False  
                        if("union" in query):
                            check_union_or_prod = "union"
                        else:
                            check_union_or_prod = "product"  
                       
                    updated_table = queryExecution(queue, check_loop, check_union_or_prod, semantic_choice)    
                    tableCreation.createTable(mydb, mycursor, updated_table, True, "T"+str(k))  
                    check_union_or_prod=""
                    check_loop=True
                k += 1;

            # to drop the tables
            for tableNo in range(k):                  
                tName = "t"+str(tableNo)
                # print(tName)
                mycursor.execute("DROP TABLE "+tName+"", mydb)
            k = 0

        else:
            if("union" in input_query or "join" in input_query):
                check_loop = False
                if("union" in input_query):
                    check_union_or_prod = "union"
                else:
                    check_union_or_prod = "product"    
            queue = pd.read_sql_query(input_query, mydb)                        
            updated_table = queryExecution(queue, check_loop, check_union_or_prod,semantic_choice)
            print(updated_table)
                                                
        
        input_question = input("Do you want to continue yes/no: ")
        if(input_question.upper() == "NO"):
            break
    
mydb = mysql.connector.connect(
host="localhost",
user="root",
passwd="",
database="mydatabase"
)

mycursor = mydb.cursor()          
f = open('test.csv', 'r')
data = pd.read_csv(f)
tableCreation.createTable(mydb, mycursor, data, False, "green_paper_table")            
readTable(mydb, mycursor)
f.close()