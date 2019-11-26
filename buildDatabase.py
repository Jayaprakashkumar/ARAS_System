import random
import pandas as pd
import mysql.connector
import csv, ast
import numpy as np
import tableCreation


def queryExecution(read_table, check_loop, check_union_or_prod):
    # read_table= pd.read_sql_query(input_query,mydb)
    print(check_loop)
    semantic_choice = input("choose the semantincs:\n 0 - Bag semantics\n 1 - Provenance semantics\n 2 - Probability semantics\n 3 - Uncertainity semantics\n 4 - Standard semantics\n " )
    
    new_annotation = []
    if(semantic_choice == str(0)):   #bag semantics
        if (check_loop == True or check_union_or_prod == "union"):
            header= read_table.columns.tolist()
            # header.remove('annotation')
            header.remove('annotation')
            datas=read_table.groupby(header)['annotation'].agg([('annotation', ', '.join)]).reset_index()
            print(datas)            
            for i in datas['annotation']:
                results = list(map(int, i.split(",")))
                new_annotation.append(sum(results))
            datas['annotation'] =new_annotation
            return(datas)            
        
        elif check_union_or_prod == "product":  
            # print("inside product")
            read_table['Annotation'] = read_table.apply(lambda row: (str(int(row['Annotation'])*int(row['Annotation1']))),axis=1)
            read_table.drop('Annotation1', axis=1, inplace=True)
            header= read_table.columns.tolist()
            header.remove('Annotation')
            datas=read_table.groupby(header)['Annotation'].agg([('Annotation', ', '.join)]).reset_index()
            for i in datas['Annotation']:
                results = list(map(int, i.split(",")))
                new_annotation.append(sum(results))
            datas['Annotation'] =new_annotation
            return(datas)           
                  
                                        
    if(semantic_choice == str(1)): #provenence semantics     
        for i in read_table['Annotation']:
            if(check_union_or_prod == "product"):
                new_annotation.append(i.replace(",", " X "))
            else: 
                new_annotation.append(i.replace(",", " + "))
        read_table['Annotation'] = new_annotation

    if(semantic_choice == str(2)): #probability semantics
        if (check_loop == True):
            for i in read_table['Annotation']:
                str_empty = i.split(",")
                summ = 1;
                for j in str_empty:
                    summ = summ * (1 - float(j))
                new_annotation.append(round((1 - summ) , 2))    
            read_table['Annotation'] = new_annotation

        else:
            if check_union_or_prod == "union":
                for i in read_table['Annotation']:
                    results = list(map(float, i.split(",")))
                    total = sum(results)        # a+b+c 
                    product = np.prod(results)  # abc
                    sumVal = results[0] * results[len(results) - 1]
                    
                    for j in range(len(results)):    # ab + bc + ac
                        if (j < len(results)-1 ):
                            sumVal = sumVal + (results[j] * results[j+1])
                    
                    new_annotation.append(round((total - product + sumVal),2))
                read_table['Annotation'] = new_annotation
            
            elif check_union_or_prod == "product": 
                read_table['Annotation'] = read_table.apply(lambda row: (float(row['Annotation'])*float(row['Annotation1'])),axis=1)  
                        
    if(semantic_choice == str(3)):  #uncertainity semantics
        if (check_loop == True or check_union_or_prod == "union"):
            for i in read_table['Annotation']:
                str_empty = i.split(",")
                new_annotation.append(max(list(map(float, str_empty))))    
            read_table['Annotation'] = new_annotation

        elif check_union_or_prod == "product": 
            read_table['Annotation'] = joinTuples(read_table)  

    if(semantic_choice == str(4)):  #standard semantics
        for i in read_table['Annotation']:
            new_annotation.append(1)    
        read_table['Annotation'] = new_annotation
        
     
    # print(read_table)    
    return (read_table)


    # print(list(read_table.columns))
    # if(len(viewName) > 0):
    #     mydb._execute_query("drop view "+viewName+"") 

def joinTuples(table):
    new_annotation = []    
    for i in table['Annotation']:
        str_empty = ""
        str_empty = i.split(",")       
        prod = 1
        for j in str_empty:
            prod = prod * float(j)
        new_annotation.append(prod)

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
                       
                    updated_table = queryExecution(queue, check_loop, check_union_or_prod)    
                    tableCreation.createTable(mydb, mycursor, updated_table, True, "T"+str(k))  
                    check_union_or_prod=""
                    check_loop=True
                k += 1;          

        else:
            if("union" in input_query or "join" in input_query):
                check_loop = False
                        # viewName = input_query.split(" ")[2]   
                        # mydb._execute_query(input_query)  
                if("union" in input_query):
                    check_union_or_prod = "union"
                else:
                    check_union_or_prod = "product"    
            queue = pd.read_sql_query(input_query, mydb)                        
            updated_table = queryExecution(queue, check_loop, check_union_or_prod)
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
tableCreation.createTable(mydb, mycursor, data, False, "krish_table")            
readTable(mydb, mycursor)
f.close()