import random
import pandas as pd
import mysql.connector
import csv, ast
import numpy as np
import tableCreation


def joinTuples(table):
    new_annotation = []
    for i in table['Annotation']:
        str_empty = ""
        str_empty = i.split(",")       
        prod = 1
        for j in str_empty:
            prod = prod * float(j)
        new_annotation.append(prod) 
        
def readTable(mydb, mycursor):
    check_loop = True
    check_union_or_prod = ""
    while True:
        input_query=input("Enter the relational algebra query: ")

        if("create view" in input_query):
            check_loop = False
            viewName = input_query.split(" ")[2]   
            mydb._execute_query(input_query)  

            if("union" in input_query):
                check_union_or_prod = "union"
            else:
                check_union_or_prod = "product"    

            print(pd.read_sql_query("select * from "+viewName, mydb))
        else:
            read_table= pd.read_sql_query(input_query,mydb)
            print(read_table)
            semantic_choice = input("choose the semantincs:\n 0 - Bag semantics\n 1 - Provenance semantics\n 2 - Probability semantics\n 3 - Uncertainity semantics\n 4 - Standard semantics\n " )
            
            new_annotation = []
            if(semantic_choice == str(0)):   #bag semantics
                if (check_loop == True or check_union_or_prod == "union"):
                    for i in read_table['Annotation']:
                        results = list(map(int, i.split(",")))
                        new_annotation.append(sum(results))    
                    read_table['Annotation'] = new_annotation
                    print(read_table) 
                elif check_union_or_prod == "product": 
                    read_table['Annotation'] = joinTuples(read_table)      
                    print(read_table)
                        
                      
            if(semantic_choice == str(1)): #provenence semantics            
                for i in read_table['Annotation']:
                    if(check_union_or_prod == "product"):
                        new_annotation.append(i.replace(",", " X "))
                    else: 
                        new_annotation.append(i.replace(",", " + "))

                read_table['Annotation'] = new_annotation
                print(read_table)

            if(semantic_choice == str(2)): #probability semantics
                if (check_loop == True):
                    for i in read_table['Annotation']:
                        str_empty = ""
                        str_empty = i.split(",")
                        summ = 1;
                        for j in str_empty:
                            summ = summ * (1 - float(j))
                        new_annotation.append(round((1 - summ) , 2))    
                    read_table['Annotation'] = new_annotation
                    print(read_table)

                else:
                    if check_union_or_prod == "union":
                        for i in read_table['Annotation']:
                            results = list(map(float, i.split(",")))
                            total = sum(results) 
                            product = np.prod(results)
                            max_array.append(total + product)
                            new_annotation.append(max_array)    
                        read_table['Annotation'] = new_annotation
                        print(read_table) 
                    
                    elif check_union_or_prod == "product": 
                        read_table['Annotation'] = joinTuples(read_table)      
                        print(read_table)
                

            if(semantic_choice == str(4)):  #standard semantics
                for i in read_table['Annotation']:
                    new_annotation.append(1)    
                read_table['Annotation'] = new_annotation
                print(read_table)

            # check_union_or_prod = True  
            check_loop = True      
        
        input_question = input("Do you want to continue yes/no: ")
        #SELECT c,GROUP_CONCAT(Annotation) FROM `dbproject1` GROUP by 
        if(input_question.upper() == "NO"):
            break
    
mydb = mysql.connector.connect(
host="localhost",
user="root",
passwd="",
database="mydatabase"
)

mycursor = mydb.cursor()          
tableCreation.createTable(mydb, mycursor)            
readTable(mydb, mycursor)