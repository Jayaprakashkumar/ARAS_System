import random
import pandas as pd
import mysql.connector
import csv, ast

f = open('C://Users//Win10//test_python//test.csv', 'r')

reader = csv.reader(f)
# print(random.randint(0, 1))
longest, headers, type_list = [], [], []
        
def dataType(val, current_type):
    try:
        # Evaluates numbers to an appropriate type, and strings an error
        t = ast.literal_eval(val)
    except ValueError:
        return 'varchar'
    except SyntaxError:
        return 'varchar'
        return 'varchar'

for row in reader:
    if len(headers) == 0:
        headers = row
        for col in row:
            longest.append(0)
            type_list.append('')
    else:
        for i in range(len(row)):
            # NA is the csv null value
            if type_list[i] == 'varchar' or row[i] == 'NA':
                pass
            else:
                var_type = dataType(row[i], type_list[i])
                type_list[i] = 'varchar'
        if len(row[i]) > longest[i]:
            longest[i] = len(row[i])
# f.close()


mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  database="mydatabase"
)

mycursor = mydb.cursor()

for itr in range(5):
    print(itr)
    tableName = "kbs_database"+str(itr)
    statement = "create table "+tableName+"("
    for i in range(len(headers)):
        if type_list[i] == 'varchar':
            statement = (statement + '\n{} varchar(255),').format(headers[i].lower(), str(longest[i]))
        else:
            statement = (statement + '\n' + '{} {}' + ',').format(headers[i].lower(), type_list[i])
    statement = statement + '\n' + "Annotation " + 'varchar(255),'
    statement = statement[:-1] + ");"

    print(statement)
    # mycursor.execute(statement)
    # mydb.commit()

    # set columns of headers in the table
    head = ""
    for lis in headers:
        if len(head) > 0:
            head = head +', '+ lis
        else:
            head = lis
    head = head +', '+ "Annotation"

    # print(head)
    # set columns for the values in the table
    rowList =""        
    for x in range(len(headers)):
        if len(rowList) > 0:
            rowList = rowList +', '+ "%s"
        else:
            rowList = "%s"  

    if(itr == 3):
        rowList = rowList +', '+ "%s"
    else:
        rowList = rowList +', '+ "%d"

    
    data = pd.read_csv("test.csv")

    # bag semantics 
    if(itr == 0):
        data3=data.groupby(data.columns.tolist(),as_index=False).size().reset_index(name='Annotation')
        # print(data3)
        cols = "`,`".join([str(i) for i in data3.columns.tolist()])
        for i,row in data3.iterrows():
            sql = "INSERT INTO " +tableName+" (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
            mycursor.execute(sql, tuple(row))
            mydb.commit()

    data2=data.drop_duplicates()    
    
    if(itr == 1):# provenence semantics
        df_provenence = pd.DataFrame(data2) 
        provArr=[]
        for x in range(df_provenence.shape[0]):
            provArr.append("t"+str(x))
        df_provenence['Annotation'] = provArr
        print(df_provenence)    
    
    if(itr == 2):# probabilty semantics
        df_probabilty = pd.DataFrame(data2) 
        probArr=[]
        for x in range(df_probabilty.shape[0]):
            probArr.append(round(random.uniform(0.0,1.0), 2))
        df_probabilty['Annotation'] = probArr
        print(df_probabilty)
    
    if(itr == 3):# certainity semantics
        df_certainity = pd.DataFrame(data2) 
        certainity=[]
        for x in range(df_certainity.shape[0]):
            certainity.append(round(random.uniform(0.0,1.0), 2))
        df_certainity['Annotation'] = certainity
        print(df_certainity)

    if(itr == 4):# standard semantics
        df_standard = pd.DataFrame(data2) 
        standard=[]
        for x in range(df_standard.shape[0]):
            standard.append(1)
        df_standard['Annotation'] = standard

    if(itr > 0)   
        cols = "`,`".join([str(i) for i in data2.columns.tolist()])
        for i,row in data2.iterrows():
            sql = "INSERT INTO " +tableName+ "(`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
            mycursor.execute(sql, tuple(row))
            mydb.commit()             
    # print(data2)

    # df = pd.read_sql_query("select * from dBproject4;", mydb)

    # print(df)