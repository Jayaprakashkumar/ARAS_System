import mysql.connector
import csv, ast
import random

f = open('C://Users//Win10//test_python//test.csv', 'r')
# f = open('/path/to/survey/data/survey_data.csv', 'r')
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
f.close()

# sample_name = "myname"
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  database="mydatabase"
)

mycursor = mydb.cursor()

for itr in range(5):
    print(itr)
    tableName = "dBproject"+str(itr)
    statement = "create table "+tableName+"("
    for i in range(len(headers)):
        if type_list[i] == 'varchar':
            statement = (statement + '\n{} varchar(255),').format(headers[i].lower(), str(longest[i]))
        else:
            statement = (statement + '\n' + '{} {}' + ',').format(headers[i].lower(), type_list[i])
    statement = statement + '\n' + "Annotation " + 'varchar(255),'
    statement = statement[:-1] + ");"
        
    # print(statement)
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
    
    # inserting the tuples into the table
    
        
    f = open('C://Users//Win10//test_python//test.csv', 'r')
    listener = csv.reader(f) 

    p=0
    for line in listener:
        value_list = ""
        if (p>0):
            for inner_li in line:
                if(len(value_list) > 1):
                    value_list = value_list + "," + "'"+inner_li+"'" 
                else:
                    value_list =value_list +"'"+inner_li+"'"
            
            # if(itr == 0):  #bag semantics
            value_list = value_list + "," + "1"
            
            value_list = value_list
            val = value_list
            # print(val)
            sql = "INSERT INTO "+tableName+" ("+head+") VALUES ("+value_list+")"
            print(sql)
            # mycursor.execute(sql, val)
            # mydb.commit()
        p = p + 1    
    
    f.close();

input_query=input("Enter the relational algebra query")
sql_select_query=input_query
mycursor.execute(sql_select_query)
record = mycursor.fetchall()
for row1 in record:
    print(row1)
    