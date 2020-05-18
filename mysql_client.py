import pymysql
import pandas as pd
import sys
from sqlalchemy import create_engine



def insert_into_target_data(host,user,password,db,data_df, target_table):
    conn = pymysql.connect(host,user,password,db)
    # you must create a Cursor object. It will let
    #  you execute all the queries you need
    cur = conn.cursor()
    cols = "`,`".join([str(i) for i in data_df.columns.tolist()])

    for i, row in data_df.iterrows():
        sql = "INSERT INTO `"+target_table+"` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        cur.execute(sql, tuple(row))

        # the connection is not autocommitted by default, so we must commit to save our changes
        conn.commit()
    conn.close()

def run_query_mysql(host, user, passwd, db, query):
    conn =pymysql.connect(host, user, passwd, db)
    cur = conn.cursor()
    x = conn.cursor()
    print(query)
    try:
       x.execute(query)
       conn.commit()
       result = x.fetchone()
       result = str(result).replace("(", "")
       result = str(result).replace(")", "")
       result = str(result).replace(",", "")
       if result == 'None':
           result = None
       print('Query succeeded ')

       return result
    except:
       print('Query Failed ')
       conn.rollback()


    conn.close()




def query_mysql_to_df(host, user, passwd, db,query,columns):
    db_connection = pymysql.connect(host, user, passwd, db)
    df = pd.DataFrame(columns=columns)
    cursor = db_connection.cursor()
    cursor.execute(query)
    table_rows = cursor.fetchall()
    df = pd.DataFrame(table_rows)

    return df

def mysql_to_csv(query, file_path, host, user, password,db):
    '''
    The function creates a csv file from the result of SQL
    in MySQL database.
    '''
    try:
        con = pymysql.connect(host=host,
                              user=user,
                              password=password,
                              db=db)
        print('Connected to DB: {}'.format(host))
        # Read table with pandas and write to csv
        df = pd.read_sql(query, con)
        df.to_csv(file_path, encoding='utf-8', header=True, doublequote=True, sep=',', index=False)
        # cursor = con.cursor()
        # cursor.execute(query)
        print('File, {}, has been created successfully'.format(file_path))
        con.close()

    except Exception as e:
        print('Error: {}'.format(str(e)))
        sys.exit(1)

def csv_to_mysql(load_sql, host, user, password, db):
    '''
    This function load a csv file to MySQL table according to
    the load_sql statement.
    '''
    try:
        con = pymysql.connect(host=host,
                              user=user,
                              password=password,
                              db=db,
                              autocommit=True,
                              local_infile=1)
        print('Connected to DB: {}'.format(host))
        # Create cursor and execute Load SQL
        cursor = con.cursor()
        cursor.execute(load_sql)
        print('Succuessfully loaded the table from csv.')
        con.close()

    except Exception as e:
        print('Error: {}'.format(str(e)))
        sys.exit(1)


def read_query(host, user, passwd, db, query):
    db_connection = pymysql.connect(host, user, passwd, db)
    df = pd.read_sql(query, con=db_connection)
    return df

def df_to_mysql_bulk(df,dest_table,host, user, passwd, db,type = 'append'):
    df = df[:]
    sh = df.shape[0]
    bulk_number = 50000
    reminder = sh % bulk_number

    engine = create_engine('mysql://'+user+':'+passwd+'@'+host+'/'+db)
    num = int((sh-reminder)/bulk_number)
    fromrow = 0
    torow = bulk_number
    for i in range(0,num+1):
        with engine.connect() as conn, conn.begin():
            print("start iteration {}".format(i))
            df[fromrow:torow].to_sql(dest_table, conn, if_exists=type,index=False)
            print("end iteration {}".format(i))
        if i == num:
            fromrow += reminder
            torow += reminder
        fromrow += bulk_number
        torow += bulk_number

def mysql_query_to_df2(host, user, passwd, db,query):
    db_connection = pymysql.connect(host, user, passwd, db)
    df = pd.read_sql(query, con=db_connection)
    return df


def mysql_query_to_df(host, user, passwd, db,query):
    engine = create_engine('mysql://'+user+':'+passwd+'@'+host+'/'+db,pool_recycle=3600)
    df = pd.read_sql(query, con=engine)
    return df