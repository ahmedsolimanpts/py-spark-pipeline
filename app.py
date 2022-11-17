from pyspark.sql import SparkSession
from pyspark.sql.functions import col , count , isnan ,isnull
from pyspark.sql.types import *
import glob as gb
import datetime as dt

SOURCE_FOLDER = "Source Data/*.csv"

def read_csv_file(FILE):
    try:
        df = spark.read.csv(FILE,header=True,inferSchema=True)
        Create_Logs('Done Reading File')
        return df

    except Exception as e:
        return e

def Read_all_csv_data(Folder):
    fiels = gb.glob(Folder)
    df = None
    if len(fiels) == 1 :
        Create_Logs('Start Reading Files ...')
        df = read_csv_file(fiels)
        Create_Logs('Finish Reading csv File')
        Create_Logs('Data Is Ready To Use')
        return df
    elif len(fiels) >1:
        df = read_csv_file(Folder)
        print(df.count())
        Create_Logs('Finish Reading All csv Files ')
        return df

    else:
        Create_Logs('Cant Read File')


def Create_Logs(message):
    LOG_FILE = "Logs/logs.txt"
    print(message)
    if gb.glob(LOG_FILE):
        with open(LOG_FILE,'a') as f:
            f.write(str(dt.datetime.now())+';    '+message+';\n')
    else:
        with open(LOG_FILE,'w') as f:
            f.write('date;    '+'message;\n')
            f.write(str(dt.datetime.now())+';    '+message+';\n')

def Fast_EDA(DF):
    Create_Logs('Start Fast EDA')
    print('-'*30)
    print('# Columns : ' , len(DF.columns))
    print('# Rows : ' , DF.count())
    print('# Row Without Duplicate', DF.dropDuplicates().count())
    print('Columns Name : ')
    print(DF.columns)
    print('COLUMN NAME , COLUMN TYPE')
    for i in DF.dtypes:
        print(i[0],',',i[1])
    print('-'*30)
    Create_Logs('Finish Fast EDA')

def clean_duplicate(DF):
    Create_Logs('Start check & Drop Duplicates')
    if DF.count() > DF.dropDuplicates().count():
        print('There Is Duplicate')
        DF = DF.dropDuplicates()
        print('Drop Duplicate Sucess')
        print(DF.count())
        Create_Logs('Finish check & Drop Duplicates')
        return DF
    Create_Logs('Finish check and NO Duplicates Found')

if __name__ == "__main__":
    Columns_Name = ['step', 'type', 'amount', 'nameOrig', 'oldbalanceOrg', 'newbalanceOrig', 'nameDest', 'oldbalanceDest',
     'newbalanceDest', 'isFraud', 'isFlaggedFraud']
    print('-'*50)
    Create_Logs('Start Creating Session ...')
    spark = SparkSession.builder.appName('MY_PIPE_LINE').getOrCreate()

    Create_Logs('Create Session Sucess')

    # Read Data
    df = Read_all_csv_data(SOURCE_FOLDER)

    # fast eda to data
    Fast_EDA(df)

    # checck & Drop Duplicate
    df = clean_duplicate(df)
    print('-' * 50)




