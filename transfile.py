df = spark.read.option("header",True).option("inferSchema",True).csv('/FileStore/tables/export.csv')


df1 = df.select('Total Gross Inpatient Revenue65','Total Gross Outpatient Revenue','Total Net Patient Revenue109')


data = df1.collect()#converting each row into list


def NIR(TGIR,TGOR,TNPR):
    op = 0
    if TGIR == 0:
        TGIR = 1
        op = (TGIR/(TGIR+TGOR))*TNPR
    else:
        op = (TGIR/(TGIR+TGOR))*TNPR
    return op

def NOR(TGIR,TGOR,TNPR):
    op = 0
    if TGIR == 0:
        TGIR = 1
        op = (TGOR/(TGIR+TGOR))*TNPR
    else:
        op = (TGOR/(TGIR+TGOR))*TNPR
    return op

def output(data):
    data1 = []
    data2 = []
    for i in range(len(data)):
        try:
            TGIR=float(data[i][0].replace(',',''))
            TGOR=float(data[i][1].replace(',',''))
            TNPR=float(data[i][2].replace(',',''))    
            data1.append(NIR(TGIR,TGOR,TNPR))
            data2.append(NOR(TGIR,TGOR,TNPR))
        except:
            print(f'error{i}')
    return data1, data2
  
  
  NIR,NOR = output(data)
  
  
  df2 = spark.createDataFrame(zip(NIR,NOR), ["Net Inpatient Revenue (est.)","Net Outpatient Revenue (est.)"])#creating new dataframe with merging two output list(NIR,NOR)
  from pyspark.sql.functions import *
from pyspark.sql.window import Window


df1 = df1.withColumn("row_idx", row_number().over(Window.orderBy(monotonically_increasing_id())))#indexing two dataframe 
df2 = df2.withColumn("row_idx", row_number().over(Window.orderBy(monotonically_increasing_id())))

final_df2 = df1.join(df2, df1.row_idx == df2.row_idx).drop("row_idx")#join two dataframe using row_idx column and drop it

final_df2.display()
