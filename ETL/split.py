import pandas as pd
source_path = "/Users/prajwalnadagouda/projects/275GC1/ETL/Parking_Violations_Issued_-_Fiscal_Year_2014.csv"

df = pd.read_csv(source_path)
cols = df.columns

for i in set(df['Issue Date']): # for classified by years files
    j=i
    j=j.split("/")
    j=j[2]+"-"+j[0]+"-"+j[1]
    # i=str(i).replace("/","-")
    filename = "./main/"+j+".csv"
    print(filename)
    df.loc[df['Issue Date'] == i].to_csv(filename,index=False,columns=cols)