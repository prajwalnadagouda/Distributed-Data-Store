import pandas as pd
source_path = "Parking_Violations_Issued_-_Fiscal_Year_2014.csv"


df = pd.read_csv(source_path)
cols = df.columns
print(cols)
for i in set(df['Issue Date']): # for classified by years files
    i=i.split("/")
    i=i[2]+"-"+i[0]+"-"+i[1]
    # i=str(i).replace("/","-")
    filename = "main/"+i+".csv"
    print(filename)
    df.loc[df['Issue Date'] == i].to_csv(filename,index=False,columns=cols)