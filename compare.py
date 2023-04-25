with open('/Users/prajwalnadagouda/projects/275GC1/code/writetest.csv', 'r') as t1, open('/Users/prajwalnadagouda/projects/275GC1/dataset/Parking_Violations_Issued_-_Fiscal_Year_2014.csv', 'r') as t2:
    fileone = t1.readlines()
    filetwo = t2.readlines()

for line in filetwo:
    if line not in fileone:
        print(line)