import csv

def split_csv_file(input_file, output_directory):
    with open(input_file, 'r') as file:
        reader = csv.reader(file)
        header = next(reader)
        column1=header.index("Issue Date")
        column2=header.index("Violation Code")
        output_files = {}
        for row in reader:
            value1 = row[column1]
            values = value1.split("/")
            value1 = values[2]+"-"+values[0]+"-"+values[1]
            # value1=value1.replace("/","-")
            value2 = row[column2]
            filename = f"{value1}-{value2}.csv"
            if filename not in output_files:
                output_files[filename] = open(f"{output_directory}/{filename}", 'w', newline='')
                writer = csv.writer(output_files[filename])
                writer.writerow(header)
            writer.writerow(row)
        for file in output_files.values():
            file.close()
