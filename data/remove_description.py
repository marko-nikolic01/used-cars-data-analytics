import csv

input_file = "./data/used_cars_data.csv"
output_file = "./data/used_cars_data_no_description.csv"

with open(input_file, mode="r", newline="", encoding="utf-8") as infile, \
     open(output_file, mode="w", newline="", encoding="utf-8") as outfile:
    
    reader = csv.DictReader(infile)
    fieldnames = [col for col in reader.fieldnames if col.lower() != "description"]
    
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()
    
    for row in reader:
        del row["description"]  # Remove the column from each row
        writer.writerow(row)

print(f"CSV file processed successfully. Output saved as {output_file}")
