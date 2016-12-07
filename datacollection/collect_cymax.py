import csv

input_file = open('input.csv', 'rt')
try:
    old_orderid = -1
    items = []
    reader = csv.reader(input_file)
    output_file = open('ploished_entries.txt','w')

    for row in reader:
        new_orderid = row[0]
        if old_orderid == new_orderid:
            items.append(row[3])

        else:
            if len(items) > 1:
                items.append(row[3])
                entry_row = str(old_orderid)+':'+' '.join(items)
                output_file.write(entry_row+'\n')
                items = []
        old_orderid = new_orderid

finally:
    input_file.close()
    output_file.close()
