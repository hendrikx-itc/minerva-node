import csv

class Prime(csv.Dialect):
    quoting = csv.QUOTE_NONE
    delimiter = ";"
    lineterminator = "\n"
