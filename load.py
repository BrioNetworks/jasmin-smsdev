# -*- coding: utf-8 -*-
import xlrd
import redis
import csv

file_xml = 'router-04_02_2016.xlsx'
# количество строк заголовка таблицы
table_header = 2

file_csv = 'Port_All_New_201603020000_833.csv'
# пропустить заголовок
skip_header = True
# разделитель
delimiter = ','

# хост Redis
host = 'localhost'
# порт Redis
port = 6379
# Прогрузка в отдельные бд Redis
db0, db1 = 0, 1


def load_csv():
    r = redis.StrictRedis(host=host, port=port, db=db0)

    with open(file_csv, 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=delimiter, quoting=csv.QUOTE_NONE)
        if skip_header:
            next(reader, None)
        for row in reader:
            mnc = ''
            if 'ВымпелКом' in row[1]:
                mnc = '99'
            elif 'Мобильные ТелеСистемы' in row[1]:
                mnc = '01'
            elif 'МегаФон' in row[1]:
                mnc = '02'
            elif 'Скартел' in row[1]:
                mnc = '11'
            elif 'Т2 Мобайл' in row[1]:
                mnc = '20'
            if mnc:
                r.set(row[0], mnc)


def normalize(row):
    return [unicode(cell).split('.')[0].strip() for cell in row if cell is not '']


def load_xml():
    book = xlrd.open_workbook(file_xml)
    sh = book.sheet_by_index(0)

    r = redis.StrictRedis(host=host, port=port, db=db1)
    r.flushdb()

    for rx in range(table_header, sh.nrows):
        row = normalize(sh.row_values(rx))
        if len(row) > 0:
            key = row[0]
            val = ':'.join(row[i] for i in range(1, len(row)))
            r.sadd(key, val)


if __name__ == '__main__':
    try:
        load_csv()
        load_xml()
    except Exception as e:
        print e
