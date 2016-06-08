# -*- coding: utf-8 -*-
import redis
import csv

file_router = '/var/jdata/router.csv'
# количество строк заголовка таблицы
table_header = 2

file_port_all = '/var/jdata/bd.csv'
# пропустить заголовок
skip_header = True
# разделитель
delimiter = ','

# хост Redis
host = 'localhost'
# порт Redis
port = 6380
# Прогрузка в отдельные бд Redis
db0, db1 = 0, 1


def load_port_all():
    r = redis.StrictRedis(host=host, port=port, db=db0)

    count_update = 0

    with open(file_port_all, 'rb') as csvfile:
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
            count_update += 1
    return count_update


def load_router():
    r = redis.StrictRedis(host=host, port=port, db=db1)

    count_update = 0

    with open(file_router, 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=delimiter, quoting=csv.QUOTE_NONE)
        if skip_header:
            next(reader, None)
        for row in reader:
            key = row[0]
            val = ':'.join(row[i] for i in range(1, 8))
            r.srem(key, val)
            r.sadd(key, val)
            count_update += 1
    return count_update


if __name__ == '__main__':
    try:
        count = load_port_all()
        print u'Обновлено записей port_all: %s' % (count,)
    except Exception as e:
        print e
    try:
        count = load_router()
        print u'Обновлено записей router: %s' % (count,)
    except Exception as e:
        print e
