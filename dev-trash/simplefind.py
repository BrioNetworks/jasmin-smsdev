# -*- coding: utf-8 -*-
import redis

phone = '79273920335'
host = 'localhost'
port = 6379
db0, db1 = 0, 1

connection = redis.Connection(host=host, port=port, db=db0)

mnc = None

try:
    connection.connect()
    connection.send_command('GET', phone[1:])
    mnc = connection.read_response()
finally:
    del connection

if not mnc:
    connection = redis.Connection(host=host, port=port, db=db1)

    def_code = phone[1:4]
    number = int(phone[-7:])

    members = set()

    try:
        connection.connect()
        connection.send_command('SMEMBERS', def_code)

        response = connection.read_response()
        members = redis.Redis.RESPONSE_CALLBACKS['SMEMBERS'](response)
    finally:
        del connection

    for m in members:
        try:
            arr = m.split(':')
            if int(arr[0]) <= number <= int(arr[1]):
                mnc = arr[5]
                break
        except IndexError:
            pass

if mnc == '99':
    print 100
elif mnc == '01':
    print 200
elif mnc == '02':
    print 300
elif mnc == '11':
    print 400
elif mnc == '20':
    print 500
