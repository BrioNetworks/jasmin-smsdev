import redis


def execute_low_level(command, *args, **kwargs):
    connection = redis.Connection(**kwargs)
    try:
        connection.connect()
        connection.send_command(command, *args)

        response = connection.read_response()
        if command in redis.Redis.RESPONSE_CALLBACKS:
            return redis.Redis.RESPONSE_CALLBACKS[command](response)
        return response

    finally:
        del connection


response = execute_low_level('GET', '9000000014', host='localhost', port=6379, db=0)

members = execute_low_level('SMEMBERS', '937', host='localhost', port=6379, db=1)

print response

for m in members:
    try:
        arr = m.split(':')
        if int(arr[0]) <= int('4161008') <= int(arr[1]):
            print m
            break
    except IndexError:
        pass

