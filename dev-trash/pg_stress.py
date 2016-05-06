from psycopg2 import Error
import thread
import psycopg2

pg_conn = 'dbname=storagesms host=localhost port=5432 user=postgres password=root'


def stress(thread_name, sms_count):
    conn = psycopg2.connect(pg_conn)
    cursor = conn.cursor()

    for i in range(0, sms_count):

        sql = 'SELECT public.add_sms (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
        data = ('message-id',
                'short_message',
                'connector',
                'routed_cid',
                0,
                'uid',
                'source',
                'destination',
                0,
                'pdu_status',
                '2000-01-01 00:00:00')
        try:
            cursor.execute(sql, data)
            conn.commit()

        except Error as e:
            print e.message
    cursor.close()
    conn.close()
    print thread_name, 'success stress'


thread.start_new_thread(stress, ('thread_1', 1000, ))
thread.start_new_thread(stress, ('thread_2', 1000, ))
thread.start_new_thread(stress, ('thread_3', 1000, ))
thread.start_new_thread(stress, ('thread_4', 1000, ))
thread.start_new_thread(stress, ('thread_5', 1000, ))

while 1:
   pass
