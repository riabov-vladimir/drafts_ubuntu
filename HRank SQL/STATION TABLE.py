import pymysql
from pymysql.cursors import DictCursor
from pprint import pprint


def db_connect():
    connection = pymysql.connect(
        host='localhost',
        user='riabowdb',
        password='Iloveksenia68',
        db='hackerrank',
        charset='utf8mb4',
        # cursorclass=DictCursor
    )
    # code
    connection.close()
    return


def read_station_lines(filename='HRank station table.txt') -> list:
    """
    Читает текстовый файл со строками из STD_OUT сайта хакерранк из раздела про SQL.
    Возвращает список кортежей.
    :param filename:
    :return: [(id, lat_n, long_w, state, city), ...]
    """

    with open(filename, 'r', encoding='utf-8') as station:
        lines = station.readlines()
        result = []
        for line in lines:

            if line > '\n':
                tmp = []
                a = line.strip().split()
                tmp.append(int(a.pop(0)))
                tmp.append(int(a.pop()))
                tmp.append(int(a.pop()))
                tmp.append(a.pop())
                tmp.append((' '.join(a)))
                result.append(tuple(tmp))
    return result


if __name__ == '__main__':
    table = read_station_lines()
    print(table)
    # quit()
    connection = pymysql.connect(
        host='localhost',
        user='riabowdb',
        password='Iloveksenia68',
        db='hackerrank',
        charset='utf8mb4',
        cursorclass=DictCursor
    )


    with connection.cursor() as cursor:

        query = 'insert into station (id, lat_n, long_w, state, city) values (%s, %s, %s, %s, %s)'
        query2 = 'insert into station (id, lat_n, long_w, state, city) values (%s, %s, %s, %s, %s)'
        query3 = 'select * from station;'
        cursor.executemany(query, table)
        # cursor.execute(query2, (794, 73, 140, 'MO', 'Kissee Mills'))
        # cursor.execute(query3)
        connection.commit()



        [print(row) for row in cursor]
        connection.close()

