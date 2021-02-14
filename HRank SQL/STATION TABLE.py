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

def read_students_lines(filename='STUDENTS.txt') -> list:
    """
    Читает текстовый файл со строками из STD_OUT сайта хакерранк из раздела про SQL.
    Возвращает список кортежей.
    :param filename:
    :return: [(id, name, marks), ...]
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
                tmp.append(a.pop())
                result.append(tuple(tmp))
    return result


def read_employee_lines(filename='employee.txt') -> list:
    """
    +-------------+----------+------+-----+---------+-------+
    | Field       | Type     | Null | Key | Default | Extra |
    +-------------+----------+------+-----+---------+-------+
    | employee_id | int      | YES  |     | NULL    |       |
    | name        | tinytext | YES  |     | NULL    |       |
    | months      | tinyint  | YES  |     | NULL    |       |
    | salary      | int      | YES  |     | NULL    |       |
    +-------------+----------+------+-----+---------+-------+

    Читает текстовый файл со строками из STD_OUT сайта хакерранк из раздела про SQL.
    Возвращает список кортежей.
    :param filename:
    :return: [(employee_id, salary, months, name), ...]
    """

    with open(filename, 'r', encoding='utf-8') as station:
        lines = station.readlines()
        result = []
        for line in lines:
            if line > '\n':
                tmp = []
                a = line.strip().split()
                tmp.append(int(a.pop(0))) # id
                tmp.append(int(a.pop()))  # salary
                tmp.append(a.pop())  # months
                tmp.append(a.pop())
                result.append(tuple(tmp))
    return result


def INSERT_INTO_STATION(table):

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

        cursor.executemany(query, table)

        connection.commit()
        [print(row) for row in cursor]
        connection.close()


def INSERT_INTO_STUDENTS(table):

    connection = pymysql.connect(
        host='localhost',
        user='riabowdb',
        password='Iloveksenia68',
        db='hackerrank',
        charset='utf8mb4',
        cursorclass=DictCursor
    )

    with connection.cursor() as cursor:

        # query_ct = 'create table students (id int, name tinytext, marks tinyint(100))'
        # cursor.execute(query_ct)

        query = 'insert into students (id, marks, name) values (%s, %s, %s)'

        cursor.executemany(query, table)

        connection.commit()
        [print(row) for row in cursor]
        connection.close()


def INSERT_INTO_EMPLOYEE(table: list):

    connection = pymysql.connect(
        host='localhost',
        user='riabowdb',
        password='Iloveksenia68',
        db='hackerrank',
        charset='utf8mb4',
        cursorclass=DictCursor
    )

    with connection.cursor() as cursor:

        query = 'insert into employee (employee_id, salary, months, name) values (%s, %s, %s, %s)'

        cursor.executemany(query, table)

        connection.commit()
        [print(row) for row in cursor]
        connection.close()

if __name__ == '__main__':

    table = read_employee_lines()

    print(table)

    INSERT_INTO_EMPLOYEE(table)