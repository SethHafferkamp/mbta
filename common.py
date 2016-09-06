#!/usr/local/bin/env python
import contextlib
import subprocess
import sys
import psycopg2

from configuration import POSTGRES_USER_NAME
from configuration import POSTGRES_LOCATION
from configuration import DB_NAME
from configuration import SCHEMA_NAME
from configuration import POSTGRES_PASSWORD


def create_db(db_name):
    # error_code = local("psql -U {} -c 'CREATE DATABASE {};'".format(POSTGRES_USER_NAME, db_name))
    error_code = do_sql(db_name, command="CREATE DATABASE {};".format(db_name))
    if error_code:
        return False
    return True

def create_schema(db_name, schema_name):
    # error_code = local("psql -U {} -c 'CREATE DATABASE {};'".format(POSTGRES_USER_NAME, db_name))
    error_code = do_sql(db_name, command="CREATE SCHEMA {};".format(schema_name))
    if error_code:
        return False
    return True

def drop_schema(db_name, schema_name):
    # error_code = local("psql -U {} -c 'CREATE DATABASE {};'".format(POSTGRES_USER_NAME, db_name))
    error_code = do_sql(db_name, command="DROP SCHEMA {};".format(schema_name))
    if error_code:
        return False
    return True

def drop_db(db_name):
    # error_code = local("psql -U {} -c 'DROP DATABASE {};'".format(POSTGRES_USER_NAME, db_name))
    error_code = do_sql(db_name, command="DROP DATABASE {};".format(db_name))
    if error_code:
        return False
    return True

def do_sql(working_db, server=POSTGRES_LOCATION, user=POSTGRES_USER_NAME, command=''):
    args = ['psql', '-h', server, '-p', '5432', '-U', user, '-d', working_db, '-c', command]
    print(' '.join(args))
    subprocess.call(args)

def local(command_string):
    error_code = subprocess.call(command_string, shell=True)
    if error_code:
        return False
    return True

def start_postgres(postgres_dir='/usr/local/var/postgres', log_file='/dev/null'):
    args = ['pg_ctl', '-D', postgres_dir, '-l', log_file, 'start']
    subprocess.call(args)

def stop_postgres(postgres_dir='/usr/local/var/postgres'):
    args = ['pg_ctl', '-D', postgres_dir, 'stop']
    subprocess.call(args)


@contextlib.contextmanager
def get_connection(db_name=DB_NAME, user=POSTGRES_USER_NAME, password=POSTGRES_PASSWORD):
    conn = psycopg2.connect(database=db_name, user=user, password=POSTGRES_PASSWORD, host=POSTGRES_LOCATION)
    try:
        yield conn
    except Exception:
        print('rolled back connection')
        conn.rollback()
        raise
    else:
        print('committed connection')
        conn.commit()
    finally:
        conn.close()
        print('closed connection')


@contextlib.contextmanager
def make_cursor(conn_function, db_name=DB_NAME, schema_name=SCHEMA_NAME, user=POSTGRES_USER_NAME):
    with conn_function(db_name=db_name, user=user) as connection:
        connection.set_session()
        with connection.cursor() as cursor:
            if schema_name:
                print('setting schema name to {}'.format(schema_name))
                cursor.execute('SET SEARCH_PATH TO %(schema_name)s', dict(schema_name=schema_name))
            yield cursor


def get_cursor(db_name=DB_NAME, schema_name=SCHEMA_NAME, user=POSTGRES_USER_NAME):
    return make_cursor(get_connection, schema_name=schema_name)

if __name__ == '__main__':
    print(sys.argv[1:])


