#!/usr/local/bin python3
import contextlib
import subprocess
from typing import Text

import psycopg2
from psycopg2.extras import DictCursor
from sqlalchemy import create_engine

from configuration import POSTGRES_USER_NAME
from configuration import POSTGRES_LOCATION
from configuration import DB_NAME
from configuration import SCHEMA_NAME
from configuration import POSTGRES_PASSWORD


SQLALCHEMY_VERBOSE = True

def connection_string(user_name=POSTGRES_USER_NAME, password=POSTGRES_PASSWORD, location=POSTGRES_LOCATION, db_name=DB_NAME):
    return 'postgresql://{}:{}@{}/{}'.format(user_name, password, location, db_name)


class lazySessionMaker():
    _Session = None

    @classmethod
    def get_session(cls, autoflush=True, echo=False):
        if not cls._Session:
            from sqlalchemy.orm import sessionmaker
            cls._Session = sessionmaker(bind=create_engine(connection_string(), echo=echo), autoflush=autoflush)
        return cls._Session()

@contextlib.contextmanager
def db_session(autoflush=True, echo=False):

    session = lazySessionMaker.get_session(autoflush=autoflush, echo=echo)

    try:
        yield session
    except Exception:
        session.rollback()
        raise
    else:
        session.commit()
    # finally:
        # session.close()

def do_sql(working_db, server=POSTGRES_LOCATION, user=POSTGRES_USER_NAME, command=''):
    # type: (Text, Text, Text, Text) -> int
    args = ['psql', '-h', server, '-p', '5432', '-U', user, '-d', working_db, '-c', command]
    print(' '.join(args))
    return subprocess.call(args)

def local(command_string):
    # type: (Text) -> bool
    error_code = subprocess.call(command_string, shell=True)
    if error_code:
        return False
    return True

@contextlib.contextmanager
def get_connection(*, db_name=DB_NAME, user=POSTGRES_USER_NAME, password=POSTGRES_PASSWORD):
    # type: (Text, Text, Text) -> Connection
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
def make_cursor(conn_function, *, db_name=DB_NAME, schema_name=SCHEMA_NAME, user=POSTGRES_USER_NAME):
    with conn_function(db_name=db_name, user=user) as connection:
        connection.set_session()
        with connection.cursor(cursor_factory=DictCursor) as cursor: #fetchall on a DictCursor returns a list of DictRows, which behave like dictionaries
            if schema_name:
                print('setting schema name to {}'.format(schema_name))
                cursor.execute('SET SEARCH_PATH TO %(schema_name)s', dict(schema_name=schema_name))
            yield cursor

def get_cursor(*, db_name=DB_NAME, schema_name=SCHEMA_NAME, user=POSTGRES_USER_NAME):
    return make_cursor(get_connection, schema_name=schema_name)
