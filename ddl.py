import subprocess

from sqlalchemy import create_engine

from common import connection_string, SQLALCHEMY_VERBOSE, do_sql
from configuration import DB_NAME, SCHEMA_NAME, LOCAL_ROOT_DB_NAME
from observations.tables import metadata


def start_postgres(*, postgres_dir='/usr/local/var/postgres', log_file='/dev/null'):
    # type: (Text, Text) -> int
    args = ['pg_ctl', '-D', postgres_dir, '-l', log_file, 'start']
    return subprocess.call(args)

def stop_postgres(*, postgres_dir='/usr/local/var/postgres'):
    # type: (Text) -> int
    args = ['pg_ctl', '-D', postgres_dir, 'stop']
    return subprocess.call(args)

def spin_up():
    create_db(DB_NAME)
    create_schema(DB_NAME, SCHEMA_NAME)
    create_tables(metadata)

def spin_down():
    return drop_db(DB_NAME)

def create_db(db_name, *, root_db_name=LOCAL_ROOT_DB_NAME):
    # type: (Text, Text) -> bool
    error_code = do_sql(root_db_name, command="CREATE DATABASE {};".format(db_name))
    if error_code:
        return False
    return True

def create_schema(db_name, schema_name):
    # type: (Text, Text) -> bool
    error_code = do_sql(db_name, command="CREATE SCHEMA {};".format(schema_name))
    if error_code:
        return False
    return True

def create_tables(metadata):
    # type: (MetaData) -> bool
    engine = create_engine(connection_string(), echo=SQLALCHEMY_VERBOSE)
    metadata.schema = SCHEMA_NAME
    metadata.bind = engine
    return metadata.create_all()

def drop_schema(db_name, schema_name):
    # type: (Text, Text) -> bool
    error_code = do_sql(db_name, command="DROP SCHEMA {};".format(schema_name))
    if error_code:
        return False
    return True

def drop_db(db_name, *, root_db_name=LOCAL_ROOT_DB_NAME):
    # type: (Text) -> bool
    error_code = do_sql(root_db_name, command="DROP DATABASE {};".format(db_name))
    if error_code:
        return False
    return True
