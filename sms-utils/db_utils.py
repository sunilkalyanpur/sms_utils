"""
DB Utilities
"""

from sqlalchemy import create_engine, sql
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import table, column, text


def get_engine_string(**kwargs):
    """ Returns a string to be used with sqlalchemy.create_engine() """
    required_keys = ['database', 'adapter', 'username', 'password', 'hostname', 'port']

    if all(key in kwargs for key in required_keys):
        return "{adapter}://{username}:{password}@{hostname}:{port}/{database}".format(
            adapter=kwargs.get('adapter'),
            username=kwargs.get('username'),
            password=kwargs.get('password'),
            hostname=kwargs.get('hostname'),
            port=kwargs.get('port'),
            database=kwargs.get('database')
        )
    else:
        raise Exception('Required keys not found: {}'.format(required_keys))


def get_engine(db_config, pool_size=10):
    """
    Returns Database engine
    """
    required_keys = ['database', 'adapter', 'username', 'password', 'hostname', 'port']

    if all(key in db_config for key in required_keys):
        try:
            connect_string = get_engine_string(database=db_config.get('database'),
                                               adapter=db_config.get('adapter'),
                                               username=db_config.get('username'),
                                               password=db_config.get('password'),
                                               hostname=db_config.get('hostname'),
                                               port=db_config.get('port'))

            return create_engine(connect_string, pool_size=pool_size)
        except Exception as error:
            raise error
    else:
        raise Exception('Required keys not found: {}'.format(required_keys))


def get_scoped_session(db_engine):
    """ Returns SQL Alchemy session object for DB for ORM use. """
    try:
        return scoped_session(sessionmaker(bind=db_engine))
    except Exception as error:
        raise error


def insert(tablename, records, db_engine):
    """
    Inserts list of dicts into table
    """
    if not records:
        return None

    connection = None
    transaction = None
    try:
        connection = db_engine.connect()
        transaction = connection.begin()
        obj = table(tablename, *(column(column_name) for column_name in records[0].keys()))
        connection.execute(obj.insert(), records)
        transaction.commit()
    except Exception as error:
        transaction.rollback()
        raise error
    finally:
        if connection:
            connection.close()


def fetchone(sql_query, db_engine, **kwargs):
    """
        Returns only one record.
        Returns None if no record found
        Raises Exception if more that one record found
    """
    connection = None
    try:
        connection = db_engine.connect()

        if kwargs:
            result_set = connection.execute(text(sql_query), **kwargs)
        else:
            result_set = connection.execute(text(sql_query))

        if result_set.rowcount > 1:
            raise Exception('Found more than one record')

        record = result_set.fetchone()
        return dict(record) if record else None

    except Exception as error:
        raise error
    finally:
        if connection:
            connection.close()


def fetchall(sql_query, db_engine, **kwargs):
    """
    Returns list of dicts
    Returns empty list if no record found
    """
    connection = None
    try:
        connection = db_engine.connect()

        if kwargs:
            result_set = connection.execute(text(sql_query), **kwargs).fetchall()
        else:
            result_set = connection.execute(text(sql_query)).fetchall()

        return [dict(row) for row in result_set]

    except Exception as error:
        raise error
    finally:
        if connection:
            connection.close()


def update(table_name, records, db_engine, key=None):
    """
    Used for updating existing records.
    Returns True if success
    """
    if not records:
        return

    connection = None
    try:
        connection = db_engine.connect()
        t = table(table_name, *(column(column_name) for column_name in records[0].keys()))
        for record in records:
            stmt = sql.update(t).where(t.c.get(key) == record[key]).values(**record)
            connection.execute(stmt)

        return True
    except Exception as error:
        raise error
    finally:
        if connection:
            connection.close()
