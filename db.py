import sqlite3
import contextlib

SELECT_LAST_RECORDS = 'SELECT host FROM records WHERE abs(last_refresh - ?) > ?'
UPDATE_RECORDS = 'UPDATE records SET ip=?, last_refresh=? WHERE host=?'
SELECT_PAGED_RECORDS = 'SELECT host, ip FROM records ' \
                       'ORDER BY id LIMIT ? OFFSET ?'


def db(db_name):
    """Opens a new database connection"""
    rv = sqlite3.connect(db_name)
    rv.row_factory = sqlite3.Row
    return rv


def query(_db, _query, *args):
    with contextlib.closing(_db.cursor()) as cursor:
        yield from cursor.execute(_query, args)
