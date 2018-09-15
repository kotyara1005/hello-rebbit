import sqlite3
import contextlib

SELECT_TRACKING_DOMAINS = '''
SELECT domain_id FROM domains
WHERE abs(checked_at - ?) > ?
'''

GET_RECORDS_A = '''
SELECT a.id, ip.name
FROM records_a as a INNER JOIN ip_addresses as ip
ON a.ip_address_id = ip.ip_address_id
WHERE a.domain_id = ? AND a.outdated_at = NULL'''

SELECT_DOMAIN_WITH_NO_RECORDS = '''SELECT host FROM records_A WHERE abs(last_refresh - ?) > ?'''
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
    _db.commit()
