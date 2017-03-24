import sqlite3
import contextlib
from datetime import datetime

import bs4
import requests

URL = 'https://domaintyper.com/top-websites/' \
      'most-popular-websites-with-com-domain'
DB_NAME = 'db.sqlite3'


def fetch_domains():
    """Download most popular domains and write them to file"""
    response = requests.get(URL)
    response.raise_for_status()
    soup = bs4.BeautifulSoup(response.text, 'html.parser')
    return [
        row[1].get_text()
        for row in (
            row.find_all('td')
            for row in soup.table.find_all('tr')
        )
        if len(row) == 3
    ]


def get_db():
    """Opens a new database connection"""
    rv = sqlite3.connect(DB_NAME)
    rv.row_factory = sqlite3.Row
    return rv


def init_db():
    """Initializes the database.
    >>> init_db()
    """
    with contextlib.closing(get_db()) as db:
        cursor = db.cursor()
        with open('schema.sql', mode='r') as file:
            cursor.executescript(file.read())
        cursor.executemany(
            'INSERT INTO records(host, last_refresh) VALUES (?, ?)',
            [(domain, datetime.now().timestamp()) for domain in fetch_domains()]
        )
        db.commit()
