import contextlib
from collections import namedtuple

from flask import Flask, request, render_template

import db

app = Flask(__name__)

Page = namedtuple('Page', 'number is_current')


class Pagination:
    """
    >>> p = Pagination(0, 10, 2)
    >>> list(p)
    >>> p = Pagination(2, 10, 2)
    >>> list(p)
    >>> p = Pagination(5, 10, 2)
    >>> list(p)
    >>> p = Pagination(9, 10, 2)
    >>> list(p)
    >>> p = Pagination(10, 10, 2)
    >>> list(p)
    """
    def __init__(self, current_page, max_page, page_range):
        self.current_page = current_page
        self.page_range = page_range
        self.max_page = max_page

    def __iter__(self):
        if self.current_page + self.page_range > self.max_page:
            right = self.max_page
        else:
            right = self.current_page + self.page_range
        if self.current_page - self.page_range <= 1:
            left = 1
        else:
            left = self.current_page - self.page_range

        return (
            Page(str(num), self.current_page == num)
            for num in range(left, right + 1)
        )

    def next_page(self):
        if self.current_page + 1 > self.max_page:
            return None
        else:
            return str(self.current_page + 1)

    def prev_page(self):
        if self.current_page - 1 < 1:
            return None
        else:
            return str(self.current_page - 1)


@app.route('/')
def index():
    with contextlib.closing(db.get_db()) as database:
        limit = request.args.get('limit', 10, int)
        page = request.args.get('page', 1, int)
        if page < 1:
            page = 1
        offset = (page - 1) * limit
        cursor = database.cursor()
        domains = cursor.execute(
            'SELECT host, ip FROM records ORDER BY id LIMIT ? OFFSET ?',
            (limit, offset)
        )
        pagination = Pagination(page, 10, 2)
        return render_template('index.html', query=domains, pagination=pagination)


if __name__ == '__main__':
    with contextlib.suppress(KeyboardInterrupt):
        app.run()

# TODO pagination
# TODO queries
# TODO records
# TODO ip view
# TODO domain view
# TODO utils
# TODO tests
