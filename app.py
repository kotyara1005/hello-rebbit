import contextlib

from flask import Flask, render_template

import db

app = Flask(__name__)


@app.route('/')
def index():
    with contextlib.closing(db.get_db()) as database:
        cursor = database.cursor()
        domains = cursor.execute('SELECT host, ip FROM records')
        return render_template('index.html', query=domains)


if __name__ == '__main__':
    with contextlib.suppress(KeyboardInterrupt):
        app.run()
