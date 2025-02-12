#!/usr/bin/env python
from flask import Flask
from flask import render_template
from hashlib import md5
from itertools import imap
from spy import parse_status
app = Flask(__name__)
app = Flask(__name__, template_folder='/usr/local/spy/templates')


@app.route("/")
def monitor():
    def add_ident(d):
        d['ident'] = md5(d['name']).hexdigest()
    items = [dict(s) for s in parse_status()]
    any(imap(add_ident, items))
    return render_template('monitor.html', items=items)

def run():
    app.run()

