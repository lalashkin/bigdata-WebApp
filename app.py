from gevent import monkey
monkey.patch_all()

import re
import os
import shutil
import sqlite3
import csv
import html
import json
from collections import defaultdict

from flask import (
    Flask, render_template, request, 
    redirect, url_for, flash,
    send_from_directory, abort
)
from werkzeug.utils import secure_filename
from flask_socketio import SocketIO, send, emit
from nltk import trigrams
import pandas as pd


if not os.path.exists('db'):
    os.makedirs('db')


app = Flask(__name__)
app.secret_key = 'A0Zr98j/3yXR~XHH!jmN]LWX/,?RT'
app.config['UPLOAD_FOLDER'] = os.path.join(os.path.dirname(os.path.abspath(__file__)), "db")
ALLOWED_EXTENSIONS = set(['db', 'sqlite3', 'sqlite', 'csv'])
socketio = SocketIO(app)

filter_symbols = re.compile(r'[A-Za-z0-9 ]')

def allowed_file(filename):
    print("checking extension")
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        r = dict(request.form)
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            file.stream.seek(0)
            if filename.endswith('csv'):
                if r['column'][0]:
                    df = pd.read_csv(file.file)
                    if r['column'][0] in df:
                        open('db/result.json', 'w+').close()
                        column = df[r['column'][0]]
                        res = defaultdict(list)
                        for _, row in column.iteritems():
                            socketio.emit('dbcommand', {"resp": html.escape(f"»{row}")}, namespace='/rdb')
                            words = ''.join(filter_symbols.findall(row[:].replace(' ', '')))
                            for item in trigrams(words):
                                res[''.join(item).lower()].append(row)
                        with open('db/result.json', 'w') as jfile:
                            json.dump(res, jfile)
                        return redirect(url_for('uploaded_file', filename='result.json'))
                    else:
                        return render_template('502.html'), 502
            if filename.endswith(('db', 'sqlite3', 'sqlite')):
                pass
            return redirect(url_for('uploaded_file', filename=filename))
    return render_template('index.html')


@app.route('/uploads/<filename>')
def uploaded_file(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'],
                               filename)


@socketio.on('dbcommand', namespace='/rdb')
def process_command(message):
    socketio.emit('dbcommand',
                  {'resp': html.escape(f">>>{message}")},
                  namespace='/rdb')


if __name__ == '__main__':
    socketio.run(app)
