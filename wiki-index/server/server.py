# -*- coding:utf8 -*-
import sys
import xml.dom.minidom

import wikimarkup
from flask import Flask, render_template, request

app = Flask(__name__)


@app.route('/search')
def search():

    def get_page_title(page_id):

        (offset, length) = get_offset_and_length(int(page_id))
        fin_wiki.seek(offset)
        page = fin_wiki.read(length)

        dom = xml.dom.minidom.parseString(page)
        root = dom.documentElement
        title = root.getElementsByTagName('title')[0].firstChild.data

        return title

    keywords_text = request.args.get('keyword')
    # print(keywords_text)
    keywords = keywords_text.split(" ")
    if len(keywords) > 2:
        keywords = keywords[0:2]

    data = []
    for page_id, terms in page_term.items():
        for keyword in keywords:
            if keyword.lower() in terms:
                title = get_page_title(page_id)
                data.append({'page_id': page_id, 'title': title})
                break

        if len(data) > 25:
            break

    if len(data) > 0:
        return render_template('results.html', data=data)
    else:
        return "404 Not Found!"


@app.route('/page')
def get_page():

    page_id = request.args.get('page_id')
    (offset, length) = get_offset_and_length(int(page_id))

    html = '404 Not Found!'

    if offset != None:

        fin_wiki.seek(offset)
        page = fin_wiki.read(length)
        dom = xml.dom.minidom.parseString(page)
        root = dom.documentElement
        text = root.getElementsByTagName('text')[0].firstChild.data

        html = wikimarkup.parse(text)

    return html


@app.route('/')
def index():
    return render_template('index.html')


def get_offset_and_length(page_id):
    if page_id in page_info:
        return page_info[page_id]
    else:
        return (None, None)


def load_page_offset_and_length():

    fin = open("../results/results_pageoffset")
    global page_info
    page_info = dict()
    while True:
        line = fin.readline()
        if len(line) == 0:
            break
        data = line.split("\t")
        page_id = int(data[0])
        info = (int(data[1]), int(data[2]))
        page_info[page_id] = info

    fin.close()


def load_page_term():
    fin = open("../results/results_selected_tfidf")
    global page_term
    page_term = dict()
    while True:
        line = fin.readline()
        if len(line) == 0:
            break
        data = line.split("\t")
        page_id = int(data[0])
        term = data[1].strip()
        # print(page_id, term)
        if not (page_id in page_term):
            page_term[page_id] = list()

        page_term[page_id].append(term)

    fin.close()


if __name__ == '__main__':

    reload(sys)
    sys.setdefaultencoding('utf-8')

    fin_wiki = open("../../enwikisource.xml", "r")

    load_page_offset_and_length()
    load_page_term()

    app.run()
    
    fin_wiki.close()