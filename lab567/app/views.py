from flask import (
    Flask, request, abort, jsonify, send_from_directory, send_file, redirect, url_for,
    render_template)
import json
import logging
from app import app
from app.data_processors import DataProcessor
import os
from app.loggers import context
from app.constants import ROWS_NUMBER

@app.before_request
def before_request_func():
    pass


@app.route('/', methods=['POST'])
def post():
    form_values = request.form.to_dict(flat=True)
    if 'url' in form_values:
        url = form_values['url']
        rows_per_time = int(form_values['rows_per_time']) if 'rows_per_time' in form_values else ROWS_PER_TIME
        rows_number = int(form_values['rows_number']) if 'rows_number' in form_values else ROWS_NUMBER
        offset = int(form_values['offset']) if 'offset' in form_values else OFFSET
        data_processor = DataProcessor(url, rows_number, rows_per_time, offset)
        get_data = data_processor.loop_data()
        return jsonify(get_data)
    else:
        response = {"error": "No URL specified", 'status_code': 404}
        return response


@app.after_request
def after_request_func(response):
    return response


@app.teardown_request
def after_all_requests(response):
    pass
