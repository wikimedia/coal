#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
  coal-web
  ~~~~~~~~
  Simple Flask webapp for serving coal metrics.

  Copyright 2015 Ori Livneh <ori@wikimedia.org>

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

"""
from __future__ import division
import flask
import numpy
import os.path
import time
import werkzeug.contrib.cache
import whisper


class CoalWeb(object):
    METRICS = (
        'responseStart',    # Time to user agent receiving first byte
        'firstPaint',       # Time to initial render
        'domInteractive',   # Time to DOM Ready event
        'loadEventEnd',     # Time to load event completion
        'saveTiming',       # Time to first byte for page edits
    )

    PERIODS = {
        'hour':  60 * 60,
        'day':   60 * 60 * 24,
        'week':  60 * 60 * 24 * 7,
        'month': 60 * 60 * 24 * 30,
        'year':  60 * 60 * 24 * 365.25,
    }

    WHISPER_DIR = '/var/lib/coal'

    app = flask.Flask(__name__)

    def __init__(self):
        self.cache = werkzeug.contrib.cache.SimpleCache()

    @app.after_request
    def add_header(self, response):
        """Add CORS and Cache-Control headers to the response."""
        response.cache_control.max_age = 30
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'GET'
        return response

    def chunks(self, items, chunk_size):
        """Split `items` into sub-lists of size `chunk_size`."""
        for index in range(0, len(items), chunk_size):
            yield items[index:index + chunk_size]

    def interpolate_missing(self, sparse_list):
        """Use linear interpolation to estimate values for missing samples."""
        dense_list = list(sparse_list)
        x_vals, y_vals, x_blanks = [], [], []
        for x, y in enumerate(sparse_list):
            if y is not None:
                x_vals.append(x)
                y_vals.append(y)
            else:
                x_blanks.append(x)
        if x_blanks:
            interpolants = numpy.interp(x_blanks, x_vals, y_vals)
            for x, y in zip(x_blanks, interpolants):
                dense_list[x] = y
        return dense_list

    def fetch_metric(self, metric, period):
        now = int(time.time())
        to_time = now - 60
        from_time = to_time - period
        wsp = os.path.join(self.WHISPER_DIR, metric + '.wsp')
        (start, end, step), all_samples = whisper.fetch(wsp, from_time, to_time)
        samples_per_point = len(all_samples) // 60
        points = []
        for chunk in self.chunks(all_samples, samples_per_point):
            samples = [sample for sample in chunk if sample]
            if samples:
                points.append(numpy.median(samples))
            else:
                points.append(None)
        if any(points):
            points = [round(pt, 1) for pt in self.interpolate_missing(points)]
        else:
            points = []
        return {
            'start': start,
            'end': end,
            'step': period // 60,
            'points': points,
        }

    @app.route('/v1/metrics')
    def get_metrics(self):
        response = self.cache.get(flask.request.url)
        if response is not None:
            return response
        period_name = flask.request.args.get('period', 'day')
        if period_name not in self.PERIODS:
            return flask.jsonify(error='Invalid value for "period".'), 401
        period = self.PERIODS.get(period_name)
        points = {}
        for metric in self.METRICS:
            data = self.fetch_metric(metric, period)
            points[metric] = data['points']
        response = flask.jsonify(
            start=data['start'],
            end=data['end'],
            step=period // 60,
            points=points
        )
        self.cache.set(flask.request.url, response, timeout=30)
        return response

    def run(self):
        self.app.run(debug=True)
