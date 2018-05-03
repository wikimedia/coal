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
import argparse
import flask
import numpy
import requests
import time
import werkzeug.contrib.cache


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
    'year':  int(60 * 60 * 24 * 365.25),
}

CACHE_RETENTION = {
    # seconds to cache values
    'hour': 30,
    'day': 30,
    'week': 60,
    'month': 300,
    'year': 21600   # 6 hours, since this takes a while to generate

}

app = flask.Flask(__name__)
cache = werkzeug.contrib.cache.SimpleCache()


@app.after_request
def add_header(response):
    """Add CORS and Cache-Control headers to the response."""
    response.cache_control.max_age = 30
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET'
    return response


def chunks(items, chunk_size):
    """Split `items` into sub-lists of size `chunk_size`."""
    for index in range(0, len(items), chunk_size):
        yield items[index:index + chunk_size]


def interpolate_missing(sparse_list):
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


def fetch_metric(metric, period):
    now = time.time()
    to_time = int(now) - 60
    from_time = to_time - period
    url = 'https://graphite.wikimedia.org/render?target=coal.{}&from={}&to={}&format=json'.format(
                metric, from_time, to_time)

    # graphite API returns an object that looks like this:
    #
    # [{
    #     "target": "datapoint.name",
    #     "datapoints": [
    #         [value, timestamp],
    #         [value, timestamp],
    #         etc
    #     ]
    # }]
    app.logger.debug('Requesting {}'.format(url))
    raw_points = requests.get(url).json()[0]['datapoints']
    if len(raw_points) > 1:
        start = raw_points[0][1]    # In case it's offset from from_time
        end = raw_points[-1][1]     # In case it's actually offset from to_time
        all_samples = [point[0] for point in raw_points]
    else:
        raise Exception('No datapoints were retrieved for metric {} in period {} - {}'.format(
                            metric, from_time, to_time))

    samples_per_point = len(all_samples) // 60
    points = []
    for chunk in chunks(all_samples, samples_per_point):
        samples = [sample for sample in chunk if sample]
        if samples:
            points.append(numpy.median(samples))
        else:
            points.append(None)
    if any(points):
        points = [round(pt, 1) for pt in interpolate_missing(points)]
    else:
        points = []
    app.logger.debug('[{}] {} seconds to retrieve {}'.format(metric, time.time() - now, url))
    return {
        'start': start,
        'end': end,
        'step': period // 60,
        'points': points,
    }


@app.route('/v1/metrics')
def get_metrics():
    response = None
    if not app.debug:
        # Don't check the cache if we're in debug mode
        response = cache.get(flask.request.url)
    if response is not None:
        return response
    fetch_start = time.time()
    period_name = flask.request.args.get('period', 'day')
    if period_name not in PERIODS:
        return flask.jsonify(error='Invalid value for "period".'), 401
    period = PERIODS.get(period_name)
    points = {}
    for metric in METRICS:
        try:
            data = fetch_metric(metric, period)
        except Exception:
            app.logger.exception('Exception thrown by fetch_metric')
            return flask.jsonify(
                error='Unable to retrieve metric coal.{} from graphite server'.format(metric)), 401
        points[metric] = data['points']
    response = flask.jsonify(
        start=data['start'],
        end=data['end'],
        step=period // 60,
        points=points
    )
    if not app.debug:
        # Only cache if we're not running in debug mode
        # Cache year metrics for 6 hours, since they take a long time to generate.
        # Cache others for 30 seconds.
        cache.set(flask.request.url, response, timeout=CACHE_RETENTION[period_name])
    app.logger.debug('{} seconds to return metrics for period {}'.format(time.time() - fetch_start, period_name))
    return response


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Coal web')
    parser.add_argument('-v', '--verbose', action='store_true', dest='verbose',
                        default=False, help='Enable debug logging')
    args = parser.parse_args()
    app.run(debug=args.verbose)
