#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
  coal
  ~~~~
  Coal logs Navigation Timing metrics to Graphite.

  More specifically, coal aggregates all of the samples for a given NavTiming
  metric that are collected within a period of time, and it writes the median
  of those values to Graphite.

  See the constants at the top of the file for configuration options.

  Copyright 2015 Ori Livneh <ori@wikimedia.org>
  Copyright 2018 Ian Marlier <imarlier@wikimedia.org>

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
import argparse
import etcd
from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata
import dateutil.parser
import dateutil.tz
import json
import logging
import pytz
import socket
import time


class Coal(object):
    UPDATE_INTERVAL = 60  # How often we log values, in seconds
    WINDOW_SPAN = UPDATE_INTERVAL * 5  # Size of sliding window, in seconds.
    RETENTION = 525949    # How many datapoints we retain. (One year's worth.)
    METRICS = (
        'connectEnd',
        'connectStart',
        'dnsLookup',
        'domainLookupStart',
        'domainLookupEnd',
        'domComplete',
        'domContentLoadedEventStart',
        'domContentLoadedEventEnd',
        'domInteractive',
        'fetchStart',
        'firstPaint',
        'loadEventEnd',
        'loadEventStart',
        'mediaWikiLoadComplete',
        'mediaWikiLoadStart',
        'mediaWikiLoadEnd',
        'redirectCount',
        'redirecting',
        'redirectStart',
        'redirectEnd',
        'requestStart',
        'responseEnd',
        'responseStart',
        'saveTiming',
        'secureConnectionStart',
        'unloadEventStart',
        'unloadEventEnd',
    )
    ARCHIVES = [(UPDATE_INTERVAL, RETENTION)]

    def __init__(self, brokers, consumer_group, schemas, graphite_host,
                 graphite_port=2003, graphite_prefix='coal', datacenter=None,
                 etcd_domain=None, etcd_path=None, etcd_refresh=10, dry_run=False,
                 verbose=False):
        self.brokers = brokers
        self.consumer_group = consumer_group
        self.schemas = schemas
        self.graphite_host = graphite_host
        self.graphite_port = graphite_port
        if graphite_prefix[-1] == '.':
            self.graphite_prefix = graphite_prefix[:-1]
        else:
            self.graphite_prefix = graphite_prefix
        self.dry_run = dry_run
        self.verbose = verbose

        # Log config
        self.log = logging.getLogger(__name__)
        self.log.setLevel(logging.DEBUG if self.verbose else logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG if self.verbose else logging.INFO)
        formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] (%(funcName)s:%(lineno)d) %(msg)s')
        formatter.converter = time.gmtime
        ch.setFormatter(formatter)
        self.log.addHandler(ch)

        # Set up etcd, if needed, and establish whether this is a session leader
        self.master = True
        self.master_last_updated = 0
        self.datacenter = datacenter
        self.etcd_path = etcd_path
        self.etcd_refresh = etcd_refresh
        if datacenter is not None and etcd_domain is not None and etcd_path is not None:
            self.log.info('Using etcd to check whether {} is master'.format(self.datacenter))
            self.etcd = etcd.Client(srv_domain=etcd_domain, protocol='https',
                                    allow_reconnect=True)
        else:
            self.etcd = None
        #
        # events is a dict, of dicts.  The keys are the schemas that we're working
        # with.  It's necessary to operate at the schema level because the data
        # for each event type is on a seperate Kafka topic, and we may be at very
        # different locations on each topic.
        #
        # The key for each item in events is a timestamp that is aligned to the
        # minute boundary.  That is:
        #     key = timestamp - (timestamp % 60)
        #
        # Each item is itself a dict, whose keys are the names of metrics.  Each
        # of those is a list of values collected for that metric within the window,
        # where the window is defined as
        #     minute_boundary <= timestamp < (minute_boundary + UPDATE_INTERVAL)
        #
        # Thus, the resulting events dict will look something like this:
        # events = {
        #       'NavigationTiming': {
        #           1522072560: {
        #               'connectEnd': [1, 2, 5, 9, 2, 3],
        #               'connectStart': [1, 1, 1, 1, 1],
        #               .....
        #           },
        #           1522072620: {
        #               'connectEnd': [.......],
        #               .....
        #           },
        #       },
        #       'SaveTiming': {
        #           1522072560: {....},
        #           ....
        #       }
        #   }
        self.events = {}
        for schema in self.schemas:
            self.events[schema] = {}

        #
        # offsets is a dict whose keys are schema names.  Each is itself a dict,
        # whose keys are timestamps aligned to the minute boundary, as with events.
        # However, the content of each key is a single value representing the
        # highest kafka offset seen within that boundary.
        #
        self.offsets = {}
        for schema in self.schemas:
            self.offsets[schema] = {}

        #
        # Keep a timestamp that tracks the last time we flushed the recorded
        # values for each schema
        #
        self.oldest_boundary = {}
        for schema in self.schemas:
            # This value will be reset the first time a message is read from Kafka
            self.oldest_boundary[schema] = None

    def is_master(self):
        if self.etcd is None:
            return True

        if time.time() - self.master_last_updated < self.etcd_refresh:
            # Whether this is the master data center was checked no more than
            # etcd_refresh seconds ago
            return self.master

        # Update the last_update timestamp whether success or no -- don't want
        # to pummel etcd if we're not able to update, and multiple instances
        # writing wouldn't be that big a deal
        self.master_last_updated = time.time()
        try:
            # >>> client.get('/conftool/v1/mediawiki-config/common/WMFMasterDatacenter').value
            # u'{"val": "eqiad"}'
            master_datacenter = json.loads(self.etcd.get(self.etcd_path).value)['val']
            if master_datacenter == self.datacenter and not self.master:
                self.log.info('{} is the master datacenter, going live'.format(
                                self.datacenter))
                self.master = True
            elif master_datacenter != self.datacenter and self.master:
                self.log.info('{} is not the master datacenter, disabling consumer'.format(
                                self.datacenter))
                self.master = False
            else:
                self.log.debug('{} was already the {} datacenter'.format(
                                self.datacenter,
                                'master' if master_datacenter == self.datacenter else 'secondary'))
        except KeyError:
            self.log.warning('etcd key {} may be malformed (KeyError raised)'.format(self.etcd_path))
        except etcd.EtcdKeyNotFound:
            self.log.warning('etcd key {} not found'.format(self.etcd_path))
        return self.master

    def topic(self, schema):
        return 'eventlogging_{}'.format(schema)

    def median(self, population):
        population = list(sorted(population))
        length = len(population)
        if length == 0:
            raise ValueError('Cannot compute median of empty list.')
        index = (length - 1) // 2
        if length % 2:
            return population[index]
        middle_terms = population[index] + population[index + 1]
        return middle_terms / 2.0

    # Submit metrics to graphite
    #
    # Use a short timeout, and if that timeout expires, then log the metric.
    # It's not clear that there's any reason to expect this to happen
    def send_to_graphite(self, metric, value, timestamp):
        # Shouldn't ever get called anyway, but be defensive
        if not self.dry_run:
            try:
                # Use a TCP socket, so that we don't end up dropping data in the event
                # that the remote end is unavailable
                sock = socket.create_connection((self.graphite_host, self.graphite_port),
                                                timeout=5)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.send('{}.{} {} {}\n'.format(self.graphite_prefix, metric, value,
                                                 timestamp))
                self.log.debug('[{}] [{}] Submitted {} to graphite at key {}.{}'.format(
                    metric, timestamp, value, self.graphite_prefix, metric))
            except Exception:
                # Generally an exception should be logged at exception() level. In this
                # case, we're intentionally not doing that.  This clause will generally
                # only be hit if the graphite server is down or slow, and the stack
                # trace isn't going to give us anything meaningful that we won't
                # get elsewhere.
                self.log.error('Failed to submit to graphite: {}.{} {} {}'.format(
                                            self.graphite_prefix, metric, value, timestamp))
            finally:
                try:
                    sock.close()
                except Exception:
                    # This is extremely defensive programming.  Can't really imagine
                    # what situation would result in sock.close() raising, but just
                    # in case...
                    pass

    #
    # Process an incoming event
    #
    # Parameters:
    #   meta: event dict (post JSON decoding)
    #
    # Returns:
    #   None if no offsets need to be commited, or int representing the max offset flushed
    #
    def handle_event(self, meta, offset):
        if 'schema' in meta:
            schema = meta['schema']
        else:
            self.log.warning('Message received with no schema defined')
            return None

        if schema not in self.schemas:
            self.log.warning('Message received with invalid schema')
            return None

        # dt is main EventCapsule timestamp field in ISO-8601
        if 'dt' in meta:
            # Here be shennanigans.
            #
            # datetime's strftime() method is broken: https://bugs.python.org/issue12750
            #
            # To work around this, we can use time.mktime(), which takes a struct
            # in _local_ time.  So:
            #     1. Read the naive meta['dt'] parameter
            #     2. Set the timezone to UTC (instead of None)
            #     3. Change the timezone to local based on the system's setting
            #     4. Use mktime to convert to a timestamp
            dt = dateutil.parser.parse(meta['dt'])
            dt = dt.replace(tzinfo=pytz.utc)
            timestamp = int(time.mktime(dt.astimezone(dateutil.tz.tzlocal()).timetuple()))
        # timestamp is backwards compatible int, this shouldn't be used anymore.
        elif 'timestamp' in meta:
            timestamp = meta['timestamp']
        # else we can't find one, so return
        else:
            self.log.warning('Message received with no timestamp')
            return None

        if 'event' not in meta:
            self.log.warning('No event data contained in message')
            return None

        minute_boundary = int(timestamp - (timestamp % self.UPDATE_INTERVAL))

        # This is the first message that we've processed for this schema since
        # startup
        if self.oldest_boundary[schema] is None:
            self.log.debug('[{}] Moving oldest boundary marker back to {}'.format(
                schema, minute_boundary))
            self.oldest_boundary[schema] = minute_boundary

        if minute_boundary < self.oldest_boundary[schema]:
            self.log.warning('[{}] Message received from boundary {}, oldest is {}'.format(
                schema, minute_boundary, self.oldest_boundary[schema]))

        if minute_boundary not in self.events[schema]:
            self.log.debug('[{}] Adding boundary at {}'.format(schema, minute_boundary))
            self.events[schema][minute_boundary] = {}

        event = meta['event']
        for metric in self.METRICS:
            value = event.get(metric)
            if value:
                if metric not in self.events[schema][minute_boundary]:
                    self.events[schema][minute_boundary][metric] = []
                self.events[schema][minute_boundary][metric].append(value)

        # If this offset is the highest that we know about for this boundary,
        # record it.
        if minute_boundary not in self.offsets:
            self.offsets[schema][minute_boundary] = offset
        else:
            if offset > self.offsets[schema][minute_boundary]:
                self.offsets[schema][minute_boundary] = offset

        #
        # Figure out whether to process the collected data, based on the timestamp
        # Of the message being processed.  The idea here is that timestamp will
        # be generally increasing.  Not monotonically, but close enough to
        # monotonically that we can fudge it by waiting an entra UPDATE_INTERVAL
        # before processing each WINDOW_SPAN.
        #
        # Generally speaking, what this means is that events will have 6 active
        # windows:
        #   events = {
        #       1522072560: {},
        #       1522072620: {},
        #       1522072680: {},
        #       1522072740: {},
        #       1522072800: {},
        #       1522072860: {}
        #   }
        #
        # As soon as a message is received that causes a new window to be created,
        # the first 5 of these 6 windows will be processed.  The resulting data
        # points will use the timestamp 1522072800.  Then, the dict with timestamp
        # 1522072560 will be deleted.
        #
        # events then looks like this:
        #   events = {
        #       1522072620: {},
        #       1522072680: {},
        #       1522072740: {},
        #       1522072800: {},
        #       1522072860: {},
        #       1522072920: {}
        #   }
        #
        if (timestamp - self.WINDOW_SPAN - self.UPDATE_INTERVAL) > self.oldest_boundary[schema]:
            return self.flush_data(schema)

    #
    # Flush the data that's been collected to graphite.  Commit the appropriate
    # Kafka offsets so that if we restart, we don't re-process them.  The offsets
    # committed will be only those in the oldest UPDATE_INTERVAL of WINDOW_SPAN
    #
    # NB: This function is intentionally written in such a way that it can easily
    # be called from either handle_event, or from an async timer function, thus
    # some of the extra sanity checking around boundary lengths and the like
    #
    # Parameters:
    #   schema: string name of the schema that this message belongs to
    #
    # Returns:
    #   None if there's no offsets to commit, int otherwise
    #
    def flush_data(self, schema):
        self.log.debug('Flushing data for schema {}'.format(schema))

        offset_to_return = None

        while True:
            #
            # Start with the oldest minute_boundary value, since it's possible
            # that we're catching up
            #
            sorted_boundaries = sorted(self.events[schema].keys())

            if len(sorted_boundaries) == 0:
                self.log.info('No data to process')
                return offset_to_return

            oldest_boundary = sorted_boundaries[0]
            newest_boundary = sorted_boundaries[-1]

            #
            # We don't want to flush the data that we've accumulated if there's a
            # chance that we might still get data in one of the relevant windows.
            #
            # We're (relatively) naively assuming that UPDATE_INTERVAL is enough
            # time to wait for any lagged messages, so we want to be sure that
            # it's been at least UPDATE_INTERVAL + WINDOW_SPAN since the oldest
            # boundary.
            #
            if (oldest_boundary + self.WINDOW_SPAN + self.UPDATE_INTERVAL) > newest_boundary or \
                    (oldest_boundary + self.WINDOW_SPAN + self.UPDATE_INTERVAL) > time.time():
                self.log.debug('All windows with sufficient data have been processed')
                self.log.debug('Returning last offset {}'.format(offset_to_return))
                return offset_to_return

            #
            # If we get here, we know that we have at least one window that can be
            # processed.
            #
            # Start by creating a list of the timestamps that are within the current
            # window.
            #
            boundaries_to_consider = [boundary for boundary in sorted_boundaries if
                                      boundary < (oldest_boundary + self.WINDOW_SPAN)]
            self.log.info('[{}] Processing events in boundaries [{}]'.format(
                                                schema, boundaries_to_consider))

            # Make a dict of the metrics that have samples within this window, and
            # put all of the collected samples into a list.  Don't assume that every
            # metric present in the window is in the first boundary.
            metrics_with_samples = {}
            for boundary in boundaries_to_consider:
                for metric in self.events[schema][boundary]:
                    if metric not in metrics_with_samples:
                        metrics_with_samples[metric] = []
                    metrics_with_samples[metric].extend(self.events[schema][boundary][metric])

            # Get the median for each metric and write:
            for metric, values in metrics_with_samples.items():
                median_value = self.median(values)
                if self.dry_run:
                    self.log.info('[{}] [{}] {}'.format(metric,
                                                        oldest_boundary + self.WINDOW_SPAN,
                                                        median_value))
                    # If this is a dry run, we don't want to actually commit, but
                    self.log.debug('[{}] Dry run, so not actually committing to offset {}'.format(
                                            schema, self.offsets[schema][oldest_boundary]))
                    offset_to_return = None
                else:
                    self.send_to_graphite(metric=metric, value=median_value,
                                          timestamp=oldest_boundary + self.WINDOW_SPAN)

            #
            # Return the highest offset from the oldest boundary, and then
            # delete the oldest boundary from the events and offsets dicts
            #
            offset_to_return = None if self.dry_run else self.offsets[schema][oldest_boundary]
            del self.events[schema][oldest_boundary]
            del self.offsets[schema][oldest_boundary]

            # Set the oldest_boundary value to the next oldest boundary
            self.oldest_boundary[schema] = sorted_boundaries[1]

        return offset_to_return

    def commit(self, consumer, schema, offset):
        # Kafka wants you to commit the NEXT offset to be read, not the LAST
        # offset that was read
        offset_to_commit = offset + 1

        self.log.debug('[{}] Committing offset {}'.format(schema, offset_to_commit))
        consumer.commit({
            TopicPartition(self.topic(schema), 0): OffsetAndMetadata(offset_to_commit, None)
            })
        self.log.info('[{}] Offset {} committed'.format(schema, offset_to_commit))

    def run(self):
        # There are basically 3 ways to handle timers
        #  1. On each received Kafka message, check whether we've tipped over into
        #     the next window, and if so, do processing.  The problem with this
        #     is that if no message is received for some period of time, then
        #     processing never happens.
        #  2. Run the Kafka poller in one thread, and the timer in another.  Use
        #     a lock to temporarily block the poller while processing is happening.
        #     While this is pretty straightforward, it does require dealing with
        #     threads and shared vars.
        #  3. Run the poller inside of one aio function, and the timer inside of
        #     another.  Leave it to asyncio to handle the coordination.  This is,
        #     honestly, the most complete and easiest to reason about of these
        #     solutions.  But it also requires py >= 3.4, aiokafka, and potentially
        #     other aio libraries that aren't packaged natively.
        #
        # This method, as written, implements #1.
        consumer = None
        while True:
            try:
                if not self.is_master():
                    time.sleep(self.etcd_refresh + 1)
                    self.log.info('Checking whether datacenter has been promoted')
                    continue
                self.log.info('Starting Kafka connection to brokers ({}).'.format(
                    self.brokers))
                consumer = KafkaConsumer(
                    bootstrap_servers=self.brokers,
                    group_id=self.consumer_group,
                    enable_auto_commit=False)

                # Work out topic names based on schemas, and subscribe to the
                # appropriate topics
                topics = [self.topic(schema) for schema in self.schemas]
                self.log.info('Subscribing to topics: {}'.format(topics))
                consumer.subscribe(topics)

                self.log.info('Beginning poll cycle')

                for message in consumer:
                    # Check whether we should be running
                    if not self.is_master():
                        self.log.info('No longer running in the master datacenter')
                        self.log.info('Committing and stopping consuming')
                        for schema in self.schemas:
                            offset_to_commit = self.flush_data(schema)
                            if offset_to_commit is not None:
                                self.commit(consumer, schema, offset_to_commit)
                        consumer.close()
                        break

                    # Message was received
                    if 'error' in message:
                        self.log.error('Kafka error: {}'.format(message.error))
                    else:
                        try:
                            value = json.loads(message.value)
                        except ValueError:
                            # If incoming messages aren't well-formatted, log them
                            # so we can see that and handle it.
                            self.log.error(
                                'ValueError raised trying to load JSON message: %s',
                                message.value())
                            continue

                        # If this is an oversample, then skip it
                        #
                        # The object uses the Javascript naming standard (camelCase),
                        # not the python standard (with_underscores)
                        if 'event' in value and 'isOversample' in value['event'] and \
                                                value['event']['isOversample']:
                            continue

                        # Get the incoming event on to the pile, and if necessary,
                        # get back the offset that we need to commit to Kafka
                        offset_to_commit = self.handle_event(value, message.offset)
                        if offset_to_commit is None:
                            continue
                        else:
                            self.commit(consumer, value['schema'], offset_to_commit)

            # Allow for a clean quit on interrupt
            except KeyboardInterrupt:
                self.log.info('Stopping the Kafka consumer and shutting down')
                if consumer is None:
                    break
                try:
                    # Take a stab at flushing any remaining data, just in case
                    self.log.info('Trying to commit any last data before exit')
                    for schema in self.schemas:
                        offset_to_commit = self.flush_data(schema)
                        if offset_to_commit is not None:
                            self.commit(consumer, schema, offset_to_commit)
                    consumer.close()
                except Exception:
                    self.log.exception('Exception closing consumer')
                break
            except IOError:
                self.log.exception('Error in main loop, restarting consumer')
            except Exception:
                self.log.exception('Unhandled exception caught, restarting consumer')


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--brokers', required=True,
                            help='Comma-separated list of Kafka brokers')
    arg_parser.add_argument('--consumer-group', required=True,
                            dest='consumer_group', help='Name of the Kafka consumer group')
    arg_parser.add_argument('--schema', required=True, action='append',
                            dest='schemas',
                            help='Schemas that we deal with, topic names are derived')
    arg_parser.add_argument('--graphite-host', required=True, dest='graphite_host',
                            help='Host to which graphite metrics are sent')
    arg_parser.add_argument('--graphite-port', required=False, dest='graphite_port',
                            type=int, default=2003, help='Graphite plaintext port')
    arg_parser.add_argument('--graphite-prefix', required=False, dest='graphite_prefix',
                            default='coal', help='Graphite metric path prefix')
    arg_parser.add_argument('--datacenter', required=False, default=None,
                            dest='datacenter', help='Current datacenter (eg, eqiad)')
    arg_parser.add_argument('--etcd-domain', required=False, default=None,
                            dest='etcd_domain', help='Domain to use for etcd srv lookup')
    arg_parser.add_argument('--etcd-path', required=False, default=None,
                            dest='etcd_path', help='Where to find the etcd MasterDatacenter value')
    arg_parser.add_argument('--etcd-refresh', required=False, default=10,
                            dest='etcd_refresh', help='Seconds to wait before refreshing etcd')
    arg_parser.add_argument('-n', '--dry-run', required=False, dest='dry_run',
                            action='store_true', default=False,
                            help='Don\'t send metrics, just log them')
    arg_parser.add_argument('-v', '--verbose', dest='verbose',
                            required=False, default=False, action='store_true',
                            help='Increase verbosity of output')
    args = arg_parser.parse_args()
    app = Coal(brokers=args.brokers,
               consumer_group=args.consumer_group,
               schemas=args.schemas,
               graphite_host=args.graphite_host,
               graphite_port=args.graphite_port,
               graphite_prefix=args.graphite_prefix,
               datacenter=args.datacenter,
               etcd_domain=args.etcd_domain,
               etcd_path=args.etcd_path,
               etcd_refresh=args.etcd_refresh,
               dry_run=args.dry_run,
               verbose=args.verbose)
    app.run()


if __name__ == '__main__':
    main()
