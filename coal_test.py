#!/usr/bin/env python

import unittest
import coal


class TestCoal(unittest.TestCase):
    def setUp(self):
        self.wl = coal.Coal(brokers=['127.0.0.1'],
                            consumer_group='coal_test',
                            schemas=['NavigationTiming', 'SaveTiming'],
                            graphite_host='localhost',
                            graphite_port=2003,
                            graphite_prefix='test',
                            dry_run=True,
                            verbose=True)

    def test_handle_event_failure_cases(self):
        # No schema provided
        # Should return None
        self.assertIsNone(self.wl.handle_event({}, 1))

        # Valid schema provided, but no timestamp
        # Should return None
        self.assertIsNone(self.wl.handle_event({
            'schema': 'NavigationTiming'
            }, 1))

        # Valid schema and timestamp, but no event object
        # Should return None
        self.assertIsNone(self.wl.handle_event({
            'schema': 'NavigationTiming',
            'dt': '2018-02-13T16:53:48'
            }, 1))

        # Event is an oversample, should return none
        self.assertIsNone(self.wl.handle_event({
            'schema': 'NavigationTiming',
            'dt': '2018-02-13T16:53:48',
            'event': {
                'isOversample': True
            }}, 1))

    def test_handle_event_with_valid_event(self):
        # Valid schema, timestamp, and event object is present
        # Will return None
        self.wl.handle_event({
            'schema': 'NavigationTiming',
            'dt': '2018-02-13T16:53:48',
            'event': {
                'firstPaint': 1
            }}, 1)
        # wl should now have an offset recorded
        self.assertEqual(self.wl.offsets,
                         {'NavigationTiming': {1518540780: 1}, 'SaveTiming': {}})
        self.assertEqual(self.wl.events,
                         {'NavigationTiming': {1518540780: {'firstPaint': [1]}}, 'SaveTiming': {}})

    def test_median_is_correct(self):
        self.assertEqual(self.wl.median([1, 2, 3, 4, 5]), 3)
        self.assertEqual(self.wl.median([1]), 1)
        self.assertEqual(self.wl.median([1, 2, 3, 4]), 2.5)


if __name__ == '__main__':
    unittest.main(verbosity=2)
