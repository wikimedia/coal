#!/usr/bin/env python

import argparse
from .coal import WhisperLogger
from .coal_web import CoalWeb
import os


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--brokers', required=True,
                            help='Comma-separated list of Kafka brokers')
    arg_parser.add_argument('--consumer-group', required=True,
                            dest='consumer_group', help='Name of the Kafka consumer group')
    arg_parser.add_argument('--schema', required=True, action='append',
                            dest='schemas',
                            help='Schemas that we deal with, topic names are derived')
    arg_parser.add_argument('--whisper-dir', default=os.getcwd(),
                            required=False,
                            help='Path for Whisper files.  Defaults to current working dir')
    arg_parser.add_argument('-n', '--dry-run', required=False, dest='dry_run',
                            action='store_true', default=False,
                            help='Don\'t create whisper files, just output')
    arg_parser.add_argument('-v', '--verbose', dest='verbose',
                            required=False, default=False, action='store_true',
                            help='Increase verbosity of output')
    args = arg_parser.parse_args()
    app = WhisperLogger(args)
    app.run()


def web():
    app = CoalWeb()
    app.run()
