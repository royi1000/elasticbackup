#!/usr/bin/env python

from __future__ import print_function

import argparse
import datetime
import logging
import json
import time
import glob
import os
import gzip
import elasticsearch
from tqdm import tqdm

logging.basicConfig(format='%(asctime)s [%(name)s] [%(levelname)s] '
                           '%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

log_levels = [logging.WARN, logging.INFO, logging.DEBUG]
log = logging.getLogger('elasticbackup')
log_es = logging.getLogger('elasticsearch')


def get_last_backup(path, index):
    backup_files = glob.glob(os.path.join(path, 'esbackup-{}-documents-*.json.gz'.format(index)))
    if not len(backup_files):
        return datetime.datetime.fromtimestamp(0)
    backup_files.sort()
    last_backup = backup_files[-1]
    last_backup_date = last_backup.split('esbackup-{}-documents-'.format(index))[-1].split('.')[0]
    log.warn(last_backup)
    return datetime.datetime.strptime(last_backup_date, "%Y%m%d-%H%M%S")


def write_mappings(es, index, f):
    mapping = es.indices.get_mapping(index)
    timestamp_field = ''
    try:
        for k in mapping[index]['mappings'].keys():
            if timestamp_field:
                break
            for prop in mapping[index]['mappings'][k]['properties']:
                if 'type' in mapping[index]['mappings'][k]['properties'][prop] and \
                                mapping[index]['mappings'][k]['properties'][prop]['type'] == 'date':
                    timestamp_field = prop
                    log.info("timestamp field: {}".format(timestamp_field))
                    break
    except KeyError, IndexError:
        pass
    json.dump(mapping[index], f)
    return timestamp_field


def write_documents(es, index, f, batch_size=1000, query=None):
    def _write_hits(results):
        hits = results['hits']['hits']
        if hits:
            for hit in hits:
                hit.pop('_index', None)
                hit.pop('_score', None)
                f.write("%s\n" % json.dumps(hit))
            return results['_scroll_id'], len(hits)
        else:
            return None, 0

    status = "got batch of %s (total: %s)"

    results = es.search(index=index, body=query, scroll="10m", size=batch_size)
    total_hits = results['hits']['total']
    scroll_id, num = _write_hits(results)
    total = num
    log.info(status, num, total)

    with tqdm(total=total_hits, desc="Backuping {}".format(index),
              unit='docs', initial=num, mininterval=2) as pbar:
        while scroll_id is not None:
            results = es.scroll(scroll_id=scroll_id, scroll='1m')
            scroll_id, num = _write_hits(results)
            total += num
            pbar.update(num)
            #printProgressBar(total, total_hits, prefix='Progress:',
            #                 suffix='{} Complete ({}:{})'.format(index, total, total_hits), length=50)
            #log.info(status, num, total)


def create_backup(index, es, now, path, query, iteration, batch_size, timestamp):
    today = now.strftime("%Y%m%d-%H%M%S")
    mappings_filename = "esbackup-%%(index_name)s-mappings-%s" % today
    documents_filename = "esbackup-%%(index_name)s-documents-%s" % today
    prefix = path
    suffix = '.baseline.json.gz'
    if iteration:
        suffix = '.iteration.json.gz'
    documents_filename = os.path.join(prefix, documents_filename + suffix)
    mappings_filename = os.path.join(prefix, mappings_filename + suffix)
    with gzip.open(mappings_filename % {'index_name': index}, 'wb') as f:
        ts = write_mappings(es, index, f)
    if timestamp:
        ts = timestamp
    if not query and ts:
        query = {"query": {"range": {ts: {"lte": now.strftime("%Y-%m-%d %H:%M:%S"),
                                                      "format": "yyyy-MM-dd HH:mm:ss"}}}}
    if iteration:
        query["query"]["range"][ts]["gte"] = get_last_backup(path, index).strftime("%Y-%m-%d %H:%M:%S")

    log.debug(json.dumps(query))

    f_name = documents_filename % {'index_name': index}
    with gzip.open(f_name + ".started", 'wb') as f:
        write_documents(es,
                        index,
                        f,
                        batch_size=batch_size,
                        query=query)
    os.rename(f_name + ".started", f_name)


def main():
    parser = argparse.ArgumentParser(
        'elasticbackup',
        description='Back up data and mappings from an ElasticSearch index')
    parser.add_argument('host',
                        help='elasticsearch host')
    parser.add_argument('index',
                        help='comma seperated elasticsearch index names')
    parser.add_argument('-b', '--batch-size',
                        help='document download batch size',
                        type=int,
                        default=1000)
    parser.add_argument('-q', '--query',
                        default='',
                        help='query to pass to elasticsearch')
    parser.add_argument('-u', '--user',
                        help='HTTP auth (in format user:pass)')
    parser.add_argument('-p', '--path',
                        help='backup path',
                        default='')
    parser.add_argument('--timestamp',
                        help='timestamp field',
                        default='')
    parser.add_argument('-v', '--verbose',
                        help='increase output verbosity',
                        action='count',
                        default=0)
    parser.add_argument('-i', '--iteration',
                        required=False,
                        action='store_true',
                        dest='iteration',
                        help='backup diff from previous')
    args = parser.parse_args()

    verbose = min(args.verbose, 2)
    log.setLevel(log_levels[verbose])
    log_es.setLevel(log_levels[verbose])

    conn_kwargs = {}
    if args.user:
        conn_kwargs['http_auth'] = args.user
    es = elasticsearch.Elasticsearch([args.host], **conn_kwargs)
    now = datetime.datetime.utcnow()
    for index in args.index.split(','):
        create_backup(index, es, now, args.path, args.query, args.iteration, args.batch_size, args.timestamp)


if __name__ == '__main__':
    main()
