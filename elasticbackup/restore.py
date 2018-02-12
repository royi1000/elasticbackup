#!/usr/bin/env python

from __future__ import print_function

import argparse
import logging
import json
import glob
import gzip
import os

import elasticsearch
from tqdm import tqdm

logging.basicConfig(format='%(asctime)s [%(name)s] [%(levelname)s] '
                           '%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

log_levels = [logging.WARN, logging.INFO, logging.DEBUG]
log = logging.getLogger('elasticbackup')
log_es = logging.getLogger('elasticsearch')


def create_index(es, index, f):
    mappings = json.load(f)
    es.indices.create(index=index, body=mappings)


def file_len(fname):
    with gzip.open(fname, 'rb') as f:
        return len(f.readlines())


def create_documents(es, index, restore_file, batch_size=1000):
    total = file_len(restore_file)
    if total:
        log.info("restoring file: {}".format(restore_file))
        with tqdm(total=total, desc="Restoring {}".format(index),
                  unit='docs', mininterval=2) as pbar:
            with gzip.open(restore_file, 'rb') as f:
                for size, batch in document_batches(f, batch_size):
                    es.bulk(index=index, body=batch)
                    pbar.update(size)
    else:
        log.warn("skipping empty file: {}".format(restore_file))

def document_batches(fp, batch_size):
    i = 0
    batch = []

    for line in fp:
        obj = json.loads(line)
        src = obj.pop('_source')
        batch.append(json.dumps({"create": obj}))
        batch.append(json.dumps(src))
        i += 1

        if i >= batch_size:
            yield i, batch
            i = 0
            batch = []

    if batch:
        yield i, batch


def main():
    parser = argparse.ArgumentParser(
        'elasticrestore',
        description='Restore data and mappings to an ElasticSearch index')
    parser.add_argument('host',
                        help='elasticsearch host')
    parser.add_argument('index',
                        help='elasticsearch index name')
    parser.add_argument('-p', '--path',
                        help='backup path',
                        default='')
    parser.add_argument('-b', '--batch-size',
                        help='document upload batch size',
                        type=int,
                        default=1000)
    parser.add_argument('-v', '--verbose',
                        help='increase output verbosity',
                        action='count',
                        default=0)
    parser.add_argument('-u', '--user',
                        help='HTTP auth (in format user:pass)')
    parser.add_argument('-c', '--create',
                        required=False,
                        action='store_true',
                        dest='create',
                        help='create index')
    args = parser.parse_args()

    verbose = min(args.verbose, 2)
    log.setLevel(log_levels[verbose])
    log_es.setLevel(log_levels[verbose])

    conn_kwargs = {}
    if args.user:
        conn_kwargs['http_auth'] = args.user
    es = elasticsearch.Elasticsearch([args.host], **conn_kwargs)

    for index in args.index.split(','):
        backup_file_list = glob.glob(os.path.join(args.path, "esbackup-{}-documents*.json.gz".format(index)))
        if not len(backup_file_list):
            raise ValueError('no backup document files for index: {} in path'.format(index))
        backup_file_list.sort(reverse=True)
        restore_file_list = []
        for backup_file in backup_file_list:
            restore_file_list.insert(0, backup_file)
            if backup_file.endswith('.baseline.json.gz'):
                break
        if args.create:
            map_list = glob.glob(os.path.join(args.path, "esbackup-{}-mappings*.json.gz".format(index)))
            if not len(map_list):
                raise ValueError('no backup mapping files for index: {} in path'.format(index))
            map_list.sort(reverse=True)
            with gzip.open(map_list[0], 'rb') as f:
                create_index(es, args.index, f)
        for restore_file in restore_file_list:
            create_documents(es, index, restore_file, batch_size=args.batch_size)


if __name__ == '__main__':
    main()
