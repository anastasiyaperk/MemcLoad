#!/usr/bin/env python
# -*- coding: utf-8 -*-
from collections import namedtuple
import glob
import gzip
from itertools import islice
import logging
from multiprocessing import Array, Process, Queue, current_process
from optparse import OptionParser
import os
import sys
from typing import Any, Dict, List, Tuple, Union

import appsinstalled_pb2
from memc_connection import get_memc_client

CONFIG = {
    "NORMAL_ERR_RATE": 0.01,
    "BATCH_SIZE": 1,
    "PROCESSES_COUNT": 2,
    }

MEMCACHE_CONNECTIONS = {}
AppsInstalled = namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


def dot_rename(path: str):
    """
    Rename processed file to prevent re-processing

    :param path: filepath
    :return: None
    """
    head, fn = os.path.split(path)
    os.rename(path, os.path.join(head, "." + fn))


def insert_appsinstalled(memc_addr: str, appsinstalled: AppsInstalled, dry_run=False) -> bool:
    """
    Insert protobuf message to memcache by address

    :param memc_addr: memcache address
    :param appsinstalled: app installed log line info
    :param dry_run: flag for dry run
    :return: True, if message insert in memcache successful, else - False
    """
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    try:
        if dry_run:
            logging.debug("%s - %s -> %s" % (memc_addr, key, str(ua).replace("\n", " ")))
        else:
            memc_client = get_memc_client(addr=memc_addr, memcache_connections=MEMCACHE_CONNECTIONS)
            memc_client.cache_set(key, packed)

    except Exception as e:
        logging.exception("Cannot write to memc %s: %s" % (memc_addr, e))
        return False

    return True


def parse_appsinstalled(line: str) -> Union[AppsInstalled, None]:
    """
    Parse app installed log line

    :param line: log line
    :return: parsed app installed log line info, or None if file unprocessable
    """
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return

    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return

    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)

    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)

    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def split_gz_on_batches(filename: str, queue: Queue, batch_size: int):
    """
    Unzip *.tsv.gz file, split it on batches of lines and put at queue

    :param filename: *.tsv.gz filename
    :param queue: queue
    :param batch_size: size of batches
    :return: None
    """
    logging.info(f'Processing of {filename}')
    fd = gzip.open(filename, 'rt')
    batch = list(islice(fd, batch_size))
    while batch:
        queue.put((filename, batch))
        batch = list(islice(fd, batch_size))

    queue.put((filename, ['EOF']))


def batch_processing(batch: List[Any], device_memc: Dict[str, str], dry: bool = False) -> Tuple[int, int]:
    """
    Processing of lines in batch

    :param batch: batch
    :param device_memc: dict with idfa, gaid, adid, dvid values from app options
    :param dry: dry run flag
    :return: number of processed lines and errors
    """
    logging.info(f'[Process {current_process()}]: do batch processing')
    errors, processed = 0, 0
    for line in batch:
        line = line.strip()
        if not line:
            continue

        appsinstalled = parse_appsinstalled(line)
        if not appsinstalled:
            errors += 1
            continue

        memc_addr = device_memc.get(appsinstalled.dev_type)
        if not memc_addr:
            errors += 1
            logging.error(f"Unknown device type: {appsinstalled.dev_type}")
            continue

        ok = insert_appsinstalled(memc_addr, appsinstalled, dry)
        if ok:
            processed += 1
        else:
            errors += 1

    return processed, errors


def batch_queue_parser(batch_queue: Queue,
                       device_memc: Dict[str, str],
                       dry: bool,
                       file_statistic: Dict[str, Any]):
    """
    Parse lines batches in queue

    :param batch_queue: batch queue
    :param device_memc: dict with idfa, gaid, adid, dvid values from app option
    :param dry: dry run flag
    :param file_statistic: file statistic dict with info of processed lines and errors
    :return: None
    """
    while True:
        file, batch = batch_queue.get()
        if not batch:
            logging.info(f"[Process {current_process()}]: empty batch")
            return

        elif batch[0] == "EOF":
            logging.info(f"[Process {current_process()}]: this is end of file - {file}")
            dot_rename(file)

        else:
            processed, errors = batch_processing(batch, device_memc, dry=dry)
            # Update statistic by batch
            ix = file_statistic["map"][file]
            file_statistic["processed"][ix] += processed
            file_statistic["errors"][ix] += errors


def log_statistic(file_statistic: Dict[str, Any], normal_error_rate: float):
    """
    Print file processing statistic in logs

    :param file_statistic: file statistic dict with info of processed lines and errors
    :param normal_error_rate: normal rate of errors
    :return: None
    """
    for file, idx in file_statistic["map"].items():
        errors = file_statistic["processed"][idx]
        processed = file_statistic["errors"][idx]
        if not processed:
            continue

        err_rate = float(errors) / processed
        if err_rate < normal_error_rate:
            logging.info(f"File: {file}: Acceptable error rate {err_rate}. Successfull load")
        else:
            logging.error(f"File: {file}: High error rate ({err_rate} > {normal_error_rate}). Failed load")


def main(options, config):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
        }

    batch_queue = Queue()
    files = list(glob.iglob(options.pattern))

    # Statistic dictionary
    file_statistic = {
        "map": {file: idx for idx, file in enumerate(files)},
        "processed": Array("i", [0 for _ in range(len(files))]),
        "errors": Array("i", [0 for _ in range(len(files))])
        }

    # Processes pool
    processes = []
    for _ in range(config["PROCESSES_COUNT"]):
        p = Process(target=batch_queue_parser, args=(batch_queue,
                                                     device_memc,
                                                     options.dry,
                                                     file_statistic))
        p.start()
        processes.append(p)

    # Processing files on batches
    for file in files:
        split_gz_on_batches(file, queue=batch_queue, batch_size=config["BATCH_SIZE"])

    for _ in range(config["PROCESSES_COUNT"]):
        batch_queue.put(("", list()))

    # Join processes
    for p in processes:
        p.join()

    log_statistic(file_statistic, normal_error_rate=config["NORMAL_ERR_RATE"])


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:11211")
    op.add_option("--gaid", action="store", default="127.0.0.1:11211")
    op.add_option("--adid", action="store", default="127.0.0.1:11211")
    op.add_option("--dvid", action="store", default="127.0.0.1:11211")
    (opts, args) = op.parse_args()

    logging.basicConfig(filename=opts.log,
                        level=logging.INFO if not opts.dry else logging.DEBUG,
                        format="[%(asctime)s] %(levelname).1s %(message)s",
                        datefmt="%Y.%m.%d %H:%M:%S")
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info(f"Memc loader started with options: {opts}")
    try:
        main(opts, config=CONFIG)
    except Exception as ex:
        logging.exception(f"Unexpected error: {ex}")
        sys.exit(1)
