#!/usr/bin/env python
# -*- coding: utf-8 -*-
import signal
import sys
import time
import os
import parse_config
from consumer_thread import RecordToLog, SendToSock
from global_args import GlobalArgs
from huawei_dialin_cancel import create_client_cancel
from producer_thread import Subscribe

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "proto_py")))


def handle(signum,frame):
    print("Stopping...")
    print("Now start to cancel subscribe in this python process...")
    for ids in ids_set:
        if ids.type == "record_id":
            create_client_cancel(ids.dialin_server, ids.subscription_id, ids.request_id)
    sys.exit(1)


if __name__ == '__main__':

    signal.signal(signal.SIGINT, handle)
    signal.signal(signal.SIGTERM, handle)


    config_dict = parse_config.get_json_dict()
    dialin_servers = config_dict.keys()


    log_set = set()
    data_queue = GlobalArgs.get_data_queue()
    global ids_set
    ids_set = log_set

    try:


        sock = GlobalArgs.get_sock()
        sock_thread = SendToSock("[ sock_thread ]", data_queue, sock, GlobalArgs.FLUSH_INTERVAL)
        sock_thread.setDaemon(True)
        sock_thread.start()

        log_thread = RecordToLog("[ log_thread ]", log_set, GlobalArgs.FLUSH_INTERVAL)
        log_thread.setDaemon(True)
        log_thread.start()

        time.sleep(GlobalArgs.CONNECT_WAIT_TIME)  


        for dialin_server in dialin_servers:
            sub_dict = config_dict.get(dialin_server)
            thread_name = "[" + dialin_server + "'s subscribe]"
            subscribe_thread = Subscribe(thread_name, log_set, data_queue, dialin_server, sub_dict)
            subscribe_thread.setDaemon(True)
            subscribe_thread.start()


        time.sleep(GlobalArgs._ONE_DAY_IN_SECONDS)
    except Exception as e:
        print(e)