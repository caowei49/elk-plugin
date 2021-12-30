#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
功能：生产数据类，该类主要用于从路由器采集数据，包括dialin(Subscribe)和dialout(DataPublish)
版权信息：华为技术有限公司，版本所有(C) 
修改记录：w30000618 创建
"""
import threading
import time
from concurrent import futures

import grpc

from data_item import DataItem
from global_args import GlobalArgs
from proto_py import huawei_grpc_dialin_pb2_grpc, huawei_grpc_dialin_pb2, huawei_grpc_dialout_pb2_grpc
from record_item import RecordItem


class DataPublish(threading.Thread):
    """ DataPublish dialout rpc method."""

    def __init__(self, t_name, data_queue, server_addr):
        threading.Thread.__init__(self, name=t_name)
        self.t_name = t_name
        self.data_queue = data_queue
        self.server_addr = server_addr

    def run(self):
        try:

            server = grpc.server(futures.ThreadPoolExecutor(max_workers=150))
            huawei_grpc_dialout_pb2_grpc.add_gRPCDataserviceServicer_to_server(
                MdtGrpcDialOutServicer(self.data_queue), server)
            server.add_insecure_port(self.server_addr)
            server.start()
            try:
                while True:
                    time.sleep(GlobalArgs._ONE_DAY_IN_SECONDS)
            except KeyboardInterrupt:
                server.stop(0)
        except Exception as e:
            print("dialout error, exception {0}".format(e))


class MdtGrpcDialOutServicer(huawei_grpc_dialout_pb2_grpc.gRPCDataserviceServicer):
    def __init__(self,data_queue):
        self.data_queue = data_queue
        return

    def dataPublish(self, request_iterator, context):

        for _MdtDialoutArgs in request_iterator:
            if (len(_MdtDialoutArgs.data) == 0):
                print("server MdtDialout JSON")

                jsonMark = 1
                jsonMarkByte = jsonMark.to_bytes(2, byteorder='big')
                json2Bytes = bytes(_MdtDialoutArgs.data_json, encoding="utf8")
                data_len = (len(json2Bytes)).to_bytes(4, byteorder='big')

                data_item = DataItem(data_len, json2Bytes, jsonMarkByte)
                self.data_queue.put(data_item)
            else:
                print("server Huawei Dialout GPB")

                gpbMark = 0
                gpbMarkByte = gpbMark.to_bytes(2, byteorder='big')
                data_len = (len(_MdtDialoutArgs.data)).to_bytes(4, byteorder='big')

                data_item = DataItem(data_len, _MdtDialoutArgs.data, gpbMarkByte)
                self.data_queue.put(data_item)


class Subscribe(threading.Thread):
    """Subscribe dialin rpc method."""
    def __init__(self, t_name, log_set, data_queue, dialin_server, sub_args_dict):
        threading.Thread.__init__(self, name=t_name)
        self.log_set = log_set
        self.data_queue = data_queue
        self.dialin_server = dialin_server
        self.sub_args_dict = sub_args_dict

    def run(self):
        try:
            metadata, subreq = Subscribe.generate_subArgs_and_metadata(self.sub_args_dict)

            server = self.dialin_server
            channel = grpc.insecure_channel(server)
            stub = huawei_grpc_dialin_pb2_grpc.gRPCConfigOperStub(channel)

            sub_resps = stub.Subscribe(subreq, metadata=metadata)
            for sub_resp in sub_resps:
                data_is_valid = Subscribe.check_sub_reply_is_data(sub_resp)
                if (data_is_valid == True):

                    data_len = (len(sub_resp.message)).to_bytes(4, byteorder='big')
                    dataMark = 0
                    dataMarkByte = dataMark.to_bytes(2, byteorder='big')
                    data_item = DataItem(data_len, sub_resp.message, dataMarkByte)
                    self.data_queue.put(data_item)

                    record = RecordItem(sub_resp.subscription_id, sub_resp.request_id, server, None, "record_id")
                    self.log_set.add(record)
                else:

                    record = RecordItem(None, None, server, sub_resp.message, "error")
                    self.log_set.add(record)
        except Exception as e:
            print("dialin error, exception {0}".format(e))


    @staticmethod
    def check_sub_reply_is_data(sub_resp):
        print(sub_resp)
        resp_code = sub_resp.response_code
        if (resp_code == ""):
            return True;
        if (resp_code != "200" and resp_code != ""):
            return False;
        if (sub_resp.message == "ok"):
            return False;


    @staticmethod
    def generate_subArgs_and_metadata(sub_args_dict):
        metadata, paths, request_id, sample_interval = sub_args_dict['metadata'], sub_args_dict['paths'], sub_args_dict[
            'request_id'], sub_args_dict['sample_interval']
        sub_req = huawei_grpc_dialin_pb2.SubsArgs()
        for path in paths:
            sub_path = huawei_grpc_dialin_pb2.Path(path=path['path'], depth=path['depth'])
            sub_req.path.append(sub_path)
        sub_req.encoding = 0
        sub_req.request_id = request_id
        sub_req.sample_interval = sample_interval
        return metadata, sub_req
