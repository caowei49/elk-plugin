#!/usr/bin/env python
# -*- coding: utf-8 -*-

import copy
from queue import Queue

class SetQueue(Queue):
    def _init(self, size):
        self.size = size
        self.queue = Queue()
        self.set_items = set()

    def __str__(self):
        return str(self.queue)

    def enqueue(self, item):
        if self.is_full():
            return -1

        temp_set = copy.deepcopy(self.set_items)
        self.set_items.add(item)
        if (temp_set == self.set_items):
            return -1
        else:
            self.queue.append(item)


    def dequeue(self):
        if self.is_empty():
            return -1
        firstElement = self.queue[0]
        self.queue.remove(firstElement)
        return firstElement


    def is_full(self):
        if len(self.queue) == self.size:
            return True
        return False

    def is_empty(self):
        if len(self.queue) == 0:
            return True
        return False
