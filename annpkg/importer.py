import numpy as np
import calendar
import time
import datetime
from helper import _to_ordinalf
import sources
import model

def import_file(fn):
    h = open(fn)
    try:
        mapping = {}
        anns = []
        cur_ann = None
        last_timestamp = None
        for ln in h:
            split = ln.split()
            timestamp = _to_ordinalf(datetime.datetime.utcfromtimestamp(float(split[0])))
            if last_timestamp is not None:
                if timestamp < last_timestamp:
                    print "Warning: timestamps not ordered"
            last_timestamp = timestamp
            cls = split[1]
            tp = split[2]
            data = map(float,split[3:])
            if tp in mapping:
                arrs = mapping[tp]
            else:
                arrs = (DynArray(),DynArray(len(data)))
                mapping[tp] = arrs
            arrs[0].append(timestamp)
            arrs[1].append(data)
            if cur_ann is None:
                cur_ann = (timestamp,cls)
            else:
                if cur_ann[1] != cls:
                    anns.append((cur_ann[1],cur_ann[0],timestamp))
                    cur_ann = (timestamp,cls)
        if cur_ann is not None:
            anns.append((cur_ann[1],cur_ann[0],timestamp))
        srcs = []
        for name,(timedata,data) in mapping.iteritems():
            srcs.append((sources.source_from_short_name(name,timedata.to_array(),data.to_array()),[]))
        return model.AnnPkg(srcs,anns)
    finally:
        h.close()

class DynArray:
    ALLOC_SIZE = 1024
    def __init__(self,width=None):
        self.arrs = []
        self.cur_arr = None
        self.size = 0
        self.width = width
    def append(self,dat):
        rest = self.size % DynArray.ALLOC_SIZE
        if rest == 0:
            if self.width is None:
                arr = np.empty(DynArray.ALLOC_SIZE,np.dtype(np.float))
            else:
                arr = np.empty((DynArray.ALLOC_SIZE,self.width),np.dtype(np.float))
            arr[0] = dat
            if self.cur_arr is not None:
                self.arrs.append(self.cur_arr)
            self.cur_arr = arr
        else:
            self.cur_arr[rest] = dat
        self.size += 1
    def to_array(self):
        if self.size == 0:
            if self.width is None:
                return np.empty(0,np.dtype(np.float))
            else:
                return np.empty((0,self.width),np.dtype(np.float))
        sz = self.size % DynArray.ALLOC_SIZE
        if self.cur_arr is None:
            arr = [] + self.arrs
        else:
            arr = [self.cur_arr[:sz]] + self.arrs
        arr.reverse()
        if self.width is None:
            return np.hstack(arr)
        else:
            return np.vstack(arr)
