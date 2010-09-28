"""
The export writer
=================
Slow as f***
"""
import threading
import heapq
import copy
import calendar
import numpy
from helper import _from_ordinalf
import dateutil.tz as tz

class ExportWriter(threading.Thread):
    """
    The thread writing the data to be exported.
    
    :param fn: The filename to write tto
    :type fn: :class:`str`
    :param sources: The sources from which to fetch the exported data
    :type sources: [:class:`annpkg.sources.Source`]
    :param cb: A status callback. Is called multiple times with the percentage of the data exported.
    :type cb: :class:`float` -> ()
    :param end_cb: A callback that is called without arguments when the process finished.
    """
    def __init__(self,fn,sources,annotations,cb,end_cb):
        threading.Thread.__init__(self)
        self.fn = fn
        self.sources = sources
        self.annotations = annotations
        self.cb = cb
        self.end_cb = end_cb
    def run(self):
        h = open(self.fn,'w')
        try:
            source_state = [SourceReadingState(src) for src in self.sources]
            if self.cb is not None:
                status_all = 0
                for src in source_state:
                    status_all += len(src.xdata)
                status_cur = 0
            heapq.heapify(source_state)
            ann_lst = copy.copy(self.annotations)
            ann_lst.sort(cmp=lambda (name1,start1,end1),(name2,start2,end2): cmp(start1,start2))
            # ann_state keeps all active annotations with the timestamp when they end
            ann_state = dict()
            while len(source_state) > 0:
                # Fetch the next data point from the sources
                ak = source_state[0].pop()
                # If the active source doesn't have any data points, drop it from the queue
                # and start anew
                if ak is None:
                    heapq.heappop(source_state)
                    continue
                # Check for annotations that have expired
                while(len(ann_lst) > 0 and ann_lst[0][2] < ak[0]):
                    #key = ann_lst[0][0]
                    #if key in ann_state:
                    #    del ann_state[key]
                    del ann_lst[0]
                for k,i in ann_state.items():
                    if i < ak[0]:
                        del ann_state[k]
                # Check for annotations that have become active
                while(len(ann_lst) > 0 and ann_lst[0][1] < ak[0]):
                    ann_state[ann_lst[0][0]] = ann_lst[0][2]
                    del ann_lst[0]
                cur_date = _from_ordinalf(ak[0],tz.tzutc())
                h.write(str(calendar.timegm(cur_date.utctimetuple())))
                h.write(cur_date.strftime(".%f"))
                h.write("\t")
                h.write(",".join(ann_state.keys()))
                h.write("\t")
                h.write(source_state[0].name)
                h.write("\t")
                if ak[1].__class__ == numpy.ndarray:
                    for v in ak[1]:
                        h.write(str(v))
                        h.write(" ")
                else:
                    h.write(str(ak[1]))
                h.write("\n")
                heapq.heapreplace(source_state,source_state[0])
                if self.cb is not None:
                    status_cur += 1
                    self.cb(float(status_cur)/status_all)
        finally:
            h.close()
        if self.end_cb is not None:
            self.end_cb()

class SourceReadingState:
    def __init__(self,src):
        self.name = src[0].short_name()
        self.xdata = src[0].get_time(False)
        self.ydata = src[0].get_data(False)
        self.idx = 0
    def __cmp__(self,other):
        if self.idx < len(self.xdata):
            if other.idx < len(other.xdata):
                return cmp(self.xdata[self.idx],other.xdata[other.idx])
            else:
                return 1
        else:
            return -1
    def cur(self):
        if self.idx < len(self.xdata):
            return (self.xdata[self.idx],self.ydata[self.idx])
        else:
            return None
    def pop(self):
        res = self.cur()
        self.idx += 1
        return res
