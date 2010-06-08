import gst
import numpy
import threading
from cStringIO import StringIO

class NumpySink:
    def __init__(self,cb_data,cb_attrs):
        self.el = gst.element_factory_make("appsink")
        self.el.bufs = None
        self.cb_data = cb_data
        self.cb_attrs = cb_attrs
        self.el.set_property("sync",False)
        self.el.set_property("emit-signals",True)
        self.el.connect("new-buffer",self.new_buffer)
        self.el.connect("eos",self.end)
    def new_buffer(self,el):
        buf = el.emit('pull-buffer')
        if el.bufs is None:
            struc = el.get_pad("sink").get_allowed_caps()[0]
            self.chans = struc['channels']
            try:
                frames,fmt = el.query_duration(gst.FORMAT_FRAMES)
            except:
                frames = None
            self.cb_attrs(struc['rate'],self.chans,frames)
            self.tp = numpy.dtype('i'+str(struc['width']/8))
            el.bufs = [numpy.frombuffer(buf,self.tp)]
        else:
            el.bufs.append(numpy.frombuffer(buf,self.tp))
    def end(self,el):
        self.cb_data(numpy.hstack(el.bufs).reshape((-1,self.chans)))
        el.bufs = None

class NumpySrc:
    def __init__(self,arr,rate):
        self.el = gst.element_factory_make("appsrc")
        self.arr = arr
        self.pos = 0
        self.rate = rate
        dims = len(arr.shape)
        if dims == 1:
            l = arr.shape[0]
            chans = 1
        else:
            l,chans = arr.shape
        self.per_sample = 1000000000 / rate
        self.fac = chans * arr.dtype.itemsize
        self.el.set_property("size",l * chans * arr.dtype.itemsize)
        self.el.set_property("format",gst.FORMAT_TIME)
        capstr = "audio/x-raw-int,width=%d,depth=%d,rate=%d,channels=%d,endianness=1234,signed=true"%(arr.dtype.itemsize*8,arr.dtype.itemsize*8,rate,chans)
        self.el.set_property("caps",gst.caps_from_string(capstr))
        self.el.set_property("stream-type",1) # Seekable
        self.el.connect("need-data",self.need_data)
        self.el.connect("seek-data",self.seek_data)
    def need_data(self,el,l):
        if self.pos >= self.arr.shape[0]:
            el.emit("end-of-stream")
        else:
            sz = 1024 * self.fac
            buf = gst.Buffer(numpy.getbuffer(self.arr,self.pos*self.fac,1024*self.fac))
            buf.timestamp = self.pos * self.per_sample
            buf.duration = int(1024*self.per_sample)
            el.emit("push-buffer", buf)
            self.pos += 1024
    def seek_data(self,el,npos):
        self.pos = npos / self.per_sample
        return True

class PySrc:
    def __init__(self,obj,size=-1):
        self.el = gst.element_factory_make("appsrc")
        self.el.src_obj = obj
        self.el.set_property("size",size)
        self.el.connect("need-data",self.need_data)
    def need_data(self,el,l):
        bytes = el.src_obj.read(l)
        if bytes == '':
            el.emit("end-of-stream")
        else:
            el.emit("push-buffer", gst.Buffer(bytes))

class PySink:
    def __init__(self):
        self.el = gst.element_factory_make("appsink")
        self.el.set_property("sync",False)
        self.el.set_property("emit-signals",True)
        self.el.connect("new-buffer",self.new_buffer)
        self.el.connect("eos",self.eos)
        self.el.stream = StringIO()
        self.data_avail = threading.Event()
    def new_buffer(self,el):
        buf = el.emit("pull-buffer")
        el.stream.write(buf.data)
    def eos(self,el):
        self.data_avail.set()
    def get_data(self):
        self.data_avail.wait()
        return self.el.stream.getvalue()
