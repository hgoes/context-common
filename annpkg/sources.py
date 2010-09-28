"""
The sources
===========
"""
import gst
import gst_numpy
import threading
#import matplotlib.dates as dates
import calendar
import datetime
import numpy as np
import tarfile
from cStringIO import StringIO
import dateutil.tz as tz
import xml.dom
from helper import _from_ordinalf,_to_ordinalf

class Source:
    """
    An abstract source
    """
    @staticmethod
    def from_annpkg(handle,rootname,attrs):
        """
        :param handle: A handle to the tar file
        :type handle: :class:`tarfile.TarFile`
        :param rootname: The name of the XML element that described the source in the index file
        :type rootname: :class:`str`
        :param attrs: The attributes of the XML element
        :type attrs: :class:`xml.dom.NamedNodeMap`
        :rtype: :class:`Source`

        Load the source from a tar file"""
        pass
    def toxml(self,root):
        """
        :param root: The XML document where the source shall reside in
        :type root: :class:`xml.dom.Document`
        :rtype: :class:`xml.dom.Element`

        Creates a XML element that describes the source"""
        abstract
    @staticmethod
    def source_type_id():
        """
        :returns: The name of an XML element that describes the source
        :rtype: :class:`str`
        """
        return ''
    @staticmethod
    def description():
        """
        :returns: A human readable description of the source
        :rtype: :class:`str`
        """
        return ""
    @staticmethod
    def arg_description():
        """
        :returns: A list with all the arguments the constructor of this class takes
        :rtype: :class:`list` of (:class:`str`,:class:`str`,?)
        """
        return []
    @staticmethod
    def from_file(fn,**args):
        """
        :param fn: The file from which to load the source
        :param args: The arguments which describe the source
        :rtype: :class:`Source`

        Construct a source from a file with a dictionary of arguments """
        return []
    def put_files(self,handle):
        """
        :param handle: The container file to write to
        :type handle: :class:`tarfile.TarFile`
        
        Write all the data of the source into a tar file. Make sure that this function writes exactly the files that the :meth:`from_annpkg` method reads.
        """
        abstract
    def get_name(self):
        return self.name
    def get_data_bounds(self):
        """
        :returns: The minimal and maximal value of the data contained in this source
        :rtype: (:class:`float`, :class:`float`)

        This defaults to iterating over all values and finding the min and max, so if you know a better method, overwrite it please!
        """
        return (self.get_data().min(),self.get_data().max())
    def get_time_bounds(self):
        """
        :returns: The start- and end-time of the data in the source
        :rtype: (:class:`float`, :class:`float`)
        """
        time = self.get_time()
        return (time[0],time[-1])
    def finish_loading(self):
        pass
    def hasCapability(self,name):
        """
        :param name: The name of the capability
        :type name: :class:`str`
        :returns: Whether the source supports the capability
        :rtype: :class:`bool`
        """
        return False
    def short_name(self):
        """
        :returns: A one or two letter identifier for the source
        """
        abstract

class UnknownSource(Source):
    def __init__(self,handle,rootname,attrs):
        Source.__init__(self,handle,rootname,attrs)
        self.attrs = attrs
        self.rootname = rootname
    def toxml(self,root):
        el = root.createElement(self.rootname)
        for i in range(0,self.attrs.length):
            attr = self.attrs.item(i)
            el.setAttribute(attr.localName,attr.nodeValue)
        return el
    def put_files(self,handle):
        pass
    def get_data_bounds(self):
        return (0.0,0.0)
    def get_time_bounds(self):
        return (0.0,0.0)

class AudioSource(Source):
    """
    :param fn: The filename of the audio source
    :type fn: :class:`str`
    :param name: An informal name that identifies the source
    :type name: :class:`str`
    :param chans: The channels of the audio data to display
    :type chans: :class:`set` of :class:`int`
    """
    def __init__(self,fn,name,chans,offset,gst_el):
        self.fn = fn
        self.name = name
        self.chans = chans
        self.offset = offset
        self.pipe = gst.Pipeline()
        sink = gst_numpy.NumpySink(self.new_data,self.new_attrs)
        decoder = gst.element_factory_make('decodebin')
        decoder.connect('new-decoded-pad',self.new_pad,sink)
        self.pipe.add(gst_el,decoder,sink.el)
        gst.element_link_many(gst_el,decoder)
        self.data_avail = threading.Event()
        self.attrs_avail = threading.Event()
        self.pipe.set_state(gst.STATE_PLAYING)
    def new_pad(self,decoder,pad,last,sink):
        tpad = sink.el.get_pad('sink')
        if tpad.is_linked():
            print "WARNING: Stream has multiple outputs, selecting the first one"
        else:
            pad.link(tpad)
    @staticmethod
    def from_annpkg(handle,rootname,attrs):
        fn = attrs['file'].nodeValue
        name = attrs['name'].nodeValue
        chans = set([int(i) for i in attrs['channels'].nodeValue.split(',')])
        member = handle.getmember(fn)
        src = gst_numpy.PySrc(handle.extractfile(member),member.size)
        return AudioSource(fn,name,chans,
                           _from_ordinalf(float(attrs['offset'].nodeValue)),
                           src.el)
    def toxml(self,root):
        el = root.createElement('audio')
        el.setAttribute('name',self.get_name())
        el.setAttribute('file',self.fn)
        el.setAttribute('offset',str(_to_ordinalf(self.offset)))
        el.setAttribute('channels',",".join([str(c) for c in self.chans]))
        return el
    @staticmethod
    def source_type_id():
        return 'audio'
    @staticmethod
    def description():
        return _("Audio data")
    @staticmethod
    def arg_description():
        return [('channel','choice',[(_("Channel")+" 1",0),(_("Channel")+" 2",1)],_("Channel")),
                ('offset','time',None,_("Offset"))]
    @staticmethod
    def from_file(fn,name,channel,offset):
        src = gst.element_factory_make('filesrc')
        src.set_property('location',fn)
        return [AudioSource(name+".flac",name,channel,offset,src)]
    def put_files(self,handle):
        self.data_avail.wait()
        pipe = gst.Pipeline()
        reader = gst_numpy.NumpySrc(self.data,self.rate)
        conv = gst.element_factory_make('audioconvert')
        encoder = gst.element_factory_make('flacenc')
        sink = gst_numpy.PySink()
        pipe.add(reader.el,conv,encoder,sink.el)
        gst.element_link_many(reader.el,conv,encoder,sink.el)
        pipe.set_state(gst.STATE_PLAYING)
        buf = sink.get_data()
        inf = tarfile.TarInfo(self.fn)
        inf.size = len(buf)
        handle.addfile(inf,StringIO(buf))
    def get_skipping(self):
        l = self.get_frames()
        if l < 10000:
            return 1
        else:
            return l / 10000
    def get_data(self,sampled=False):
        self.data_avail.wait()
        if sampled:
            return self.data[::self.get_skipping(),[x for x in self.chans]]
        else:
            l,chans = self.data.shape
            if chans == len(self.chans):
                return self.data
            else:
                return self.data[:,[x for x in self.chans]]
    def new_data(self,dat):
        self.data = dat
        self.data_avail.set()
        self.pipe.set_state(gst.STATE_NULL)
        self.pipe = None
    def new_attrs(self,rate,chans,frames):
        self.rate = rate
        self.nchans = chans
        self.nframes = frames
        self.attrs_avail.set()
    def get_rate(self):
        self.attrs_avail.wait()
        return self.rate
    def get_frames(self):
        self.attrs_avail.wait()
        if self.nframes is None:
            return len(self.get_data())
    def get_time(self,sampled=False):
        nframes = self.get_frames()
        rate = self.get_rate()
        if sampled:
            skip = self.get_skipping()
        else:
            skip = 1
        roffset = _to_ordinalf(self.offset)
        return np.fromiter(( roffset + float(i) / rate / 86400
                             for i in range(0,nframes,skip)),dtype=np.dtype(np.float))
    def get_time_bounds(self):
        return (_to_ordinalf(self.offset),_to_ordinalf(self.offset + datetime.timedelta(seconds = self.get_frames()/self.get_rate())))
    def finish_loading(self):
        self.data_avail.wait()
    def hasCapability(self,name):
        if name=='play':
            return True
        else:
            return False
    def getPlayData(self,start,end):
        rstart = start - self.offset
        rend = end - self.offset
        fstart = rstart.microseconds*0.000001 + rstart.seconds + rstart.days*24*3600
        fend = rend.microseconds*0.000001 + rend.seconds + rend.days*24*3600
        return (self.data[int(fstart*self.rate):int(fend*self.rate)],self.rate)
    def short_name(self):
        return 'a'
    def get_preprocessed_data(self):
        idx = 0
        while idx*512 < self.xdata.shape[0]:
            dat = np.absolute(np.fft.fft(self.xdata[idx*512:(idx+1)*512]))
            dat1 = dat[0:256]
            dat2 = dat[256:512]
            yield (self.xdata[idx*512],
                   np.mean(dat1),np.var(dat1),centroid(dat1),
                   np.mean(dat2),np.var(dat2),centroid(dat2))
            idx += 1
    def get_preprocessed_length(self):
        return self.xdata.shape[0] / 512

class MovementSource(Source):
    def __init__(self,fn,name,timedata,ydata,sensor=0):
        self.fn = fn
        self.name = name
        self.timedata = timedata
        self.ydata = ydata
        self.sensor = sensor
    @staticmethod
    def from_annpkg(handle,rootname,attrs):
        fn = attrs['file'].nodeValue
        name = attrs['name'].nodeValue
        sensor = int(attrs['sensor'].nodeValue)
        member = handle.extractfile(fn)
        lines = member.readlines()
        sz = len(lines)
        timedata = np.empty(sz,np.dtype(np.float))
        ydata = np.empty((sz,3),np.dtype(np.float))
        for i in range(sz):
            (timestamp,x,y,z) = lines[i].split()
            timedata[i] = _to_ordinalf(datetime.datetime.fromtimestamp(float(timestamp),tz.tzutc()))
            ydata[i,0] = x
            ydata[i,1] = y
            ydata[i,2] = z
        return MovementSource(fn,name,timedata,ydata,sensor)
    @staticmethod
    def from_file(fn,name,sensor):
        res = {}
        h = open(fn)
        try:
            lines = h.readlines()
            sz = len(lines)
            for s in sensor:
                res[s] = (np.empty(sz,np.dtype(np.float)),np.empty((sz,3),np.dtype(np.float)))
            for i in range(sz):
                splt = lines[i].split()
                rest = len(splt) % 3
                if rest == 2:
                    timedata = _to_ordinalf(datetime.datetime.fromtimestamp(int(splt[0]),tz.tzutc())
                                           + datetime.timedelta(seconds = float("0."+splt[1])))
                elif rest == 1:
                    timedata = _to_ordinalf(datetime.datetime.fromtimestamp(float(splt[0]),tz.tzutc()))
                for s in sensor:
                    res[s][0][i] = timedata
                    res[s][1][i,0] = splt[rest + s*3]
                    res[s][1][i,1] = splt[rest + s*3 + 1]
                    res[s][1][i,2] = splt[rest + s*3 + 2]
        finally:
            h.close()
        return [ MovementSource(name+str(s)+".log",name+str(s),res[s][0],res[s][1],s) for s in sensor ]
    def put_files(self,handle):
        w = StringIO()
        for i in range(len(self.ydata)):
            stamp = _from_ordinalf(self.timedata[i])
            ms = str(stamp.microsecond)
            ms = "0"*(6-len(ms)) + ms
            
            print >>w,str(calendar.timegm(stamp.timetuple()))+"."+ms,self.ydata[i,0],self.ydata[i,1],self.ydata[i,2]
            #print >>w,str(self.timedata[i]),self.ydata[i,0],self.ydata[i,1],self.ydata[i,2]
        buf = w.getvalue()
        inf = tarfile.TarInfo(self.fn)
        inf.size = len(buf)
        handle.addfile(inf,StringIO(buf))
    def toxml(self,root):
        el = root.createElement('movement')
        el.setAttribute('name',self.get_name())
        el.setAttribute('file',self.fn)
        el.setAttribute('sensor',str(self.sensor))
        return el
    @staticmethod
    def source_type_id():
        return 'movement'
    @staticmethod
    def description():
        return _("Movement data")
    @staticmethod
    def arg_description():
        return [('sensor','choice',[(_("Sensor")+" 1",0),(_("Sensor")+" 2",1)],_("Sensor"))]
    def get_data(self,sampled=False):
        return self.ydata
    def get_time(self,sampled=False):
        return self.timedata
    def short_name(self):
        return 'm'+str(self.sensor)
    def get_preprocessed_data(self):
        buf = np.empty((8,3))
        idx = 0
        for ts,dat in zip(self.timedata,self.ydata):
            buf[idx % 8] = dat
            if idx % 8 == 7:
                meanx,meany,meanz = np.mean(buf,0)
                varx,vary,varz = np.var(buf,0)
                yield (ts,varx,vary,varz,meanx,meany,meanz)
            idx += 1
    def get_preprocessed_length(self):
        return self.xdata.shape[0] / 8
    def get_preprocessed_width(self):
        return 6
            
all_sources = [AudioSource,MovementSource]

#all_sources = [AudioSource,MovementSource,SpeedSource]
def source_by_name(name):
    """
    Get the source for a given name.
    This is used to find the right class for a source definition in an annotation package.
    """
    for src in all_sources:
        if src.source_type_id() == name:
            return src
    return UnknownSource
