import sys
import os
import re
from datetime import datetime

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, DateTime

def parse_message(m):
    m = re.match('<(\d+)>(\w+\s+\d+\s+\d+:\d+:\d+)\s+(\S+):\s+(.*)',m)
    if m is not None:
        return m.groups()

def parse_timestamp(timestamp):
    """ convert to datetime, adding the current year & convert to utc """
    pt = datetime.strptime(timestamp,'%b %d %H:%M:%S')
    nt = datetime.now()
    if pt.month == nt.month:
        year = nt.year
    else:
        # handle new year's wrap between logged timestamp & now()
        year  = nt.year - 1
    ct = datetime(year,pt.month,pt.day,pt.hour,pt.minute,pt.second)  
    return ct + (datetime.utcnow() - datetime.now())

def parse_fields(data):
    fields = {}
    for f in data.split():
        vals = [x.strip() for x in f.split('=')]
        if len(vals) == 1:
             fields[vals[0]] = True
        if len(vals) == 2:    
            fields[vals[0]] = vals[1]
    return fields

def process_message(m):
    try:
        groups = parse_message(m)
        if groups is not None:
            (n,timestamp,action,data) = groups
            ts = parse_timestamp(timestamp)
            return (n,ts,action,data)
        else:
            return (0,datetime.now(),"UNKNOWN","")
    except Exception as exc:
        print 'error processing %s' % exc

def create_db(path=':memory:'):
    engine = create_engine('sqlite:///%s' % path, echo=False)
    metadata = MetaData()
    accepts = Table('accepts', metadata,
        Column('id',Integer, primary_key=True),
        Column('timestamp', DateTime),
        Column('proto',String),
        Column('src',String),
        Column('dst',String),
        Column('spt',Integer),
        Column('dpt',Integer),
        Column('inp',String),
        Column('out',String)
        )
    # create the tables, if necessary
    metadata.create_all(engine) 
    conn = engine.connect()

    return conn,accepts
    

if __name__ == "__main__":
    import time

    tstart = time.time()
    logpath = 'youlogfile.log'
    assert os.path.exists(logpath)
    conn,accepts = create_db(path='%s.db' % logpath)

    nrecords = 0
    for l in  open(logpath,'r'):
        (n,ts,src,d) = process_message(l)
        if d.startswith('ACCEPT'):
            fields = parse_fields(d)
            ins = accepts.insert().values(timestamp=ts,
                                            proto = fields.get('PROTO',''),
                                            src = fields.get('SRC',''),
                                            dst = fields.get('DST',''),
                                            spt = fields.get('SPT',0),
                                            dpt = fields.get('DPT',0),
                                            inp = fields.get('IN',''),
                                            out = fields.get('OUT',''),
                                            )
            result = conn.execute(ins)

            nrecords += 1
            if nrecords % 100 == 0:
               sys.stdout.write('.')
            if nrecords % 1000 == 0:
               sys.stdout.write('\n')

    timer = time.time()-tstart
    print '\n%d records inserted in %.1f seconds(%.1f records/s)' % (nrecords,timer,float(nrecords)/timer)
    
