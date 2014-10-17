import sys
import os
import re
from datetime import datetime
from database import Database

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

if __name__ == "__main__":
    import time

    tstart = time.time()
    logpath = r"C:\Users\rlloyd\repos\netwatch\syslog_2014-10-12.log"
    assert os.path.exists(logpath)
    db = Database(path='%s.db' % logpath)

    nrecords = 0
    for l in  open(logpath,'r'):
        (n,ts,src,d) = process_message(l)
        if d.startswith('ACCEPT'):
            fields = parse_fields(d)
            ins = db.accepts.insert().values(
                timestamp=ts,
                proto = fields.get('PROTO',''),
                src = fields.get('SRC',''),
                dst = fields.get('DST',''),
                spt = fields.get('SPT',0),
                dpt = fields.get('DPT',0),
                inp = fields.get('IN',''),
                out = fields.get('OUT',''),
                )
            result = db.execute(ins)
        elif d.startswith('DROP'):
            fields = parse_fields(d)
            drp = db.drops.insert().values(
                timestamp=ts,
                proto = fields.get('PROTO',''),
                src = fields.get('SRC',''),
                dst = fields.get('DST',''),
                spt = fields.get('SPT',0),
                dpt = fields.get('DPT',0),
                inp = fields.get('IN',''),
                out = fields.get('OUT',''),
                mac = fields.get('MAC',''),
                )
            result = db.execute(drp)
        elif d.startswith('DHCPACK'):
            try:
                fields = parse_fields(d)
                (type,ip,mac,name) = d.split()
                arpack = db.arpacks.insert().values(
                    timestamp=ts,
                    ip = ip,
                    mac = mac,
                    name = name
                    )
                result = db.execute(arpack)
            except:
                print('error parsing DHCPACK:{}'.format(d))
                pass
        else:
            continue

        nrecords += 1
        if nrecords % 100 == 0:
           sys.stdout.write('.')
        if nrecords % 1000 == 0:
           sys.stdout.write('\n')

    timer = time.time()-tstart
    print '\n%d records inserted in %.1f seconds(%.1f records/s)' % (nrecords,timer,float(nrecords)/timer)
    
#
