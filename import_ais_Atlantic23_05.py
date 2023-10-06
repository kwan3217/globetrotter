"""
Import AIS messages from the record of Atlantic23.05
"""
import bz2
import gzip
import re
import warnings
from datetime import datetime
from glob import glob
from os.path import basename

import pytz

from database.postgres import PostgresDatabase
from packet import ensure_timeseries_tables, register_file_start, register_file_finish
from packet.ais import parse_payload, parse_aivdm, msg4, NotHandled, ensure_tables


def smart_open(fn, mode: str = None):
    if ".bz2" in fn:
        return bz2.open(fn, mode)
    elif ".gz" in fn:
        return gzip.open(fn, mode)
    else:
        return open(fn, mode)


def make_utc(y=None,m=None,d=None,h=None,n=None,s=None,match=None,local=False,tzname="America/Denver"):
    if match is not None:
        y=match.group("year")
        m = match.group("month")
        d = match.group("day")
        h = match.group("hour")
        n = match.group("minute")
        s = match.group("second")
    if type(y) is str:
        y=2000+int(y)%100
    if type(m) is str:
        m=int(m)
    if type(d) is str:
        d=int(d)
    if type(h) is str:
        h=int(h)
    if type(n) is str:
        n=int(n)
    if type(s) is str:
        s=int(s)
    if local:
        dt = pytz.timezone(tzname).localize(datetime(year=y, month=m, day=d,
                                                     hour=h, minute=n, second=s,
                                                     microsecond=0)).astimezone(pytz.utc)
    else:
        dt = pytz.utc.localize(datetime(year=y, month=m, day=d,
                                        hour=h, minute=n, second=s,
                                        microsecond=0))
    return dt


ttycat_fn_timestamp=re.compile(r"daisy_(?P<year>[0-9][0-9])(?P<month>[0-9][0-9])(?P<day>[0-9][0-9])"
                                "_(?P<hour>[0-9][0-9])(?P<minute>[0-9][0-9])(?P<second>[0-9][0-9]).nmea(.bz2)?")
putty_fn_timestamp =re.compile(r"daisy(?P<year>[0-9][0-9][0-9][0-9])-(?P<month>[0-9][0-9])-(?P<day>[0-9][0-9])"
                                "T(?P<hour>[0-9][0-9])(?P<minute>[0-9][0-9])(?P<second>[0-9][0-9]).log")
def get_fn_dt(infn,file=None):
    binfn=basename(infn)
    if match:=ttycat_fn_timestamp.match(binfn):
        #ttycat recording -- timestamp in UTC
        dt=make_utc(match=match)
        print(f"ttycat date in filename {infn=} {dt=}")
    elif match := putty_fn_timestamp.match(binfn):
        #putty log -- timestamp in local time (America/Denver, MDT=UTC-6 during Atlantic23.05)
        dt=make_utc(match=match,local=True)
        print(f"putty  date in filename {infn=} {dt=}")
    else:
        raise ValueError(f"{binfn} Didn't match any known filename format")
    return dt



def line_iterator(inf):
    line=inf.readline()
    yield line.strip()
    while line:
        line=inf.readline()
        if not line:
            return
        yield line.strip()


def main():
    dream = 311042900
    dbname="Atlantic23_05"
    import_files=True
    drop=True
    puttylog = re.compile(
        r"=~=~=~=~=~=~=~=~=~=~=~= PuTTY log (?P<year>[0-9][0-9][0-9][0-9]).(?P<month>[0-9][0-9]).(?P<day>[0-9][0-9]) (?P<hour>[0-9][0-9]):(?P<minute>[0-9][0-9]):(?P<second>[0-9][0-9]).*")
    line_timestamp=re.compile(r"^(?P<year>[0-9][0-9][0-9][0-9])-(?P<month>[0-9][0-9])-(?P<day>[0-9][0-9])T(?P<hour>[0-9][0-9]):(?P<minute>[0-9][0-9]):(?P<second>[0-9][0-9])\s*(?P<line>.*)")
    radioline=re.compile(r".*R?adio(?P<radio>[0-9])\s+Channel=(?P<channel>[AB])\s+RSSI=(?P<rssi>-?[0-9]+)dBm\s+MsgType=(?P<msgtype>[0-9]+)(\s+MMSI=(?P<mmsi>[0-9]+))?.*")
    errorline=re.compile(r".*error:\s+(?P<error>.*)")
    aivdm=re.compile(r".*(!AIVDM.*)(\*[A-F0-9][A-F0-9])")
    debug_start=re.compile("dAISy v5\.13 - dAISy 2\+ \(5503\) \(C\)2014-2021 Adrian Studer")
    debug_end=re.compile("> entering AIS receive mode")
    transmitted_dt={}
    with PostgresDatabase(user="jeppesen", password="Locking1blitz", database=dbname) as db:
        if import_files:
            with db.transaction():
                ensure_timeseries_tables(db,drop=False)
                ensure_tables(db,drop=drop)
            infns = sorted(glob("/mnt/big/Atlantic23.05/daisy/2023/05/*/*",recursive=True))
            for i_infn,infn in enumerate(infns):
                this_ofs = 0
                next_ofs=this_ofs
                file_dt = get_fn_dt(infn)
                print(f"{i_infn}/{len(infns)} {basename(infn)}")
                with db.transaction():
                    fileid = register_file_start(db, basename(infn))
                with db.transaction(), smart_open(infn,"rt") as inf:
                    in_debug=False
                    for i_line,line in enumerate(line_iterator(inf)):
                        # Because we are counting lines, *DO NOT* advance inf yourself. Don't use
                        # any of read(), readline(), or readlines(). If you are done with this line,
                        # use `continue`. If you want to skip a bunch of lines, keep track of a flag.
                        if i_line % 10 == 0:
                            print('.', end='')
                            if i_line % 1000 == 0:
                                print(i_line)
                        this_ofs = next_ofs
                        next_ofs = inf.tell()
                        split_time=False
                        original_line=None
                        # Time that a message was received. On Atlantic23.05, AIS data was recorded
                        # on the laptop. The laptop was connected by wired ethernet to Fluttershy and
                        # was kept in sync with Chrony, so the absolute time is believed to be
                        # millisecond-accurate or better.
                        #
                        # The data was recorded with ttycatnet.cpp, which timestamps each line with
                        # the time of receipt of the 0x0a (linefeed) before each line. This has
                        # the effect of timestamping each line with the receipt time of the line
                        # *before* it. This is counteracted by the fact that the debug messages were
                        # on, which means that each timestamp of a message is the actual receipt time
                        # of the end of the debug message before it, which is fine because the debug
                        # message is sent immediately (no delay) before the AIVDM message.
                        #
                        # Also, ttycatnet.c wasn't used until later in the voyage, starting at
                        # 2023-05-09T04:23:13 UTC
                        received_dt=None
                        if time_match := line_timestamp.match(line):
                            original_line=line
                            received_dt=make_utc(match=time_match)
                            line = time_match.group("line")
                        if len(line)<2:
                            # Just skip over blank lines
                            continue
                        if in_debug and debug_end.match(line):
                            in_debug=False
                            continue
                        if not in_debug:
                            if debug_start.match(line):
                                in_debug=True
                                continue
                            if putty_match:=puttylog.match(line):
                                # Putty log header -- these are done in local time which
                                # was always America/Denver during Atlantic23.05
                                line_dt=make_utc(match=putty_match,local=True)
                                continue
                            if radio_match := radioline.match(line):
                                radio = {"radio_" + k: (l(radio_match.group(k)) if (radio_match.group(k) is not None) else None) for k, l in
                                         [("radio", int), ("channel", str), ("rssi", int), ("msgtype", int),
                                          ("mmsi", int)]}
                                continue
                            if error_match:=errorline.match(line):
                                print("V", end='')
                                #warnings.warn(f"dAISy-detected error: {basename(infn)}, {i_line=} {line_dt=}\n{line}")
                                continue
                            if line[0]=="!" or line[0:5]=="AIVDM":
                                try:
                                    msg,cksum=line.split("*")
                                except ValueError:
                                    print("W",end='')
                                    # warnings.warn(f"Unable to split checksum: {basename(infn)}, {i_line=}\n{line}")
                                    continue
                                try:
                                    msg=parse_aivdm(msg)
                                except NotHandled:
                                    print("X",end='')
                                    warnings.warn(f"Unable to parse message: {basename(infn)}, {i_line=}\n{line}\ndue to")
                                    import traceback
                                    traceback.print_exc()
                                    continue
                            else:
                                #print("Y",end='')
                                warnings.warn(f"Unrecognized line in file: {basename(infn)}, {i_line=}\n{original_line=}\n{line=}")
                                continue
                        if msg is not None:
                            msg.utc_recv=received_dt
                            if msg.mmsi in transmitted_dt:
                                this_transmitted_dt=transmitted_dt[msg.mmsi]
                            else:
                                this_transmitted_dt=[file_dt.year,file_dt.month,file_dt.day,None,None,None]
                            if msg.msgtype==4:
                                # Only message type we have seen that has a complete datetime. Unfortunately
                                # only for fixed markers, but Dream did transmit this a couple of times.
                                this_transmitted_dt=[msg.year,msg.month,msg.day,msg.hour,msg.minute,msg.second]
                            else:
                                if hasattr(msg,'second'):
                                    new_second=msg.second
                                else:
                                    new_second=this_transmitted_dt[5]
                                sec_rollover=(new_second is not None and
                                              this_transmitted_dt[5] is not None and
                                              new_second<15 and
                                              this_transmitted_dt[5]>45)
                                if hasattr(msg, 'utcm') and msg.utcm is not None:
                                    new_minute = msg.utcm
                                else:
                                    new_minute = this_transmitted_dt[4]
                                    min_rollover=False
                                    if sec_rollover and new_minute is not None:
                                        new_minute+=1
                                        if new_minute>=60:
                                            new_minute-=60
                                            min_rollover=True
                                if hasattr(msg,'utch') and msg.utch is not None:
                                    new_hour=msg.utch
                                else:
                                    new_hour=this_transmitted_dt[3]
                                    if min_rollover:
                                        new_hour+=1
                                        if new_hour>=24:
                                            new_hour-=24
                                if new_hour==0 and this_transmitted_dt[3]==23:
                                    # Since Atlantic23.05 was all in 1 month,
                                    # we don't have to worry about month rollover
                                    this_transmitted_dt[2]+=1
                                this_transmitted_dt[3]=new_hour
                                this_transmitted_dt[4]=new_minute
                                this_transmitted_dt[5]=new_second
                            transmitted_dt[msg.mmsi]=this_transmitted_dt
                            has_time=True
                            for x in this_transmitted_dt:
                                if x is None:
                                    has_time=False
                            if has_time:
                                msg.utc_xmit=datetime(*this_transmitted_dt)
                            else:
                                msg.utc_xmit=None
                            msg.write(db, fileid=fileid, ofs=this_ofs)
                            msg=None
                with db.transaction():
                    register_file_finish(db, fileid)


if __name__=="__main__":
    main()


