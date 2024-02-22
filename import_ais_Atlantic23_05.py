"""
Import AIS messages from the record of Atlantic23.05
"""
import bz2
import gzip
import re
import warnings
from copy import copy

from datetime import datetime, timedelta
from glob import glob
from math import floor
from os.path import basename
from typing import Union, Match

import pytz

from database.postgres import PostgresDatabase
from packet import ensure_timeseries_tables, register_file_start, register_file_finish, Packet
from packet.ais import parse_payload, parse_aivdm, msg4, NotHandled, ensure_tables, TxTimeSource, decode_mmsi


dream = 311042900
ouf_dream=open("log/dream_trust.csv","wt")


def smart_open(fn, mode: str = None):
    if ".bz2" in fn:
        return bz2.open(fn, mode)
    elif ".gz" in fn:
        return gzip.open(fn, mode)
    else:
        return open(fn, mode)


def make_utc(y:int=None,
             m:int=None,
             d:int=None,
             h:int=None,
             n:int=None,
             s:Union[int,float]=None,
             match:Match=None,
             local=False,tzname="America/Denver"):
    """
    Make a set of timestamp fields into a timezone-aware datetime object with the UTC timestamp.
    :param y: Year in AD era including century part, IE 2024 not 24.
    :param m: Month, 1-12
    :param d: Day of month, 1-31
    :param h: hour of day, 0-23
    :param n: minute of hour, 0-59
    :param s: second of minute, [0,60)
    :param match: If passed, then this is the result of a regular expression match with named groups
                  'year', 'month', 'day', 'hour', 'minute', 'second'.
    :param local: If passed, then the input time is in local time
    :param tzname: If the input is in local time, this is the local time zone. Default is the local
                   time of the laptop that was recording Atlantic23.05.
    :return: A timezone-aware datetime object, expressed in UTC. If the input is in local time,
             the time is properly converted to UTC, based on the local time offset in effect
             at the given time and time zone.
    """
    if match is not None:
        y=match.group("year")
        m = match.group("month")
        d = match.group("day")
        h = match.group("hour")
        n = match.group("minute")
        s = match.group("second")
    if type(y) is str:
        # Sometimes a regular expression only includes a two-digit year. All data
        # handled by this program was recorded after AD 2000.
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
        if '.' in s:
            s=float(s)
        else:
            s=int(s)
    if type(s) is float:
        u = int(1_000_000 * (s - floor(s)))
        s = int(s)
    else:
        u=0
    if local:
        dt = pytz.timezone(tzname).localize(datetime(year=y, month=m, day=d,
                                                     hour=h, minute=n, second=s,
                                                     microsecond=u)).astimezone(pytz.utc)
    else:
        dt = pytz.utc.localize(datetime(year=y, month=m, day=d,
                                        hour=h, minute=n, second=s,
                                        microsecond=u))
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
    elif match := putty_fn_timestamp.match(binfn):
        #putty log -- timestamp in local time (America/Denver, MDT=UTC-6 during Atlantic23.05)
        dt=make_utc(match=match,local=True)
    else:
        raise ValueError(f"{binfn} Didn't match any known filename format")
    return dt



puttylog = re.compile(
    r"=~=~=~=~=~=~=~=~=~=~=~= PuTTY log "
    r"(?P<year>[0-9][0-9][0-9][0-9])."
    r"(?P<month>[0-9][0-9])."
    r"(?P<day>[0-9][0-9]) "
    r"(?P<hour>[0-9][0-9]):"
    r"(?P<minute>[0-9][0-9]):"
    r"(?P<second>[0-9][0-9]).*")
line_timestamp = re.compile(
    r"^(?P<year>[0-9][0-9][0-9][0-9])-"
    r"(?P<month>[0-9][0-9])-"
    r"(?P<day>[0-9][0-9])T"
    r"(?P<hour>[0-9][0-9]):"
    r"(?P<minute>[0-9][0-9]):"
    r"(?P<second>[0-9][0-9])\s*"
    r"(?P<line>.*)")
debug_start = re.compile("dAISy v5\.13 - dAISy 2\+ \(5503\) \(C\)2014-2021 Adrian Studer")
debug_end = re.compile("> entering AIS receive mode")
radioline = re.compile(
    r".*R?adio(?P<radio>[0-9])\s+"
    r"Channel=(?P<channel>[AB])\s+"
    r"RSSI=(?P<rssi>-?[0-9]+)dBm\s+"
    r"MsgType=(?P<msgtype>[0-9]+)(\s+MMSI=(?P<mmsi>[0-9]+))?.*")
errorline = re.compile(r".*error:\s+(?P<error>.*)")


def packet_iterator(infn):
    def line_iterator(inf)->str|None:
        """
        Emulate the action of iterating through the lines of a text input stream
        :param inf: Text input stream
        :return: None if the stream ends
        :yield: The next line

        We need this because if you *do* use the normal line iteration, somewhere
        in the internals it calls next(), which interferes with tell().
        """
        line = inf.readline()
        yield line.strip()
        while line:
            line = inf.readline()
            if not line:
                return
            yield line.strip()

    in_debug = False
    this_ofs = 0
    next_ofs = this_ofs
    with smart_open(infn, "rt") as inf:
        for i_line,line in enumerate(line_iterator(inf)):
            this_ofs = next_ofs
            next_ofs = inf.tell()
            original_line = None
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
            received_dt = None
            if time_match := line_timestamp.match(line):
                original_line = line
                received_dt = make_utc(match=time_match)
                line = time_match.group("line")
            if len(line) < 2:
                # Just skip over blank lines
                continue
            if in_debug and debug_end.match(line):
                in_debug = False
                continue
            if not in_debug:
                if debug_start.match(line):
                    in_debug = True
                    continue
                if putty_match := puttylog.match(line):
                    # Putty log header -- these are done in local time which
                    # was always America/Denver during Atlantic23.05
                    line_dt = make_utc(match=putty_match, local=True)
                    continue
                if radio_match := radioline.match(line):
                    radio = {"radio_" + k: (l(radio_match.group(k)) if (radio_match.group(k) is not None) else None) for
                             k, l in
                             [("radio", int), ("channel", str), ("rssi", int), ("msgtype", int),
                              ("mmsi", int)]}
                    marker = '-'
                    continue
                if error_match := errorline.match(line):
                    # warnings.warn(f"dAISy-detected error: {basename(infn)}, {i_line=} {line_dt=}\n{line}")
                    continue
                if line[0] == "!" or line[0:5] == "AIVDM":
                    try:
                        payload, cksum = line.split("*")
                    except ValueError:
                        # warnings.warn(f"Unable to split checksum: {basename(infn)}, {i_line=}\n{line}")
                        continue
                    try:
                        msg = parse_aivdm(payload)
                        if msg is None:
                            # It's not an error for msg to be None. It means that this line is the first
                            # (or non-last) part of a multipart packet. Don't yield a packet until we
                            # have seen the end of it.
                            pass
                        else:
                            msg.utc_recv=received_dt
                            yield msg,this_ofs
                    except NotHandled:
                        warnings.warn(f"Unable to parse message: {basename(infn)}, {i_line=}\n{line}\ndue to")
                        import traceback
                        traceback.print_exc()
                        continue
                else:
                    warnings.warn(f"Unrecognized line in file: {basename(infn)}, {i_line=}\n{original_line=}\n{line=}")
                    continue


def check_trust(msg:Packet, last_trusted_txmt_dt:datetime, mmsi_trust_strike:set)->datetime:
    """
    Check if the transmit time in the given packet is trusted
    :param msg: packet with a utc_txmt
    :param mmsi_trust_strike:
    :return:  trusted transmit timestamp
    Side effect: Update the untrusted_mmsi set as appropriate
    """
    # Strikes measure trust, or rather lack thereof. Three strikes and you're out, but
    # we leave room for a time source to regain our trust.
    max_strikes=5
    trust_threshold=3
    strike_delta=-1
    trust=True
    if (msg.utc_txmt-last_trusted_txmt_dt).total_seconds() <-60:
        # If the new packet is *before* the last trusted time, the clock is
        # going backwards. Give some leeway, but not much
        strike_delta=1
        trust=False
    elif (msg.utc_txmt-last_trusted_txmt_dt).total_seconds() > 300:
        # If the new packet is too far in the future, that's bad too. Give
        # more leeway here
        strike_delta=1
        trust=False
    if msg.mmsi not in mmsi_trust_strike:
        mmsi_trust_strike[msg.mmsi]=max(0,strike_delta)
    else:
        mmsi_trust_strike[msg.mmsi]=max(0,min(max_strikes,mmsi_trust_strike[msg.mmsi]+strike_delta))
    print(f"{msg.mmsi:09d},{mmsi_trust_strike[msg.mmsi]:2d},{str(last_trusted_txmt_dt)},{str(msg.utc_txmt)},{(msg.utc_txmt-last_trusted_txmt_dt).total_seconds():7.0f},{trust},{msg.msgtype:2d},{str(msg.utc_txtimesrc)}",file=ouf_dream)
    if strike_delta>0:
        # Don't trust this time, but leave ourselves open for trust in the future
        pass
    elif mmsi_trust_strike[msg.mmsi]<trust_threshold:
        # We trust you enough, so set the time
        last_trusted_txmt_dt=copy(msg.utc_txmt)
        trust=True
    else:
        # Your time seems ok now, but we didn't trust you enough in the past to trust you now.
        trust=False
    #if msg.mmsi==dream:
    return trust,last_trusted_txmt_dt


def main():
    print(decode_mmsi(dream))
    mmsi_trust_strikes={}
    import_files=True
    drop=True
    last_msg4_dt=None
    seen_msg4_mmsi=set()
    aivdm=re.compile(r".*(!AIVDM.*)(\*[A-F0-9][A-F0-9])")
    transmitted_tl={} #transmitted time expressed as a list of (y,m,d,h,n,s)
    with PostgresDatabase(host="192.168.217.102",port=5432,
                          user="globetrotter", password="globetrotter", database="globetrotter",
                          schema="atlantic23_05_ais",drop_schema=True,ensure_schema=True) as db:
        if import_files:
            with db.transaction():
                ensure_timeseries_tables(db,drop=False)
                ensure_tables(db,drop=drop)
            infns = sorted(glob("/mnt/big/kwanometry/Atlantic23.05/daisy/2023/05/*/*",recursive=True))
            for i_infn,infn in enumerate(infns):
                file_dt = get_fn_dt(infn)
                last_trusted_txmt_dt=file_dt
#                print(f"{i_infn}/{len(infns)} {basename(infn)}")
                with db.transaction():
                    fileid = register_file_start(db, basename(infn))
                with db.transaction():
                    for msg,ofs in packet_iterator(infn):
                        # Timing
                        # We are sucking on a continuous stream of data, which unfortunately sometimes
                        # does not have a solid timestamp. In fact, a *lot* of the data doesn't have
                        # a solid transmitted timestamp, and *none* of the data before the ttycat
                        # cutover (2023-05-09T04:23:13 UTC) has a recorded received timestamp. So,
                        # we do this:
                        #
                        # If the message has a complete timestamp (either msg4 or utch and utcm along with seconds)
                        #   If not a msg4, use the file date as the date part
                        #   Use that as the transmitted timestamp.
                        # Elif the message has a received timestamp:
                        #   Use that as the candidate transmitted timestamp
                        # Else:
                        #   Use the last trusted timestamp as the candidate transmitted timestamp
                        # If this packet has a seconds field:
                        #   Replace the seconds field of the candidate timestamp with the seconds field
                        #   Rollover minutes and hours as needed.
                        # Use the upated timestamp as our transmitted timestamp
                        #
                        # If we "trust" the timestamp:
                        #   Keep note of it as our last trusted timestamp
                        if msg.utc_recv is None:
                            msg.utc_txtimesrc = TxTimeSource.TRUST
                        else:
                            last_trusted_txmt_dt=copy(msg.utc_recv)
                            msg.utc_txtimesrc=TxTimeSource.RECV
                        if hasattr(msg,'utch') and msg.utch is not None and hasattr(msg,'utcm') and msg.utcm is not None:
                            if hasattr(msg,'second') and msg.second is None:
                                # This happens, when the utc hour and minute are reported in the radio field,
                                # but not in a packet that has a second field (IE not a posA).
                                pass
                            else:
                                msg.utc_txmt=make_utc(last_trusted_txmt_dt.year,last_trusted_txmt_dt.month,last_trusted_txmt_dt.day,
                                                      msg.utch,msg.utcm,msg.second)
                                msg.utc_txtrust,last_trusted_txmt_dt=check_trust(msg,last_trusted_txmt_dt,mmsi_trust_strikes)
                                msg.utc_txtimesrc=TxTimeSource.RADIO
                        elif type(msg)==msg4:
                            if (msg.year is None or msg.month is None or msg.day is None or
                                msg.hour is None or msg.minute is None or msg.second is not None):
                                # If any of the time fields are invalid, ignore the packet
                                pass
                            else:
                                msg.utc_txmt=make_utc(msg.year,msg.month,msg.day,msg.hour,msg.minute,msg.second)
                                msg.utc_txtrust,last_trusted_txmt_dt = check_trust(msg, last_trusted_txmt_dt, mmsi_trust_strikes)
                                msg.utc_txtimesrc=TxTimeSource.MSG4
                        if msg.utc_txtimesrc not in (TxTimeSource.RADIO,TxTimeSource.MSG4):
                            if hasattr(msg,'second') and msg.second is not None:
                                new_second=msg.second
                                msg.utc_txtimesrc|=TxTimeSource.SECOND
                            else:
                                new_second=last_trusted_txmt_dt.second
                            if new_second<15 and last_trusted_txmt_dt.second>45:
                                sec_rollover = timedelta(seconds=60)
                                msg.utc_txtimesrc|=TxTimeSource.ROLLOVER
                            else:
                                sec_rollover = timedelta(seconds=0)
                            if hasattr(msg, 'utcm') and msg.utcm is not None:
                                new_minute = msg.utcm
                            else:
                                new_minute = last_trusted_txmt_dt.minute
                            if hasattr(msg,'utch') and msg.utch is not None:
                                new_hour=msg.utch
                            else:
                                new_hour=last_trusted_txmt_dt.hour
                            new_day=last_trusted_txmt_dt.day
                            msg.utc_txmt=make_utc(last_trusted_txmt_dt.year,
                                                  last_trusted_txmt_dt.month,
                                                  new_day,
                                                  new_hour,
                                                  new_minute,
                                                  new_second)+sec_rollover
                            msg.utc_txtrust,last_trusted_txmt_dt = check_trust(msg, last_trusted_txmt_dt, mmsi_trust_strikes)
                        msg.write(db, fileid=fileid, ofs=ofs)
                with db.transaction():
                    register_file_finish(db, fileid)
    for mmsi in sorted(mmsi_trust_strikes.keys()):
        print(f"{mmsi:09d} - {mmsi_trust_strikes[mmsi]:3d}")
#                print(f"\nDone with {basename(infn)} {i_infn}/{len(infns)}")


if __name__=="__main__":
    main()


