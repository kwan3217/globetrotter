"""
Read a file which contains AIS data. Documentation for it comes from
https://gpsd.gitlab.io/gpsd/AIVDM.html

AIS data recorded by the shipometer contains a few more layers, inconsistently applied:

0. Data is broken into roughly 10-minute chunks, each of which is in ~/daisy/daisy_YYMMDD_HHNNSS.nmea[.bz2]
   where the date is the date of the start of the file in UTC.
1. (optional) data is BZIP2 compressed
2. Lines delimited by 0d0a (CRLF)
3. Timestamps for each line of received data is prepended, in ISO format. Some
   lines have a bad century (0123 instead of 2023).
4. (optional) after timestamp, lines are either AIVDM messages as documented above
   OR debug lines. Each debug line is sent after the reciever receives and checks the message, but
   before it transmits that message. Debug lines are in the following format:
   Radio1	Channel=A RSSI=-75dBm MsgType=1 MMSI=311042900
5. AIVDM messages, starting with !AVI and ending with the line.
"""
import bz2
import re
from datetime import datetime, timedelta
from enum import Enum
from glob import glob
from math import sqrt
from os.path import basename

import pytest as pytest
import pytz
from matplotlib import pyplot as plt

from track import Track

#Dictionary of lists of fragments. Key is fragid, value is list of payloads. Once all payloads
#are collected, they are concatenated and parsed, then removed from dict.
frags={}


def wraparound_delta(a:int,b:int,limit:int):
    """
    Given a time a and b on a clock that rolls over in limit, calculate the
    number of ticks between them taking into account the fact that the clock can
    roll over and therefore that b can be less than a. For now, ignore the fact
    that there are really two rollovers (ticks in 1 minute and 32-bit limit)
    and consider that we will never hit the 32-bit limit.

    :param a:
    :param b:
    :param limit:
    :return:
    """
    if(b<a):
      result=limit+b-a
    else:
      result=b-a
    return result



def dearmor_payload(payload,shift=0):
    """
    Given a payload in ASCII armor, remove the armor and return
    an integer with all the bits

    :param payload: string representing armored payload
    :param shift: final answer is shifted this many bits right
    :return: tuple of:
       number of bits in payload
       de-armored payload in an int. This int is likely to be hundreds of bits long.
    """
    def dearmor_char(c):
        """
        Given a character, return the 6-byte integer it encodes

        :param c:
        :return:

        Each character encodes a 6-byte character. To recover the bits,
        convert the character to its ASCII code point. Then subtract 48,
        and if the answer is greater than 40, subtract 8 more.
        """
        a=ord(c)
        a=a-48
        if a>40:
            a=a-8
        return a
    nbits=0
    result=0
    for c in payload:
        result=result*64+dearmor_char(c)
        nbits+=6
    result//=2**shift
    nbits-=shift
    return nbits,result


def get_bitfield(nbits,payload,startbit,field_nbits):
    """
    Get a bitfield from a payload

    :param nbits: Number of bits in payload
    :param payload: Payload in a single (unlimited-length) int
    :param startbit: Start bit of field to extract, numbered such that MSB=0
    :param field_nbits: Number of bits in the field to extract
    :return: Extracted bitfield


    """
    #Example: Extract a field starting at bit 2 and extending for 2 bits, from a 10-bit payload.
    # 0123456789 (MSB numbering from documentation)
    # xxXXxxxxxx
    # 9876543210 (LSB numbering for bit manipulation)
    #The mask is 2 bits wide, so 2**field_nbits-1=2**2-1
    mask=2**field_nbits-1
    #the mask needs to be shifted so that its lowest bit is at LSB 6. This is from the
    #bit length(10) minus the highest bit position in MSB(2) minus the bit width(2)
    shift=nbits-startbit-field_nbits
    shifted_mask=mask<<shift
    #Now we can grab the field and shift it back down
    field=(payload & shifted_mask)>>shift
    return field

@pytest.mark.parametrize(
    "nbits,payload,start,field_len,expected",
    [(10,0b0011000000,2,2,0b11)]
)
def exercise_get_bitfield(nbits,payload,start,field_len,expected):
    assert get_bitfield(nbits,payload,start,field_len)==expected


def sixbit(nbits,string):
          #           1         2         3                4         5         6
          # 0123456789012345678901234567890123   4    56789012345678901234567890123
    chars=r"@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\]^_ !"+'"'+r"#$%&'()*+,-./0123456789:;<=>?"
    result=""
    for i in range(nbits//6):
        char=get_bitfield(nbits,string,i*6,6)
        if char==0:
            return result.strip()
        result+=chars[char]
    return result.strip()


def signed(nbits,val):
    signbit=get_bitfield(nbits,val,0,1)
    if signbit:
        val=val-2**nbits
    return val


@pytest.mark.parametrize(
    "nbits,val,exp",
    [(32,0xffffffff,-1)]
)
def exercise_signed(nbits,val,exp):
    assert signed(nbits,val)==exp


def u(nbits,payload):
    return int(payload)


def b(nbits,payload):
    return bool(payload)


t=sixbit


def scale_turn(nbits,payload):
    payload=signed(nbits,payload)
    if payload==0:
        return 0
    elif abs(payload)<=126:
        return 4.733*sqrt(abs(payload))*(1 if payload>0 else -1)
    elif abs(payload)==127:
        return float('Inf')*(1 if payload>0 else -1)
    else:
        return float('nan')


class Status(Enum):
    UNDERWAY_ENGINE=0
    ANCHOR=1
    NOT_COMMAND=2
    RESTRICT_MANV=3
    CONSTRAIN_DRAFT=4
    MOORED=5
    AGROUND=6
    FISHING=7
    UNDERWAY_SAIL=8
    RESV9=9
    RESV10=10
    TOWING_ASTERN=11
    TOWING_PUSH=12
    RESV13=13
    AIS_SART=14
    UNDEFINED=15



class Maneuver(Enum):
    NA=0
    NO_SPECIAL=1
    SPECIAL=2


posA={
    "msgtype" :(  0, 6, u),
    "repeat"  :(  6, 2, u),
    "mmsi"    :(  8,30, u),
    "status"  :( 38, 4, lambda nbits,payload:Status(payload)),
    "turn"    :( 42, 8, scale_turn),
    "speed"   :( 50,10, lambda nbits,payload:payload/10),
    "accuracy":( 60, 1, b),
    "lon"     :( 61,28, lambda nbits,payload:signed(nbits,payload)/(60*10000)),
    "lat"     :( 89,27, lambda nbits, payload: signed(nbits, payload) / (60 * 10000)),
    "course"  :(116,12, lambda nbits,payload:payload/10),
    "heading" :(128, 9, u),
    "second"  :(137, 6, u),
    "maneuver":(143, 2, lambda nbits,payload:Maneuver(payload)),
    "raim"    :(148, 1, b),
    "radio"   :(149,19, u)
}

msgtypes={
    1:posA,
    2:posA,
    3:posA,
    4:{
        "msgtype": (0, 6, u),
        "repeat": (6, 2, u),
        "mmsi": (8, 30, u),
        "year": (38, 14, u),
        "month": (52, 4, u),
        "day": (56, 5, u),
        "hour": (61, 5, u),
        "minute": (66, 6, u),
        "second": (72, 6, u),
        "accuracy":( 78, 1, b),
        "lon"     :( 79,28, lambda nbits,payload:signed(nbits,payload)/(60*10000)),
        "lat"     :(107,27, lambda nbits, payload: signed(nbits, payload) / (60 * 10000)),
        "efpd":(134,4,u),
        "raim": (148, 1, b),
        "radio": (149, 19, u)
    },
    5:{
        "msgtype":    (  0,  6, u),
        "repeat":     (  6,  2, u),
        "mmsi":       (  8, 30, u),
        "ais_version":( 38,  2, u),
        "imo":        ( 40, 30, u),
        "callsign":   ( 70, 42, t),
        "shipname":   (112,120, t),
        "shiptype":   (232,  8, u),
        "to_bow":     (240,  9, u),
        "to_stern":   (249,  9, u),
        "to_port":    (258,  6, u),
        "to_stbd":    (264,  6, u),
        "epfd":       (270,  4, u),
        "month":      (274,  4, u),
        "day":        (278,  5, u),
        "hour":       (283,  5, u),
        "minute":     (288,  6, u),
        "draft":      (294,  8, lambda nbits,payload:payload/10),
        "dest":       (302,120, t),
        "dte":        (422,  1, b),
    },
    8:{
        "msgtype": (0, 6, u),
        "repeat": (6, 2, u),
        "mmsi": (8, 30, u),
        "dac":(40,10,u),
        "fid":(50,6,u)
    },
    15:{
        "msgtype": (0, 6, u),
        "repeat": (6, 2, u),
        "mmsi": (8, 30, u),
        "mmsi1": (40,30,u),
        "type1_1":(70,6,u),
        "offset1_1":(76,12,u)
    },
    18:{
        "msgtype": (0, 6, u),
        "repeat": (6, 2, u),
        "mmsi": (8, 30, u),
        "speed":(46,10, lambda nbits,payload:payload/10),
        "accuracy":(56,1,b),
        "lon": (57, 28, lambda nbits, payload: signed(nbits, payload) / (60 * 10000)),
        "lat": (85, 27, lambda nbits, payload: signed(nbits, payload) / (60 * 10000)),
        "course": (112, 12, lambda nbits, payload: payload / 10),
        "heading": (124, 9, u),
        "second": (133, 6, u)
    },
    21:{
        "msgtype": (  0,  6, u),
        "repeat":  (  6,  2, u),
        "mmsi":    (  8, 30, u),
        "aid_type":( 38,  5, u),
        "name":    ( 43,120,sixbit),
        "accuracy":(163,  1,b),
        "lon":     (164, 28, lambda nbits, payload: signed(nbits, payload) / (60 * 10000)),
        "lat":     (192, 27, lambda nbits, payload: signed(nbits, payload) / (60 * 10000)),
        "bow":     (219,  9,u),
        "stern":   (228,  9,u),
        "port":    (237,  6,u),
        "stbd":    (243,  6,u)
    },
    "24a":{
        "msgtype": (0, 6, u),
        "repeat": (6, 2, u),
        "mmsi": (8, 30, u),
        "partno":(38,2,u),
        "shipname":(40,120,sixbit)
    },
    "24b": {
        "msgtype": (0, 6, u),
        "repeat": (6, 2, u),
        "mmsi": (8, 30, u),
        "partno": (38, 2, u),
        "shiptype": (40, 8, u),
        "vendorid": (48,18, sixbit),
        "model":(66,4,u),
        "serial":(70,20,u),
        "callsign":(90,42,sixbit),
        "to_bow":(132,9,u),
        "to_stern": (141, 9, u),
        "to_port": (150, 6, u),
        "to_stbd": (1, 6, u),
    }
}


def parse_payload(payload,shift=0):
    nbits,payload=dearmor_payload(payload,shift)
    msgtype=get_bitfield(nbits,payload,0,6)
    result={"msgtype": msgtype}
    if msgtype==24:
        partno=get_bitfield(nbits,payload,38,2)
        msgtype=("24a" if partno==0 else "24b")
    if msgtype in msgtypes:
        for k,(start,field_len,scale) in msgtypes[msgtype].items():
            try:
                result[k]=scale(field_len,get_bitfield(nbits,payload,start,field_len))
            except ValueError:
                #This means we requested a field off the end of the data
                pass
        if "radio" in result:
            # decode according to 3.3.7.2.3 from clarification
            result["syncstate"]=get_bitfield(19,result["radio"],0,2)
            result["slotout"]=get_bitfield(19,result["radio"],2,3)
            if result["slotout"] in (3,5,7):
                result["nstation"]=get_bitfield(19,result["radio"],5,14)
            elif result["slotout"] in (2,4,6):
                result["slot"]=get_bitfield(19,result["radio"],5,14)
            elif result["slotout"]==1:
                result["utch"]=get_bitfield(19,result["radio"],5,5)
                result["utcm"]=get_bitfield(19,result["radio"],10,7)
            elif result["slotout"] == 0:
                result["slotofs"] = get_bitfield(19, result["radio"], 5, 14)
    return result


def parse_aivdm(msg):
    parts=msg.split(",")
    nfrag=int(parts[1])
    ifrag=int(parts[2])
    channel=parts[4]
    payload=parts[5]
    bitsleft=int(parts[6])
    if nfrag>1:
        fragid=int(parts[3])
        if fragid not in frags:
            frags[fragid]=[None]*nfrag
        frags[fragid][ifrag-1]=payload
        if ifrag<nfrag:
            assert bitsleft==0,"Nonzero number of bits left in nonfinal fragment"
        if None not in frags[fragid]:
            #concatenate payloads
            payload="".join(frags[fragid])
            #now remove frags from dict
            del frags[fragid]
            try:
                return parse_payload(payload,bitsleft)
            except Exception:
                import traceback
                traceback.print_exc()
    else:
        try:
            return parse_payload(payload, bitsleft)
        except Exception:
            import traceback
            traceback.print_exc()


def fix_shipname(inname:str):
    outname=""
    for char in inname:
        if char in ["/"," "]:
            char="_"
        outname+=char
    return outname


def save_path(track:list[dict],oufn:str,shipname:str):
    """

    :param track:
    :param oufn:
    :return:
    """
    with open(oufn,"wt") as ouf:
        print(fr"""<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2" xmlns:gx="http://www.google.com/kml/ext/2.2" xmlns:kml="http://www.opengis.net/kml/2.2" xmlns:atom="http://www.w3.org/2005/Atom">
<Document>
	<name>{shipname}</name>
	<Style id="s_ylw-pushpin">
		<IconStyle>
			<scale>1.1</scale>
			<Icon>
				<href>http://maps.google.com/mapfiles/kml/pushpin/ylw-pushpin.png</href>
			</Icon>
			<hotSpot x="20" y="2" xunits="pixels" yunits="pixels"/>
		</IconStyle>
	</Style>
	<StyleMap id="m_ylw-pushpin">
		<Pair>
			<key>normal</key>
			<styleUrl>#s_ylw-pushpin</styleUrl>
		</Pair>
		<Pair>
			<key>highlight</key>
			<styleUrl>#s_ylw-pushpin_hl</styleUrl>
		</Pair>
	</StyleMap>
	<Style id="s_ylw-pushpin_hl">
		<IconStyle>
			<scale>1.3</scale>
			<Icon>
				<href>http://maps.google.com/mapfiles/kml/pushpin/ylw-pushpin.png</href>
			</Icon>
			<hotSpot x="20" y="2" xunits="pixels" yunits="pixels"/>
		</IconStyle>
	</Style>
	<Placemark>
		<name>Untitled Path</name>
		<styleUrl>#m_ylw-pushpin</styleUrl>
		<LineString>
			<tessellate>1</tessellate>
			<coordinates>""",file=ouf)
        for msg in track:
            print(f"{msg['lon']},{msg['lat']},0",file=ouf)
        print(r"""			</coordinates>
		</LineString>
	</Placemark>
</Document>
</kml>
""",file=ouf)


colors=[
    "000000",
    "aa5500",
    "ff0000",
    "ffaa00",
    "ffff00",
    "00ff00",
    "0000ff",
    "aa00ff",
    "888888",
    "ffffff"
]


def save_track(dts:list[datetime],track:list[dict],oufn:str,shipname:str,i_day:int):
    """

    :param track:
    :param oufn:
    :return:
    """
    d={}
    for dt,msg in zip(dts,track):
        d[dt]=msg
    sorted_dts=sorted(d.keys())
    with open(oufn,"wt") as ouf:
        print(fr"""<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2" xmlns:gx="http://www.google.com/kml/ext/2.2" xmlns:kml="http://www.opengis.net/kml/2.2" xmlns:atom="http://www.w3.org/2005/Atom">
<Document>
	<name>{basename(oufn)}</name>
	<Style id="multiTrack_n">
		<IconStyle>
			<Icon>
				<href>http://earth.google.com/images/kml-icons/track-directional/track-0.png</href>
			</Icon>
		</IconStyle>
		<LineStyle>
			<color>99{colors[i_day%10][::-1]}</color>
			<width>6</width>
		</LineStyle>
	</Style>
	<Style id="multiTrack_h">
		<IconStyle>
			<scale>1.2</scale>
			<Icon>
				<href>http://earth.google.com/images/kml-icons/track-directional/track-0.png</href>
			</Icon>
		</IconStyle>
		<LineStyle>
			<color>99{colors[i_day%10][::-1]}</color>
			<width>8</width>
		</LineStyle>
	</Style>
	<StyleMap id="multiTrack">
		<Pair>
			<key>normal</key>
			<styleUrl>#multiTrack_n</styleUrl>
		</Pair>
		<Pair>
			<key>highlight</key>
			<styleUrl>#multiTrack_h</styleUrl>
		</Pair>
	</StyleMap>
	<Placemark>
		<name>{shipname}</name>
		<styleUrl>#multiTrack</styleUrl>
		<gx:balloonVisibility>1</gx:balloonVisibility>
		<gx:Track>
    		<gx:altitudeMode>clampToSeaFloor</gx:altitudeMode>""",file=ouf)
        for dt in sorted_dts:
            print(f"			<when>{dt.isoformat()[0:19]}Z</when>",file=ouf)
        for dt in sorted_dts:
            msg=d[dt]
            print(f"			<gx:coord>{msg['lon']} {msg['lat']} 0</gx:coord>",file=ouf)
        print(r"""		</gx:Track>
	</Placemark>
</Document>
</kml>""",file=ouf)


dream=311042900

trust_mmsi=(dream,)


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
                               +"_(?P<hour>[0-9][0-9])(?P<minute>[0-9][0-9])(?P<second>[0-9][0-9]).nmea(.bz2)?")
putty_fn_timestamp =re.compile(r"daisy(?P<year>[0-9][0-9][0-9][0-9])-(?P<month>[0-9][0-9])-(?P<day>[0-9][0-9])"
                               +"T(?P<hour>[0-9][0-9])(?P<minute>[0-9][0-9])(?P<second>[0-9][0-9]).log")
def get_fn_dt(infn,file=None):
    binfn=basename(infn)
    if match:=ttycat_fn_timestamp.match(binfn):
        #ttycat recording -- timestamp in UTC
        dt=make_utc(match=match)
        if file is not None:
            print(f"Date in filename {dt}", file=file)
    elif match := putty_fn_timestamp.match(binfn):
        #putty log -- timestamp in local time (America/Denver, MDT=UTC-6 during Atlantic23.05)
        dt=make_utc(match=match,local=True)
        if file is not None:
            print(f"Putty date in filename {dt}", file=file)
    else:
        raise ValueError(f"{binfn} Didn't match any known filename format")
    return dt


def dt_sorted(infns):
    tagged_infns=[(get_fn_dt(infn),infn) for infn in infns]
    sorted_tagged_infns=sorted(tagged_infns)
    sorted_infns=[infn for _,infn in sorted_tagged_infns]
    return sorted_infns


manual_times={
    ('daisy_230508_014221.nmea.bz2',    4):make_utc(2023, 5, 8, 1,41,20),
    ('daisy2023-05-07T202212.log'  , 1602):make_utc(2023, 5, 8, 2,47, 2),
    ('daisy2023-05-07T202212.log'  , 2260):make_utc(2023, 5, 8, 3, 6, 7),
    ('daisy2023-05-07T202212.log'  ,16913):make_utc(2023, 5, 8, 8,24, 1),
    ('daisy2023-05-07T202212.log'  ,17931):make_utc(2023, 5, 8, 8,53,55),
    ('daisy2023-05-07T202212.log'  ,18335):make_utc(2023, 5, 8, 9, 5,55),
    ('daisy2023-05-07T202212.log'  ,20177):make_utc(2023, 5, 8, 9,59,55),
    ('daisy2023-05-08T041944.log'  ,  560):make_utc(2023, 5, 8,10,36, 1),
    ('daisy2023-05-08T041944.log'  ,  647):make_utc(2023, 5, 8,10,38, 1),
    ('daisy_230508_121100.nmea'    ,    3):make_utc(2023, 5, 8,11,59,25),
    ('daisy_230508_121100.nmea'    , 2188):make_utc(2023, 5, 8,12,48,13),
    ('daisy_230508_121100.nmea'    , 2466):make_utc(2023, 5, 8,12,54, 7),
    ('daisy_230508_121100.nmea'    , 4388):make_utc(2023, 5, 8,13,36,13),
    ('daisy_230508_121100.nmea'    , 8984):make_utc(2023, 5, 8,15,19,19),
    ('daisy_230508_121100.nmea'    ,11492):make_utc(2023, 5, 8,16,12,25),
    ('daisy_230508_121100.nmea'    ,20101):make_utc(2023, 5, 8,19,24,37),
}

def get_msg_dt(binfn,i_line,msg,old_dt,file=None):
    if (binfn, i_line) in manual_times:
        msg_dt=manual_times[(binfn, i_line)]
        if file is not None:
            print(binfn,i_line,f"manual date/time: {msg_dt}",file=file)
        return msg_dt
    msg_dt=None
    if "year" in msg:
        # Message with full UTC date and time
        try:
            msg_dt = make_utc(msg["year"],msg["month"],msg["day"],
                              msg["hour"],msg["minute"],msg["second"])
            if file is not None:
                print(binfn, i_line, f"explicit date/time: {msg_dt}", file=file)
        except ValueError:
            msg_dt = None
            if file is not None:
                print(binfn, i_line, f"broken explicit date/time", file=file)
    elif "utch" in msg and msg["mmsi"] in trust_mmsi:
        if "second" in msg:
            if msg["second"] < 60:
                msg_dt=old_dt.replace(second=msg["second"])
                if msg["utch"] > 23:
                    if file is not None:
                        print(binfn, i_line, f"utch broken: {msg['utch']}", file=file)
                elif msg["utcm"] > 59:
                    if file is not None:
                        print(binfn, i_line, f"utcm broken: {msg['utcm']}", file=file)
                else:
                    msg_dt = old_dt.replace(hour=msg["utch"],
                                            minute=msg["utcm"])
            else:
                msg_dt=None
    elif "second" in msg and msg["mmsi"] in trust_mmsi:
        if msg["second"] < 60 and old_dt is not None:
            # No date message in this line -- presume that the dt was accurate, and update it with
            # the utc second from this message.
            if msg["second"] < 15 and old_dt.second > 45:
                msg_dt = old_dt + timedelta(seconds=60)
                if file is not None:
                    print(binfn, i_line, f"old: {old_dt}, new: {msg['second']}, minute fwd", file=file)
            elif msg["second"] > 45 and old_dt.second < 15:
                msg_dt = old_dt - timedelta(seconds=60)
                if file is not None:
                    print(binfn, i_line, f"old: {old_dt}, new: {msg['second']}, minute back", file=file)
            else:
                msg_dt=old_dt
            msg_dt = msg_dt.replace(second=msg["second"])
    if msg_dt is not None:
        if msg_dt < old_dt:
            delta = old_dt - msg_dt
        else:
            delta = msg_dt - old_dt
        if delta.seconds > 3600:
            print(binfn, i_line, f"candidate dt difference {delta} from old dt {old_dt}, rejecting", file=file)
            msg_dt=None
    return msg_dt if msg_dt is not None else old_dt



def main():
    puttylog = re.compile(
        r"=~=~=~=~=~=~=~=~=~=~=~= PuTTY log (?P<year>[0-9][0-9][0-9][0-9]).(?P<month>[0-9][0-9]).(?P<day>[0-9][0-9]) (?P<hour>[0-9][0-9]):(?P<minute>[0-9][0-9]):(?P<second>[0-9][0-9]).*")
    line_timestamp=re.compile(r"^(?P<year>[0-9][0-9][0-9][0-9])-(?P<month>[0-9][0-9])-(?P<day>[0-9][0-9])T(?P<hour>[0-9][0-9]):(?P<minute>[0-9][0-9]):(?P<second>[0-9][0-9]).*")
    radioline=re.compile(r".*Radio(?P<radio>[01])\s+Channel=(?P<channel>[AB])\s+RSSI=(?P<rssi>-?[0-9]+)dBm\s+MsgType=(?P<msgtype>[0-9]+)\s+MMSI=(?P<mmsi>[0-9]+)")
    aivdm=re.compile(r".*(!AIVDM.*)(\*[A-F0-9][A-F0-9])")
    infns = []
    for i in range(8,9):
        i_day=i-6
        infns=dt_sorted(glob(f"/mnt/big/Atlantic23.05/daisy/2023/05/{i:02d}/daisy*.log",recursive=True)+
                        glob(f"/mnt/big/Atlantic23.05/daisy/2023/05/{i:02d}/daisy*.nmea*",recursive=True))
        dt=None
        has_date=False
        mmsis = {}
        with open(f"log/2023-05-{i:02d}.log","wt") as logf, open(f"dt/2023-05-{i:02d}.txt",'wt') as dtf, open(f"msg/2023-05-{i:02d}.txt","wt") as msgf:
            for infn in infns:
                binfn=basename(infn)
                print(binfn,file=logf)
                dt=get_fn_dt(infn,file=logf)
                radio=None
                with bz2.open(infn,"rt") if ".bz2" in infn else open(infn,"rt") as inf:
                    for i_linem,line in enumerate(inf):
                        i_line=i_linem+1
                        line=line.strip()
                        line_dt=None
                        if match:=puttylog.match(line):
                            #Putty log header -- these are done in local time
                            line_dt=make_utc(match=match,local=True)
                        elif match:=line_timestamp.match(line):
                            line_dt=make_utc(match=match)
                        if dt is None:
                            dt=line_dt
                        if match:=radioline.match(line):
                            radio={"radio_"+k:l(match.group(k)) for k,l in [("radio",int),("channel",str),("rssi",int),("msgtype",int),("mmsi",int)]}
                        msg_dt=None
                        if match:=aivdm.match(line):
                            msg=parse_aivdm(match.group(1))
                            if msg is not None and "mmsi" in msg:
                                if radio is not None:
                                    msg.update(radio)
                                if line_dt is not None:
                                    dt=line_dt
                                else:
                                    dt=get_msg_dt(binfn,i_line,msg,dt,file=logf)
                                if msg["mmsi"] not in mmsis:
                                    # Newly-seen MMSI
                                    mmsis[msg["mmsi"]] = [None, [], [], [], [], []]
                                if msg["msgtype"] in (1,2,3):
                                    mmsis[msg["mmsi"]][1].append(dt)
                                    mmsis[msg["mmsi"]][2].append(msg)
                                    mmsis[msg["mmsi"]][3].append('k')
                                    mmsis[msg["mmsi"]][4].append(binfn)
                                    mmsis[msg["mmsi"]][5].append(i_line)
                                if msg["msgtype"] in (5,):
                                    mmsis[msg["mmsi"]][0]=msg
                                print(binfn,i_line,dt, msg, file=msgf)
                                radio = None
            name,dts,poss,colors,binfns,lines=mmsis[dream]
            lats=[]
            lons=[]
            lbls=[]
            for dt,pos,binfn,i_line in zip(dts,poss,binfns,lines):
                lats.append(pos["lat"])
                lons.append(pos["lon"])
                lbls.append(f"{str(dt)} {binfn}")
                print(f"{binfn},{i_line},{dt}", file=dtf)
        plt.figure(basename(infn))
        plt.plot(dts,lines,'-')
        #for lat,lon,lbl,has_date in zip(lats,lons,lbls,has_dates):
        #    plt.text(lon,lat,lbl,color=has_date)
        #plt.axis('equal')
        #plt.show()
        names=0
        for mmsi,(name,_,poss,_,_,_) in mmsis.items():
            print(f"{mmsi:09d} {len(poss):6d} {name['shipname'] if (name is not None and 'shipname' in name) else ''}")
            if name is not None:
                names+=1
        print(f"Total MMSIs seen: {len(mmsis)}, named: {names}")
        name,dts,poss,_,_,_=mmsis[dream]
        save_track(dts,poss,f"kml/2023_05_{i:02d}_mmsi{dream:09d}_{fix_shipname(name['shipname'])}.kml",name["shipname"]+f" 2023-05-{i:02d}",i_day)
        plt.show()


if __name__=="__main__":
    main()



