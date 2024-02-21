"""
Read a file which contains AIS data. Documentation for it comes from:
* https://gpsd.gitlab.io/gpsd/AIVDM.html
*

AIS data recorded by the shipometer contains a few more layers, inconsistently applied:

0. Data is broken into roughly 10-minute chunks, each of which is in ~/daisy/daisy_YYMMDD_HHNNSS.nmea[.bz2]
   where the date is the date of the start of the file in UTC.
1. (optional) data is BZIP2 compressed
2. Lines delimited by 0d0a (CRLF)
3. Timestamps for each line of received data is prepended, in ISO format. Some
   lines have a bad century (0123 instead of 2023).
4. (optional) after timestamp, lines are either AIVDM messages as documented above
   OR debug lines. Each debug line is sent after the reciever receives and checks
   the message, but before it transmits that message. Debug lines are in the
   following format:
   Radio1	Channel=A RSSI=-75dBm MsgType=1 MMSI=311042900
5. AIVDM messages, starting with !AVI and ending with the line. Once we get to this point, we are home free,
   as AIVDM is standardized.

## Timestamps
AIS is designed for real-time collision avoidance. Logging is secondary. Therefore, the focus
is on detailed position and velocity vectors, but only just enough time information to account
for the time lag between a ship taking a GPS fix and broadcasting it.

All AIS packets are recorded in the order that they are received. This does simplify some things,
as we don't need a sequence number. The speed of light is high enough, the packets are short enough,
the frequency is low enough, and the ranges are short enough, that each packet is
transmitted, received, and recorded before the next packet is started. A large part of the AIS
spec is the self-organization of the transmitters into broadcast slots such that they don't
step on each other, but if they do, then both packets are lost, and no packets are recorded
out-of-order.

In the future, we should route the AIS through GPSD. GPSD knows how to read an AIS stream
from the hardware I have, and does properly record it in its raw output.

As a result:
* PositionA messages (Type 1, 2, and 3 -- most common and useful ship position reports)
  contain a UTC second field. The position is presumed to be valid at exactly the given
  UTC second, but most PositionA fields don't have enough information to disambiguate
  the rest of the timestamp.
* Many messages include a radio status. The radio status cycles through several different
  field types, but one of them includes current UTC hour and UTC minute
* Type 4 (and type 10) are broadcast by fixed base stations, and include complete UTC date
  and time to second precision.

I discovered this in the middle of the voyage, and changed the AIS recorder so that the received
timestamp is recorded in the log as well. On Atlantic23.05, AIS data was recorded on the laptop.
The laptop was connected by wired ethernet to Fluttershy and was kept in sync with Chrony, so
the absolute time is believed to be millisecond-accurate or better.

The data was recorded with ttycatnet.cpp, which timestamps each line with the time of receipt
of the 0x0a (linefeed) before each line. This has the effect of timestamping each line with the
receipt time of the line *before* it. This is counteracted by the fact that the debug messages
were on, which means that each timestamp of a message is the actual receipt time of the end of
the debug message before it, which is fine because the debug message is sent immediately (no
delay) before the AIVDM message.

Also, ttycatnet.c wasn't used until later in the voyage, starting at 2023-05-09T04:23:13 UTC.
Before that, the AIS logs do not have the received timestamp.

So, we have packets like this:
* Packets that include a complete UTC timestamp, sometimes including date too.
* Packets that only include the UTC second.
* Packets that have a received time recorded.

From this, we need to disambiguate those packets that need it. This problem becomes much easier
after the changeover to record reception times.
"""
import bz2
import re
import warnings
from collections import namedtuple
from dataclasses import dataclass, field, fields
from datetime import datetime, timedelta
from enum import Enum
from glob import glob
from math import sqrt
from os.path import basename
from typing import Callable, Any

from database import Database
from packet import Packet, ensure_table

import pytest as pytest
import pytz
from matplotlib import pyplot as plt

from track import Track

class NotHandled(Exception):
    pass

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
    if shift<=-field_nbits:
        # Whole field is off the end of the data
        return None
    elif shift<0:
        # Partial field, append enough zeros to fill out the field, and correct the shift
        payload=payload<<-shift
        shift=0
    shifted_mask=mask<<shift
    #Now we can grab the field and shift it back down
    field=(payload & shifted_mask)>>shift
    return field


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


class EPFD(Enum):
    UNDEFINED = 0
    GPS = 1
    GLONASS = 2
    COMBINED_GPS_GLONASS = 3
    LORAN_C = 4
    CHAYKA = 5
    INTEGRATED_NAV_SYS = 6
    SURVEYED = 7
    GALILEO = 8
    RESERVED9 = 9
    RESERVED10 = 10
    RESERVED11 = 11
    RESERVED12 = 12
    RESERVED13 = 13
    RESERVED14 = 14
    INTERNAL_GNSS = 15


def parse_aivdm(msg):
    if not hasattr(parse_aivdm,'frags'):
        # Dictionary of lists of fragments. Key is fragid, value is list of payloads. Once all payloads
        # are collected, they are concatenated and parsed, then removed from dict.
        parse_aivdm.frags={}
    parts=msg.split(",")
    nfrag=int(parts[1])
    ifrag=int(parts[2])
    channel=parts[4]
    payload=parts[5]
    bitsleft=int(parts[6])
    if nfrag>1:
        fragid=int(parts[3])
        if fragid not in parse_aivdm.frags:
            parse_aivdm.frags[fragid]=[None]*nfrag
        parse_aivdm.frags[fragid][ifrag-1]=payload
        if ifrag<nfrag:
            assert bitsleft==0,"Nonzero number of bits left in nonfinal fragment"
        if None not in parse_aivdm.frags[fragid]:
            #concatenate payloads
            payload="".join(parse_aivdm.frags[fragid])
            #now remove frags from dict
            del parse_aivdm.frags[fragid]
            return parse_payload(payload,bitsleft)
        else:
            return None
    else:
        return parse_payload(payload, bitsleft)


def parse_payload(payload, shift=0):
    nbits, payload = dearmor_payload(payload, shift)
    msgtype = get_bitfield(nbits, payload, 0, 6)
    if msgtype == 24:
        partno = get_bitfield(nbits, payload, 38, 2)
        msgtype = ("24a" if partno == 0 else "24b")
    if msgtype==6:
        dac = get_bitfield(nbits, payload, 72, 10)
        fid = get_bitfield(nbits, payload, 82, 6)
        msgtype=(6,dac,fid)
        if msgtype not in parse_payload.classes:
            warnings.warn(f"Unhandled type 6 subtype {dac=}, {fid=}")
            msgtype=6
    if msgtype==0:
        return None
    if msgtype not in parse_payload.classes:
        raise NotHandled(f"No handler for message type {msgtype}\n{payload:x}")
    return parse_payload.classes[msgtype](nbits,payload)
parse_payload.classes={}
def register_msg(msgtype,msgcls):
    parse_payload.classes[msgtype]=msgcls
    if type(msgtype)==tuple:
        msgcls.table_name=f'ais_{"_".join([str(x) for x in msgtype])}'
    else:
        msgcls.table_name=f'ais_{msgtype}'


def e(cls):
    def inner(n,payload):
        try:
            return cls(payload)
        except ValueError:
            return None
    return inner


def aismsg(msgcls):
    """
    Decoration for AIS message class. This translates a given dataclass from one that
    describes a packet to one that actually can parse a packet.

    :param msgcls: Packet class to decorate, usually a subclass of Packet. Must have
                   a class-level member field for each packet field. Each such field
                   must have a type annotation indcating the final decoded/parsed/scaled
                   field and a value returned by field()
    :return: A class that can actually parse packets. It has the appropriate methods
             attached, has additional fields, and is a subclass of Dataclass. The class
             will have
    """
    def compile(pktcls: dataclass) -> None:
        """
        Compile a field_dict from the form that most closely matches the
        book to something more usable at runtime

        :param pktclass: Dataclass with names annotated with field(...,metadata=md()).
        :return: named tuple
          * b: number of bytes before repeating block
          * m: number of bytes in repeating block
          * c: number of bytes after repeating block
          for the following, ? is header, block, or footer
          * *_fields: iterable of header field names in order (possibly empty)
          * *_type: string suitable for handing to struct.unpack, for names before repeating block
          * *_unpack: iterable of index of struct.unpack result to use for this field
          * *_scale: iterable of lambdas which scale the field for names before repeating block
          * *_units: iterable units for names before repeating block
          * *_b0: iterable of bitfield position 0, if this is a bitfield.
          * *_b1: iterable of bitfield position 1, if this is a bitfield.

        At parse time, the number of repeats of the repeating block is determined as follows:

        d: full packet size
        n: number of repeats
        d=b+m*n+c
        d-b-c=m*n
        (d-b-c)/m=n
        """

        units = []
        record_names = []
        has_cache=False
        for field in fields(pktcls):
            if field.metadata.get('record', True):
                record_names.append(field.name)
            if field.metadata.get('cache', False):
                has_cache=True
            units.append(field.metadata.get('unit',None))
        b, m, c = None, None, None
        header_fields, block_fields, footer_fields = None, None, None
        header_types, block_types, footer_types = None, None, None
        header_scale, block_scale, footer_scale = None, None, None
        header_units, block_units, footer_units = units, None, None
        header_format, block_format, footer_format = None, None, None
        header_widths, block_widths, footer_widths = None, None, None
        header_b0, block_b0, footer_b0 = None, None, None
        header_b1, block_b1, footer_b1 = None, None, None
        header_unpack, block_unpack, footer_unpack = None, None, None
        header_records, block_records, footer_records = record_names, [], []
        pktcls.compiled_form = namedtuple("packet_desc",
                                          "b m c hn ht hs hu hf hw h0 h1 hp hq bn bt bs bu bf bw b0 b1 bp bq fn ft fs fu ff fw f0 f1 fp fq")._make(
            (b, m, c,
             header_fields, header_types, header_scale, header_units, header_format, header_widths, header_b0,
             header_b1, header_unpack, header_records,
             block_fields, block_types, block_scale, block_units, block_format, block_widths, block_b0, block_b1,
             block_unpack, block_records,
             footer_fields, footer_types, footer_scale, footer_units, footer_format, footer_widths, footer_b0,
             footer_b1, footer_unpack, footer_records))

    def __init__(self, nbits:int,payload: int):
        for field in fields(self):
            if "b0" in field.metadata:
                raw=get_bitfield(nbits, payload, field.metadata["b0"], field.metadata["nb"])
                if raw is None or ("nan" in field.metadata and raw==field.metadata["nan"]):
                    setattr(self,field.name,None)
                else:
                    setattr(self,field.name,field.metadata["scale"](field.metadata["nb"],raw))
        if hasattr(self,"fixup"):
            self.fixup()
    msgcls.__init__ = __init__
    if hasattr(msgcls,'radio'):
        msgcls.syncstate = None
        msgcls.slotout = None
        msgcls.nstation = None
        msgcls.slot = None
        msgcls.utch = None
        msgcls.utcm = None
        msgcls.slotofs = None
        msgcls.__annotations__["syncstate"] = int
        msgcls.__annotations__["slotout"] = int
        msgcls.__annotations__["nstation"] = int
        msgcls.__annotations__["slot"] = int
        msgcls.__annotations__["utch"] = int
        msgcls.__annotations__["utcm"] = int
        msgcls.__annotations__["slotofs"] = int
        def fixup_radio(self):
            if self.radio is not None:
                # decode according to 3.3.7.2.3 from clarification
                self.syncstate = get_bitfield(19, self.radio, 0, 2)
                self.slotout = get_bitfield(19, self.radio, 2, 3)
                if self.slotout in (3, 5, 7):
                    self.nstation = get_bitfield(19, self.radio, 5, 14)
                elif self.slotout in (2, 4, 6):
                    self.slot = get_bitfield(19, self.radio, 5, 14)
                elif self.slotout == 1:
                    this_h=get_bitfield(19, self.radio, 5, 5)
                    this_m=get_bitfield(19, self.radio, 10, 7)
                    if this_h<24 and this_m<60:
                        self.utch = this_h
                        self.utcm = this_m
                elif self.slotout == 0:
                    self.slotofs = get_bitfield(19, self.radio, 5, 14)
        if hasattr(msgcls,'fixup'):
            old_fixup=msgcls.fixup
            def new_fixup(self):
                fixup_radio(self)
                old_fixup(self)
            msgcls.fixup=new_fixup
        else:
            msgcls.fixup=fixup_radio
    msgcls.utc_xmit = None
    msgcls.__annotations__["utc_xmit"] = datetime
    msgcls.utc_recv = None
    msgcls.__annotations__["utc_recv"] = datetime
    msgcls=dataclass(msgcls)
    compile(msgcls)
    msgcls.use_epoch=False
    return msgcls


def md(b0:int,nb:int,scale:Callable[[int,int],Any], **kwargs):
    """
    Annotate a field with the necessary data to extract it from a binary packet. Raw type is required, any
    other named parameter will be included in the resulting metadata dictionary. Any value may be provided,
    but parameters below have special meaning to other parts of the code.

    :param raw_type: type of raw data in UBX form (UBX manual 3.3.5) field type in the binary packet data
              U1 - unsigned 8-bit int (B)
              I1 - signed 8-bit int (b)
              X1 - 8-bit bitfield or padding, treat as unsigned to make bit-manipulation easier (B)
              U2 - unsigned 16-bit int (<H)
              I2 - signed 16-bit int (<h)
              X2 - 16-bit bitfield (<H)
              U4 - unsigned 32-bit int (<I)
              I4 - signed 32-bit int (<i)
              X4 - 32-bit bitfield (<I)
              R4 - IEEE-754 32-bit floating point (<f)
              R8 - IEEE-754 64-bit floating point (<d)
    :param scale: either a number or a callable. If a number, the raw value in the binary data
               is multiplied by this value to get the scaled value. If a callable, it must
               take a single parameter and will be passed the raw binary data.
               The return type should match the declared type of the field. Default will
               result in the scaled value being the same type and value as the raw value.
    :param unit: Unit of the final scaled value. Generally we will scale values such that the
               units can be an SI base or derived unit with no prefixes -- IE if a field is
               integer milliseconds, use a scale of 1e-3 and declare the units to be "s" for
               seconds. Default is no unit.
    :param fmt: Format string for displaying the field
    :param b1: declares the value as a bitfield. All consecutive fields with the same type
               are considered to be the same bitfield. This is the lower bit (LSB is bit 0)
    :param b0: First bit in bitfield
    :param nb: Number of bits in bitfield
    :param comment: Used to add the appropriate comment to the table field
    :param
    :return: A dictionary appropriate for passing to field(metadata=)
    """
    kwargs['b0']=b0
    kwargs['nb']=nb
    kwargs['scale']=scale
    return kwargs


def utcsec(nbits,payload):
    if payload<60:
        return payload
    return None


@aismsg
class posA(Packet):
    msgtype :int   =field(metadata=md(  0, 6, u))
    repeat  :int   =field(metadata=md(  6, 2, u,record=False))
    mmsi    :int   =field(metadata=md(  8,30, u,index=True))
    status  :Status=field(metadata=md(38, 4, e(Status)))
    @staticmethod
    def scale_turn(nbits, payload):
        payload = signed(nbits, payload)
        if payload == 0:
            return 0
        elif abs(payload) <= 126:
            return (abs(payload)/4.733)**2 * (1 if payload > 0 else -1)
        elif abs(payload) == 127:
            return float('Inf') * (1 if payload > 0 else -1)
        else:
            return float('NaN')
    turn    :float =field(metadata=md( 42, 8, scale_turn))
    speed   :float =field(metadata=md( 50,10, lambda nbits,payload:payload/10,nan=511))
    accuracy:bool  =field(metadata=md( 60, 1, b))
    lon     :float =field(metadata=md( 61,28, lambda nbits,payload:signed(nbits,payload)/(60*10000),nan=181*60*10000))
    lat     :float =field(metadata=md( 89,27, lambda nbits, payload: signed(nbits, payload) / (60 * 10000),nan=91*60*10000))
    course  :float =field(metadata=md(116,12, lambda nbits,payload:payload/10,nan=3600))
    heading :int   =field(metadata=md(128, 9, u,nan=511))
    second  :int   =field(metadata=md(137, 6, utcsec))
    maneuver:Maneuver =field(metadata=md(143, 2, e(Maneuver)))
    raim    :bool  =field(metadata=md(148, 1, b))
    radio   :int   =field(metadata=md(149,19, u,record=False))
register_msg(1,posA)
register_msg(2,posA)
register_msg(3,posA)


@aismsg
class msg4(Packet):
    msgtype :int  =field(metadata=md(  0, 6, u))
    repeat  :int  =field(metadata=md(  6, 2, u,record=False))
    mmsi    :int  =field(metadata=md(  8,30, u,index=True))
    year    :int  =field(metadata=md(38, 14, u,nan=0))
    month   :int  =field(metadata=md(52, 4, u,nan=0))
    day     :int  =field(metadata=md(56, 5, u,nan=0))
    hour    :int  =field(metadata=md(61, 5, u,nan=24))
    minute  :int  =field(metadata=md(66, 6, u,nan=60))
    second  :int  =field(metadata=md(72, 6, utcsec))
    accuracy:bool =field(metadata=md(78, 1, b))
    lon     :float=field(metadata=md(79, 28, lambda nbits, payload: signed(nbits, payload) / (60 * 10000),nan=181*60*10000))
    lat     :float=field(metadata=md(107, 27, lambda nbits, payload: signed(nbits, payload) / (60 * 10000),nan=91*60*10000))
    epfd    :EPFD =field(metadata=md(134, 4, e(EPFD)))
    raim    :bool =field(metadata=md(148, 1, b))
    radio   :int  =field(metadata=md(149, 19, u,record=False))
register_msg(11,msg4)
register_msg( 4,msg4)


@aismsg
class msg5(Packet):
    msgtype:int  =field(metadata=md(0, 6, u,record=False))
    repeat:int  =field(metadata=md(6, 2, u,record=False))
    mmsi:int  =field(metadata=md(8, 30, u,index=True,cacheindex=True))
    ais_version: int  =field(metadata=md(38, 2, u))
    imo: int  =field(metadata=md(40, 30, u,cache=True))
    callsign: str  =field(metadata=md(70, 42, t,cache=True))
    shipname: str  =field(metadata=md(112, 120, t,cache=True))
    shiptype: int  =field(metadata=md(232, 8, u,cache=True))
    to_bow: int  =field(metadata=md(240, 9, u,cache=True))
    to_stern: int  =field(metadata=md(249, 9, u,cache=True))
    to_port: int  =field(metadata=md(258, 6, u,cache=True))
    to_stbd: int  =field(metadata=md(264, 6, u,cache=True))
    epfd: int  =field(metadata=md(270, 4, u,cache=True))
    month: int  =field(metadata=md(274, 4, u,cache=True))
    day: int  =field(metadata=md(278, 5, u,cache=True))
    hour: int  =field(metadata=md(283, 5, u,cache=True))
    minute: int  =field(metadata=md(288, 6, u,cache=True))
    draft: float  =field(metadata=md(294, 8, lambda nbits, payload: payload / 10,cache=True))
    dest: str  =field(metadata=md(302, 120, t,cache=True))
    dte: bool  =field(metadata=md(422, 1, b,cache=True))
register_msg(5,msg5)


@aismsg
class msg6(Packet):
    """
    Type 6 - Binary addressed message. This has a Designated Area Code (dac)
    field and a functional ID (fid) field. The message has a binary payload
    which may encode any data at the pleasure of the area authority. Some
    DAC are international and standardized.
    """
    msgtype: int  =field(metadata=md(0, 6, u,record=False))
    repeat:  int  =field(metadata=md(6, 2, u,record=False))
    mmsi:    int  =field(metadata=md(8, 30, u))
    seqno:   int  =field(metadata=md(38, 2, u))
    dest_mmsi:int =field(metadata=md(40, 30, u))
    retransmit:bool=field(metadata=md(70, 1, b))
    dac:     int  =field(metadata=md(72,10, u))
    fid:     int  =field(metadata=md(82, 6, u))
    data:    str  =field(metadata=md(88,920, lambda n, payload: f"{payload:x}"))
register_msg(6,msg6)


@aismsg
class msg6_1_0(Packet):
    """
    Type 6, DAC=1, FID=0. This is "Text using 6-bit ASCII". I am
    decoding this to see if anyone ever actually sends anything
    interesting.
    """
    msgtype: int  =field(metadata=md(0, 6, u,record=False))
    repeat:  int  =field(metadata=md(6, 2, u,record=False))
    mmsi:    int  =field(metadata=md(8, 30, u))
    seqno:   int  =field(metadata=md(38, 2, u))
    dest_mmsi:int =field(metadata=md(40, 30, u))
    retransmit:bool=field(metadata=md(70, 1, b))
    dac:     int  =field(metadata=md(72,10, u))
    fid:     int  =field(metadata=md(82, 6, u))
    ack_reqd:bool=field(metadata=md(88, 1, b))
    text_seq:int =field(metadata=md(89,11, u))
    txt:    str  =field(metadata=md(100,906, t))
register_msg((6,1,0),msg6_1_0)


@aismsg
class msg7(Packet):
    """
    Type 7 acknowledges the recipt of a previous type 6 message
    """
    msgtype: int  =field(metadata=md( 0, 6, u))
    repeat:  int  =field(metadata=md( 6, 2, u,record=False))
    mmsi:    int  =field(metadata=md( 8,30, u))
    mmsi1:   int  =field(metadata=md(40+32*(1-1),30, u))
    mmsiseq1:int  =field(metadata=md(70+32*(1-1), 2, u))
    mmsi2   :int = field(metadata=md(40+32*(2-1),30, u))
    mmsiseq2:int = field(metadata=md(70+32*(2-1), 2, u))
    mmsi3   :int = field(metadata=md(40+32*(3-1),30, u))
    mmsiseq3:int = field(metadata=md(70+32*(3-1), 2, u))
    mmsi4   :int = field(metadata=md(40+32*(4-1),30, u))
    mmsiseq4:int = field(metadata=md(70+32*(4-1), 2, u))
register_msg(13,msg7)
register_msg( 7,msg7)


@aismsg
class msg8(Packet):
    """
    Binary broadcast message, with DAC and FID just like binary addressed messages (type 6).
    """
    msgtype: int  =field(metadata=md(0, 6, u,record=False))
    repeat:  int  =field(metadata=md(6, 2, u,record=False))
    mmsi:    int  =field(metadata=md(8, 30, u))
    dac:     int  =field(metadata=md(40, 10, u))
    fid:     int  =field(metadata=md(50, 6, u))
register_msg(8,msg8)


@aismsg
class msg9(Packet):
    """
    Search-and-rescue (SAR) report. Decoding these to see if we actually heard any such messages.
    """
    msgtype :int  =field(metadata=md(  0, 6, u,record=False))
    repeat  :int  =field(metadata=md(  6, 2, u,record=False))
    mmsi    :int  =field(metadata=md(  8,30, u))
    alt     :int  =field(metadata=md( 38,12, u))
    speed   :int  =field(metadata=md( 50,10, u))
    accuracy:bool =field(metadata=md( 60, 1, b))
    lon     :float=field(metadata=md( 61,28,lambda nbits, payload: signed(nbits, payload) / (60 * 10000)))
    lat     :float=field(metadata=md( 89,27,lambda nbits, payload: signed(nbits, payload) / (60 * 10000)))
    course  :int  =field(metadata=md(116,12, u))
    second  :int  =field(metadata=md(128, 6, utcsec))
    regional:int  =field(metadata=md(134, 8, u))
    dte     :bool =field(metadata=md(142, 1, b))
    assigned:bool =field(metadata=md(146, 1, b))
    raim    :bool =field(metadata=md(147, 1, b))
    radio   :int  =field(metadata=md(148,20, u,record=False))
register_msg(9,msg9)


@aismsg
class msg10(Packet):
    """
    UTC/Date inquiry. Request the destination MMSI to transimit the current UTC
    date and time.
    """
    msgtype  :int  =field(metadata=md(  0, 6, u,record=False))
    repeat   :int  =field(metadata=md(  6, 2, u,record=False))
    mmsi     :int  =field(metadata=md(  8,30, u,index=True))
    dest_mmsi:int  =field(metadata=md(40,30, u))
register_msg(10,msg10)


# Type 11 is identical to type 4, and registered above.


@aismsg
class msg12(Packet):
    """
    Point-to-point text message. Decoded to see if we actually get anything.
    """
    msgtype :int  =field(metadata=md(  0, 6, u,record=False))
    repeat  :int  =field(metadata=md(  6, 2, u,record=False))
    mmsi    :int  =field(metadata=md(  8,30, u,index=True))
    seqno   :int  =field(metadata=md(38, 2, u))
    dest_mmsi:int  =field(metadata=md(40,30, u))
    retransmit:bool =field(metadata=md(70, 1, b))
    text    :str  =field(metadata=md(72, 936, t))
register_msg(12,msg12)


@aismsg
class msg14(Packet):
    """
    Broadcast text message. Decoded to see if we actually get anything.
    """
    msgtype :int  =field(metadata=md(  0, 6, u,record=False))
    repeat  :int  =field(metadata=md(  6, 2, u,record=False))
    mmsi    :int  =field(metadata=md(  8,30, u,index=True))
    text    :str  =field(metadata=md(40, 968, t))
register_msg(14,msg14)


@aismsg
class msg15(Packet):
    msgtype:      int  =field(metadata=md(0, 6, u,record=False))
    repeat:      int  =field(metadata=md(6, 2, u,record=False))
    mmsi:      int  =field(metadata=md(8, 30, u))
    mmsi1:      int  =field(metadata=md(40, 30, u))
    type1_1:      int  =field(metadata=md(70, 6, u))
    offset1_1:      int  =field(metadata=md(76, 12, u))
register_msg(15,msg15)


@aismsg
class msg17(Packet):
    msgtype :int  =field(metadata=md( 0, 6, u,record=False))
    repeat  :int  =field(metadata=md( 6, 2, u,record=False))
    mmsi    :int  =field(metadata=md( 8,30, u,index=True))
    lon     :float=field(metadata=md(40,18,lambda nbits, payload: signed(nbits, payload) / (60 * 10)))
    lat     :float=field(metadata=md(58,17,lambda nbits, payload: signed(nbits, payload) / (60 * 10)))
    data    :str  =field(metadata=md(80,736,lambda n, payload: f"{payload:x}"))
register_msg(17,msg17)


@aismsg
class msg18(Packet):
    msgtype:      int=field(metadata=md(0, 6, u,record=False))
    repeat:       int=field(metadata=md(6, 2, u,record=False))
    mmsi:         int=field(metadata=md(8, 30, u))
    speed:        float=field(metadata=md(46, 10, lambda nbits, payload: payload / 10))
    accuracy:     bool=field(metadata=md(56, 1, b))
    lon:          float=field(metadata=md(57, 28, lambda nbits, payload: signed(nbits, payload) / (60 * 10000)))
    lat:          float=field(metadata=md(85, 27, lambda nbits, payload: signed(nbits, payload) / (60 * 10000)))
    course:       float=field(metadata=md(112, 12, lambda nbits, payload: payload / 10))
    heading:      int=field(metadata=md(124, 9, u))
    second:       int=field(metadata=md(133, 6, utcsec))
register_msg(18,msg18)


@aismsg
class msg20(Packet):
    msgtype:      int=field(metadata=md( 0,  6, u,record=False))
    repeat:       int=field(metadata=md( 6,  2, u,record=False))
    mmsi:         int=field(metadata=md( 8, 30, u))
    offset1:      int=field(metadata=md(40+30*(1-1), 12, u))
    number1:      int=field(metadata=md(52+30*(1-1),  4, u))
    timeout1:     int=field(metadata=md(56+30*(1-1),  3, u))
    increment1:   int=field(metadata=md(59+30*(1-1), 11, u))
    offset2:      int=field(metadata=md(40+30*(2-1), 12, u))
    number2:      int=field(metadata=md(52+30*(2-1),  4, u))
    timeout2:     int=field(metadata=md(56+30*(2-1),  3, u))
    increment2:   int=field(metadata=md(59+30*(2-1), 11, u))
    offset3:      int=field(metadata=md(40+30*(3-1), 12, u))
    number3:      int=field(metadata=md(52+30*(3-1),  4, u))
    timeout3:     int=field(metadata=md(56+30*(3-1),  3, u))
    increment3:   int=field(metadata=md(59+30*(3-1), 11, u))
    offset4:      int=field(metadata=md(40+30*(4-1), 12, u))
    number4:      int=field(metadata=md(52+30*(4-1),  4, u))
    timeout4:     int=field(metadata=md(56+30*(4-1),  3, u))
    increment4:   int=field(metadata=md(59+30*(4-1), 11, u))
register_msg(20,msg20)


@aismsg
class msg21(Packet):
    msgtype:      int=field(metadata=md(0, 6, u,record=False))
    repeat:      int=field(metadata=md(6, 2, u,record=False))
    mmsi:      int=field(metadata=md(8, 30, u,cacheindex=True))
    class AidTypes(Enum):
        Default=0
        Reference_point=1
        RACON=2
        Fixed_structure_offshore=3
        Spare4=4
        Light_without_sectors=5
        Light_with_sectors=6
        Leading_Light_Front=7
        Leading_Light_Rear=8
        Beacon_Cardinal_N=9
        Beacon_Cardinal_E=10
        Beacon_Cardinal_S=11
        Beacon_Cardinal_W=12
        Beacon_Port=13
        Beacon_Stbd = 14
        Beacon_Preferred_Channel_Port = 15
        Beacon_Preferred_Channel_Stbd = 16
        Beacon_Isolated_Danger = 17
        Beacon_Safe_Water=18
        Beacon_Special_Mark = 19
        Cardinal_Mark_N = 20
        Cardinal_Mark_E = 21
        Cardinal_Mark_S = 22
        Cardinal_Mark_W = 23
        Port_Mark = 24
        Stbd_Mark = 25
        Preferred_Channel_Port = 26
        Preferred_Channel_Stbd = 27
        Isolated_Danger = 28
        Safe_Water = 29
        Special_Mark = 30
        Light_Vessel = 31
    aid_type:      AidTypes=field(metadata=md(38, 5, lambda nbits,payload:msg21.AidTypes(payload),cache=True))
    name:      str=field(metadata=md(43, 120, sixbit,cache=True))
    accuracy:      bool=field(metadata=md(163, 1, b,cache=True))
    lon:      float=field(metadata=md(164, 28, lambda nbits, payload: signed(nbits, payload) / (60 * 10000),cache=True))
    lat:      float=field(metadata=md(192, 27, lambda nbits, payload: signed(nbits, payload) / (60 * 10000),cache=True))
    bow:      int=field(metadata=md(219, 9, u,cache=True))
    stern:      int=field(metadata=md(228, 9, u,cache=True))
    port:      int=field(metadata=md(237, 6, u,cache=True))
    stbd:      int=field(metadata=md(243, 6, u,cache=True))
    epfd    :EPFD =field(metadata=md(249, 4, e(EPFD),cache=True))
    second  :int = field(metadata=md(253, 6, utcsec))
    off_position: bool = field(metadata=md(259, 1, b,cache=True))
    regional: int = field(metadata=md(260, 8, u,cache=True))
    raim    :bool  =field(metadata=md(268, 1, b,cache=True))
    virtual: bool = field(metadata=md(269, 8, b,cache=True))
    assigned: bool = field(metadata=md(270, 8, b,cache=True))
register_msg(21,msg21)


@aismsg
class msg24a(Packet):
    """
    Static data report. A transmitter is supposed to send a type A and a type B message,
    in adjacent pairs. The real world means that it is up to the decoder to match up the pairs.
    Part A includes the vessel name, while part B includes a lot of ship information
    including the vessel dimensions.
    """
    msgtype:      int  =field(metadata=md (0, 6, u,record=False))
    repeat:      int  =field(metadata=md (6, 2, u,record=False))
    mmsi:      int  =field(metadata=md (8, 30, u))
    partno:      int  =field(metadata=md(38,2,u))
    shipname:      str  =field(metadata=md(40,120,sixbit))
register_msg("24a",msg24a)


@aismsg
class msg24b(Packet):
    msgtype:    int  =field(metadata=md(0, 6, u,record=False))
    repeat:     int  =field(metadata=md(6, 2, u,record=False))
    mmsi:       int  =field(metadata=md(8, 30, u))
    partno:     int  =field(metadata=md(38, 2, u))
    shiptype:   int  =field(metadata=md(40, 8, u))
    vendorid:   str  =field(metadata=md(48, 18, sixbit))
    model:      int  =field(metadata=md(66, 4, u))
    serial:     int  =field(metadata=md(70, 20, u))
    callsign:   str  =field(metadata=md(90, 42, sixbit))
    to_bow:     int  =field(metadata=md(132, 9, u))
    to_stern:   int  =field(metadata=md(141, 9, u))
    to_port:    int  =field(metadata=md(150, 6, u))
    to_stbd:    int  =field(metadata=md(156, 6, u))
register_msg("24b",msg24b)


@aismsg
class msg27(Packet):
    msgtype :int   =field(metadata=md(  0, 6, u,record=False))
    repeat  :int   =field(metadata=md(  6, 2, u,record=False))
    mmsi    :int   =field(metadata=md(  8,30, u,index=True))
    accuracy:bool  =field(metadata=md( 38, 1, b))
    raim    :bool  =field(metadata=md( 39, 1, b))
    status  :Status=field(metadata=md( 40, 4, e(Status)))
    lon     :float =field(metadata=md( 44,18, lambda nbits,payload:signed(nbits,payload)/(60*10)))
    lat     :float =field(metadata=md( 62,17, lambda nbits, payload: signed(nbits, payload) / (60 * 10)))
    speed   :int   =field(metadata=md( 79, 6, u))
    course  :int   =field(metadata=md( 85, 9, u))
    gnss    :bool  =field(metadata=md( 94, 1, lambda nbits,payload:not bool(payload)))
register_msg(27,msg27)


def ensure_tables(db:Database,drop:bool=False):
    for msgtype,msgdef in parse_payload.classes.items():
        ensure_table(db,msgdef,drop=drop,table_name=msgdef.table_name)


"""
def fix_shipname(inname:str):
    outname=""
    for char in inname:
        if char in ["/"," "]:
            char="_"
        outname+=char
    return outname

# MMSIs for ships whose data we "trust". The intent is to use the timestamps
# from the messages from the ships we "trust" to assist in disambiguating the
# timestamps of other messages.
dream=311042900 #MMSI for MV Disney Dream, the vessel used in Atlantic23.05
trust_mmsi=(dream,)


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


"""




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

"""

def main():
    exercise_get_bitfield()


if __name__=="__main__":
    main()

