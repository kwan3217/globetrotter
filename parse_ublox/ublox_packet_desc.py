"""
Packet descriptions for UBLOX binary packets
"""
from collections import namedtuple
from dataclasses import dataclass, field, fields
from datetime import datetime
from enum import Enum
from functools import partial
from struct import unpack
from typing import BinaryIO, Callable, Any
import re


def read_packet(inf:BinaryIO)->'Packet':
    """
    Read a packet. This is a factory function, which reads
    the next byte from the file, figures out what kind of packet
    it is from that, and calls the correct constructor for that.

    :param inf: binary stream open for reading
    :return: object describing the next packet
    :raises: The constructors must raise an exception if the packet
             cannot be parsed. The input stream will still be open and
             valid, but advanced the minimum number of bytes possible.
             The caller can try again, and won't get caught in an infinte
             loop because this reader always advances the stream at least
             one byte.

    To register a packet type, add an entry to the read_packet.classes
    dictionary. The key is the first byte of the packet, and the value
    is a callable that takes a byte array and binary input stream and
    builds a packet from that. The callable must advance the input
    stream to the first byte after the current packet. The callable must
    return an object representing the class, or raise an exception if
    it can't.
    """
    header=inf.read(1)
    return read_packet.classes[header[0]](header,inf)
read_packet.classes={}


def fletcher8(buf:bytes):
    """
    Calculate the 8-bit Fletcher checksum according to the algorithm in
    section 3.4
    :param buf: Combined header and payload
    :return: two-byte buffer with ck_a as element 0 and ck_b as element 1.
             This can be directly compared with the checksum as-read.
    """
    ck_a=0
    ck_b=0
    for byte in buf:
        ck_a=(ck_a+byte) & 0xFF
        ck_b=(ck_b+ck_a) & 0xFF
    return bytes(ck_a,ck_b)


def read_ublox_packet(header:bytes,inf:BinaryIO):
    """
    Read a ublox packet. This is also a factory function, which reads
    the rest of the header, figures out which packet this is, then
    calls the __init__ for the correct dataclass

    :param header:
    :param inf:
    :return:
    """
    header=header+inf.read(1)
    #Check the second byte and punch out if it's wrong. This way we use up less
    #of the input stream and have more potential bytes to get back in sync.
    if header[1]!=0x62:
        raise ValueError(f"Bad ublox packet signature {header[0]:02x}{header[1]:02x}")
    header=header+inf.read(4)
    cls = header[2]
    id = header[3]
    length = unpack('<H', header[4:6])[0]
    if length==0:
        payload=bytes()
    else:
        payload=inf.read(length)
    read_ck=inf.read(2)
    calc_ck=fletcher8(header+payload)
    if read_ck!=calc_ck:
        raise ValueError(f"Checksum doesn't match: Calculated {calc_ck[0]:02x}{calc_ck[1]:02x}, read {read_ck[0]:02x}{read_ck[1]:02x}")
    read_ublox_packet.classes[cls][id](payload)
read_ublox_packet.classes={}
read_packet.classes[0xb5]=read_ublox_packet


class UBloxPacket:
    """
    Subclasses should be dataclasses. Each field in the packet is represented by a
    field in the dataclass. The type of the field is the type of the *scaled* value.
    If the type is a list, then this is considered to be part of the repeating
    section of a packet.

    """
    def __init__(self,payload:bytes):
        pass


def bin_field(raw_type:str, scale: None | int | float | Callable[[int], Any]=None, unit:str=None, fmt:str=None, b1:int=None, b0:int=None):
    """
    Annotate a field with the necessary data to extract it from a binary packet

    :param type: type of raw data in UBX form (UBX manual 3.3.5) field type in the binary packet data
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
    :param b0: If a bitfield, this is the upper bit
    :return: A dictionary appropriate for passing to field(metadata=)
    """
    result={'type':raw_type}
    if scale is not None:
        result['scale']=scale
    if unit is not None:
        result['unit']=unit
    if fmt is not None:
        result['fmt']=fmt
    if b0 is not None:
        result['b0']=b0
    if b1 is not None:
        result['b1']=b1
    return result


class FIX(Enum):
    NONE=0
    DR_ONLY=1
    TWOD=2
    THREED=3
    GPS_DR=4
    TIME=5


class PSM(Enum):
    NOT_ACTIVE=0
    ENABLED=1
    ACQ=2
    TRACKING=3
    POWER_OPT_TRACKING=4
    INACTIVE=5


class CARR_SOLN(Enum):
    NO_SOLN=0
    SOLN_WITH_FLOAT_AMB=1
    SOLN_WITH_FIXED_AMB=2


def register_ublox(cls:int,id:int,pktcls:dataclass)->None:
    #register the class after it is compiled
    if cls not in read_ublox_packet.classes:
        read_ublox_packet.classes[cls]={}
    read_ublox_packet.classes[cls][id]=pktcls


def compile_ublox(pktcls:dataclass)->None:
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

    def make_scale(scale):
        if scale is None:
            return lambda x: x
        elif callable(scale):
            return scale
        else:
            return partial(lambda s, x: s * x, scale)

    def fmt_width(fmt):
        match = re.match("( *)[^1-9]*(\d+).*", fmt)
        return len(match.group(1)) + int(match.group(2))

    def fmt_set_width(fmt, width):
        match = re.match("(?P<spaces> *)(?P<prefix>[^1-9]*)(?P<sigwidth>\d+)(?P<suffix>.*)", fmt)
        old_width = int(match.group("sigwidth"))
        if width < old_width:
            return match.group("spaces") + match.group("prefix") + str(width) + match.group("suffix")
        else:
            return " " * (width - old_width) + match.group("prefix") + str(old_width) + match.group("suffix")

    size_dict={"U1":("B",1,"%3d"),
               "U2":("H",2,"%5d"),
               "U4":("I",4,"%9d"),
               "I1":("b",1,"%4d"),
               "I2":("h",2,"%6d"),
               "I4":("i",4,"%10d"),
               "X1":("B",1,"%02x"),
               "X2":("H",2,"%04x"),
               "X4":("I",4,"%08x"),
               "R4":("f",4,"%14.7e"),
               "R8":("d",8,"%21.14e")}
    last_x=None
    lengths=[0,0,0]
    names=[[],[],[]]
    types=["","",""]
    scales=[[],[],[]]
    unpacks=[[],[],[]]
    units=[[],[],[]]
    fmts=[[],[],[]]
    widths=[[],[],[]]
    b0s=[[],[],[]]
    b1s=[[],[],[]]
    part=0
    i_struct=0
    last_b1=None
    for field in fields(pktcls):
        print(i_struct,lengths[part],field.name)
        if field.type==list:
            if part==0:
                part=1
                last_x=None
                i_struct=0
        else:
            if part==1:
                part=2
                last_x=None
                i_struct = 0
        names[part].append(field.name)
        ublox_type=field.metadata['type']
        if 'b0' in field.metadata:
            # Handle bitfields
            if (ublox_type==last_x) and (last_b1 is not None) and (field.metadata['b0']>last_b1):
                i_struct-=1
            else:
                types[part] += size_dict[ublox_type][0]
                lengths[part] += size_dict[ublox_type][1]
            last_x=ublox_type
            b0s[part].append(field.metadata['b0'])
            if 'b1' in field.metadata:
                last_b1=field.metadata['b1']
            else:
                last_b1=field.metadata['b0']
            b1s[part].append(last_b1)
        else:
            if ublox_type[0:2]=="CH":
                #handle strings
                types[part]+=ublox_type[2:]+"s"
                lengths[part]+=int(ublox_type[2:])
            else:
                #handle numbers
                types[part] += size_dict[ublox_type][0]
                lengths[part] += size_dict[ublox_type][1]
            b0s[part].append(None)
            b1s[part].append(None)
            last_x=None
        unpacks[part].append(i_struct)
        i_struct+=1
        if 'scale' in field.metadata:
            scales[part].append(make_scale(field.metadata['scale']))
        else:
            scales[part].append(None)
        if 'unit' in field.metadata:
            units[part].append(field.metadata['unit'])
        else:
            units[part].append(None)
        if 'fmt' in field.metadata:
            fmt=field.metadata['fmt']
        else:
            fmt=size_dict[ublox_type][2]
        if part==1:
            colhead_width=len(field.name)+(0 if units[part][-1] is None else 3+len(units[part][-1]))
            if fmt_width(fmt)<colhead_width:
                fmt=fmt_set_width(fmt,colhead_width)
        fmts[part].append(fmt)
        widths[part].append(fmt_width(fmt))
    b,m,c=lengths
    header_fields,block_fields,footer_fields=names
    header_types,block_types,footer_types=["<"+x for x in types]
    header_scale,block_scale,footer_scale=scales
    header_units,block_units,footer_units=units
    header_format,block_format,footer_format=fmts
    header_widths,block_widths,footer_widths=widths
    header_b0,block_b0,footer_b0=b0s
    header_b1,block_b1,footer_b1=b1s
    header_unpack,block_unpack,footer_unpack=unpacks
    pktcls.compiled_form=namedtuple("packet_desc","b m c hn ht hs hu hf hw h0 h1 hp bn bt bs bu bf bw b0 b1 bp fn ft fs fu ff fw f0 f1 fp")._make((b,m,c,
            header_fields,header_types,header_scale,header_units,header_format,header_widths,header_b0,header_b1,header_unpack,
            block_fields,block_types,block_scale,block_units,block_format,block_widths,block_b0,block_b1,block_unpack,
            footer_fields, footer_types, footer_scale, footer_units, footer_format,footer_widths,footer_b0,footer_b1,footer_unpack))


def ublox_packet(cls:int,id:int):
    def inner(pktcls):
        pktcls=dataclass(pktcls)
        compile_ublox(pktcls)
        register_ublox(cls,id,pktcls)
        return pktcls
    return inner


@ublox_packet(0x01,0x07)
class UBX_NAV_PVT(UBloxPacket):
    iTOW         :float    =field(metadata=bin_field("U4", unit="s", scale=1e-3, fmt="%10.3f"))
    year         :int      =field(metadata=bin_field("U2", unit="y"))
    month        :int      =field(metadata=bin_field("U1", unit="month"))
    day          :int      =field(metadata=bin_field("U1", unit="d"))
    hour         :int      =field(metadata=bin_field("U1", unit="h"))
    min          :int      =field(metadata=bin_field("U1", unit="min"))
    sec          :int      =field(metadata=bin_field("U1", unit="s"))
    validDate    :bool     =field(metadata=bin_field("X1", b0=0, scale=bool))
    validTime    :bool     =field(metadata=bin_field("X1", b0=1, scale=bool))
    fullyResolved:bool     =field(metadata=bin_field("X1", b0=2, scale=bool))
    validMag     :bool     =field(metadata=bin_field("X1", b0=3, scale=bool))
    tAcc         :float    =field(metadata=bin_field("U4", unit="s", scale=1e-9, fmt="%12.9f"))
    nano         :float    =field(metadata=bin_field("I4", unit="s", scale=1e-9, fmt="%12.9f"))
    fixType      :FIX      =field(metadata=bin_field("X1", scale=FIX))
    gnssFixOK    :bool     =field(metadata=bin_field("X1", scale=bool, b0=0))
    diffSoln     :bool     =field(metadata=bin_field("X1", scale=bool, b0=1))
    psmState     :PSM      =field(metadata=bin_field("X1", scale=PSM, b1=4, b0=2))
    headVehValid :bool     =field(metadata=bin_field("X1", scale=bool, b0=5))
    carrSoln     :CARR_SOLN=field(metadata=bin_field("X1", scale=CARR_SOLN, b1=7, b0=6))
    confirmedAvai:bool     =field(metadata=bin_field("X1", scale=bool, b0=5))
    confirmedDate:bool     =field(metadata=bin_field("X1", scale=bool, b0=6))
    confirmedTime:bool     =field(metadata=bin_field("X1", scale=bool, b0=7))
    numSV        :int      =field(metadata=bin_field("U1"))
    lon          :float    =field(metadata=bin_field("I4", scale=1e-7, unit="deg", fmt="%12.7f"))
    lat          :float    =field(metadata=bin_field("I4", scale=1e-7, unit="deg", fmt="%12.7f"))
    height       :float    =field(metadata=bin_field("I4", scale=1e-3, unit="m"  , fmt="%12.3f"))
    hMSL         :float    =field(metadata=bin_field("I4", scale=1e-3, unit="m"  , fmt="%12.3f"))
    hAcc         :float    =field(metadata=bin_field("U4", scale=1e-3, unit="m"  , fmt="%12.3f"))
    vAcc         :float    =field(metadata=bin_field("U4", scale=1e-3, unit="m"  , fmt="%12.3f"))
    velN         :float    =field(metadata=bin_field("I4", scale=1e-3, unit="m/s", fmt="%12.3f"))
    velE         :float    =field(metadata=bin_field("I4", scale=1e-3, unit="m/s", fmt="%12.3f"))
    velD         :float    =field(metadata=bin_field("I4", scale=1e-3, unit="m/s", fmt="%12.3f"))
    gSpeed       :float    =field(metadata=bin_field("I4", scale=1e-3, unit="m/s", fmt="%12.3f"))
    headMot      :float    =field(metadata=bin_field("I4", scale=1e-5, unit="deg", fmt="%12.5f"))
    sAcc         :float    =field(metadata=bin_field("U4", scale=1e-3, unit="m/s", fmt="%12.3f"))
    headAcc      :float    =field(metadata=bin_field("U4", scale=1e-5, unit="deg", fmt="%12.5f"))
    pDOP         :float    =field(metadata=bin_field("U2", scale=0.01,             fmt="%6.2f"))
    invalidLlh   :bool     =field(metadata=bin_field("X2",scale=bool,b0=0))
    lastCorrectionAge:float=field(metadata=bin_field("X2",scale=lambda x:(float('NaN'),1,2,5,10,15,20,30,45,60,90,120,float('Inf'))[x],b1=4,b0=1))
    reserved0    :None     =field(metadata=bin_field("U4"))
    headVeh      :float    =field(metadata=bin_field("I4",scale=1e-5,unit="deg",fmt="%12.5f"))
    magDec       :float    =field(metadata=bin_field("I2",scale=1e-2,unit="deg",fmt="%8.5f"))
    magAcc       :float    =field(metadata=bin_field("I2",scale=1e-2,unit="deg",fmt="%8.5f"))
    def __init__(self, payload):
        super().__init__(payload)


"""
@dataclass
class Epoch(TableWriter):
    """"""
    Epoch record. All GPS (and possibly sensor) data is delimited by UBX-NAV-EOE packets. The EOE
    packet is sent after all other UBX and NMEA packets associated with this time. It is intended
    specifically for delimiting packets -- all data in between two EOE packets is effective
    at the time recorded in the EOE packet.

    :param cur: cursor to write with
    :param iTOW: scaled iTOW timestamp. iTOW is transmitted as a millisecond
                 count, but the packet reader scales this to seconds.
    :param week: week number, can be pulled from UBX_NAV_TIMEGPS message. That message
                 is marked as I2 (int16_t) which means it can count up to 32,767 weeks
                 from the GPS epoch (2608-01-03). That's far enough in the future to
                 never have to hear the words "week number rollover"
    :param utc:  UTC of this epoch
    """"""
    iTOW:float=0.0
    week:int=0
    utc:datetime=None
    def ensure_table(self,cur:'psycopg2.cursor'):
        pass

    def write(self,cur):
        pass
"""


def main():
    print(UBX_NAV_PVT.compiled_form)


if __name__=="__main__":
    main()