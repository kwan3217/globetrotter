"""
Packet descriptions for UBLOX binary packets
"""
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from struct import unpack
from typing import BinaryIO, Callable, Any


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
    read_ublox_packet.classes[cls][id](length,payload)
read_packet.classes[0xb5]=read_ublox_packet


class UBloxPacket:
    """
    Subclasses should be dataclasses. Each field in the packet is represented by a
    field in the dataclass. The type of the field is the type of the *scaled* value.
    If the type is a list, then this is considered to be part of the repeating
    section of a packet.

    Each field should have its metadata defined as a dictionary with the following
    entries:
     * type - field type in the binary packet data, as defined either in struct.unpack
              or in the ublox documentation (preferred). ublox types are always
              little-endian as follows:
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
     * scale - either a number or a callable. If a number, the raw value in the binary data
               is multiplied by this value to get the scaled value. If a callable, the
               callable must take a single parameter and will be passed the raw binary data.
               The return type should match the declared type of the field. Default will
               result in the scaled value being the same type and value as the raw value.
     * unit - Unit of the final scaled value. Generally we will scale values such that the
              units can be an SI base or derived unit with no prefixes -- IE if a field is
              integer milliseconds, use a scale of 1e-3 and declare the units to be "s" for
              seconds. Default is no unit.
     * fmt - Format string for displaying the field
     * bit1 - declares the value as a bitfield. All consecutive
    """
    def __init__(self,inf:BinaryIO):
        pass


def bin_field(type:str, scale: None | int | float | Callable[[int], Any]=None, unit:str=None, fmt:str=None, b1:int=None, b0:int=None,*args,**kwargs):
    result={'type':type}
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
    return metadata


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


@dataclass
class UBX_NAV_PVT(UBloxPacket):
    iTOW         :float    =field(metadata=bin_field("U4", unit="s", scale=1e-3, fmt="%10.3f"))
    year         :int      =field(metadata=bin_field("U2", unit="y"))
    month        :int      =field(metadata=bin_field("U1", unit="month"))
    day          :int      =field(metadata=bin_field("U1", unit="d"))
    hour         :int      =field(metadata=bin_field("U1", unit="h"))
    min          :int      =field(metadata=bin_field("U1", unit="min"))
    sec          :int      =field(metadata=bin_field("U1", unit="s"))
    valid        :bool     =field(metadata=bin_field("X1", b0=0, scale=bool))
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
    confirmedAvai:bool =field(metadata=bin_field("X1", scale=bool, b0=5))
    confirmedDate:bool =field(metadata=bin_field("X1", scale=bool, b0=6))
    confirmedTime:bool =field(metadata=bin_field("X1", scale=bool, b0=7))
    numSV        :int  =field(metadata=bin_field("U1"),
                   "lon": ("I4", 1e-7, "deg", "%12.7f"),
                   "lat": ("I4", 1e-7, "deg", "%12.7f"),
                   "height": ("I4", 1e-3, "m", "%12.3f"),
                   "hMSL": ("I4", 1e-3, "m", "%12.3f"),
                   "hAcc": ("U4", 1e-3, "m", "%12.3f"),
                   "vAcc": ("U4", 1e-3, "m", "%12.3f"),
                   "velN": ("I4", 1e-3, "m/s", "%12.3f"),
                   "velE": ("I4", 1e-3, "m/s", "%12.3f"),
                   "velD": ("I4", 1e-3, "m/s", "%12.3f"),
                   "gSpeed": ("I4", 1e-3, "m/s", "%12.3f"),
                   "headMot": ("I4", 1e-5, "deg", "%12.5f"),
                   "sAcc": ("U4", 1e-3, "m/s", "%12.3f"),
                   "headAcc": ("U4", 1e-5, "deg", "%12.5f"),
                   "pDOP": ("U2", 0.01, None, "%6.2f"),
                   "flags3": ("X2", flags([("invalidLlh", 0, 0, bool),
                                           ("lastCorrectionAge", 4, 1, None)]), None, None),
                   "reserved0": ("U4", None, None, None),
                   "headVeh": ("I4", 1e-5, "deg", "%12.5f"),
                   "magDec": ("I2", 1e-2, "deg", "%8.5f"),
                   "magAcc": ("I2", 1e-2, "deg", "%8.5f"),
                   }),

    @dataclass
class Epoch(TableWriter):
    """
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
    """
    iTOW:float=0.0
    week:int=0
    utc:datetime=None
    def ensure_table(self,cur:'psycopg2.cursor'):
        pass

    def write(self,cur):
        pass
