"""
Code to parse a stream of data from a UBlox reveiver (not necessarily just UBlox packets)
and do useful things with the data.
"""
import struct
import traceback
import bz2
import gzip

from struct import unpack
from enum import Enum
from .bin import dump_bin
from packet.rtcm.parse_rtcm import parse_rtcm
from .parse_ublox import parse_ublox, print_ublox


class PacketType(Enum):
    """
    Enumeration for packet types
    """
    NMEA = 1
    UBLOX = 2
    RTCM = 3
    JSON = 4


def ublox_ck_valid(payload:bytes,ck_a:int,ck_b:int):
    """
    Check the checksum of a UBlox packet

    :param payload:
    :param ck_a:
    :param ck_b:
    :return:
    """
    return True


def nmea_ck_valid(packet:bytes,has_checksum):
    """
    Check the checksum of an NMEA packet

    :param packet:
    :return:
    """
    return True


def rtcm_ck_valid(packet:bytes):
    """
    Check the checksum of an RTCM packet

    :param packet:
    :return:
    """
    return True


def next_packet(inf,reject_invalid=True,nmea_max=None):
    """
    Get the next packet from a binary stream

    :param inf: File-like object, open in binary read mode
    :param reject_invalid: If True, check packet checksums and don't return the packet if the checksum fails
    :param nmea_max:
    :return: Tuple of:
      * Packet type from PacketType enum
      * Packet data, exact format of which depends on the packet type
    """
    header_peek=inf.read(1)
    if len(header_peek)<1:
        #Not enough data to read even one byte of next packet (EOF at end of packet)
        return None,None
    if header_peek[0]==ord('{'):
        #Looks like JSON, read until the 0D0A
        result=header_peek
        while result[-1]!=0x0A:
            result+=inf.read(1)
        return PacketType.JSON,str(result,encoding='cp437')
    if header_peek[0]==ord('$'):
        #Looks like an NMEA packet, read until the asterisk
        result=header_peek
        while result[-1]!=ord('*'):
            result+=inf.read(1)
        #Read either 0D0A or checksum
        result+=inf.read(2)
        has_checksum=False
        if not (result[0]==0x0d and result[1]==0x0a):
            result+=inf.read(2)
            has_checksum=True
        if not reject_invalid or nmea_ck_valid(result,has_checksum):
            return PacketType.NMEA, str(result,encoding='cp437').strip()
        else:
            return None, None
    elif header_peek[0]==0xb5:
        #Start of UBlox header
        header=header_peek+inf.read(1)
        if header[1]==0x62:
            #Header is valid, read the entire packet
            header=header+inf.read(4)
            cls=header[2]
            id=header[3]
            length=unpack('<H',header[4:6])[0]
            payload=inf.read(length)
            if len(payload)<length:
                #Incomplete packet (IE chopped off by EOF)
                return None,None
            ck=inf.read(2)
            if len(ck)<2:
                #Incomplete checksum (IE chopped off by EOF)
                return None,None
            if not reject_invalid or ublox_ck_valid(payload,ck[0],ck[1]):
                return PacketType.UBLOX, header+payload+ck
            else:
                #Checksum failed. Advanced past the whole packet, but packet is not returned.
                return None,None
        else:
            #Not a ublox packet. We wish we could push back, but won't for now. Know that
            #the stream has been advanced by two bytes. If there is a stray 0xb5 (mu) before
            #an actual packet, this will cause the packet to be missed.
            return None,None
    elif header_peek[0]==0xd3:
        # Start of RTCM packet. One byte preamble, two-byte big-endian length (only 10 ls bits
        # are significant), n-byte payload, three byte CRC
        #Start of UBlox header
        header=header_peek+inf.read(2)
        length=unpack('>H',header[1:3])[0] & 0x3ff
        payload=inf.read(length)
        ck=inf.read(3)
        if not reject_invalid or ublox_ck_valid(payload,ck[0],ck[1]):
            return PacketType.RTCM, header+payload+ck
        else:
            #Checksum failed. Advanced past the whole packet, but packet is not returned.
            return None,None
    else:
        # Not either kind of packet we can recognize. Return None, and know that the
        # data stream has had one byte consumed.
        return None,None


def smart_open(infn:str,mode:str="rb"):
    if ".bz2" in infn:
        return bz2.open(infn,mode)
    elif ".gz" in infn:
        return gzip.open(infn,mode)
    else:
        return open(infn,mode)


def parse_gps_file(infn):
    """
    Parse an entire file, one packet at a time
    :param infn:
    :yield: A tuple of:
      * offset into the file in bytes, first byte in file is 0
      * packet type as a PacketType enum
      * packet data in whatever form the packet parser uses.
    """
    with smart_open(infn,"rb") as inf:
        ofs=0
        while True:
            packet_type,packet=next_packet(inf)
            #print(f"ofs: {ofs:08x}, pkt_len: {len(packet)}")
            if packet_type is None:
                return
            if packet_type==PacketType.NMEA or packet_type==PacketType.JSON:
                yield ofs,packet_type,packet
            elif packet_type==PacketType.UBLOX:
                try:
                    parsed_packet=parse_ublox(packet)
                    yield ofs,packet_type,parsed_packet
                except struct.error:
                    traceback.print_exc()
                    dump_bin(packet)
            elif packet_type==PacketType.RTCM:
                try:
                    parsed_packet=parse_rtcm(packet,verbose=False)
                    yield ofs,packet_type,parsed_packet
                except AssertionError:
                    traceback.print_exc()
                    dump_bin(packet)
            ofs+=len(packet)



def main():
    for packet_type,packet in parse_gps_file("fluttershy_survey_in_220404_205357.ubx"):
        if packet_type==PacketType.NMEA:
            print(packet)
        elif packet_type==PacketType.UBLOX:
            print_ublox(packet)
        elif packet_type==PacketType.RTCM:
            print(packet)


if __name__=="__main__":
    main()
