"""
Code to parse a stream of data from a UBlox reveiver (not necessarily just UBlox packets)
and do useful things with the data.
"""
import bz2
import struct
import traceback
from glob import glob
from os.path import basename

from struct import unpack
from enum import Enum

import numpy as np
from kwanmath.vector import vcomp, vlength
from matplotlib import pyplot as plt

from parse_ublox.bin import dump_bin
from parse_ublox.parse_rtcm import parse_rtcm
from parse_ublox import parse_ublox, print_ublox, DataType


class PacketType(Enum):
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

    :param inf:
    :param reject_invalid:
    :param nmea_max:
    :return:
    """
    header_peek=inf.read(1)
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
            ck=inf.read(2)
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
    elif header_peek[0]==ord('{'):
        #json -- for now, we count on the fact that gpsd writes a whole json packet on one line, so watch for 0d0a
        payload=header_peek
        done=False
        while not done:
            next_byte=inf.read(1)
            done=(next_byte[0]==0x0a)
            payload+=next_byte
        return PacketType.JSON,str(payload,encoding='cp437').strip()
    else:
        # Not either kind of packet we can recognize. Return None, and know that the
        # data stream has had one byte consumed.
        return None,None


def parse_gps_file(infn,file=None):
    with bz2.open(infn,"rb") if ".bz2" in infn else open(infn,"rb") as inf:
        ofs=0
        while True:
            try:
                packet_type,packet=next_packet(inf)
            except Exception:
                return None
            print(f"ofs: {ofs:08x}, pkt_len: {len(packet)}",file=file)
            ofs+=len(packet)
            if packet_type in (PacketType.NMEA,PacketType.JSON):
                yield packet_type,packet
            elif packet_type==PacketType.UBLOX:
                try:
                    parsed_packet=parse_ublox(packet)
                    yield packet_type,parsed_packet
                except struct.error:
                    traceback.print_exc()
                    dump_bin(packet)
            elif packet_type==PacketType.RTCM:
                try:
                    parsed_packet=parse_rtcm(packet,verbose=False)
                    yield packet_type,parsed_packet
                except AssertionError:
                    traceback.print_exc()
                    dump_bin(packet)


def plot_waves(wildcard:str="/mnt/big/Atlantic23.05/Fluttershy/FluttershyBase/2023/05/10/*.ubx.bz2",i0:int=0,i1:int=1,di:int=1):
    """
    Plot the accelerometer data from one or more ubx capture files

    :param wildcard: Pattern to match
    :param i0: index of first file to read
    :param i1: index past last file to read
    :param di:
    :return:
    """
    infns=sorted(glob(wildcard))
    has_pvt=False
    esf_timestamps=[]
    pvt_itow=[]
    wave_timestamps=[]
    wave_xs=[]
    wave_ys=[]
    wave_zs=[]
    gyro_timestamps=[]
    gyro_xs=[]
    gyro_ys=[]
    gyro_zs=[]
    gyro_ts=[]
    with open("dump.txt","wt") as ouf:
        for infn in (infns[0],):
            print(basename(infn),file=ouf)
            for packet_type,packet in parse_gps_file(infn,file=ouf):
                if packet_type==PacketType.NMEA:
                    print(packet,file=ouf)
                elif packet_type==PacketType.UBLOX:
                    print_ublox(packet,file=ouf)
                    if packet.name=="UBX-ESF-MEAS":
                        if has_pvt:
                            esf_timestamps.append(packet.timeTag)
                            has_pvt=False
                        has_ts=False
                        has_gyro_ts=False
                        for field,type,raw,scale,units in packet.data:
                            if type in [DataType.ACC_X,DataType.ACC_Y,DataType.ACC_Z]:
                                if not has_ts:
                                    wave_timestamps.append(packet.timeTag)
                                    has_ts=True
                            if type==DataType.ACC_X:
                                wave_xs.append(scale)
                            if type == DataType.ACC_Y:
                                wave_ys.append(scale)
                            if type == DataType.ACC_Z:
                                wave_zs.append(scale)
                            if type in [DataType.GYRO_X, DataType.GYRO_Y, DataType.GYRO_Z]:
                                if not has_gyro_ts:
                                    gyro_timestamps.append(packet.timeTag)
                                    has_gyro_ts = True
                            if type == DataType.GYRO_X:
                                gyro_xs.append(scale)
                            if type == DataType.GYRO_Y:
                                gyro_ys.append(scale)
                            if type == DataType.GYRO_Z:
                                gyro_zs.append(scale)
                            if type == DataType.GYRO_T:
                                gyro_ts.append(scale)
                    elif packet.name=="UBX-NAV-PVT":
                        print(packet.iTOW)
                        pvt_itow.append(packet.iTOW)
                        has_pvt=True
                elif packet_type == PacketType.JSON:
                    print(packet,file=ouf)
                elif packet_type==PacketType.RTCM:
                    print(packet,file=ouf)
    with open("timestamp.csv","wt") as ouf:
        for i,(itow,timestamp) in enumerate(zip(pvt_itow,esf_timestamps)):
            print(f"{i},{itow},{timestamp}",file=ouf)
    #plt.figure("Timestamps")
    #plt.plot(pvt_itow,esf_timestamps)
    #plt.xlabel("itow")
    #plt.ylabel("esf timestamp")
    plt.figure("Waves")
    wave_xs=np.array(wave_xs)
    wave_ys=np.array(wave_ys)
    wave_zs=np.array(wave_zs)
    wave_ls=vlength(vcomp((wave_xs,wave_ys,wave_zs)))
    wave_timestamps=np.array(wave_timestamps)
    plt.plot((wave_timestamps-wave_timestamps[0])/1000,wave_xs,'r-',label='X acceleration m/s**2')
    plt.plot((wave_timestamps-wave_timestamps[0])/1000,wave_ys,'g-',label='Y acceleration m/s**2')
    plt.plot((wave_timestamps-wave_timestamps[0])/1000,wave_zs,'b-',label='Z acceleration m/s**2')
    plt.plot((wave_timestamps-wave_timestamps[0])/1000,wave_ls,'k-',label='Total acceleration m/s**2')
    plt.legend()
    plt.figure("Gyro")
    gyro_xs=np.array(gyro_xs)
    gyro_ys=np.array(gyro_ys)
    gyro_zs=np.array(gyro_zs)
    gyro_ls=vlength(vcomp((gyro_xs,gyro_ys,gyro_zs)))
    gyro_ts=np.array(gyro_ts)
    gyro_timestamps=np.array(gyro_timestamps)
    plt.plot((gyro_timestamps-gyro_timestamps[0])/1000,gyro_xs,'r-',label='X rate deg/s')
    plt.plot((gyro_timestamps-gyro_timestamps[0])/1000,gyro_ys,'g-',label='Y rate deg/s')
    plt.plot((gyro_timestamps-gyro_timestamps[0])/1000,gyro_zs,'b-',label='Z rate deg/s')
    plt.plot((gyro_timestamps-gyro_timestamps[0])/1000,gyro_ls,'k-',label='Total rate deg/s')
    plt.plot((gyro_timestamps-gyro_timestamps[0])/1000,gyro_ts,'y-',label='sensor T degC')
    plt.legend()
    plt.show()


def main():
    plot_waves()


if __name__=="__main__":
    main()
