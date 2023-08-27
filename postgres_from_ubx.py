"""
Import gpsd pseudo-NMEA, which are created from the input binary datastream (in this case uBlox).
"""
import os
from glob import glob
from os.path import basename

import datetime
from typing import Iterable

import matplotlib.pyplot as plt
import psycopg2
import pytz

from parse_ublox.parse_gps import parse_gps_file, PacketType
from parse_ublox.parse_l1ca_nav import parse_l1ca_subframe
from parse_ublox.parse_ublox import GPS_SigID


def dm_to_deg(dm:float,sign:str)->float:
    deg=dm//100
    min=dm%100
    result=deg+min/60
    if sign=='S' or sign=='W':
        result=-result
    return result


def utc_to_time(utc:float)->datetime.time:
    hh=int(utc//10000)
    mmss=utc-hh*10000
    mm=int(mmss//100)
    ss=mmss%100
    ff=int((ss%1)*1_000_000)
    ss=int(ss)
    return datetime.time(hour=hh,minute=mm,second=ss,microsecond=ff)


def to_date(ddmmyy:int)->datetime.date:
    dd=ddmmyy//10000
    mmyy=ddmmyy%10000
    mm=mmyy//100
    yy=2000+mmyy%100
    return datetime.date(year=yy,month=mm,day=dd)


def pvt_utc(packet):
    """
    Calculate a Python datetime object associated with a UBX-NAV-PVT packet.
    :param packet: A named tuple or something similar with the following named attributes, all presumed to be UTC:
      * year - 4-digit year in AD epoch, IE the current year is 2023, not 23
      * month - month from 1 to 12
      * day - day of month from 1 to 31
      * hour - hour of day from 0 to 23
      * min - minute of hour from 0 to 59
      * sec - second of minute from 0 to 60 (only hits 60 during positive leap second)
      * nano - fraction of second, may be either positive or negative. This field comes from
               UBX-NAV-PVT nano field, but scaled to seconds. That field may be either positive or negative, adjusting
               the time by up to 1 second. For instance, hour=12,min=34,sec=56,nano=0.78 would give 12:34:56.78 UTC.
               In this case, the original value in the nano field of the packet would have been 780,000,000ns. We could
               represent the same time as hour=12,min=34,sec=57,nano=-0.22 (nano field is -220,000,000ns).

               The GPS receiver operates internally on a 1ms cadence. This loop is not carefully synced to the top of the
               UTC second -- instead, the time difference is measured, and reported in this packet. Therefore, the packet
               time is usually within 1ms of the top of the second, but may be slightly before or after. The nano field
               is then between -1ms and +1ms.
    :return: tuple of:
      * Time-zone aware datetime object, accurate to the nearest lower microsecond
      * Fraction of second to add to get full-accuracy timestamp
    For instance, if the time is 12:34:56.012_345_678, the datetime object will be
    12:34:56.012_345 and the fraction part will be 0.000_000_678. The fraction part
    will *always* be non-negative, between 0 and 999ns (0.0 and 0.000_000_999s).
    Due to floating-point precision, the fraction may not be a whole number of nanoseconds.
    However, the extra precision beyond nanosecond is false and should be disregarded.
    """
    utc = pytz.utc.localize(datetime.datetime(year=packet.year, month=packet.month, day=packet.day, hour=packet.hour, minute=packet.min,
                            second=packet.sec))
    microseconds = int((packet.nano * 1e6) // 1)
    nanoseconds = ((packet.nano * 1e6) % 1) * 1e-6
    utc += datetime.timedelta(microseconds=microseconds)
    return utc,nanoseconds


def already_has_epochId(cur,epochId:int,table:str)->bool:
    cur.execute(f'select * from {table} where "epochId"=%s',(epochId,))
    return cur.rowcount>0


def write_nav_pvt(cur, epochId:int, packet, infid:int, ofs:int):
    if already_has_epochId(cur,epochId,"public.nav_pvt"):
        return
    utc,nanoseconds=pvt_utc(packet)
    values = {"epochId":epochId,
              "utc": utc,
              "nano": nanoseconds,
              "validDate": packet.valid['validDate'],
              "validTime": packet.valid['validTime'],
              "fullyResolved": packet.valid['fullyResolved'],
              "validMag": packet.valid['validMag'],
              "tAcc": packet.tAcc,
              "fixType": packet.fixType.value,
              "gnssFixOK": packet.flags["gnssFixOK"],
              "diffSoln": packet.flags["diffSoln"],
              "psmState": packet.flags["psmState"].value,
              "headVehValid": packet.flags["headVehValid"],
              "carrSoln": packet.flags["carrSoln"].value,
              "confirmedAvai": packet.flags2["confirmedAvai"],
              "confirmedDate": packet.flags2["confirmedDate"],
              "confirmedTime": packet.flags2["confirmedTime"],
              "numSV": packet.numSV,
              "longitude_deg": packet.lon,
              "latitude_deg": packet.lat,
              "hSPH_m": packet.height,
              "hMSL_m": packet.hMSL,
              "hAcc_m": packet.hAcc,
              "vAcc_m": packet.vAcc,
              "velN_m_s": packet.velN,
              "velE_m_s": packet.velE,
              "velD_m_s": packet.velD,
              "gSpeed_m_s": packet.gSpeed,
              "headMot_deg": packet.headMot,
              "sAcc_m_s": packet.sAcc,
              "headAcc_deg": packet.headAcc,
              "pDOP":packet.pDOP,
              "invalidLlh":packet.flags3["invalidLlh"],
              "lastCorrectionAge":packet.flags3["lastCorrectionAge"],
              "headVeh_deg":packet.headVeh,
              "magDec_deg":packet.magDec,
              "magAcc_deg":packet.magAcc,
              "file":infid,
              "ofs":ofs,
              }
    write_packet(cur,"public.nav_pvt",values)


def write_nav_timeutc(cur, epochId:int, packet, infid:int, ofs:int):
    if already_has_epochId(cur,epochId,"public.nav_timeutc"):
        return
    utc,nanoseconds=pvt_utc(packet)
    values = {"epochId":epochId,
              "utc": utc,
              "nano": nanoseconds,
              "tAcc": packet.tAcc,
              "validTOW": packet.valid['validTOW'],
              "validWKN": packet.valid['validWKN'],
              "validUTC": packet.valid['validUTC'],
              "utcStandard": packet.valid['utcStandard'],
              "file":infid,
              "ofs":ofs,
              }
    write_packet(cur,"public.nav_timeutc",values)


def write_epoch(cur:'psycopg2.cursor',iTOW:float,week:int,utc:datetime.datetime)->int:
    """
    Write an epoch record. This is in response to an EOE (End of epoch) packet. This packet
    is specifically for delimiting packets -- all data in between two EOE packets is effective
    at the time recorded in the EOE packet, and no packet data associated with this epoch will
    be transmitted after this packet.

    :param cur: cursor to write with
    :param iTOW: scaled iTOW timestamp. iTOW is transmitted as a millisecond
                 count, but the packet reader scales this to seconds.
    :param week: week number, can be pulled from UBX_NAV_TIMEGPS message. That message
                 is marked as I2 (int16_t) which means it can count up to 32,767 weeks
                 from the GPS epoch (2608-01-03). That's far enough in the future to
                 never have to hear the words "week number rollover"
    :param utc:  UTC of this epoch
    :return:
    """
    cur.execute('select id from epoch where "iTOW"=%s and "wn"=%s',(iTOW,week))
    if cur.rowcount>0:
        return cur.fetchone()[0]
    else:
        cur.execute('insert into epoch (wn,"iTOW",utc) values (%s,%s,%s) returning id;',(week,iTOW,utc))
        return cur.fetchone()[0]


def write_esf_meas(cur:'psycopg2.cursor',current_utc,packet,infid,ofs):
    values = {"utc_nearest":current_utc,
              "timeTag": packet.timeTag,
              "timeMarkSent": packet.flags["timeMarkSent"],
              "timeMarkEdge": packet.flags["timeMarkEdge"],
              "calibTtagValid": packet.flags["calibTtagValid"],
              "numMeas":packet.flags["numMeas"],
              "provider_id": packet.dataId,
              "file":infid,
              "ofs":ofs,
              }
    measId=write_packet(cur,"public.esf_meas",values)
    for dtype,value,units in packet.data:
        if dtype.value==0:
            continue
        values={"measId":measId,
                "dataType":dtype.value,
                "dataValue":value}
        write_packet(cur,"public.esf_meas_sensor",values)


def write_rxm_rawx(cur,epochId,packet,infid,ofs):
    if already_has_epochId(cur,epochId,"public.rxm_rawx"):
        return
    values = {"epochId":epochId,
              "rcvTow": packet.rcvTow,
              "week": packet.week,
              "leapS": packet.leapS,
              "rcvUtc":(datetime.datetime(1980,1,6,0,0,0)
                        +datetime.timedelta(days=packet.week*7,
                                            seconds=int(packet.rcvTow)-packet.leapS,
                                            microseconds=int((packet.rcvTow%1)*1e6))),
              "file":infid,
              "ofs":ofs,
              }
    measId=write_packet(cur,"public.rxm_rawx",values)
    p=packet
    for   prMes,  cpMes,  doMes,  gnssId,  svId,  sigId,  freqId,  lockTime,  cno,  prStdev,  cpStdev,  doStdev,  trkStat in zip(
        p.prMes,p.cpMes,p.doMes,p.gnssId,p.svId,p.sigId,p.freqId,p.locktime,p.cno,p.prStdev,p.cpStdev,p.doStdev,p.trkStat):
        values={"measId":measId,
                "prMes":prMes,
                "cpMes":cpMes,
                "doMes":doMes,
                "gnssId":gnssId.value,
                "svId":svId,
                "sigId":sigId.value,
                "freqId":freqId,
                "locktime":lockTime,
                "cno":cno,
                "prStdev":prStdev,
                "cpStdev":cpStdev,
                "doStdev":doStdev,
                "prValid":trkStat["prValid"],
                "cpValid":trkStat["cpValid"],
                "halfCyc":trkStat["halfCyc"],
                "subHalfCyc":trkStat["subHalfCyc"],
                }
        write_packet(cur,"public.rxm_rawx_meas",values)


def write_nav_sat(cur,epochId,packet,infid,ofs):
    if already_has_epochId(cur,epochId,"public.nav_sat"):
        return
    values = {"epochId":epochId,
              "numSvs": packet.numSvs,
              "file":infid,
              "ofs":ofs,
              }
    headId=write_packet(cur,"public.nav_sat",values)
    p=packet
    for   gnssId,  svId,  cno,  elev,  azim,  prRes,  flags in zip(
        p.gnssId,p.svId,p.cno,p.elev,p.azim,p.prRes,p.flags):
        values={"headId":headId,
                "gnssId":gnssId.value,
                "svId":  svId,
                "cno":  cno,
                "elev": elev,
                "azim":  azim,
                "prRes":  prRes,
                "qualityInd":  flags["qualityInd"],
                "svUsed":  flags["svUsed"],
                "health":  flags["health"],
                "diffCorr":  flags["diffCorr"],
                "smoothed":   flags["smoothed"],
                "orbitSource":  flags["orbitSource"],
                "ephAvail":  flags["ephAvail"],
                "almAvail":  flags["almAvail"],
                "aopAvail":  flags["aopAvail"],
                "sbasCorrUsed":  flags["sbasCorrUsed"],
                "rtcmCorrUsed":  flags["rtcmCorrUsed"],
                "slasCorrUsed":  flags["slasCorrUsed"],
                "spartnCorrUsed":  flags["spartnCorrUsed"],
                "prCorrUsed":  flags["prCorrUsed"],
                "crCorrUsed":  flags["crCorrUsed"],
                "doCorrUsed":  flags["doCorrUsed"],
                "clasCorrUsed":  flags["clasCorrUsed"]}
        write_packet(cur,"public.nav_sat_sat",values)


def write_nav_sig(cur,epochId,packet,infid,ofs):
    if already_has_epochId(cur,epochId,"public.nav_sig"):
        return
    values = {"epochId":epochId,
              "numSigs": packet.numSigs,
              "file":infid,
              "ofs":ofs,
              }
    headId=write_packet(cur,"public.nav_sig",values)
    p=packet
    for   gnssId,  svId,  sigId,  freqId,  prRes,  cno,  qualityInd,  corrSource,  ionoModel,     flags in zip(
        p.gnssId,p.svId,p.sigId,p.freqId,p.prRes,p.cno,p.qualityInd,p.corrSource,p.ionoModel,p.sigFlags):
        values={"headId":headId,
                "gnssId":gnssId.value,
                "svId":  svId,
                "sigId":  sigId.value,
                "freqId":  freqId,
                "prRes":  prRes,
                "cno":  cno,
                "qualityInd": qualityInd.value,
                "corrSource": corrSource.value,
                "ionoModel": ionoModel.value,
                "health":  flags["health"],
                "prSmoothed":   flags["prSmoothed"],
                "prUsed":  flags["prUsed"],
                "crUsed":  flags["crUsed"],
                "doUsed":  flags["doUsed"],
                "prCorrUsed":  flags["prCorrUsed"],
                "crCorrUsed":  flags["crCorrUsed"],
                "doCorrUsed":  flags["doCorrUsed"]}
        write_packet(cur,"public.nav_sig_sig",values)


def write_esf_status(cur,epochId,packet,infid,ofs):
    if already_has_epochId(cur,epochId,"public.esf_status"):
        return
    values = {"epochId":epochId,
              "version": packet.version,
              "fusionMode": packet.fusionMode,
              "numSens": packet.numSens,
              "file":infid,
              "ofs":ofs,
              }
    measId=write_packet(cur,"public.esf_status",values)
    p=packet
    for   sensStatus1,  sensStatus2,  freq,  faults in zip(
        p.sensStatus1,p.sensStatus2,p.freq,p.faults):
        values={"esfStatusId":measId,
                "type":sensStatus1["type"].value,
                "used":sensStatus1["used"],
                "ready":sensStatus1["used"],
                "calibStatus":sensStatus2["calibStatus"],
                "timeStatus":sensStatus2["timeStatus"],
                "freq":freq,
                "badMeas":faults["badMeas"],
                "badTTag":faults["badTTag"],
                "missingMeas":faults["missingMeas"],
                "noisyMeas":faults["noisyMeas"],
                }
        write_packet(cur,"public.esf_status_sensor",values)


def write_nav_hpposllh(cur, epochId, packet, infid, ofs):
    if already_has_epochId(cur,epochId,"public.nav_hpposllh"):
        return
    values = {"epochId":epochId,
              "invalidLlh": packet.flags["invalidLlh"],
              "longitude_deg": packet.lon+packet.lonHp,
              "latitude_deg": packet.lat+packet.latHp,
              "hSPH_m":packet.height+packet.heightHp,
              "hMSL_m":packet.hMSL+packet.hMSLHp,
              "hAcc_m":packet.hAcc,
              "vAcc_m":packet.vAcc,
              "file":infid,
              "ofs":ofs,
              }
    write_packet(cur,"public.nav_hpposllh",values)


def write_nav_hpposecef(cur, epochId, packet, infid, ofs):
    if already_has_epochId(cur,epochId,"public.nav_hpposecef"):
        return
    values = {"epochId":epochId,
              "ecefX": packet.ecefX+packet.ecefXHp,
              "ecefY": packet.ecefY+packet.ecefYHp,
              "ecefZ": packet.ecefZ+packet.ecefZHp,
              "invalidEcef": packet.flags["invalidEcef"],
              "pAcc": packet.pAcc,
              "file":infid,
              "ofs":ofs,
              }
    write_packet(cur,"public.nav_hpposecef",values)


def write_nav_posecef(cur, epochId, packet, infid, ofs):
    if already_has_epochId(cur,epochId,"public.nav_posecef"):
        return
    values = {"epochId":epochId,
              "ecefX": packet.ecefX,
              "ecefY": packet.ecefY,
              "ecefZ": packet.ecefZ,
              "pAcc": packet.pAcc,
              "file":infid,
              "ofs":ofs,
              }
    write_packet(cur,"public.nav_posecef",values)


def write_nav_velecef(cur, epochId, packet, infid, ofs):
    if already_has_epochId(cur,epochId,"public.nav_velecef"):
        return
    values = {"epochId":epochId,
              "ecefVX": packet.ecefVX,
              "ecefVY": packet.ecefVY,
              "ecefVZ": packet.ecefVZ,
              "sAcc": packet.sAcc,
              "file":infid,
              "ofs":ofs,
              }
    write_packet(cur,"public.nav_velecef",values)


def write_nav_clock(cur,epochId:int,packet,infid:int,ofs:int):
    if already_has_epochId(cur,epochId,"nav_clock"):
        return
    values = {"epochId":epochId,
              "clkB": packet.clkB,
              "clkD": packet.clkD,
              "tAcc": packet.tAcc,
              "fAcc": packet.fAcc,
              "file":infid,
              "ofs":ofs,
              }
    write_packet(cur,"public.nav_clock",values)


def write_esf_alg(cur,epochId:int,packet,infid:int,ofs:int):
    if already_has_epochId(cur,epochId,"esf_alg"):
        return
    values = {"epochId":epochId,
              "autoMntAlgOn": packet.flags["autoMntAlgOn"],
              "status": packet.flags["status"],
              "tiltAlgError": packet.error["tiltAlgError"],
              "yawAlgError": packet.error["yawAlgError"],
              "angleError": packet.error["angleError"],
              "yaw": packet.yaw,
              "pitch": packet.pitch,
              "roll": packet.roll,
              "file":infid,
              "ofs":ofs,
              }
    write_packet(cur,"public.esf_alg",values)


def write_esf_ins(cur,epochId:int,packet,infid:int,ofs:int):
    if already_has_epochId(cur,epochId,"esf_ins"):
        return
    values = {"epochId":epochId,
              "xAngRateValid": packet.bitfield0["xAngRateValid"],
              "yAngRateValid": packet.bitfield0["yAngRateValid"],
              "zAngRateValid": packet.bitfield0["zAngRateValid"],
              "xAccelValid": packet.bitfield0["xAccelValid"],
              "yAccelValid": packet.bitfield0["yAccelValid"],
              "zAccelValid": packet.bitfield0["zAccelValid"],
              "xAngRate": packet.xAngRate,
              "yAngRate": packet.yAngRate,
              "zAngRate": packet.zAngRate,
              "xAccel": packet.xAccel,
              "yAccel": packet.yAccel,
              "zAccel": packet.zAccel,
              "file":infid,
              "ofs":ofs,
              }
    write_packet(cur,"public.esf_ins",values)


def write_nav_status(cur,epochId:int,packet,infid:int,ofs:int):
    if already_has_epochId(cur,epochId,"nav_status"):
        return
    values = {"epochId":epochId,
              "gpsFix": packet.gpsFix.value,
              "gpsFixOk": packet.flags["gpsFixOk"],
              "diffSoln": packet.flags["diffSoln"],
              "wknSet": packet.flags["wknSet"],
              "towSet": packet.flags["towSet"],
              "diffCorr": packet.fixStat["diffCorr"],
              "carrSolnValid": packet.fixStat["carrSolnValid"],
              "mapMatching": packet.fixStat["mapMatching"],
              "psmState": packet.flags2["psmState"],
              "spoofDetState": packet.flags2["spoofDetState"],
              "carrSoln": packet.flags2["carrSoln"],
              "ttff": packet.ttff,
              "msss": packet.msss,
              "file":infid,
              "ofs":ofs,
              }
    write_packet(cur,"public.nav_status",values)


def write_nav_dop(cur,epochId:int,packet,infid:int,ofs:int):
    if already_has_epochId(cur,epochId,"nav_dop"):
        return
    values = {"epochId":epochId,
              "gDOP": packet.gDOP,
              "pDOP": packet.pDOP,
              "tDOP": packet.tDOP,
              "vDOP": packet.vDOP,
              "hDOP": packet.hDOP,
              "nDOP": packet.nDOP,
              "eDOP": packet.eDOP,
              "file":infid,
              "ofs":ofs,
              }
    write_packet(cur,"public.nav_dop",values)


def write_nav_timels(cur,epochId:int,packet,infid:int,ofs:int):
    if already_has_epochId(cur,epochId,"nav_timels"):
        return
    values = {"epochId":epochId,
              "version": packet.version,
              "srcOfCurrLs": packet.srcOfCurrLs,
              "currLs": packet.currLs,
              "srcOfLsChange": packet.srcOfLsChange,
              "lsChange": packet.lsChange,
              "timeToLsEvent": packet.timeToLsEvent,
              "dateOfLsGpsWn": packet.dateOfLsGpsWn,
              "dateOfLsGpsDn": packet.dateOfLsGpsDn,
              "validCurrLs": packet.valid["validCurrLs"],
              "validTimeToLsEvent": packet.valid["validTimeToLsEvent"],
              "file":infid,
              "ofs":ofs,
              }
    write_packet(cur,"public.nav_timels",values)


def write_nav_cov(cur,epochId:int,packet,infid:int,ofs:int):
    if already_has_epochId(cur,epochId,"nav_cov"):
        return
    values = {"epochId":epochId,
              "posCovValid": packet.posCovValid,
              "velCovValid": packet.velCovValid,
              "posCovNN": packet.posCovNN,
              "posCovNE": packet.posCovNE,
              "posCovND": packet.posCovND,
              "posCovEE": packet.posCovEE,
              "posCovED": packet.posCovED,
              "posCovDD": packet.posCovDD,
              "velCovNN": packet.velCovNN,
              "velCovNE": packet.velCovNE,
              "velCovND": packet.velCovND,
              "velCovEE": packet.velCovEE,
              "velCovED": packet.velCovED,
              "velCovDD": packet.velCovDD,
              "file":infid,
              "ofs":ofs,
              }
    write_packet(cur,"public.nav_cov",values)


def write_tim_tp(cur,packet,infid,ofs):
    tow=packet.towMS+packet.towSubMS
    cur.execute(f'select * from tim_tp where "tow"=%s and "week"=%s',(tow,packet.week))
    if cur.rowcount>0:
        return
    towRemainder=((tow*1e6)%1)/1e6
    values = {"tow":tow,
              "qErr": packet.qErr,
              "week": packet.week,
              "timeBase": packet.flags["timeBase"],
              "utc":packet.flags["utc"],
              "raim":packet.flags["raim"],
              "qErrInvalid":packet.flags["qErrInvalid"],
              "timeRefGnss":packet.refInfo["timeRefGnss"],
              "utcStandard":packet.refInfo["utcStandard"],
              "timestamp":(datetime.datetime(1980,1,6,0,0,0)
                          +datetime.timedelta(days=packet.week*7,
                                              seconds=int(tow),
                                              microseconds=int((tow%1)*1e6))),
              "timestamp_remainder":towRemainder,
              "file":infid,
              "ofs":ofs,
              }
    write_packet(cur,"public.tim_tp",values)


def write_l1ca_subframe1(cur,utc_nearest,packet,subframe,infid:int,ofs:int)->int:
    cur.execute('select id from public.l1ca_subframe1 where "svid"=%s and "tow_count"=%s',(packet.svId,subframe.tow_count))
    if cur.rowcount>0:
        return
    values = {"utc_nearest":utc_nearest,
              "svid": packet.svId,
              "tow_count": subframe.tow_count,
              "wn": subframe.wn,
              "alert": subframe.alert,
              "antispoof": subframe.antispoof,
              "msg_on_l2": subframe.msg_on_l2,
              "ura":       subframe.ura,
              "sv_health": subframe.sv_health,
              "iodc":      subframe.iodc,
              "t_gd":      subframe.t_gd,
              "t_oc":      subframe.t_oc,
              "a_f2":      subframe.a_f2,
              "a_f1":      subframe.a_f1,
              "a_f0":      subframe.a_f0,
              "file": infid,
              "ofs": ofs,
             }
    write_packet(cur, "public.l1ca_subframe1", values)


def write_l1ca_subframe2(cur,utc_nearest,packet,subframe,infid:int,ofs:int)->int:
    cur.execute('select id from public.l1ca_subframe2 where "svid"=%s and "tow_count"=%s',(packet.svId,subframe.tow_count))
    if cur.rowcount>0:
        return
    values = {"utc_nearest":utc_nearest,
              "svid": packet.svId,
              "tow_count": subframe.tow_count,
              "alert":     subframe.alert,
              "antispoof": subframe.antispoof,
              "iode":      subframe.iode,
              "c_rs":      subframe.c_rs,
              "delta_n":   subframe.delta_n,
              "M_0":       subframe.M_0,
              "c_uc":      subframe.c_uc,
              "e":         subframe.e,
              "c_us":      subframe.c_us,
              "A":         subframe.A,
              "t_oe":      subframe.t_oe,
              "fit":       subframe.fit,
              "aodo":      subframe.aodo,
              "file": infid,
              "ofs": ofs,
             }
    write_packet(cur, "public.l1ca_subframe2", values)


def write_l1ca_subframe3(cur,utc_nearest,packet,subframe,infid:int,ofs:int)->int:
    cur.execute('select id from public.l1ca_subframe3 where "svid"=%s and "tow_count"=%s',(packet.svId,subframe.tow_count))
    if cur.rowcount>0:
        return
    values = {"utc_nearest":utc_nearest,
              "svid": packet.svId,
              "tow_count": subframe.tow_count,
              "alert":     subframe.alert,
              "antispoof": subframe.antispoof,
              "iode":      subframe.iode,
              "c_ic":      subframe.c_ic,
              "Omega_0":   subframe.Omega_0,
              "c_is":      subframe.c_is,
              "i_0":       subframe.i_0,
              "c_rc":      subframe.c_rc,
              "omega":     subframe.omega,
              "Omegad":    subframe.Omegad,
              "idot":      subframe.idot,
              "file": infid,
              "ofs": ofs,
             }
    write_packet(cur, "public.l1ca_subframe3", values)


def write_l1ca_alm(cur,utc_nearest,packet,subframe,infid:int,ofs:int)->int:
    cur.execute('select id from public.l1ca_alm where "svid"=%s and "tow_count"=%s and "sv_id"=%s',(packet.svId,subframe.tow_count,subframe.sv_id))
    if cur.rowcount>0:
        return
    values = {"utc_nearest":utc_nearest,
              "svid": packet.svId,
              "tow_count": subframe.tow_count,
              "alert":     subframe.alert,
              "antispoof": subframe.antispoof,
              "data_id":   subframe.data_id,
              "sv_id":     subframe.sv_id,
              "e":         subframe.e,
              "t_oa":      subframe.t_oa,
              "delta_i":   subframe.delta_i,
              "Omegad":    subframe.Omegad,
              "sv_health": subframe.sv_health,
              "A":         subframe.A,
              "Omega_0":   subframe.Omega_0,
              "omega":     subframe.omega,
              "M_0":       subframe.M_0,
              "a_f0":      subframe.a_f0,
              "a_f1":      subframe.a_f1,
              "file": infid,
              "ofs": ofs,
             }
    write_packet(cur, "public.l1ca_alm", values)


def write_l1ca_msg(cur,utc_nearest,packet,subframe,infid:int,ofs:int)->int:
    cur.execute('select id from public.l1ca_msg where "svid"=%s and "tow_count"=%s and "sv_id"=%s',(packet.svId,subframe.tow_count,subframe.sv_id))
    if cur.rowcount>0:
        return
    values = {"utc_nearest":utc_nearest,
              "svid": packet.svId,
              "tow_count": subframe.tow_count,
              "alert":     subframe.alert,
              "antispoof": subframe.antispoof,
              "data_id":   subframe.data_id,
              "sv_id":     subframe.sv_id,
              "msg":       subframe.msg,
              "file": infid,
              "ofs": ofs,
             }
    write_packet(cur, "public.l1ca_msg", values)


def import_ublox(infn:str,infid:int,conn:psycopg2.connect)->None:
    seen_packets=set()
    with conn, conn.cursor() as rmc_cur:
        nav_timegps_packet=None
        nav_timeutc_packet=None
        nav_timels_packet=None
        nav_pvt_packet=None
        current_utc=None
        nav_hpposllh_packet=None
        nav_hpposecef_packet=None
        nav_posecef_packet=None
        nav_velecef_packet=None
        nav_clock_packet=None
        nav_dop_packet=None
        nav_cov_packet=None
        nav_sat_packet=None
        nav_sig_packet=None
        esf_alg_packet=None
        esf_ins_packet=None
        esf_status_packet=None
        rxm_rawx_packet=None
        nav_status_packet=None
        for i_packet,(ofs,packet_type,packet) in enumerate(parse_gps_file(infn)):
            #print(packet_type,packet)
            if packet_type==PacketType.UBLOX:
                if packet.name=="UBX-NAV-TIMEGPS":
                    nav_timegps_packet=packet
                elif packet.name == "UBX-NAV-TIMEUTC":
                    nav_timeutc_packet = packet
                elif packet.name == "UBX-NAV-TIMELS":
                    nav_timels_packet = packet
                elif packet.name == "UBX-NAV-COV":
                    nav_cov_packet = packet
                elif packet.name == "UBX-ESF-ALG":
                    esf_alg_packet = packet
                elif packet.name == "UBX-ESF-INS":
                    esf_ins_packet = packet
                elif packet.name=="UBX-NAV-PVT":
                    nav_pvt_packet=packet
                    current_utc,current_nano=pvt_utc(nav_pvt_packet)
                elif packet.name=="UBX-ESF-MEAS":
                    if current_utc is not None:
                        write_esf_meas(rmc_cur,current_utc,packet,infid,ofs)
                elif packet.name=="UBX-TIM-TP":
                    write_tim_tp(rmc_cur,packet,infid,ofs)
                elif packet.name=="UBX-ESF-STATUS":
                    esf_status_packet=packet
                elif packet.name == "UBX-NAV-STATUS":
                    nav_status_packet = packet
                elif packet.name == "UBX-NAV-HPPOSLLH":
                    nav_hpposllh_packet = packet
                elif packet.name == "UBX-NAV-HPPOSECEF":
                    nav_hpposecef_packet = packet
                elif packet.name == "UBX-NAV-POSECEF":
                    nav_posecef_packet = packet
                elif packet.name == "UBX-NAV-VELECEF":
                    nav_velecef_packet = packet
                elif packet.name == "UBX-NAV-CLOCK":
                    nav_clock_packet = packet
                elif packet.name == "UBX-NAV-DOP":
                    nav_dop_packet = packet
                elif packet.name == "UBX-NAV-SAT":
                    nav_sat_packet = packet
                elif packet.name == "UBX-NAV-SIG":
                    nav_sig_packet = packet
                elif packet.name=="UBX-RXM-RAWX":
                    rxm_rawx_packet=packet
                elif packet.name=="UBX-RXM-SFRBX":
                    if packet.sigId==GPS_SigID.L1CA:
                        subframe,values,units,fmts=parse_l1ca_subframe(packet)
                        if subframe==1:
                            if current_utc is not None:
                                write_l1ca_subframe1(rmc_cur,current_utc,packet,values,infid,ofs)
                        elif subframe == 2:
                            if current_utc is not None:
                                write_l1ca_subframe2(rmc_cur, current_utc, packet, values, infid, ofs)
                        elif subframe == 3:
                            if current_utc is not None:
                                write_l1ca_subframe3(rmc_cur, current_utc, packet, values, infid, ofs)
                        elif subframe=="alm":
                            if current_utc is not None:
                                write_l1ca_alm(rmc_cur,current_utc,packet,values,infid,ofs)
                        elif subframe=="msg":
                            if current_utc is not None:
                                write_l1ca_msg(rmc_cur,current_utc,packet,values,infid,ofs)
                        else:
                            print(subframe,values,units,fmts)

                elif packet.name=="UBX-NAV-EOE":
                    try:
                        with conn:
                            assert nav_pvt_packet is not None,"No PVT for this epoch"
                            assert nav_timegps_packet is not None,"No TIMEGPS for this epoch"
                            assert nav_pvt_packet.iTOW==packet.iTOW,"PVT packet out of phase"
                            assert nav_timegps_packet.iTOW==packet.iTOW,"TIMEGPS packet out of phase"
                            epochId=write_epoch(rmc_cur,packet.iTOW,nav_timegps_packet.week,current_utc)
                            write_nav_pvt(rmc_cur, epochId, nav_pvt_packet, infid, ofs)
                            if nav_hpposllh_packet is not None and nav_hpposllh_packet.iTOW==packet.iTOW:
                                write_nav_hpposllh(rmc_cur, epochId, nav_hpposllh_packet, infid, ofs)
                                nav_hpposllh_packet = None
                            if nav_hpposecef_packet is not None and nav_hpposecef_packet.iTOW==packet.iTOW:
                                write_nav_hpposecef(rmc_cur, epochId, nav_hpposecef_packet, infid, ofs)
                                nav_hpposecef_packet = None
                            if nav_posecef_packet is not None and nav_posecef_packet.iTOW==packet.iTOW:
                                write_nav_posecef(rmc_cur, epochId, nav_posecef_packet, infid, ofs)
                                nav_posecef_packet = None
                            if nav_velecef_packet is not None and nav_velecef_packet.iTOW==packet.iTOW:
                                write_nav_velecef(rmc_cur, epochId, nav_velecef_packet, infid, ofs)
                                nav_velecef_packet = None
                            if nav_status_packet is not None and nav_status_packet.iTOW==packet.iTOW:
                                write_nav_status(rmc_cur, epochId, nav_status_packet, infid, ofs)
                                nav_status_packet = None
                            if nav_sat_packet is not None and nav_sat_packet.iTOW==packet.iTOW:
                                write_nav_sat(rmc_cur, epochId, nav_sat_packet, infid, ofs)
                                nav_sat_packet = None
                            if nav_sig_packet is not None and nav_sig_packet.iTOW==packet.iTOW:
                                write_nav_sig(rmc_cur, epochId, nav_sig_packet, infid, ofs)
                                nav_sig_packet = None
                            if rxm_rawx_packet is not None:
                                write_rxm_rawx(rmc_cur, epochId, rxm_rawx_packet, infid, ofs)
                                rxm_rawx_packet = None
                            if esf_alg_packet is not None and esf_alg_packet.iTOW==packet.iTOW:
                                write_esf_alg(rmc_cur,epochId,esf_alg_packet,infid,ofs)
                                esf_alg_packet = None
                            if esf_ins_packet is not None and esf_ins_packet.iTOW==packet.iTOW:
                                write_esf_ins(rmc_cur,epochId,esf_ins_packet,infid,ofs)
                                esf_ins_packet = None
                            if nav_clock_packet is not None and nav_clock_packet.iTOW==packet.iTOW:
                                write_nav_clock(rmc_cur,epochId,nav_clock_packet,infid,ofs)
                                nav_clock_packet = None
                            if nav_dop_packet is not None and nav_dop_packet.iTOW==packet.iTOW:
                                write_nav_dop(rmc_cur,epochId,nav_dop_packet,infid,ofs)
                                nav_dop_packet = None
                            if nav_cov_packet is not None and nav_cov_packet.iTOW==packet.iTOW:
                                write_nav_cov(rmc_cur,epochId,nav_cov_packet,infid,ofs)
                                nav_cov_packet = None
                            if nav_timels_packet is not None and nav_timels_packet.iTOW==packet.iTOW:
                                write_nav_timels(rmc_cur,epochId,nav_timels_packet,infid,ofs)
                                nav_timels_packet = None
                            if nav_timeutc_packet is not None and nav_timeutc_packet.iTOW==packet.iTOW:
                                write_nav_timeutc(rmc_cur,epochId,nav_timeutc_packet,infid,ofs)
                                nav_timeutc_packet = None
                            if esf_status_packet is not None:
                                write_esf_status(rmc_cur,epochId,esf_status_packet,infid,ofs)
                                esf_status_packet = None
                    except:
                        import traceback
                        traceback.print_exc()
                        print(f"File {infn} ofs {ofs} i_packet {i_packet}")
                    nav_pvt_packet=None
                    nav_timegps_packet=None
                    conn.commit()
                elif not packet.name in seen_packets:
                    print(packet)
                    seen_packets.add(packet.name)



def import_nmea(infn:str,infid:int,conn:psycopg2.connect)->None:
    with open(infn,"rt") as inf:
        ofs=inf.tell()
        rmc_utc=None
        gga_utc=None

        #Do this instead of `for line in inf` so that inf.tell() works
        line=inf.readline()
        rmc_cur=conn.cursor()
        i_sentence=0
        while line:
            parts=line.split(",")
            talker=parts[0][1:6]
            if talker[2:]=="RMC":
                try:
                    rmc_utc=utc_to_time(float(parts[1]))
                except ValueError:
                    rmc_utc=None
                    ofs = inf.tell()  # Position after reading the current line, IE position of next line
                    line = inf.readline()  # Read the next line
                    continue # Can't log a measurement with no timestamp
                try:
                    rmc_lat=dm_to_deg(float(parts[3]),parts[4])
                except Exception:
                    rmc_lat=None
                try:
                    rmc_lon=dm_to_deg(float(parts[5]),parts[6])
                except Exception:
                    rmc_lon=None
                rmc_valid=(parts[2]=="A")
                try:
                    rmc_spd_kts=float(parts[7])
                except Exception:
                    rmc_spd_kts=None
                rmc_track_deg=float(parts[8])
                rmc_date=to_date(int(parts[9]))
                rmc_magvar=float(parts[10])*(-1 if parts[11]=="W" else 1)
                rmc_cur.execute("insert into rmc (utc_timestamp,talker,valid,latitude_deg,longitude_deg,spd_kts,true_track_deg,magvar_deg,file,ofs) "+
                                "values          (%s           ,%s    ,%s   ,%s          ,%s           ,%s     ,%s            ,%s        ,%s  ,%s )"+
                                "on conflict do nothing returning id;",
                                                 (datetime.datetime.combine(rmc_date,rmc_utc),talker[:2],rmc_valid,rmc_lat ,rmc_lon      ,rmc_spd_kts,rmc_track_deg,rmc_magvar,infid,ofs))
                rmc_id=rmc_cur.fetchall()[0][0]
                conn.commit()
            if talker[2:]=="GGA":
                try:
                    gga_utc=utc_to_time(float(parts[1]))
                except ValueError:
                    gga_utc=None
                    ofs = inf.tell()  # Position after reading the current line, IE position of next line
                    line = inf.readline()  # Read the next line
                    continue # Can't log a measurement with no timestamp
                try:
                    gga_lat=dm_to_deg(float(parts[2]),parts[3])
                except Exception:
                    gga_lat=None
                try:
                    gga_lon=dm_to_deg(float(parts[4]),parts[5])
                except Exception:
                    gga_lon=None
                gga_q=int(parts[6],10)
                gga_nsat=int(parts[7],10)
                gga_hdop=float(parts[8])
                gga_msl=float(parts[9])
                gga_geoid=float(parts[11])
            if gga_utc is not None and rmc_utc is not None and gga_utc==rmc_utc:
                assert gga_lat==rmc_lat,f"Latitude doesn't match between GGA {gga_lat} and RMC {rmc_lat}, file {infn} line {i_sentence}"
                assert gga_lon==rmc_lon,f"Longitude doesn't match between GGA {gga_lon} and RMC {rmc_lon}, file {infn} line {i_sentence}"
                rmc_cur.execute("insert into gga (utc_timestamp,latitude_deg,longitude_deg,quality,n_sats,hdop,alt_msl_m,geoid_m,file,ofs) "+
                                "values          (%s           ,%s          ,%s           ,%s     ,%s    ,%s  ,%s       ,%s     ,%s  ,%s )"+
                                "on conflict do nothing returning id;",
                                                 (datetime.datetime.combine(rmc_date,gga_utc),gga_lat ,gga_lon,gga_q,gga_nsat,gga_hdop,gga_msl,gga_geoid,infid,ofs))
                gga_id=rmc_cur.fetchall()[0][0]
                rmc_cur.execute(
                    "insert into nmea_match (gga_id,rmc_id) " +
                    "values                 (%s    ,%s    )" +
                    "on conflict do nothing;",
                    (gga_id,rmc_id))
                conn.commit()
                gga_utc=None
                rmc_utc=None
            i_sentence+=1
            if i_sentence%100==0:
                print(".",end='')
            ofs=inf.tell() #Position after reading the current line, IE position of next line
            line=inf.readline() #Read the next line


def query_nmea(conn:psycopg2.connect,t0:datetime,t1:datetime):
    result=conn.cursor()
    return result.execute(r"""select 
rmc.utc_timestamp as utc_timestamp,
gga.latitude_deg as latitude_deg,
gga.longitude_deg as longitude_deg,
gga.alt_msl_m as alt_msl_m,
gga.geoid_m as geoid_m,
rmc.spd_kts*1852/3600 as spd_m_s,
rmc.true_track_deg as true_track_deg,
rmc.magvar_deg as magvar_deg,
rmc.valid as valid,
gga.quality as quality,
gga.n_sats as n_sats,
gga.hdop as hdop
from nmea_match
inner join gga on nmea_match.gga_id=gga.id
inner join rmc on nmea_match.rmc_id=rmc.id
where rmc.utc_timestamp>=%s and rmc.utc_timestamp<%s and gga.n_sats>0;
    """,(t0,t1))


def query_nmea_kml(conn:psycopg2.connect, t0:datetime, t1:datetime):
    """
    Query the RMC table and generate a KML file within the given time range [t0,t1)

    :param t0: Naive datetime object with beginning of time range in UTC
    :param t1: Naive datetime object with ending of time range in UTC
    :return: output filename in form fluttershy_yyyymmddhhnnss_yyyymmddhhnnss.kml
    """
    oufn=f"Bahamas22.08/kml/fluttershy_{t0.year:04d}{t0.month:02d}{t0.day:02d}{t0.hour:02d}{t0.minute:02d}{t0.second:02d}_{t1.year:04d}{t1.month:02d}{t1.day:02d}{t1.hour:02d}{t1.minute:02d}{t1.second:02d}.kml"
    with open(oufn,"wt") as ouf:
        print(f"""<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2" xmlns:gx="http://www.google.com/kml/ext/2.2" xmlns:kml="http://www.opengis.net/kml/2.2" xmlns:atom="http://www.w3.org/2005/Atom">
<Document>
	<name>{basename(oufn)}</name>
	<Style id="sn_track-04">
		<IconStyle>
			<Icon>
				<href>http://earth.google.com/images/kml-icons/track-directional/track-0.png</href>
			</Icon>
		</IconStyle>
		<LineStyle>
			<color>99ffac59</color>
			<width>6</width>
		</LineStyle>
	</Style>
	<Style id="sh_track-04">
		<IconStyle>
			<scale>1.2</scale>
			<Icon>
				<href>http://earth.google.com/images/kml-icons/track-directional/track-0.png</href>
			</Icon>
		</IconStyle>
		<LineStyle>
			<color>99ffac59</color>
			<width>8</width>
		</LineStyle>
	</Style>
	<StyleMap id="msn_track-03">
		<Pair>
			<key>normal</key>
			<styleUrl>#sn_track-04</styleUrl>
		</Pair>
		<Pair>
			<key>highlight</key>
			<styleUrl>#sh_track-04</styleUrl>
		</Pair>
	</StyleMap>
	<Placemark>
		<name>Payload</name>
		<styleUrl>#msn_track-03</styleUrl>
		<gx:balloonVisibility>1</gx:balloonVisibility>
		<gx:Track>
			<altitudeMode>clampedToGround</altitudeMode>""",file=ouf)
        rmc_cur = query_nmea(conn, t0, t1)
        rows=rmc_cur.fetchall()
        print(len(rows))
        times=[x[0] for x in rows]
        lats=[x[1] for x in rows]
        lons=[x[2] for x in rows]
        for time,lat,lon in zip(times,lats,lons):
            print(f"			<when>{time.year:04d}-{time.month:02d}-{time.day:02d}T{time.hour:02d}:{time.minute:02d}:{time.second:02d}.{time.microsecond//10000:02d}Z</when><gx:coord>{lon:05f} {lat:05f} 0</gx:coord>",file=ouf)
        print("""		</gx:Track>
	</Placemark>
</Document>
</kml>""",file=ouf)

def query_rmc_plt(conn:psycopg2.connect,t0:datetime,t1:datetime):
    """
    Query the RMC table and plot the results within the given time range [t0,t1)

    :param t0: Naive datetime object with beginning of time range in UTC
    :param t1: Naive datetime object with ending of time range in UTC
    :return: output filename in form fluttershy_yyyymmddhhnnss_yyyymmddhhnnss.kml
    """
    rmc_cur = query_nmea(conn, t0, t1)
    rows=rmc_cur.fetchall()
    print(len(rows))
    times=[x[0] for x in rows]
    lats=[x[1] for x in rows]
    lons=[x[2] for x in rows]
    plt.figure()
    plt.plot(lons,lats)


def get_file_id(conn:psycopg2.connect,infn:str)->int:
    with conn.cursor() as cur:
        cur.execute("select id from files where filename=%s", (basename(infn),))
        if cur.rowcount>0:
            result=cur.fetchone()[0]
        else:
            size = os.path.getsize(infn)
            cur.execute("insert into files (filename,size) values (%s,%s) returning id;", (basename(infn),size))
            result=cur.fetchone()[0]
            conn.commit()
    return result


def finish_file(conn:psycopg2.connect,infn:str,id:int):
    with conn.cursor() as cur:
        cur.execute("update files set process_finish_time=now() where id=%s",(id,))
    conn.commit()


class TableWriter:
    def __init__(self,*,table_name:str,table_comment:str=None,
                      fields:Iterable[tuple[str,str,str,str]],create:bool=True,trunc:bool=False):
        self.table_name=table_name
        self.table_comment=table_comment
        self.fields=fields
        self.create=create
        self.trunc=trunc
        self.created_table=not create
    def create_table(self,conn:'psycopg2.connection'):
        self.created_table=True
        fields=(("id",  "serial",  "primary key",None),)+self.fields
        table_sql=f"CREATE TABLE IF NOT EXISTS {self.table_name} (\n"
        maxnamelen=0
        maxftypelen=0
        maxflagslen=0
        for name,ftype,flags,_ in fields:
            if len(name)>maxnamelen:
                maxnamelen=len(name)
            if len(ftype)>maxftypelen:
                maxftypelen=len(ftype)
            if len(flags)>maxflagslen:
                maxflagslen=len(flags)
        for name, ftype, flags, _ in fields:
            table_sql+=f'    "{name}"{" "*(maxnamelen-len(name))} {ftype:{maxftypelen}s} {flags},\n'
        table_sql=table_sql[:-2]+"\n" #get rid of last comma
        table_sql+=");"
        with conn,conn.cursor() as cur:
            cur.execute(table_sql)
            if self.table_comment is not None:
                table_comment_sql=f"COMMENT ON TABLE {self.table_name} IS %s;"
                cur.execute(table_comment_sql,(self.table_comment,))
            for name,_,_,comment in fields:
                if comment is not None:
                    field_comment_sql=f'COMMENT ON COLUMN {self.table_name}."{name}" IS %s;'
                    cur.execute(field_comment_sql,(comment,))


class PacketImporter(TableWriter):
    def __init__(self,*,table_name:str,table_comment:str=None,
                      fields:Iterable[tuple[str,str,str,str]],create:bool=True,trunc:bool=False):
        super().__init__(table_name=table_name,
                         table_comment=table_comment,
                         fields=fields+(
                           ("file", "integer", "not null", 'Foreign key for file that this row was derived from'),
                           ("ofs",  "integer", "not null", 'Offset in bytes in file of beginning of packet that this row was derived from')
                         ),create=create,trunc=trunc)
    def write(self,conn:'psycopg2.connection',fileid,ofs,pkt):
        if not self.created_table:
            self.create_table(conn)
        fieldNames = ['"' + k + '"' for k, v in pkt.items()]
        placeholds = ["%s" for k, v in pkt.items()]
        fieldValues = tuple([v for k, v in pkt.items()+{'file':fileid,'ofs':ofs}])
        sql = f"insert into {self.table_name} ({','.join(fieldNames)}) values ({','.join(placeholds)}) on conflict do nothing returning id;"
        with conn,conn.cursor() as cur:
            cur.execute(sql,fieldValues)
            return cur.fetchone()[0]


class UBX_ESF_MEASImporter(PacketImporter):
    def __init__(self,*,create:bool=True,trunc:bool=False):
        super().__init__(create=create,trunc=trunc,
                         table_name="esf_meas",
                         table_comment='Measurement epoch, comes from the header for a UBX-ESF-MEAS packet and is associated with all measurements in this packet',
            fields=(
            ("utc_nearest",    "timestamp",       "not null",'UTC timestamp of PVT packet immediately before this measurement'),
            ("timeTag",        "double precision","not null",'Time tag of measurement in seconds with an arbitrary epoch'),
            ("timeMarkSent",   "smallint",        "not null",'Time mark signal that was supplied just prior to sending this measurement'),
            ("timeMarkEdge",   "smallint",        "not null",'Trigger on rising (0) or falling (1) edge of time mark signal'),
            ("calibTtagValid", "boolean",         "not null",'Calibration time tag available'),
            ("numMeas",        "smallint",        "not null",'Number of measurements contained in this message (redundant with size of packet)'),
            ("provider_id",    "smallint",        "not null",'Identification number of data provider'),
             ))


def main():
    dbname="Atlantic23_05"
    conn = psycopg2.connect("dbname=Atlantic23_05 user=jeppesen password=Locking1blitz")
    create=True
    trunc=True
    UBX_ESF_MEASImporter().create_table(conn)
    if create:
        # Create tables not associated with any particular packet
        with conn,conn.cursor() as cur:
            # files
            cur.execute("""CREATE TABLE IF NOT EXISTS files (
                   id                  serial                  primary key,
                   filename            varchar(255)            not null,
                   process_start_time  timestamp default now() not null,
                   size                bigint                  not null,
                   process_finish_time timestamp
               );""")
            cur.execute("COMMENT ON COLUMN files.filename           is 'Base filename of file being processed';")
            cur.execute("COMMENT ON COLUMN files.process_start_time is 'Time that this record was entered, "
                                                                       "and therefore time that processing "
                                                                       "of this file started.';")
            cur.execute("COMMENT ON COLUMN files.size               is 'Size of processed file';")
            # epoch
            cur.execute("""CREATE TABLE IF NOT EXISTS epoch (
                   id     serial           primary key,
                   utc    timestamp        not null,
                   wn     integer          not null,
                   "iTOW" double precision not null
            );""")
            cur.execute("""CREATE UNIQUE INDEX IF NOT EXISTS epoch_week_itow
                   on epoch (wn, "iTOW");""")
    if trunc:
        with conn,conn.cursor() as cur:
            cur.execute("truncate table files restart identity;")
            cur.execute("truncate table epoch restart identity;")
    if False:
        trunc_cur=conn.cursor()
        trunc_cur.execute("truncate table epoch restart identity cascade;")
        trunc_cur.execute("truncate table esf_alg restart identity;")
        trunc_cur.execute("truncate table esf_meas restart identity cascade;")
        trunc_cur.execute("truncate table esf_meas_sensor restart identity;")
        trunc_cur.execute("truncate table esf_status restart identity cascade;")
        trunc_cur.execute("truncate table esf_status_sensor restart identity;")
        trunc_cur.execute("truncate table files restart identity;")
        trunc_cur.execute("truncate table l1ca_alm restart identity;")
        trunc_cur.execute("truncate table l1ca_msg restart identity;")
        trunc_cur.execute("truncate table l1ca_subframe1 restart identity;")
        trunc_cur.execute("truncate table l1ca_subframe2 restart identity;")
        trunc_cur.execute("truncate table l1ca_subframe3 restart identity;")
        trunc_cur.execute("truncate table nav_clock restart identity;")
        trunc_cur.execute("truncate table nav_cov restart identity;")
        trunc_cur.execute("truncate table nav_dop restart identity;")
        trunc_cur.execute("truncate table nav_hpposecef restart identity;")
        trunc_cur.execute("truncate table nav_hpposllh restart identity;")
        trunc_cur.execute("truncate table nav_posecef restart identity;")
        trunc_cur.execute("truncate table nav_pvt restart identity;")
        trunc_cur.execute("truncate table nav_sat, nav_sat_sat restart identity;")
        trunc_cur.execute("truncate table nav_sig, nav_sig_sig restart identity;")
        trunc_cur.execute("truncate table nav_status restart identity;")
        trunc_cur.execute("truncate table nav_timels restart identity;")
        trunc_cur.execute("truncate table nav_timeutc restart identity;")
        trunc_cur.execute("truncate table l1ca_subframe1 restart identity;")
        trunc_cur.execute("truncate table l1ca_subframe2 restart identity;")
        trunc_cur.execute("truncate table l1ca_subframe3 restart identity;")
        trunc_cur.execute("truncate table rxm_rawx restart identity cascade;")
        trunc_cur.execute("truncate table rxm_rawx_meas restart identity;")
        trunc_cur.execute("truncate table tim_tp restart identity;")
        conn.commit()
        trunc_cur.close()
    for infn in sorted(glob("/mnt/big/Atlantic23.05/Fluttershy/FluttershyBase/2023/05/07/*.ubx.bz2")):
        print(infn)
        id=get_file_id(conn,infn)
        import_ublox(infn,id,conn)
        finish_file(conn,infn,id)


    if False:
        trunc_cur=conn.cursor()
        trunc_cur.execute("truncate table files restart identity;")
        trunc_cur.execute("truncate table gga restart identity;")
        trunc_cur.execute("truncate table rmc restart identity;")
        trunc_cur.execute("truncate table nmea_match restart identity;")
        conn.commit()
        trunc_cur.close()
        for infn in sorted(glob("Bahamas22.08/Fluttershy/*.nmea")):
            print(infn)
            id=get_file_id(conn,infn)
            import_nmea(infn,id,conn)


    if False:
        breakpoints=[datetime.datetime(2022, 8,20,10, 0, 0),  # 8/20  4:00am MDT
                     #Drive to SLC, fly to MCO, ride to WDW
                     datetime.datetime(2022, 8,21, 4,50, 0),  # 8/20 24:00 EDT
                     #Day in WDW
                     datetime.datetime(2022, 8,22, 4, 0, 0),  # 8/21 24:00 EDT
                     #Drive to PCN
                     datetime.datetime(2022, 8,22,18, 0, 0),  # 8/22  2:00pm EDT
                     #Sail to Nassau
                     datetime.datetime(2022, 8,24,18, 0, 0),  # 8/24  2:00pm EDT
                     #Sail to Castaway Cay
                     datetime.datetime(2022, 8,25,18, 0, 0),  # 8/25  2:00pm EDT
                     #Sail to PCN
                     datetime.datetime(2022, 8,26,18, 0, 0)   # 8/26  2:00pm EDT
                     ]
        for t0,t1 in zip(breakpoints[:-1],breakpoints[1:]):
            query_rmc_plt(conn,t0,t1)
        plt.show()


    conn.close()


if __name__=="__main__":
    main()