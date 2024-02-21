"""
Packets defined by protocol 33.21, as used by F9 HPS 1.21 (ZED-F9R-01B). This part was used on
Shipometer23.04 during Atlantic23.05 .

Many fields in these packets are transmitted in the form of scaled integers. When the scale factor
is a power of 10, we will use the Decimal type for it, and make sure that the field type is a Decimal
also. This preserves perfect precision for storage, since Postgres has a Decimal-like field-type,
but should probably be converted to float for any actual math. Scales of a power of two are likewise
perfectly preserved by IEEE754 floating-point numbers, so those are used. Other scales (typically a
power of 2 multiplied by a power of 10) are also stored as float.

Fields which make sense as enumerations should be declared as subclasses of the Enum class. If the
enum is only used with one packet, declare it inside of that packet, just before the (first) field
that uses it. If not, hoist it above the first packet that uses it.
Table comments will be copied from the class docstring. Field comments will be in the comment= parameter
of the bin_field function. It makes sense to copy both of these verbatim from the source document,
possibly editing things which are obvious from the
"""
from dataclasses import field
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum, nonmember
import pytz

from packet.bin import signed
from packet.ublox import ublox_packet, UBloxPacket, bin_field

packet_names={0x01:("NAV",{0x13:"HPPOSECEF",
                           0x01:"POSECEF",
                           0x14:"HPPOSLLH",
                           0x03:"STATUS",
                           0x04:"DOP",
                           0x07:"PVT",
                           0x11:"VELECEF",
                           0x20:"TIMEGPS",
                           0x21:"TIMEUTC",
                           0x22:"CLOCK",
                           0x26:"TIMELS",
                           0x32:"SBAS",
                           0x34:"ORB",
                           0x35:"SAT",
                           0x36:"COV",
                           0x43:"SIG",
                           0x61:"EOE"}),
              0x02:("RXM",{0x13:"SFRBX", # From protocol version F9 HPG 1.32 (version 27.31)
                           0x15:"RAWX"}),
              0x05:("ACK",{0x01:"ACK",
                           0x00:"NAK"}),
              0x0a:("MON",{0x04:"VER",
                           0x31:"SPAN",
                           0x36:"COMMS",
                           0x38:"RF",
                           }),
              0x0d:("TIM",{0x01:"TP"}),
              0x10:("ESF",{0x02:"MEAS",
                           0x10:"STATUS",
                           0x14:"ALG",
                           0x15:"INS"})
              }
class SENSORTYPE(Enum):
    NONE = 0
    RESERVED1 = 1
    RESERVED2 = 2
    RESERVED3 = 3
    RESERVED4 = 4
    Z_GYRO = 5
    RESERVED6 = 6
    RESERVED7 = 7
    REAR_LEFT_WHEEL_TICK = 8
    REAR_RIGHT_WHEEL_TICK = 9
    SINGLE_TICK = 10
    SPEED = 11
    GYRO_TEMP = 12
    Y_GYRO = 13
    X_GYRO = 14
    RESERVED15 = 15
    X_ACC = 16
    Y_ACC = 17
    Z_ACC = 18
sensorUnitsScale={
    SENSORTYPE.NONE:(None,None),
    SENSORTYPE.RESERVED1:(None,None),
    SENSORTYPE.RESERVED2:(None,None),
    SENSORTYPE.RESERVED3:(None,None),
    SENSORTYPE.RESERVED4:(None,None),
    SENSORTYPE.Z_GYRO:("deg/s",2.0**-12),
    SENSORTYPE.RESERVED6:(None,None),
    SENSORTYPE.RESERVED7:(None,None),
    SENSORTYPE.REAR_LEFT_WHEEL_TICK:('ticks',1),
    SENSORTYPE.REAR_RIGHT_WHEEL_TICK:('ticks',1),
    SENSORTYPE.SINGLE_TICK:('ticks',1),
    SENSORTYPE.SPEED:('m/s',1e-3),
    SENSORTYPE.GYRO_TEMP:("degC",1e-2),
    SENSORTYPE.Y_GYRO:("deg/s",2.0**-12),
    SENSORTYPE.X_GYRO:("deg/s",2.0**-12),
    SENSORTYPE.RESERVED15:(None,None),
    SENSORTYPE.X_ACC:("m/s**2",2.0**-10),
    SENSORTYPE.Y_ACC:("m/s**2",2.0**-10),
    SENSORTYPE.Z_ACC:("m/s**2",2.0**-10)
}


class GNSSID(Enum):
    GPS=0
    SBAS=1
    Galileo=2
    BeiDou=3
    IMES=4
    QZSS=5
    GLONASS=6
    NavIC=7


class SIGID(Enum):
    bases=nonmember({})
    bases[GNSSID.GPS]=0
    GPS_L1CA=bases[GNSSID.GPS]+0
    GPS_L2CL=bases[GNSSID.GPS]+3
    GPS_L2CM=bases[GNSSID.GPS]+4
    GPS_L5I=bases[GNSSID.GPS]+6
    GPS_L5Q=bases[GNSSID.GPS]+7
    bases[GNSSID.SBAS]=bases[GNSSID.GPS]+8
    SBAS_L1CA=bases[GNSSID.SBAS]+0
    bases[GNSSID.Galileo]=bases[GNSSID.SBAS]+1
    Galileo_E1C=bases[GNSSID.Galileo]+0
    Galileo_E1B=bases[GNSSID.Galileo]+1
    Galileo_E5aI=bases[GNSSID.Galileo]+3
    Galileo_E5aQ=bases[GNSSID.Galileo]+4
    Galileo_E5bI=bases[GNSSID.Galileo]+5
    Galileo_E5bQ=bases[GNSSID.Galileo]+6
    bases[GNSSID.BeiDou]=bases[GNSSID.Galileo]+7
    BeiDou_B1ID1=bases[GNSSID.BeiDou]+0
    BeiDou_B1ID2=bases[GNSSID.BeiDou]+1
    BeiDou_B2ID1=bases[GNSSID.BeiDou]+2
    BeiDou_B2ID2=bases[GNSSID.BeiDou]+3
    BeiDou_B1C=bases[GNSSID.BeiDou]+5
    BeiDou_B2a=bases[GNSSID.BeiDou]+7
    bases[GNSSID.QZSS]=bases[GNSSID.BeiDou]+8
    QZSS_L1CA=bases[GNSSID.QZSS]+0
    QZSS_L1S =bases[GNSSID.QZSS]+1
    QZSS_L2CM=bases[GNSSID.QZSS]+4
    QZSS_L2CL=bases[GNSSID.QZSS]+5
    QZSS_L5I=bases[GNSSID.QZSS]+8
    QZSS_L5Q=bases[GNSSID.QZSS]+9
    bases[GNSSID.GLONASS]=bases[GNSSID.QZSS]+10
    GLONASS_L1OF=bases[GNSSID.GLONASS]+0
    GLONASS_L2OF=bases[GNSSID.GLONASS]+2
    bases[GNSSID.NavIC]=bases[GNSSID.GLONASS]+3
    NavIC_L5A=bases[GNSSID.NavIC]+0
    @staticmethod
    def get_sigid(gnssId:GNSSID, sigId:int):
        return SIGID(SIGID.bases[gnssId]+sigId)


class QIND(Enum):
    """Signal quality indicator"""
    NOSIG = 0
    SEARCH = 1
    ACQ = 2
    DET_UNU = 3
    CODE_SYNC = 4
    CODE_CAR_SYNC5 = 5
    CODE_CAR_SYNC6 = 6
    CODE_CAR_SYNC7 = 7


class FIX(Enum):
    NO_FIX = 0
    DEAD_RECKONING_ONLY = 1
    TWO_D_FIX = 2
    THREE_D_FIX = 3
    GNSS_AND_DEAD_RECKONING_COMBINED = 4
    TIME_ONLY = 5


class CARR_SOLN(Enum):
    NO_CARRIER_SOLUTION = 0
    CARRIER_SOLUTION_WITH_FLOATINT_AMBIGUITIES = 1
    CARRIER_SOLUTION_WITH_FIXED_AMBIGUITIES = 2


class UTCSTD(Enum):
    NA = 0
    CRL = 1
    NIST = 2
    USNO = 3
    BIPM = 4
    EU_LABS = 5
    SU = 6
    CHINA_NTSC = 7
    NPL_INDIA = 8
    NOT_UTC = 14
    UNKNOWN = 15


class HEALTH(Enum):
    UNKNOWN = 0
    HEALTHY = 1
    UNHEALTHY = 2


@ublox_packet(0x01,0x01,use_epoch=True)
class UBX_NAV_POSECEF(UBloxPacket):
    """This message combines position, velocity and time solution, including accuracy figures."""
    iTOW         :Decimal  =field(metadata=bin_field("U4", unit="s", scale=Decimal('1e-3'), fmt="%10.3f",
                                                     comment="GPS time of week of the navigation epoch."))
    ecefX        :Decimal  =field(metadata=bin_field("I4", unit="m", scale=Decimal('1e-2'), fmt="%10.2f", comment="ECEF X coordinate"))
    ecefY        :Decimal  =field(metadata=bin_field("I4", unit="m", scale=Decimal('1e-2'), fmt="%10.2f", comment="ECEF Y coordinate"))
    ecefZ        :Decimal  =field(metadata=bin_field("I4", unit="m", scale=Decimal('1e-2'), fmt="%10.2f", comment="ECEF Z coordinate"))
    pAcc         :Decimal  =field(metadata=bin_field("U4", unit="m", scale=Decimal('1e-2'), fmt="%10.2f", comment="Position Accuracy Estimate"))


@ublox_packet(0x0a,0x04)
class UBX_MON_VER(UBloxPacket):
    swVersion   :str      =field(metadata=bin_field("CH30"))
    hwVersion   :str      =field(metadata=bin_field("CH10"))
    extension   :list[str]=field(metadata=bin_field("CH30"))
    def fixup(self):
        self.swVersion=str(self.swVersion,encoding='cp437').strip('\0')
        self.hwVersion=str(self.hwVersion,encoding='cp437').strip('\0')
        self.extension=[str(x,encoding='cp437').strip('\0') for x in self.extension]


@ublox_packet(0x05,0x01)
class UBX_ACK_ACK(UBloxPacket):
    clsID_ack   :int      =field(metadata=bin_field("U1"))
    msgId_ack   :int      =field(metadata=bin_field("U1"))


@ublox_packet(0x05,0x00)
class UBX_ACK_NAK(UBloxPacket):
    clsID_nak   :int      =field(metadata=bin_field("U1"))
    msgId_nak   :int      =field(metadata=bin_field("U1"))


@ublox_packet(0x01,0x11,use_epoch=True)
class UBX_NAV_VELECEF(UBloxPacket):
    """This message combines position, velocity and time solution, including accuracy figures."""
    iTOW         :Decimal  =field(metadata=bin_field("U4", unit="s", scale=Decimal('1e-3'), fmt="%10.3f",
                                                     comment="GPS time of week of the navigation epoch."))
    ecefVX        :Decimal  =field(metadata=bin_field("I4", unit="m", scale=Decimal('1e-2'), fmt="%10.2f", comment="ECEF X velocity"))
    ecefVY        :Decimal  =field(metadata=bin_field("I4", unit="m", scale=Decimal('1e-2'), fmt="%10.2f", comment="ECEF Y velocity"))
    ecefVZ        :Decimal  =field(metadata=bin_field("I4", unit="m", scale=Decimal('1e-2'), fmt="%10.2f", comment="ECEF Z velocity"))
    sAcc          :Decimal  =field(metadata=bin_field("U4", unit="m", scale=Decimal('1e-2'), fmt="%10.2f", comment="Speed Accuracy Estimate"))


@ublox_packet(0x01,0x13,use_epoch=True,required_version=0x00)
class UBX_NAV_HPPOSECEF(UBloxPacket):
    """This message combines position, velocity and time solution, including accuracy figures."""
    version:int=field(metadata=bin_field("U1",record=False))
    reserved0a:int=field(metadata=bin_field("U1",record=False))
    reserved0b:int=field(metadata=bin_field("U2",record=False))
    iTOW         :Decimal  =field(metadata=bin_field("U4", unit="s", scale=Decimal('1e-3'), fmt="%10.3f",
                                                     comment="GPS time of week of the navigation epoch."))
    ecefX        :Decimal  =field(metadata=bin_field("I4", unit="m", scale=Decimal('1e-2'), dec_scale=4,dec_precision=13,fmt="%10.2f", comment="ECEF X coordinate"))
    ecefY        :Decimal  =field(metadata=bin_field("I4", unit="m", scale=Decimal('1e-2'), dec_scale=4,dec_precision=13, fmt="%10.2f", comment="ECEF Y coordinate"))
    ecefZ        :Decimal  =field(metadata=bin_field("I4", unit="m", scale=Decimal('1e-2'), dec_scale=4,dec_precision=13, fmt="%10.2f", comment="ECEF Z coordinate"))
    ecefXHp      :Decimal  =field(metadata=bin_field("I1", unit="m", scale=Decimal('1e-4'), fmt="%10.2f", record=False))
    ecefYHp      :Decimal  =field(metadata=bin_field("I1", unit="m", scale=Decimal('1e-4'), fmt="%10.2f", record=False))
    ecefZHp      :Decimal  =field(metadata=bin_field("I1", unit="m", scale=Decimal('1e-4'), fmt="%10.2f", record=False))
    validEcef    :bool     =field(metadata=bin_field("X1",b0=0,scale=lambda x:not bool(x)))
    pAcc         :Decimal  =field(metadata=bin_field("U4", unit="m", scale=Decimal('1e-4'), fmt="%10.2f", comment="Position Accuracy Estimate"))
    def fixup(self):
        super().fixup()
        self.ecefX+=self.ecefXHp
        self.ecefY+=self.ecefYHp
        self.ecefZ+=self.ecefZHp


@ublox_packet(0x01,0x14,use_epoch=True,required_version=0x00)
class UBX_NAV_HPPOSLLH(UBloxPacket):
    """This message combines position, velocity and time solution, including accuracy figures."""
    version:int=field(metadata=bin_field("U1",record=False))
    reserved0a:int=field(metadata=bin_field("U1",record=False))
    reserved0b:int=field(metadata=bin_field("U2",record=False))
    iTOW         :Decimal  =field(metadata=bin_field("U4", unit="s", scale=Decimal('1e-3'), fmt="%10.3f",
                                                     comment="GPS time of week of the navigation epoch."))
    lon          :Decimal  =field(metadata=bin_field("I4", unit="deg", scale=Decimal('1e-7'), dec_scale=9,dec_precision=13, comment="Longitude"))
    lat          :Decimal  =field(metadata=bin_field("I4", unit="deg", scale=Decimal('1e-7'), dec_scale=9,dec_precision=13, comment="Geodetic latitude"))
    height       :Decimal  =field(metadata=bin_field("I4", unit="m", scale=Decimal('1e-3'), dec_scale=4,dec_precision=13, comment="Height above ellipsoid"))
    hMSL         :Decimal  =field(metadata=bin_field("I4", unit="m", scale=Decimal('1e-3'), dec_scale=4,dec_precision=13, fmt="%10.2f"))
    lonHp        :Decimal  =field(metadata=bin_field("I1", unit="deg", scale=Decimal('1e-4'), fmt="%10.2f", record=False))
    latHp        :Decimal  =field(metadata=bin_field("I1", unit="deg", scale=Decimal('1e-4'), fmt="%10.2f", record=False))
    heightHp     :Decimal  =field(metadata=bin_field("I1", unit="m", scale=Decimal('1e-4'), fmt="%10.2f", record=False))
    hMSLHp       :Decimal  =field(metadata=bin_field("I1", unit="m", scale=Decimal('1e-4'), fmt="%10.2f", record=False))
    hAcc         :Decimal  =field(metadata=bin_field("U4", unit="m", scale=Decimal('1e-4'), fmt="%10.2f", comment="Horizontal Accuracy Estimate"))
    vAcc         :Decimal  =field(metadata=bin_field("U4", unit="m", scale=Decimal('1e-4'), fmt="%10.2f", comment="Vertical Accuracy Estimate"))
    def fixup(self):
        super().fixup()
        self.lon+=self.lonHp
        self.lat+=self.latHp
        self.height+=self.heightHp
        self.hMSL+=self.hMSLHp


@ublox_packet(0x01,0x22,use_epoch=True)
class UBX_NAV_CLOCK(UBloxPacket):
    """This message combines position, velocity and time solution, including accuracy figures."""
    iTOW         :Decimal  =field(metadata=bin_field("U4", unit="s", scale=Decimal('1e-3'), fmt="%10.3f",
                                                     comment="GPS time of week of the navigation epoch."))
    clkB         :Decimal  =field(metadata=bin_field("I4", unit="s", scale=Decimal('1e-9'), comment="Clock bias"))
    clkD         :Decimal  =field(metadata=bin_field("I4", unit="s/s", scale=Decimal('1e-9'), comment="Clock drift"))
    tAcc         :Decimal  =field(metadata=bin_field("U4", unit="s", scale=Decimal('1e-9'), comment="Time accuracy estimate"))
    fAcc         :Decimal  =field(metadata=bin_field("U4", unit="s/s", scale=Decimal('1e-12'), comment="Frequency accuracy Estimate"))


@ublox_packet(0x01,0x04,use_epoch=True)
class UBX_NAV_DOP(UBloxPacket):
    """This message combines position, velocity and time solution, including accuracy figures."""
    iTOW         :Decimal  =field(metadata=bin_field("U4", unit="s", scale=Decimal('1e-3'), fmt="%10.3f",
                                                     comment="GPS time of week of the navigation epoch."))
    gDOP         :Decimal  =field(metadata=bin_field("U2", unit="s", scale=Decimal('1e-2'), comment="Geometric DOP"))
    pDOP         :Decimal  =field(metadata=bin_field("U2", unit="s", scale=Decimal('1e-2'), comment="Geometric DOP"))
    tDOP         :Decimal  =field(metadata=bin_field("U2", unit="s", scale=Decimal('1e-2'), comment="Geometric DOP"))
    vDOP         :Decimal  =field(metadata=bin_field("U2", unit="s", scale=Decimal('1e-2'), comment="Geometric DOP"))
    hDOP         :Decimal  =field(metadata=bin_field("U2", unit="s", scale=Decimal('1e-2'), comment="Geometric DOP"))
    nDOP         :Decimal  =field(metadata=bin_field("U2", unit="s", scale=Decimal('1e-2'), comment="Geometric DOP"))
    eDOP         :Decimal  =field(metadata=bin_field("U2", unit="s", scale=Decimal('1e-2'), comment="Geometric DOP"))


@ublox_packet(0x01,0x07,use_epoch=True)
class UBX_NAV_PVT(UBloxPacket):
    """This message combines position, velocity and time solution, including accuracy figures."""
    iTOW         :Decimal  =field(metadata=bin_field("U4", unit="s", scale=Decimal('1e-3'), fmt="%10.3f",
                                                     comment="GPS time of week of the navigation epoch."))
    year         :int      =field(metadata=bin_field("U2", unit="y",comment="Year (UTC)",record=False))
    month        :int      =field(metadata=bin_field("U1", unit="month",comment="Month, range 1..12 (UTC)",record=False))
    day          :int      =field(metadata=bin_field("U1", unit="d",comment="Day of month, range 1..31 (UTC)",record=False))
    hour         :int      =field(metadata=bin_field("U1", unit="h", comment="Hour of day, range 0..23 (UTC)",record=False))
    min          :int      =field(metadata=bin_field("U1", unit="min", comment="Minute of hour, range 0..59 (UTC)",record=False))
    sec          :int      =field(metadata=bin_field("U1", unit="s",comment="Seconds of minute, range 0..60 (UTC). "
                                                                            "Note that during a leap second there may "
                                                                            "be more or less than 60 seconds in a "
                                                                            "minute. See description of leap seconds "
                                                                            "in the integration manual for details.",record=False))
    utc          :datetime =field(metadata=bin_field(None,unique=True,comment="UTC timestamp of this packet to microsecond precision. ",fmt="%20s"))
    validDate    :bool     =field(metadata=bin_field("X1", b0=0, scale=bool,comment="Date part of UTC is valid"))
    validTime    :bool     =field(metadata=bin_field("X1", b0=1, scale=bool,comment="Time part of UTC is valid"))
    fullyResolved:bool     =field(metadata=bin_field("X1", b0=2, scale=bool,comment="UTC time of day is fully resolved "
                                                                                    "(no seconds uncertainty). Cannot be "
                                                                                    "used to check if time is completely "
                                                                                    "solved."))
    validMag     :bool     =field(metadata=bin_field("X1", b0=3, scale=bool,comment="Magnetic declination is valid"))
    tAcc         :Decimal  =field(metadata=bin_field("U4", unit="s", scale=Decimal('1e-9'), fmt="%12.9f",
                                                                            comment="Time accuracy estimate"))
    nano         :Decimal  =field(metadata=bin_field("I4", unit="s", scale=Decimal('1e-9'), fmt="%12.9f",
                                                                            comment="Fraction of second, range 0..1e-6. "
                                                                                    "Add this to the UTC field to get "
                                                                                    "the time to nanosecond precision"))
    fixType      :FIX      =field(metadata=bin_field("X1", scale=FIX))
    gnssFixOK    :bool     =field(metadata=bin_field("X1", scale=bool, b0=0))
    diffSoln     :bool     =field(metadata=bin_field("X1", scale=bool, b0=1,comment="Differential corrections were applied"))
    class PSM(Enum):
        NOT_ACTIVE = 0
        ENABLED = 1
        ACQUISITION = 2
        TRACKING = 3
        POWER_OPTIMIZED_TRACKING = 4
        INACTIVE = 5
    psmState     :PSM      =field(metadata=bin_field("X1", scale=PSM, b1=4, b0=2,comment="Power save mode state"))
    headVehValid :bool     =field(metadata=bin_field("X1", scale=bool, b0=5,comment="Heading of vehicle is valid, only "
                                                                                    "set if the receiver is in sensor "
                                                                                    "fusion mode."))
    carrSoln     :CARR_SOLN=field(metadata=bin_field("X1", scale=CARR_SOLN, b1=7, b0=6))
    confirmedAvai:bool     =field(metadata=bin_field("X1", scale=bool, b0=5,comment="Information about UTC Date and "
                                                                                    "Time of Day validity confirmation "
                                                                                    "is available"))
    confirmedDate:bool     =field(metadata=bin_field("X1", scale=bool, b0=6,comment="UTC Date validity could be confirmed"))
    confirmedTime:bool     =field(metadata=bin_field("X1", scale=bool, b0=7,comment="UTC Time of Day could be confirmed"))
    numSV        :int      =field(metadata=bin_field("U1",comment="Number of satellites used in Nav solution"))
    lon          :Decimal  =field(metadata=bin_field("I4", scale=Decimal('1e-7'), unit="deg", fmt="%12.7f",comment="Longitude"))
    lat          :Decimal  =field(metadata=bin_field("I4", scale=Decimal('1e-7'), unit="deg", fmt="%12.7f",comment="Latitude"))
    height       :Decimal  =field(metadata=bin_field("I4", scale=Decimal('1e-3'), unit="m"  , fmt="%12.3f",comment="Height above ellipsoid"))
    hMSL         :Decimal  =field(metadata=bin_field("I4", scale=Decimal('1e-3'), unit="m"  , fmt="%12.3f",comment="Height above mean sea level"))
    hAcc         :Decimal  =field(metadata=bin_field("U4", scale=Decimal('1e-3'), unit="m"  , fmt="%12.3f",comment="Horizontal accuracy estimate"))
    vAcc         :Decimal  =field(metadata=bin_field("U4", scale=Decimal('1e-3'), unit="m"  , fmt="%12.3f",comment="Vertical accuracy estimate"))
    velN         :Decimal  =field(metadata=bin_field("I4", scale=Decimal('1e-3'), unit="m/s", fmt="%12.3f",comment="NED north velocity"))
    velE         :Decimal  =field(metadata=bin_field("I4", scale=Decimal('1e-3'), unit="m/s", fmt="%12.3f",comment="NED east velocity"))
    velD         :Decimal  =field(metadata=bin_field("I4", scale=Decimal('1e-3'), unit="m/s", fmt="%12.3f",comment="NED down velocity"))
    gSpeed       :Decimal  =field(metadata=bin_field("I4", scale=Decimal('1e-3'), unit="m/s", fmt="%12.3f",comment="Ground speed (2-D)"))
    headMot      :Decimal  =field(metadata=bin_field("I4", scale=Decimal('1e-5'), unit="deg", fmt="%12.5f",comment="Heading of motion (2-D)"))
    sAcc         :Decimal  =field(metadata=bin_field("U4", scale=Decimal('1e-3'), unit="m/s", fmt="%12.3f",comment="Speed accuracy estimate"))
    headAcc      :Decimal  =field(metadata=bin_field("U4", scale=Decimal('1e-5'), unit="deg", fmt="%12.5f",comment="Heading accuracy estimate (both motion and vehicle)"))
    pDOP         :Decimal  =field(metadata=bin_field("U2", scale=Decimal('0.01'),             fmt="%6.2f",comment="Position DOP"))
    invalidLlh   :bool     =field(metadata=bin_field("X2",scale=bool,b0=0,comment="Invalid lon, lat, height, and hMSL"))
    lastCorrectionAge:float=field(metadata=bin_field("X2",b1=4,b0=1,unit="s",
        scale=lambda x:(float('NaN'),1.0,2.0,5.0,10.0,15.0,20.0,30.0,45.0,60.0,90.0,120.0,float('Inf'))[x],
        comment="Age of the most recently received differential correction. This is sent as a range, and "
                "the stored value is the upper bound of that range. NaN means no differential correction "
                "has ever been received, while Inf means more than the highest finite value (120s)."))
    reserved0    :None     =field(metadata=bin_field("U4",record=False))
    headVeh      :Decimal  =field(metadata=bin_field("I4",scale=Decimal('1e-5'),unit="deg",fmt="%12.5f",
                                                     comment="Heading of vehicle (2-D), this is only valid when "
                                                             "headVehValid is set, otherwise the output is set to the "
                                                             "heading of motion"))
    magDec       :Decimal  =field(metadata=bin_field("I2",scale=Decimal('1e-2'),unit="deg",fmt="%8.5f",comment="Magnetic declination"))
    magAcc       :Decimal  =field(metadata=bin_field("I2",scale=Decimal('1e-2'),unit="deg",fmt="%8.5f",comment="Magnetic declination accuracy"))
    def fixup(self):
        super().fixup()
        # Example: nano=-0.123_456_789
        # timestamp will have microsecond 876544 (1,000,000-123456) and
        # nano will be -789e-9, so 789 nanoseconds before timestamp.
        self.utc=pytz.utc.localize(datetime(year=self.year,month=self.month,day=self.day,
                                            hour=self.hour,minute=self.min,second=self.sec))
        delta=timedelta(microseconds=int(self.nano*Decimal('1e6')))
        self.utc+=delta
        self.nano=self.nano%Decimal('1e-6')


@ublox_packet(0x01,0x34,use_epoch=True,required_version=0x01)
class UBX_NAV_ORB(UBloxPacket):
    """This message combines position, velocity and time solution, including accuracy figures."""
    iTOW         :Decimal  =field(metadata=bin_field("U4", unit="s", scale=Decimal('1e-3'), fmt="%10.3f",
                                                     comment="GPS time of week of the navigation epoch."))
    version      :int      =field(metadata=bin_field("U1"))
    numSv        :int      =field(metadata=bin_field("U1"))
    reserved0    :int      =field(metadata=bin_field("U2",record=False))
    gnssId       :list[GNSSID]=field(metadata=bin_field("U1",scale=GNSSID))
    svId         :list[int]=field(metadata=bin_field("U1"))
    health       :list[HEALTH]=field(metadata=bin_field("X1",b1=1,b0=0,scale=HEALTH))
    class VISIBILITY(Enum):
        UNKNOWN=0
        BELOW_HORIZON=1
        ABOVE_HORIZON=2
        ABOVE_ELEVATION_MASK=3
    visibility   :list[VISIBILITY]=field(metadata=bin_field("X1",b1=3,b0=2,scale=VISIBILITY))
    ephUsability :list[float]=field(metadata=bin_field("X1",b1=4,b0=0,scale=lambda x:float('NaN') if x==31 else float('Inf') if x==30 else x*15))
    class EPHSOURCE(Enum):
        NA=0
        GNSS=1
        EXTERNAL=2
        OTHER3=3
        OTHER4=4
        OTHER5=5
        OTHER6=6
        OTHER7=7
    ephSource :list[EPHSOURCE]=field(metadata=bin_field("X1",b1=7,b0=5,scale=EPHSOURCE))
    almUsability :list[float]=field(metadata=bin_field("X1",b1=4,b0=0,scale=lambda x:float('NaN') if x==31 else float('Inf') if x==30 else x))
    almSource :list[EPHSOURCE]=field(metadata=bin_field("X1",b1=7,b0=5,scale=EPHSOURCE))
    anoAopUsability :list[float]=field(metadata=bin_field("X1",b1=4,b0=0,scale=lambda x:float('NaN') if x==31 else float('Inf') if x==30 else x))
    class ORBTYPE(Enum):
        NONE=0
        ASSISTNOW_OFFLINE=1
        ASSISTNOW_AUTONOMOUS=2
        OTHER3=3
        OTHER4=4
        OTHER5=5
        OTHER6=6
        OTHER7=7
    orbtype :list[ORBTYPE]=field(metadata=bin_field("X1",b1=7,b0=5,scale=ORBTYPE))


@ublox_packet(0x01,0x32,use_epoch=True)
class UBX_NAV_SBAS(UBloxPacket):
    """This message combines position, velocity and time solution, including accuracy figures."""
    iTOW         :Decimal  =field(metadata=bin_field("U4", unit="s", scale=Decimal('1e-3'), fmt="%10.3f",
                                                     comment="GPS time of week of the navigation epoch."))
    geo          :int      =field(metadata=bin_field("U1"))
    class SBASMODE(Enum):
        DISABLED=0
        ENABLED_INTEGRITY=1
        ENABLED_TEST=3
    mode          :SBASMODE=field(metadata=bin_field("U1",scale=SBASMODE))
    class SBASSYS(Enum):
        UNKNOWN=255
        WAAS=0
        EGNOS=1
        MSAS=2
        GAGAN=3
        GPS=16
    sys           :SBASSYS=field(metadata=bin_field("U1",scale=SBASSYS))
    Ranging       :bool=field(metadata=bin_field("X1",scale=bool,b0=0,comment="GEO may be used as a ranging source"))
    Corrections   :bool=field(metadata=bin_field("X1",scale=bool,b0=1,comment="GEO is providing correction data"))
    Integrity     :bool=field(metadata=bin_field("X1",scale=bool,b0=2,comment="GEO is providing integrity"))
    Testmode      :bool=field(metadata=bin_field("X1",scale=bool,b0=3,comment="GEO is in test mode"))
    Bad           :bool=field(metadata=bin_field("X1",scale=bool,b0=4,comment="Problem with signal or broadcast data indicated"))
    cnt           :int =field(metadata=bin_field("U1"))
    class SBASMODE(Enum):
        DISABLED=0
        ENABLED_INTEGRITY=1
        ENABLED_TEST=3
    class SBASINTEG(Enum):
        UNKNOWN=0
        INTEG_NOT_AVAIL_OR_UNUSED=1
        ONLY_GPS_WITH_INTEG=2
    integrityUsed :SBASINTEG=field(metadata=bin_field("X1",b1=1,b0=0,scale=SBASINTEG))
    reserved0     :int =field(metadata=bin_field("U2",record=False))
    svid          :list[int] =field(metadata=bin_field("U1"))
    flags         :list[int] =field(metadata=bin_field("U1"))
    udre          :list[int] =field(metadata=bin_field("U1"))
    svSys         :list[SBASSYS] =field(metadata=bin_field("U1",scale=SBASSYS))
    svRanging     :list[bool]=field(metadata=bin_field("X1",scale=bool,b0=0,comment="GEO may be used as a ranging source"))
    svCorrections :list[bool]=field(metadata=bin_field("X1",scale=bool,b0=1,comment="GEO is providing correction data"))
    svIntegrity   :list[bool]=field(metadata=bin_field("X1",scale=bool,b0=2,comment="GEO is providing integrity"))
    svTestmode    :list[bool]=field(metadata=bin_field("X1",scale=bool,b0=3,comment="GEO is in test mode"))
    svBad         :list[bool]=field(metadata=bin_field("X1",scale=bool,b0=4,comment="Problem with signal or broadcast data indicated"))
    reserved1     :list[int] =field(metadata=bin_field("U1",record=False))
    prc           :list[Decimal]=field(metadata=bin_field("I2",scale=Decimal('1e-2'),unit='m',comment="Pseudorange correction"))
    reserved2     :list[int] =field(metadata=bin_field("U2",record=False))
    ic            :list[Decimal]=field(metadata=bin_field("I2",scale=Decimal('1e-2'),unit='m',comment="Ionosphere correction"))


@ublox_packet(0x01,0x35,use_epoch=False,required_version=0x01)
class UBX_NAV_SAT(UBloxPacket):
    "This message displays information about SVs that are either "\
    "known to be visible or currently tracked by the receiver. "\
    "All signal related information corresponds to the subset of signals specified in Signal Identifiers."
    iTOW      :Decimal      =field(metadata=bin_field("U4", scale=Decimal('1e-3'), unit="s", fmt="%10.3f"))
    version   :int          =field(metadata=bin_field("U1",comment="Message version"))
    numSvs    :int          =field(metadata=bin_field("U1",comment="Number of satellites"))
    reserved0 :int          =field(metadata=bin_field("X2",record=False))
    gnssId    :list[GNSSID] =field(metadata=bin_field("U1",scale=GNSSID,fmt="%10s"),default_factory=list)
    svId      :list[int]    =field(metadata=bin_field("U1"),default_factory=list)
    cno       :list[int]    =field(metadata=bin_field("U1", unit="dBHz",comment="Carrier-to-noise density ratio (signal strength)"),default_factory=list)
    elev      :list[int]    =field(metadata=bin_field("I1", unit="deg", fmt="%5.1f",comment="Elevation"),default_factory=list)
    azim      :list[int]    =field(metadata=bin_field("I2", unit="deg", fmt="%5.1f",comment="Pseudorange residual"),default_factory=list)
    prRes     :list[Decimal]=field(metadata=bin_field("I2",scale=Decimal('1e-1'), unit="m", fmt="%5.1f",comment="Pseudorange residual"),default_factory=list)
    qualityInd:list[QIND]   =field(metadata=bin_field("X4",b1=2,b0=0,scale=QIND,comment="Signal quality indicator"),default_factory=list)
    svUsed    :list[bool]   =field(metadata=bin_field("X4",b0=3,scale=bool,comment="Signal in the subset specified "
                                                                                   "in Signal Identifiers is currently "
                                                                                   "being used for navigation"),default_factory=list)
    health    :list[HEALTH] =field(metadata=bin_field("X4", scale=HEALTH,b1=5,b0=4,comment="Signal health flag"),default_factory=list)
    diffCorr  :list[bool]   =field(metadata=bin_field("X4", scale=bool,b0=6,comment="Differential correction available for this SV"),default_factory=list)
    smoothed  :list[bool]   =field(metadata=bin_field("X4", scale=bool,b0=7,comment="Carrier-smoothed pseudorange used"),default_factory=list)
    class ORBSRC(Enum):
        NONE=0
        EPHEMERIS=1
        ALMANAC=2
        ASSISTNOW_OFFLINE=3
        ASSISTNOW_AUTONOMOUS=4
        OTHER5=5
        OTHER6=6
        OTHER7=7
    orbitSource   :list[ORBSRC] =field(metadata=bin_field("X4", scale=ORBSRC,b1=10,b0=8,comment="Orbit source"),default_factory=list)
    ephAvail      :list[bool]   =field(metadata=bin_field("X4", scale=bool,b0=11),default_factory=list)
    almAvail      :list[bool]   =field(metadata=bin_field("X4", scale=bool,b0=12),default_factory=list)
    anoAvail      :list[bool]   =field(metadata=bin_field("X4", scale=bool,b0=13),default_factory=list)
    aopAvail      :list[bool]   =field(metadata=bin_field("X4", scale=bool,b0=14),default_factory=list)
    sbasCorrUsed  :list[bool]   =field(metadata=bin_field("X4", scale=bool,b0=16),default_factory=list)
    rtcmCorrUsed  :list[bool]   =field(metadata=bin_field("X4", scale=bool,b0=17),default_factory=list)
    slasCorrUsed  :list[bool]   =field(metadata=bin_field("X4", scale=bool,b0=18),default_factory=list)
    spartnCorrUsed:list[bool]   =field(metadata=bin_field("X4", scale=bool,b0=19),default_factory=list)
    prCorrUsed    :list[bool]   =field(metadata=bin_field("X4", scale=bool,b0=20),default_factory=list)
    crCorrUsed    :list[bool]   =field(metadata=bin_field("X4", scale=bool,b0=21),default_factory=list)
    doCorrUsed    :list[bool]   =field(metadata=bin_field("X4", scale=bool,b0=22),default_factory=list)
    clasCorrUsed  :list[bool]   =field(metadata=bin_field("X4", scale=bool,b0=23),default_factory=list)


@ublox_packet(0x01,0x36,use_epoch=False, required_version=0x00)
class UBX_NAV_COV(UBloxPacket):
    """This message contains information on the timing of the next pulse at the TIMEPULSE0 output."""
    iTOW        :Decimal  =field(metadata=bin_field("U4", scale=Decimal('1e-3'),unit="s"))
    version     :int      =field(metadata=bin_field("U1",comment="Message version"))
    posCovValid :bool     =field(metadata=bin_field("U1",scale=bool))
    velCovValid :bool     =field(metadata=bin_field("U1",scale=bool))
    reserved0a  :int      =field(metadata=bin_field("X1",record=False))
    reserved0b  :int      =field(metadata=bin_field("X4",record=False))
    reserved0c  :int      =field(metadata=bin_field("X4",record=False))
    posCovNN    :float    =field(metadata=bin_field("R4",unit="m**2"))
    posCovNE    :float    =field(metadata=bin_field("R4",unit="m**2"))
    posCovND    :float    =field(metadata=bin_field("R4",unit="m**2"))
    posCovEE    :float    =field(metadata=bin_field("R4",unit="m**2"))
    posCovED    :float    =field(metadata=bin_field("R4",unit="m**2"))
    posCovDD    :float    =field(metadata=bin_field("R4",unit="m**2"))
    velCovNN    :float    =field(metadata=bin_field("R4",unit="m**2/s**2"))
    velCovNE    :float    =field(metadata=bin_field("R4",unit="m**2/s**2"))
    velCovND    :float    =field(metadata=bin_field("R4",unit="m**2/s**2"))
    velCovEE    :float    =field(metadata=bin_field("R4",unit="m**2/s**2"))
    velCovED    :float    =field(metadata=bin_field("R4",unit="m**2/s**2"))
    velCovDD    :float    =field(metadata=bin_field("R4",unit="m**2/s**2"))


@ublox_packet(0x01,0x43,use_epoch=False,required_version=0x00)
class UBX_NAV_SIG(UBloxPacket):
    "This message displays information about signals currently tracked by the receiver. On the F9 platform the maximum number of signals is 120."
    class CSRC(Enum):
        NONE = 0
        SBAS = 1
        BDS = 2
        RTCM2 = 3
        RTCM3_OSR = 4
        RTCM3_SSR = 5
        QZSS = 6
        SPARTN = 7
        CLAS = 8

    class IONO(Enum):
        NONE = 0
        KLOBUCHAR_GPS = 1
        SBAS = 2
        KLOBUCHAR_BDS = 3
        DUAL_FREQ = 8

    iTOW      :Decimal      =field(metadata=bin_field("U4", scale=Decimal('1e-3'), unit="s", fmt="%10.3f"))
    version   :int          =field(metadata=bin_field("U1",comment="Message version. This parser only handles version 0x00"))
    numSigs   :int          =field(metadata=bin_field("U1",comment="Number of signals"))
    reserved0 :int          =field(metadata=bin_field("X2",record=False))
    gnssId    :list[GNSSID] =field(metadata=bin_field("U1",scale=GNSSID,fmt="%10s"),default_factory=list)
    svId      :list[int]    =field(metadata=bin_field("U1"),default_factory=list)
    sigId     :list[SIGID]  =field(metadata=bin_field("U1",fmt="%15s"),default_factory=list)
    freqId    :list[int]    =field(metadata=bin_field("U1"),default_factory=list)
    prRes     :list[Decimal]=field(metadata=bin_field("I2",scale=Decimal('1e-1'), unit="m", fmt="%5.1f",comment="Pseudorange residual"),default_factory=list)
    cno       :list[int]    =field(metadata=bin_field("U1", unit="dBHz",comment="Carrier-to-noise density ratio (signal strength)"),default_factory=list)
    qualityInd:list[QIND]   =field(metadata=bin_field("U1", scale=QIND,fmt="%20s",comment="Signal quality indicator"),default_factory=list)
    corrSource:list[CSRC]   =field(metadata=bin_field("U1", scale=CSRC,fmt="%20s",comment="Correction source"),default_factory=list)
    ionoModel :list[IONO]   =field(metadata=bin_field("U1", scale=IONO,fmt="%20s",comment="Ionospheric model used"),default_factory=list)
    health    :list[HEALTH] =field(metadata=bin_field("X2", scale=HEALTH,b1=1,b0=0,comment="Signal health flag"),default_factory=list)
    prSmoothed:list[bool]   =field(metadata=bin_field("X2", scale=bool,b0=2,comment="Pseudorange has been smoothed"),default_factory=list)
    prUsed    :list[bool]   =field(metadata=bin_field("X2", scale=bool,b0=3,comment="Pseudorange has been used for this signal"),default_factory=list)
    crUsed    :list[bool]   =field(metadata=bin_field("X2", scale=bool,b0=4,comment="Carrier range has been used for this signal"),default_factory=list)
    doUsed    :list[bool]   =field(metadata=bin_field("X2", scale=bool,b0=5,comment="Range rate (Doppler) has been used for this signal"),default_factory=list)
    prCorrUsed:list[bool]   =field(metadata=bin_field("X2", scale=bool,b0=6,comment="Pseudorange corrections have been used for this signal"),default_factory=list)
    crCorrUsed:list[bool]   =field(metadata=bin_field("X2", scale=bool,b0=7,comment="Carrier range corrections have been used for this signal"),default_factory=list)
    doCorrUsed:list[bool]   =field(metadata=bin_field("X2", scale=bool,b0=8,comment="Range rate (Doppler) corrections have been used for this signal"),default_factory=list)
    reserved1 :list[int]    =field(metadata=bin_field("X4",record=False),default_factory=list)
    def fixup(self):
        super().fixup()
        self.sigId=[SIGID.get_sigid(gnssId,sigId) for gnssId,sigId in zip(self.gnssId,self.sigId)]


@ublox_packet(0x01,0x20,use_epoch=True)
class UBX_NAV_TIMEGPS(UBloxPacket):
    """This message reports the precise GPS time of the most recent navigation solution including validity flags and
an accuracy estimate."""
    iTOW      :Decimal=field(metadata=bin_field("U4", unit="s", scale=Decimal('1e-3'), fmt="%10.3f",comment="GPS time of week of the navigation epoch."))
    fTOW      :Decimal=field(metadata=bin_field("I4", scale=Decimal('1e-9'), unit="s", fmt="%12.9f"))
    week      :int    =field(metadata=bin_field("I2", unit="week"))
    leapS     :int    =field(metadata=bin_field("I1", unit="s"))
    towValid  :bool   =field(metadata=bin_field("X1", b0=0, scale=bool))
    weekValid :bool   =field(metadata=bin_field("X1", b0=1, scale=bool))
    leapSValid:bool   =field(metadata=bin_field("X1", b0=2, scale=bool))
    tAcc      :Decimal=field(metadata=bin_field("U4", scale=Decimal('1e-9'), unit="s", fmt="%11.9f"))


@ublox_packet(0x01,0x26,use_epoch=True,required_version=0x00)
class UBX_NAV_TIMELS(UBloxPacket):
    """This message reports the precise GPS time of the most recent navigation solution including validity flags and
an accuracy estimate."""
    iTOW      :Decimal=field(metadata=bin_field("U4", unit="s", scale=Decimal('1e-3'), fmt="%10.3f",comment="GPS time of week of the navigation epoch."))
    version   :int    =field(metadata=bin_field("U1"))
    reserved0a:int    =field(metadata=bin_field("U1",record=False))
    reserved0b:int    =field(metadata=bin_field("U2",record=False))
    class LSSRC(Enum):
        FIRMWARE=0
        GPS_GLONASS_DIFF=1
        GPS=2
        SBAS=3
        BeiDou=4
        Galileo=5
        Aided=6
        Configured=7
        NavIC=8
        UNKNOWN=255
    srcOfCurrLs:LSSRC=field(metadata=bin_field("U1",scale=LSSRC))
    currLs     :int  =field(metadata=bin_field("I1",unit="s",comment="Current number of leap seconds since start of GPS "
                                                                     "time (Jan 6, 1980). It reflects how much GPS time is "
                                                                     "ahead of UTC time. Galileo number of leap seconds is "
                                                                     "the same as GPS. BeiDou number of leap seconds is 14 "
                                                                     "less than GPS. GLONASS follows UTC time, so no leap "
                                                                     "seconds."))
    class LSCHANGESRC(Enum):
        NOSOURCE=0
        GPS=2
        SBAS=3
        BeiDou=4
        Galileo=5
        GLONASS=6
        NavIC=7
    srcOfLsChange:LSCHANGESRC=field(metadata=bin_field("U1",scale=LSCHANGESRC))
    lsChange     :int        =field(metadata=bin_field("I1",unit="s"))
    timeToLsEvent:int        =field(metadata=bin_field("I4",unit="s"))
    dateOfLsGpsWn:int        =field(metadata=bin_field("U2",unit="week"))
    dateOfLsGpsDn:int        =field(metadata=bin_field("U2",unit="day"))
    reserved1a:int    =field(metadata=bin_field("U1",record=False))
    reserved1b:int    =field(metadata=bin_field("U2",record=False))
    validCurrLs:bool=field(metadata=bin_field("X1",b0=0,scale=bool))
    validTimeToLsEvent:bool=field(metadata=bin_field("X1",b0=1,scale=bool))


@ublox_packet(0x01,0x21,use_epoch=True)
class UBX_NAV_TIMEUTC(UBloxPacket):
    """This message reports the precise GPS time of the most recent navigation solution including validity flags and
an accuracy estimate."""
    iTOW      :Decimal      =field(metadata=bin_field("U4", scale=Decimal('1e-3'), unit="s", fmt="%10.3f"))
    tAcc         :Decimal  =field(metadata=bin_field("U4", unit="s", scale=Decimal('1e-9'), fmt="%12.9f",
                                                                            comment="Time accuracy estimate"))
    nano         :Decimal  =field(metadata=bin_field("I4", unit="s", scale=Decimal('1e-9'), fmt="%12.9f",
                                                                            comment="Fraction of second, range 0..1e-6. "
                                                                                    "Add this to the UTC field to get "
                                                                                    "the time to nanosecond precision"))
    year         :int      =field(metadata=bin_field("U2", unit="y",comment="Year (UTC)",record=False))
    month        :int      =field(metadata=bin_field("U1", unit="month",comment="Month, range 1..12 (UTC)",record=False))
    day          :int      =field(metadata=bin_field("U1", unit="d",comment="Day of month, range 1..31 (UTC)",record=False))
    hour         :int      =field(metadata=bin_field("U1", unit="h", comment="Hour of day, range 0..23 (UTC)",record=False))
    min          :int      =field(metadata=bin_field("U1", unit="min", comment="Minute of hour, range 0..59 (UTC)",record=False))
    sec          :int      =field(metadata=bin_field("U1", unit="s",comment="Seconds of minute, range 0..60 (UTC). "
                                                                            "Note that during a leap second there may "
                                                                            "be more or less than 60 seconds in a "
                                                                            "minute. See description of leap seconds "
                                                                            "in the integration manual for details.",record=False))
    utc          :datetime =field(metadata=bin_field(None,comment="UTC timestamp of this packet to microsecond precision. ",fmt="%20s"))
    validTOW     :bool     =field(metadata=bin_field("X1",b0=0,scale=bool))
    validWKN     :bool     =field(metadata=bin_field("X1",b0=1,scale=bool))
    validUTC     :bool     =field(metadata=bin_field("X1",b0=2,scale=bool))
    utcStandard  :UTCSTD   =field(metadata=bin_field("X1",b1=7,b0=4,scale=UTCSTD))
    def fixup(self):
        super().fixup()
        # Example: nano=-0.123_456_789
        # timestamp will have microsecond 876544 (1,000,000-123456) and
        # nano will be -789e-9, so 789 nanoseconds before timestamp.
        self.utc=pytz.utc.localize(datetime(year=self.year,month=self.month,day=self.day,
                                            hour=self.hour,minute=self.min,second=self.sec))
        delta=timedelta(microseconds=int(self.nano*Decimal('1e6')))
        self.utc+=delta
        self.nano=self.nano%Decimal('1e-6')


@ublox_packet(0x01,0x03,use_epoch=True)
class UBX_NAV_STATUS(UBloxPacket):
    iTOW      :Decimal      =field(metadata=bin_field("U4", scale=Decimal('1e-3'), unit="s", fmt="%10.3f"))
    gpsFix    :FIX          =field(metadata=bin_field("U1",scale=FIX))
    gpsFixOk  :bool         =field(metadata=bin_field("X1", b0=0,scale=bool))
    diffSoln  :bool         =field(metadata=bin_field("X1", b0=1,scale=bool))
    wknSet    :bool         =field(metadata=bin_field("X1", b0=2,scale=bool))
    towSet    :bool         =field(metadata=bin_field("X1", b0=3,scale=bool))
    diffCorr  :bool         =field(metadata=bin_field("X1", b0=0,scale=bool))
    carrSolnValid:bool      =field(metadata=bin_field("X1", b0=1,scale=bool))
    class MAPMATCHING(Enum):
        NONE=0
        VALID_UNUSED=1
        VALID_USED=2
        VALID_USED_DR_ENABLED = 3
    mapMatching:bool         =field(metadata=bin_field("X1",b1=7,b0=6,scale=bool))
    class PSM(Enum):
        ACQUISITION_OR_DISABLED=0
        TRACKING=1
        POWER_OPTIMIZED_TRACKING=2
        INACTIVE=3
    psmState   :PSM          =field(metadata=bin_field("X1",b1=1,b0=0,scale=PSM))
    class SPOOFDET(Enum):
        UNKNOWN_OR_DEACTIVATED=0
        NO_SPOOFING_INDICATED=1
        SPOOFING_INDICATED=2
        MULTIPLE_SPOOFING_INDICATIONS=3
    spoofDetState:SPOOFDET   =field(metadata=bin_field("X1",b1=4,b0=3,scale=SPOOFDET))
    carrSoln     :CARR_SOLN  =field(metadata=bin_field("X1", scale=CARR_SOLN, b1=7, b0=6))
    ttff         :Decimal    =field(metadata=bin_field("U4",scale=Decimal('1e-3')))
    msss         :Decimal    =field(metadata=bin_field("U4",scale=Decimal('1e-3')))


@ublox_packet(0x01,0x61,use_epoch=True)
class UBX_NAV_EOE(UBloxPacket):
    iTOW        :Decimal  =field(metadata=bin_field("U4", scale=Decimal('1e-3'),unit="s"))


@ublox_packet(0x10,0x15,use_epoch=False,required_version=0x01)
class UBX_ESF_INS(UBloxPacket):
    """This message combines position, velocity and time solution, including accuracy figures."""
    version      :int          =field(metadata=bin_field("X4",b1=7,b0=0,comment="Message version, only 0x01 is handled here"))
    xAngRateValid:bool         =field(metadata=bin_field("X4",scale=bool,b0= 8,comment="Compensated x-axis angular rate data flag is valid"))
    yAngRateValid:bool         =field(metadata=bin_field("X4",scale=bool,b0= 9,comment="Compensated y-axis angular rate data flag is valid"))
    zAngRateValid:bool         =field(metadata=bin_field("X4",scale=bool,b0=10,comment="Compensated z-axis angular rate data flag is valid"))
    xAccelValid  :bool         =field(metadata=bin_field("X4",scale=bool,b0=11,comment="Compensated x-axis acceleration data flag is valid"))
    yAccelValid  :bool         =field(metadata=bin_field("X4",scale=bool,b0=12,comment="Compensated y-axis acceleration data flag is valid"))
    zAccelValid  :bool         =field(metadata=bin_field("X4",scale=bool,b0=13,comment="Compensated z-axis acceleration data flag is valid"))
    reserved0    :bool         =field(metadata=bin_field("U4",record=False))
    iTOW         :Decimal      =field(metadata=bin_field("U4",scale=Decimal('1e-3'), unit="s", fmt="%10.3f"))
    xAngRate     :Decimal      =field(metadata=bin_field("I4",scale=Decimal('1e-3'), unit="deg/s", fmt="%5.1f"))
    yAngRate     :Decimal      =field(metadata=bin_field("I4",scale=Decimal('1e-3'), unit="deg/s", fmt="%5.1f"))
    zAngRate     :Decimal      =field(metadata=bin_field("I4",scale=Decimal('1e-3'), unit="deg/s", fmt="%5.1f"))
    xAccel       :Decimal      =field(metadata=bin_field("I4",scale=Decimal('1e-2'), unit="m/s**2", fmt="%5.1f"))
    yAccel       :Decimal      =field(metadata=bin_field("I4",scale=Decimal('1e-2'), unit="m/s**2", fmt="%5.1f"))
    zAccel       :Decimal      =field(metadata=bin_field("I4",scale=Decimal('1e-2'), unit="m/s**2", fmt="%5.1f"))


@ublox_packet(0x10,0x14,use_epoch=True, required_version=0x01)
class UBX_ESF_ALG(UBloxPacket):
    "This message outputs the IMU alignment angles which define the "\
    "rotation from the installation-frame to the IMU-frame. In addition, "\
    "it indicates the automatic IMU-mount alignment status."
    iTOW         :Decimal      =field(metadata=bin_field("U4",scale=Decimal('1e-3'), unit="s", fmt="%10.3f"))
    version      :int          =field(metadata=bin_field("U1",comment="Message version, only 0x01 is handled here"))
    autoMntAlgOn :bool         =field(metadata=bin_field("U1",b0=0,scale=bool,comment="Automatic IMU-mount alignment on"))
    class STATUS(Enum):
        USER_DEFINED_FIXED_ANGLES=0
        IMU_MOUNT_ROLL_PITCH_ALIGNMENT_ONGOING=1
        IMU_MOUNT_ROLL_PITCH_YAW_ALIGNMENT_ONGOING=2
        COARSE_IMU_MOUNT_ALIGNMENT_USED=3
        FINE_IMU_MOUNT_ALIGNMENT_USED=4
    status       :STATUS       =field(metadata=bin_field("U1",scale=STATUS,b1=3,b0=1,comment="Status of the IMU-mount alignment"))
    tiltAlgError :bool         =field(metadata=bin_field("U1",scale=bool,b0=0,comment="IMU-mount tilt (roll and/or pitch) alignment error"))
    yawAlgError  :bool         =field(metadata=bin_field("U1",scale=bool,b0=1,comment="IMU-mount yaw alignment error"))
    angleError   :bool         =field(metadata=bin_field("U1",scale=bool,b0=2,comment="IMU-mount misalignment Euler angle singularity error."
                                                                           "If true, the IMU-mount roll and IMU-mount yaw angles "
                                                                           "cannot uniquely be defined due to the singularity "
                                                                           "issue happening with installations mounted with a "
                                                                           "+/- 90 degrees misalignment around pitch axis. This "
                                                                           "is also known as the 'gimbal-lock' problem affecting "
                                                                           "rotations described by Euler angles."))
    reserved0    :int          =field(metadata=bin_field("U1",record=False))
    yaw          :Decimal      =field(metadata=bin_field("U4",scale=Decimal('1e-2'), unit="deg", fmt="%5.1f"))
    pitch        :Decimal      =field(metadata=bin_field("I2",scale=Decimal('1e-2'), unit="deg", fmt="%5.1f"))
    roll         :Decimal      =field(metadata=bin_field("I2",scale=Decimal('1e-2'), unit="deg", fmt="%5.1f"))


@ublox_packet(0x10,0x02,use_epoch=False)
class UBX_ESF_MEAS(UBloxPacket):
    "Contains sensor measurements with timestamp. Optionally, can include timestamp that the message was "\
    "received at the receiver. Multiple measurements can be included in a single message. (1 measurement per "\
    "sensor type.)"
    timeTag      :int          =field(metadata=bin_field("U4"))
    class TIMEMARKSENT(Enum):
        NONE=0
        ON_EXT0=1
        ON_EXT1=2
    timeMarkSent :TIMEMARKSENT =field(metadata=bin_field("X2",b1=1,b0=0,scale=TIMEMARKSENT))
    class TIMEMARKEDGE(Enum):
        RISING=0
        FALLING=1
        NONE=2
    timeMarkSent  :TIMEMARKEDGE =field(metadata=bin_field("X2",     b0=2,scale=TIMEMARKEDGE))
    calibTtagValid:bool         =field(metadata=bin_field("X2",     b0=3,scale=bool))
    numMeas       :int          =field(metadata=bin_field("X2",b1=15,b0=11))
    dataProviderId:int          =field(metadata=bin_field("U2"))
    data          :list[float]  =field(metadata=bin_field("U4",b1=23,b0=0,scale=lambda x:signed(x,24)))
    dataType      :list[SENSORTYPE]=field(metadata=bin_field("U4",b1=29,b0=24,scale=SENSORTYPE))
    calibTtag     :Decimal      =field(metadata=bin_field("U4",scale=Decimal('1e-3')))
    def fixup(self):
        super().fixup()
        for i in range(len(self.data)):
            if sensorUnitsScale[self.dataType[i]][1] is not None:
                self.data[i]=self.data[i]*sensorUnitsScale[self.dataType[i]][1]


@ublox_packet(0x10,0x10,use_epoch=True,required_version=0x02)
class UBX_ESF_STATUS(UBloxPacket):
    """This message combines position, velocity and time solution, including accuracy figures."""
    iTOW         :Decimal      =field(metadata=bin_field("U4",scale=Decimal('1e-3'), unit="s", fmt="%10.3f"))
    version      :int          =field(metadata=bin_field("U1",comment="Message version"))
    reserved0a   :int          =field(metadata=bin_field("U1",record=False))
    reserved0b   :int          =field(metadata=bin_field("U2",record=False))
    reserved0c   :int          =field(metadata=bin_field("U4",record=False))
    class FUSIONMODE(Enum):
        INITIALIZATION=0
        FUSION=1
        SUSPENDED_FUSION=2
        DISABLED_FUSION=3
    fusionMode   :FUSIONMODE   =field(metadata=bin_field("U1",scale=FUSIONMODE))
    reserved1    :int          =field(metadata=bin_field("U2",record=False))
    numSens      :int          =field(metadata=bin_field("U1"))
    sensType     :list[SENSORTYPE]=field(metadata=bin_field("U1",b1=5,b0=0,scale=SENSORTYPE))
    used         :list[bool]      =field(metadata=bin_field("U1",     b0=6,scale=bool))
    ready        :list[bool]      =field(metadata=bin_field("U1",     b0=7,scale=bool))
    class CALIBSTATUS(Enum):
        NOT_CALIBRATED=0
        CALIBRATING=1
        CALIBRATED2=2
        CALIBRATED3=3
    calibStatus  :list[CALIBSTATUS]=field(metadata=bin_field("U1",b1=1,b0=0,scale=CALIBSTATUS))
    class TIMESTATUS(Enum):
        NO_DATA=0
        TIME_ON_FIRST_BYTE=1
        TIME_ON_EVENT=2
        TIME_WITH_DATA=3
    timeStatus   :list[TIMESTATUS]=field(metadata=bin_field("U1",b1=3,b0=2,scale=TIMESTATUS))
    freq         :list[int]       =field(metadata=bin_field("U1",unit="Hz"))
    badMeas      :list[bool]      =field(metadata=bin_field("U1",     b0=0,scale=bool))
    badTTag      :list[bool]      =field(metadata=bin_field("U1",     b0=1,scale=bool))
    missingMeas  :list[bool]      =field(metadata=bin_field("U1",     b0=2,scale=bool))
    noisyMeas    :list[bool]      =field(metadata=bin_field("U1",     b0=3,scale=bool))


@ublox_packet(0x0d,0x01,use_epoch=False)
class UBX_TIM_TP(UBloxPacket):
    """This message contains information on the timing of the next pulse at the TIMEPULSE0 output."""
    towMS        :Decimal  =field(metadata=bin_field("U4", scale=Decimal('1e-3'),unit="s", comment="Time pulse time of week according to time base"))
    towSubMS     :float    =field(metadata=bin_field("U4", unit="s", scale=2**-32*1e-3,comment="Submillisecond part of towMS"))
    qErr         :Decimal  =field(metadata=bin_field("I4", unit="s", scale=Decimal('1e-12'),comment="Quantization error of time pulse"))
    week         :int      =field(metadata=bin_field("U2", unit="ms", comment="Time pulse week number according to time base"))
    class TIMEBASE(Enum):
        GNSS=0
        UTC=1
    timeBase     :TIMEBASE=field(metadata=bin_field("X1",b0=0,scale=TIMEBASE))
    utcAvailable :bool=field(metadata=bin_field("X1",b0=1,scale=bool))
    class RAIM(Enum):
        NA=0
        NOT_ACTIVE=1
        ACTIVE=2
    raimtimeBase     :RAIM=field(metadata=bin_field("X1",b1=3,b0=2,scale=RAIM))
    qErrValid        :bool=field(metadata=bin_field("X1",b0=4,record=False,scale=lambda x:not bool(x),comment="Quantization error valid (note this is inverted from transmitted value qErrInvalid)"))
    class TIMEREFGNSS(Enum):
        GPS=0
        GLONASS=1
        BeiDou=2
        Galileo=3
        NavIC=4
        UNKNOWN=15
        NOT_GNSS=6
    timeRefGnss    :TIMEREFGNSS=field(metadata=bin_field("X1",b1=3,b0=0,scale=TIMEREFGNSS,comment="GNSS reference information. Only valid if timeBase is GNSS."))
    utcStandard:UTCSTD=field(metadata=bin_field("X1",b1=7,b0=4,scale=UTCSTD,comment="UTC standard identifier. Only valid if timeBase is UTC."))
    def fixup(self):
        if not self.qErrValid:
            self.qErr=Decimal('NaN')


@ublox_packet(0x02,0x13,use_epoch=True,required_version=0x02)
class UBX_RXM_SFRBX(UBloxPacket):
    "This message reports a complete subframe of broadcast navigation data decoded from a single signal. The "\
    "number of data words reported in each message depends on the nature of the signal. This message is documented "\
    "in F9 HPG 1.32, version 27.31, which is the document for the F9P, not the F9R taken on Atlantic23.05."
    gnssId         :GNSSID      =field(metadata=bin_field("U1",scale=GNSSID))
    svId           :int         =field(metadata=bin_field("U1"))
    sigId          :SIGID       =field(metadata=bin_field("U1",index=True))
    freqId         :int         =field(metadata=bin_field("U1",comment="GLONASS only"))
    numWords       :int         =field(metadata=bin_field("U1"))
    chn            :int         =field(metadata=bin_field("U1"))
    version        :int         =field(metadata=bin_field("U1"))
    reserved0      :int         =field(metadata=bin_field("U1",record=False))
    dwrd           :list[int]   =field(metadata=bin_field("U4"))
    def fixup(self):
        super().fixup()
        self.sigId=SIGID.get_sigid(self.gnssId, self.sigId)


@ublox_packet(0x02,0x15,use_epoch=True,required_version=0x01)
class UBX_RXM_RAWX(UBloxPacket):
    """This message contains the information needed to be able to generate a RINEX 3 multi-GNSS observation file
(see ftp://ftp.igs.org/pub/data/format/).

This message contains pseudorange, Doppler, carrier phase, phase lock and signal quality information for
GNSS satellites once signals have been synchronized. This message supports all active GNSS."""
    rcvTow         :float       =field(metadata=bin_field("R8",unit="s",comment="""Measurement time of week in receiver local time
approximately aligned to the GPS time system.

The receiver local time of week, week number and leap
second information can be used to translate the time
to other time systems. More information about the
difference in time systems can be found in the RINEX
3 format documentation. For a receiver operating in
GLONASS only mode, UTC time can be determined by
subtracting the leapS field from GPS time regardless
of whether the GPS leap seconds are valid."""))
    week           :int         =field(metadata=bin_field("U2",unit="week",comment="GPS week number in receiver local time."))
    leapS          :int         =field(metadata=bin_field("I1",unit="s",comment="""GPS leap seconds (GPS-UTC). This field represents the
receiver's best knowledge of the leap seconds offset.
A flag is given in the recStat bitfield to indicate if the
leap seconds are known."""))
    numMeas        :int         =field(metadata=bin_field("U1"))
    leapSec        :bool        =field(metadata=bin_field("X1",b0=0,scale=bool))
    clkReset       :bool        =field(metadata=bin_field("X1",b0=1,scale=bool))
    version        :int         =field(metadata=bin_field("U1"))
    reserved0      :int         =field(metadata=bin_field("U2",record=False))
    prMes          :list[float] =field(metadata=bin_field("R8",unit="m",comment="Pseudorange measurement. GLONASS interfrequency"
                                                                        "channel delays are compensated with an internal "
                                                                        "calibration table."))
    cpMes          :list[float] =field(metadata=bin_field("R8",unit="cycle",comment="Carrier phase measurement [cycles]. The carrier "
                                                                        "phase initial ambiguity is initialized using an "
                                                                        "approximate value to make the magnitude of the "
                                                                        "phase close to the pseudorange measurement. "
                                                                        "Clock resets are applied to both phase and "
                                                                        "code measurements in accordance with the RINEX "
                                                                        "specification."))
    doMes          :list[float] =field(metadata=bin_field("R4",unit="Hz",comment="Doppler measurement (positive sign for "
                                                                                 "approaching satellites)"))
    gnssId         :list[GNSSID]=field(metadata=bin_field("U1",scale=GNSSID))
    svId           :list[int]   =field(metadata=bin_field("U1"))
    sigId          :list[SIGID] =field(metadata=bin_field("U1"))
    freqId         :list[int]   =field(metadata=bin_field("U1",comment="GLONASS only"))
    locktime       :list[Decimal]=field(metadata=bin_field("U2",unit="s",scale=Decimal('1e-3'),comment="Carrier phase locktime counter, saturates at 64.5s"))
    cno            :list[int]    =field(metadata=bin_field("U1"))
    prStdev        :list[Decimal]=field(metadata=bin_field("U1",b1=3,b0=0,scale=lambda x:Decimal('1e-2')*2**x,dec_scale=2,dec_precision=5))
    cpStdev        :list[Decimal]=field(metadata=bin_field("U1",b1=3,b0=0,scale=Decimal('0.004')))
    doStdev        :list[Decimal]=field(metadata=bin_field("U1",b1=3,b0=0,scale=lambda x:Decimal('0.002')*2**x,dec_scale=3,dec_precision=5))
    prValid        :list[bool]   =field(metadata=bin_field("U1",b0=0,scale=bool))
    cpValid        :list[bool]   =field(metadata=bin_field("U1",b0=1,scale=bool))
    halfCycValid   :list[bool]   =field(metadata=bin_field("U1",b0=2,scale=bool))
    subHalfCyc     :list[bool]   =field(metadata=bin_field("U1",b0=3,scale=bool))
    reserved1      :list[int]   =field(metadata=bin_field("U1",record=False))
    def fixup(self):
        super().fixup()
        self.sigId=[SIGID.get_sigid(gnssId,sigId) for gnssId,sigId in zip(self.gnssId,self.sigId)]


@ublox_packet(0x0a,0x31,use_epoch=True,required_version=0x00)
class UBX_MON_SPAN(UBloxPacket):
    """This message is to be used as a basic spectrum analyzer, where it displays one spectrum for each of the
receiver's existing RF paths. The spectrum is conveyed with the following parameters: The frequency span
in Hz, the frequency bin resolution in Hz, the center frequency in Hz, and 256 bins with amplitude data.
Additionally, in order to give further insight on the signal captured by the receiver, the current gain of the
internal programmable gain amplifier (PGA) is provided.

This message gives information for comparative analysis rather than absolute and precise spectrum
overview. Users should not expect highly accurate spectrum amplitude.

Note that the PGA gain is not included in the spectrum data but is available as a separate field. Neither the
spectrum, nor the PGA gain considers the internal fixed LNA gain or an external third-party LNA."""
    version:int=field(metadata=bin_field("U1"))
    numRfBlocks:int=field(metadata=bin_field("U1"))
    reserved0:int=field(metadata=bin_field("U2",record=False))
    spectrum:list[bytes]=field(metadata=bin_field("U[256]"))
    span:list[int]=field(metadata=bin_field("U4",unit="Hz"))
    res:list[int]=field(metadata=bin_field("U4",unit="Hz"))
    center:list[int]=field(metadata=bin_field("U4",unit="Hz"))
    pga:list[int]=field(metadata=bin_field("U1",unit="dB"))
    reserved1a:list[int]=field(metadata=bin_field("U1",record=False))
    reserved1b:list[int]=field(metadata=bin_field("U2",record=False))


@ublox_packet(0x0a,0x36,use_epoch=True,required_version=0x00)
class UBX_MON_COMMS(UBloxPacket):
    """Consolidated communications information for all ports. The size of the message is determined by the number
of ports that are in use on the receiver. A port is only included if communication, either send or receive, has
been initiated on that port."""
    version:int=field(metadata=bin_field("U1"))
    nPorts:int=field(metadata=bin_field("U1"))
    memError:bool=field(metadata=bin_field("U1",b0=0,scale=bool))
    allocError:bool=field(metadata=bin_field("U1",b0=1,scale=bool))
    reserved0:int=field(metadata=bin_field("U1",record=False))
    class PROTOCOL(Enum):
        UBX=0
        NMEA=1
        RTCM2=2
        RTCM3=5
        SPARTN=6
        NONE=0x55
    protId0:PROTOCOL=field(metadata=bin_field("U1",scale=PROTOCOL))
    protId1:PROTOCOL=field(metadata=bin_field("U1",scale=PROTOCOL))
    protId2:PROTOCOL=field(metadata=bin_field("U1",scale=PROTOCOL))
    protId3:PROTOCOL=field(metadata=bin_field("U1",scale=PROTOCOL))
    class PORT(Enum):
        I2C=0x0000
        UART1=0x0100
        PORT0x101=0x0101
        PORT0x200=0x0200
        UART2=0x0201
        USB=0x0300
        SPI=0x0400
    portId     :list[PORT]=field(metadata=bin_field("U2",scale=PORT))
    txPending  :list[int] =field(metadata=bin_field("U2",unit="bytes"))
    txBytes    :list[int] =field(metadata=bin_field("U4",unit="bytes"))
    txUsage    :list[int] =field(metadata=bin_field("U1",unit="%"))
    txPeakUsage:list[int] =field(metadata=bin_field("U1",unit="%"))
    rxPending  :list[int] =field(metadata=bin_field("U2",unit="bytes"))
    rxBytes    :list[int] =field(metadata=bin_field("U4",unit="bytes"))
    rxUsage    :list[int] =field(metadata=bin_field("U1",unit="%"))
    rxPeakUsage:list[int] =field(metadata=bin_field("U1",unit="%"))
    overrunErrs:list[int] =field(metadata=bin_field("U2"))
    msgs0      :list[int] =field(metadata=bin_field("U2"))
    msgs1      :list[int] =field(metadata=bin_field("U2"))
    msgs2      :list[int] =field(metadata=bin_field("U2"))
    msgs3      :list[int] =field(metadata=bin_field("U2"))
    reserved1  :list[int] =field(metadata=bin_field("U[8]",record=False))
    skipped    :list[int] =field(metadata=bin_field("U4",unit="bytes"))


@ublox_packet(0x0a,0x38,use_epoch=True,required_version=0x00)
class UBX_MON_RF(UBloxPacket):
    """Information for each RF block. There are as many RF blocks reported as bands supported by this receiver."""
    version:int=field(metadata=bin_field("U1"))
    nPorts:int=field(metadata=bin_field("U1"))
    reserved0:int=field(metadata=bin_field("U2",record=False))
    class BAND(Enum):
        L1=0
        L2_OR_L5=1
    blockId     :list[BAND]=field(metadata=bin_field("U1",scale=BAND))
    class JAMSTATE(Enum):
        UNKNOWN_OR_DISABLED=0
        NO_JAMMNG_DETECTED=1
        JAMMING_WARNING=2
        JAMMING_CRITICAL=3
    jammingState:list[JAMSTATE]=field(metadata=bin_field("X1",b1=1,b0=0,scale=JAMSTATE))
    class ANTSTATUS(Enum):
        INIT=0
        DONTKNOW=1
        OK=2
        SHORT=3
        OPEN=4
    antStatus:list[ANTSTATUS]=field(metadata=bin_field("U1",scale=ANTSTATUS))
    class ANTPOWER(Enum):
        OFF=0
        ON=1
        DONTKNOW=2
    antPower:list[ANTPOWER]=field(metadata=bin_field("U1",scale=ANTPOWER))
    postStatus:list[int]=field(metadata=bin_field("U4"))
    reserved1:list[int]=field(metadata=bin_field("U4",record=False))
    noisePerMS:list[int]=field(metadata=bin_field("U2"))
    agcCnt:list[int]=field(metadata=bin_field("U2"))
    jamInd:list[int]=field(metadata=bin_field("U1"))
    ofsI:list[int]=field(metadata=bin_field("I1"))
    magI:list[int]=field(metadata=bin_field("U1"))
    ofsQ:list[int]=field(metadata=bin_field("I1"))
    magQ:list[int]=field(metadata=bin_field("U1"))
    reserved2a:list[int]=field(metadata=bin_field("U1",record=False))
    reserved2b:list[int]=field(metadata=bin_field("U2",record=False))
