import json
from typing import BinaryIO

from packet.packet import read_packet


def read_ublox_packet(header: bytes, inf: BinaryIO):
    """
    Read a ublox packet. This is also a factory function, which reads
    the rest of the header, figures out which packet this is, then
    calls the __init__ for the correct dataclass

    :param header:
    :param inf:
    :return:
    """
    # Looks like JSON, read until the 0D0A
    result = header
    while result[-1] != 0x0A:
        result += inf.read(1)
    try:
        return json.loads(result)
    except:
        return str(result, encoding='cp437')
read_packet.classes[ord('{')]=read_ublox_packet

