"""

"""
import pytest

from packet.ais import get_bitfield


@pytest.mark.parametrize(
    "nbits,payload,start,field_len,expected",
    [
        (10,0b0011000000, 2,2,0b11),
        (10,0b0000000011, 8,2,0b11),
        (10,0b0000000011,10,2,None),
        (10,0b0000000011, 4,6,0b11),
        (10,0b0000000011, 4,12,0b11000000),
    ]
)
def test_get_bitfield(nbits,payload,start,field_len,expected):
    assert get_bitfield(nbits,payload,start,field_len)==expected
