#  x0    x1    x2    x3    x4    x5    x6    x7    x8    x9    xa    xb    xc    xd    xe    xf
low_sub = ('\u2400\u263A\u263b\u2665\u2666\u2663\u2660\u2022\u25d8\u25cb\u25d9\u2642\u2640\u266a\u266b\u263c' +
           '\u25ba\u25c4\u2195\u203c\u00b6\u00a7\u25ac\u21a8\u2191\u2193\u2192\u2190\u221f\u2194\u25b2\u25bc\u2420')


def dump_bin(buf, word_len=4, words_per_line=8):
    line_len = word_len * words_per_line
    for i_line in range((len(buf) // line_len) + 1):
        i_line0 = i_line * line_len
        i_line1 = (i_line + 1) * line_len
        print(f"{i_line0:04x} - ", end='')
        for i_byte_in_line, i_byte in enumerate(range(i_line0, i_line1)):
            if i_byte < len(buf):
                print(f"{buf[i_byte]:02x}", end='')
            else:
                print("  ", end='')
            if (i_byte_in_line + 1) % 4 == 0:
                print(" ", end='')
        print("|", end='')
        for i_byte_in_line, i_byte in enumerate(range(i_line0, i_line1)):
            if i_byte < len(buf):
                if buf[i_byte] < len(low_sub):
                    print(low_sub[buf[i_byte]], end='')
                else:
                    print(str(buf[i_byte:i_byte + 1], encoding='iso8859-1'), end='')
            else:
                print(" ", end='')
        print("")


def signed(data: int, nbits: int) -> int:
    signbit = nbits - 1
    if data & (1 << signbit) == 0:
        return data
    else:
        return -(1 << signbit) + (data & ((1 << signbit) - 1))


def test_signed():
    assert signed(0x80, 8) == -128
    assert signed(0xFF, 8) == -1
    assert signed(0x00, 8) == 0
    assert signed(0x7f, 8) == 127
    assert signed(0xFFFFFF, 24) == -1
    assert signed(0x000000, 24) == 0
    assert signed(0x7fffff, 24) == 0x7fffff


def get_bits(source: int, b1: int, b0: int):
    size = b1 - b0 + 1
    mask = (1 << (size)) - 1
    result = (source >> b0) & mask
    return result


if __name__ == "__main__":
    test_signed()