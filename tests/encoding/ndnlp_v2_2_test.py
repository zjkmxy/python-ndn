from ndn.encoding.ndnlp_v2_2 import (
    LpPacketValue,
    LpTypeNumber,
    NackReason,
    NetworkNack,
    make_network_nack,
    parse_network_nack,
    parse_lp_packet_v2,
)
from ndn.encoding.ndn_format_0_3_2 import (
    InterestParam,
    make_interest,
    parse_interest,
)
from ndn.encoding import DecodeError, Name, tlv_encode, write_tl_num
import pytest


def test_network_nack_wire_format():
    interest = make_interest(
        '/localhost/nfd/faces/events',
        InterestParam(must_be_fresh=True, can_be_prefix=True),
    )
    lp_packet = make_network_nack(interest, NackReason.NO_ROUTE)

    assert lp_packet == (
        b"\x64\x36\xfd\x03\x20\x05\xfd\x03\x21\x01\x96"
        b"\x50\x2b\x05\x29\x07\x1f\x08\tlocalhost\x08\x03nfd"
        b"\x08\x05faces\x08\x06events\x21\x00\x12\x00\x0c\x02\x0f\xa0"
    )

    reason, encoded_interest = parse_network_nack(lp_packet)
    name, params, _, _ = parse_interest(encoded_interest)
    assert reason == NackReason.NO_ROUTE
    assert name == Name.from_str('/localhost/nfd/faces/events')
    assert params.can_be_prefix
    assert params.must_be_fresh


def test_network_nack_parser_accepts_fragment_metadata():
    value = tlv_encode(LpPacketValue(
        frag_index=0,
        frag_count=1,
        nack=NetworkNack(nack_reason=NackReason.NO_ROUTE),
        fragment=b'\x05\x00',
    ))
    wire = bytearray(2 + len(value))
    offset = write_tl_num(LpTypeNumber.LP_PACKET, wire, 0)
    offset += write_tl_num(len(value), wire, offset)
    wire[offset:] = value

    assert parse_network_nack(wire) == (NackReason.NO_ROUTE, b'\x05\x00')


def test_nested_unknown_critical_field_is_rejected():
    wire = b'\x64\x06\xfd\x03\x20\x02\x01\x00'
    with pytest.raises(DecodeError):
        parse_lp_packet_v2(wire)
