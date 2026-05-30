#!/usr/bin/env python3
"""Decode a Nostr bech32 entity to 64-char hex for the nagg GraphQL API.

Handles npub / note / nevent / nprofile. If the input is already 64-char hex it
is passed through (lower-cased). nevent/nprofile are TLV-encoded; the type-0
("special") record holds the event id / pubkey.

Usage:  to-hex.py <npub|note|nevent|nprofile|hex>
"""
import sys

CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"


def bech32_decode(bech: str):
    bech = bech.strip()
    pos = bech.rfind("1")
    if pos < 1 or pos + 7 > len(bech):
        raise ValueError("not a bech32 string")
    data = []
    for c in bech[pos + 1:]:
        d = CHARSET.find(c)
        if d == -1:
            raise ValueError(f"invalid bech32 char {c!r}")
        data.append(d)
    return bech[:pos], data[:-6]  # drop the 6-symbol checksum


def convertbits(data, frm=5, to=8):
    acc = bits = 0
    out = []
    maxv = (1 << to) - 1
    for value in data:
        acc = (acc << frm) | value
        bits += frm
        while bits >= to:
            bits -= to
            out.append((acc >> bits) & maxv)
    return bytes(out)


def first_tlv_special(raw: bytes) -> bytes:
    """Return the value of the first type-0 (special) TLV record."""
    i = 0
    while i + 2 <= len(raw):
        t, length = raw[i], raw[i + 1]
        value = raw[i + 2: i + 2 + length]
        if t == 0 and len(value) == 32:
            return value
        i += 2 + length
    raise ValueError("no 32-byte special record in TLV")


def to_hex(s: str) -> str:
    s = s.strip()
    if len(s) == 64 and all(c in "0123456789abcdefABCDEF" for c in s):
        return s.lower()
    hrp, data = bech32_decode(s)
    raw = convertbits(data)
    if hrp in ("npub", "note"):
        return raw[:32].hex()
    if hrp in ("nevent", "nprofile"):
        return first_tlv_special(raw).hex()
    raise ValueError(f"unsupported prefix {hrp!r} (use npub/note/nevent/nprofile/hex)")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: to-hex.py <npub|note|nevent|nprofile|hex>", file=sys.stderr)
        sys.exit(2)
    try:
        print(to_hex(sys.argv[1]))
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)
