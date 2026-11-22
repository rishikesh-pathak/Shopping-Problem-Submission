use parquet::data_type::ByteArray;
use std::{error::Error, fmt, hash::Hash};

#[derive(Debug)]
pub struct WideIdentifier;

impl fmt::Display for WideIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("identifier needs the byte-string representation")
    }
}

impl Error for WideIdentifier {}

pub trait Identifier<'a>: Copy + Eq + Ord + Hash + Send + Sync + 'a {
    const EMPTY: Self;
    fn encode(text: &'a str) -> std::result::Result<Self, WideIdentifier>;
    fn parquet(self) -> ByteArray;
}

impl<'a> Identifier<'a> for &'a str {
    const EMPTY: Self = "";

    fn encode(text: &'a str) -> std::result::Result<Self, WideIdentifier> {
        Ok(text)
    }

    fn parquet(self) -> ByteArray {
        ByteArray::from(self)
    }
}

/// Exact bytewise ordering for up to nine nonzero ASCII bytes. Each byte
/// occupies seven bits, starting at the most significant end. Zero padding
/// distinguishes prefixes without reserving any nonzero ASCII characters.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PackedId(u64);

impl Identifier<'_> for PackedId {
    const EMPTY: Self = Self(0);

    fn encode(text: &str) -> std::result::Result<Self, WideIdentifier> {
        let bytes = text.as_bytes();
        if let [first @ .., last] = bytes {
            if first.len() == 8 {
                let word = u64::from_be_bytes(first.try_into().unwrap());
                const HIGH_BITS: u64 = 0x8080_8080_8080_8080;
                const LOW_BITS: u64 = 0x0101_0101_0101_0101;
                let has_zero = (word.wrapping_sub(LOW_BITS) & !word & HIGH_BITS) != 0;
                if word & HIGH_BITS != 0 || has_zero || !(1..=127).contains(last) {
                    return Err(WideIdentifier);
                }
                // Pack eight seven-bit bytes in three fixed-width steps.
                let pairs = ((word >> 1) & 0x3f80_3f80_3f80_3f80) | (word & 0x007f_007f_007f_007f);
                let halves =
                    ((pairs >> 2) & 0x0fff_c000_0fff_c000) | (pairs & 0x0000_3fff_0000_3fff);
                let packed =
                    ((halves >> 4) & 0x00ff_ffff_f000_0000) | (halves & 0x0000_0000_0fff_ffff);
                return Ok(Self((packed << 7) | u64::from(*last)));
            }
        }
        if bytes.len() > 9 {
            return Err(WideIdentifier);
        }
        let mut packed = 0;
        for &byte in bytes {
            if !(1..=127).contains(&byte) {
                return Err(WideIdentifier);
            }
            packed = (packed << 7) | u64::from(byte);
        }
        Ok(Self(packed << ((9 - bytes.len()) * 7)))
    }

    fn parquet(self) -> ByteArray {
        let mut bytes = [0; 9];
        let mut remaining = self.0;
        for byte in &mut bytes {
            *byte = ((remaining >> 56) & 127) as u8;
            remaining <<= 7;
        }
        let len = bytes.iter().position(|&byte| byte == 0).unwrap_or(9);
        ByteArray::from(&bytes[..len])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packed_ids_round_trip_and_keep_bytewise_order() {
        let values = [
            "",
            "\t",
            " 4",
            "A",
            "A\r",
            "A00000z!?",
            "A00000z@?",
            "b",
            "u",
            "u00000000",
            "\x7f\x7f\x7f\x7f\x7f\x7f\x7f\x7f\x7f",
        ];
        for (i, value) in values.iter().enumerate() {
            let packed = PackedId::encode(value).unwrap();
            assert_eq!(packed.parquet().data(), value.as_bytes());
            for other in &values[i + 1..] {
                assert_eq!(
                    packed.cmp(&PackedId::encode(other).unwrap()),
                    value.cmp(other)
                );
            }
        }
        for value in ["abcdefghijkl", "u\0", "1234\x005678", "é1234567"] {
            assert!(PackedId::encode(value).is_err(), "{value:?}");
        }
    }
}
