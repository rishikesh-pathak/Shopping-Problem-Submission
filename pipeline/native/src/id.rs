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
    fn hybrid_ids_preserve_exact_order_and_equality() {
        use std::collections::HashSet;
        let values = [
            "", "a", "a0", "e6ucdkcdv", "u2tyd06sf", "zzzzzzzzz",
            "aa_bbbbbbbbb", "u123456789a", "u123456789b", "é",
            "long-å-identifier", "long-å-identifiér", "\t", "日本語",
        ];
        for (i, value) in values.iter().enumerate() {
            let id = HybridId::encode(value).unwrap();
            assert_eq!(id.parquet().data(), value.as_bytes());
            for other in &values[i+1..] {
                let id2 = HybridId::encode(other).unwrap();
                assert_eq!(id.cmp(&id2), value.cmp(other), "pair: {value:?} {other:?}");
                assert_eq!(id == id2, value == other);
            }
        }
        let mut set = HashSet::new();
        for s in values { set.insert(HybridId::encode(s).unwrap()); }
        assert_eq!(set.len(), values.len());
        assert_eq!(HybridId::encode("repeat").unwrap(), HybridId::encode("repeat").unwrap());
        assert_eq!(
            HybridId::encode("u012345678901").unwrap(),
            HybridId::encode("u012345678901").unwrap()
        );
    }

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


/// A 16-byte lossless identifier with fast packed ASCII keys and a borrowed
/// fallback for arbitrary UTF-8. Borrowed bytes live in input mmaps owned by
/// Dataset for the full duration of process(). The MSB in len_tag marks wide
/// identifiers; no ordinary input can allocate 2^63 bytes.
#[derive(Clone, Copy, Debug)]
pub struct HybridId<'a> {
    value: u64,
    len_tag: u64,
    lifetime: std::marker::PhantomData<&'a str>,
}

impl<'a> HybridId<'a> {
    const FLAG: u64 = 1_u64 << 63;
    fn is_wide(self) -> bool {
        self.len_tag & Self::FLAG != 0
    }
    fn wide(self) -> &'a str {
        debug_assert!(self.is_wide());
        let ptr = self.value as usize as *const u8;
        let len = (self.len_tag & !Self::FLAG) as usize;
        // SAFETY: created only by encode(&'a str), whose buffer is pinned
        // in the memory-mapped dataset; lifetime 'a prevents use after drop.
        unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr, len)) }
    }
    fn compact_bytes(value: u64) -> ([u8; 9], usize) {
        let mut bytes = [0u8; 9];
        let mut remaining = value;
        for byte in &mut bytes {
            *byte = ((remaining >> 56) & 127) as u8;
            remaining <<= 7;
        }
        let len = bytes.iter().position(|&byte| byte == 0).unwrap_or(9);
        (bytes, len)
    }
    fn compact_against_wide(value: u64, wide: &str) -> std::cmp::Ordering {
        let (bytes, len) = Self::compact_bytes(value);
        bytes[..len].cmp(wide.as_bytes())
    }
}

impl<'a> Identifier<'a> for HybridId<'a> {
    const EMPTY: Self = Self {
        value: 0,
        len_tag: 0,
        lifetime: std::marker::PhantomData,
    };

    fn encode(text: &'a str) -> std::result::Result<Self, WideIdentifier> {
        match PackedId::encode(text) {
            Ok(packed) => Ok(Self {
                value: packed.0,
                len_tag: 0,
                lifetime: std::marker::PhantomData,
            }),
            Err(_) => Ok(Self {
                value: text.as_ptr() as usize as u64,
                len_tag: Self::FLAG | text.len() as u64,
                lifetime: std::marker::PhantomData,
            }),
        }
    }

    fn parquet(self) -> ByteArray {
        if self.is_wide() {
            ByteArray::from(self.wide())
        } else {
            PackedId(self.value).parquet()
        }
    }
}

impl<'a> PartialEq for HybridId<'a> {
    fn eq(&self, other: &Self) -> bool {
        match (self.is_wide(), other.is_wide()) {
            (false, false) => self.value == other.value,
            (true, true) => self.wide() == other.wide(),
            _ => false,
        }
    }
}
impl<'a> Eq for HybridId<'a> {}
impl<'a> std::hash::Hash for HybridId<'a> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        if self.is_wide() {
            1u8.hash(state);
            self.wide().hash(state);
        } else {
            0u8.hash(state);
            self.value.hash(state);
        }
    }
}
impl<'a> PartialOrd for HybridId<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<'a> Ord for HybridId<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self.is_wide(), other.is_wide()) {
            (false, false) => self.value.cmp(&other.value),
            (true, true) => self.wide().cmp(other.wide()),
            (false, true) => Self::compact_against_wide(self.value, other.wide()),
            (true, false) => Self::compact_against_wide(other.value, self.wide()).reverse(),
        }
    }
}
