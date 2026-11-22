use crate::{csv_input::parse_line, Result};
use memchr::memchr_iter;

pub fn for_rows<'a, const N: usize>(
    bytes: &'a [u8],
    visit: impl FnMut([&'a str; N]) -> Result<()>,
) -> Result<()> {
    #[cfg(target_arch = "x86_64")]
    if bytes.len() >= 32 && is_x86_feature_detected!("avx2") {
        // SAFETY: the feature check guards the vector loads and comparisons.
        // The ASCII check makes all field boundaries UTF-8 boundaries.
        return unsafe { x86::scan(bytes, visit) };
    }
    scalar(bytes, visit)
}

fn scalar<'a, const N: usize>(
    bytes: &'a [u8],
    mut visit: impl FnMut([&'a str; N]) -> Result<()>,
) -> Result<()> {
    let mut start = 0;
    for end in memchr_iter(b'\n', bytes) {
        if let Some(row) = parse_line(&bytes[start..end]) {
            visit(row)?;
        }
        start = end + 1;
    }
    if let Some(row) = parse_line(&bytes[start..]) {
        visit(row)?;
    }
    Ok(())
}

#[cfg(target_arch = "x86_64")]
mod x86 {
    use super::{Result, csv_input::parse_line};
    use std::arch::x86_64::*;

    struct Row<'a, const N: usize> {
        bytes: &'a [u8],
        columns: [&'a str; N],
        start: usize,
        fields: usize,
        quotes: usize,
        first_quote: usize,
        last_quote: usize,
        valid: bool,
        row_start: usize,
        non_ascii: bool,
    }

    impl<'a, const N: usize> Row<'a, N> {
        fn quote(&mut self, at: usize) {
            if self.quotes == 0 {
                self.first_quote = at;
            }
            self.quotes += 1;
            self.last_quote = at;
        }

        fn field(&mut self, end: usize) {
            if !self.non_ascii && self.valid && self.fields < N {
                let bounds = match self.quotes {
                    0 => Some(self.start..end),
                    2 if self.first_quote == self.start && self.last_quote + 1 == end => {
                        Some(self.start + 1..end - 1)
                    }
                    _ => None,
                };
                if let Some(bounds) = bounds {
                    // SAFETY: scan accepts only ASCII input.
                    self.columns[self.fields] =
                        unsafe { std::str::from_utf8_unchecked(&self.bytes[bounds]) };
                } else {
                    self.valid = false;
                }
            } else {
                self.valid = false;
            }
            self.start = end + 1;
            self.fields += 1;
            self.quotes = 0;
        }

        fn structural(
            &mut self,
            byte: u8,
            at: usize,
            visit: &mut impl FnMut([&'a str; N]) -> Result<()>,
        ) -> Result<()> {
            match byte {
                b'"' => self.quote(at),
                b',' => self.field(at),
                b'\n' => {
                    if self.non_ascii {
                        // Validate only rows with high bytes on the scalar path.
                        if let Some(columns) = parse_line::<N>(&self.bytes[self.row_start..at]) {
                            visit(columns)?;
                        }
                    } else {
                        let empty = self.fields == 0 && self.start == at;
                        self.field(at);
                        if !empty && self.valid && self.fields == N {
                            visit(self.columns)?;
                        }
                    }
                    self.row_start = at + 1;
                    self.start = at + 1;
                    self.fields = 0;
                    self.quotes = 0;
                    self.valid = true;
                    self.non_ascii = false;
                }
                _ => {}
            }
            Ok(())
        }
    }

    /// The caller must provide ASCII input and an AVX2-capable processor.
    #[target_feature(enable = "avx2")]
    pub unsafe fn scan<'a, const N: usize>(
        bytes: &'a [u8],
        mut visit: impl FnMut([&'a str; N]) -> Result<()>,
    ) -> Result<()> {
        let commas = _mm256_set1_epi8(b',' as i8);
        let quotes = _mm256_set1_epi8(b'"' as i8);
        let newlines = _mm256_set1_epi8(b'\n' as i8);
        let mut row = Row {
            bytes,
            columns: [""; N],
            start: 0,
            fields: 0,
            quotes: 0,
            first_quote: 0,
            last_quote: 0,
            valid: true,
            row_start: 0,
            non_ascii: false,
        };
        let mut base = 0;
        while bytes.len() - base >= 32 {
            // SAFETY: each load is contained within the slice.
            let block = unsafe { _mm256_loadu_si256(bytes.as_ptr().add(base).cast()) };
            let comma_or_quote = _mm256_or_si256(
                _mm256_cmpeq_epi8(block, commas),
                _mm256_cmpeq_epi8(block, quotes),
            );
            let marks = _mm256_or_si256(comma_or_quote, _mm256_cmpeq_epi8(block, newlines));
            let mut mask = _mm256_movemask_epi8(marks) as u32;
            let mut high = _mm256_movemask_epi8(block) as u32;
            while mask != 0 {
                let offset = mask.trailing_zeros();
                let prefix = u32::MAX >> (31 - offset);
                if high & prefix != 0 {
                    row.non_ascii = true;
                    high &= !prefix;
                }
                let at = base + offset as usize;
                row.structural(bytes[at], at, &mut visit)?;
                mask &= mask - 1;
            }
            if high != 0 {
                row.non_ascii = true;
            }
            base += 32;
        }
        for (i, &byte) in bytes[base..].iter().enumerate() {
            if !byte.is_ascii() {
                row.non_ascii = true;
            }
            row.structural(byte, base + i, &mut visit)?;
        }
        if bytes.last() != Some(&b'\n') {
            row.structural(b'\n', bytes.len(), &mut visit)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identical<const N: usize>(input: &[u8]) {
        let mut expected = Vec::new();
        let mut actual = Vec::new();
        scalar::<N>(input, |row| {
            expected.push(row);
            Ok(())
        })
        .unwrap();
        for_rows::<N>(input, |row| {
            actual.push(row);
            Ok(())
        })
        .unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn scanner_matches_scalar_for_bad_rows_and_block_boundaries() {
        for pad in 0..32 {
            let input = format!("{}id,\"str\",z\nquoted,\"a,comma\",a\nx,,y\n\n\"x\",\"y\",\"z\"\r\nx,y,z,w\nx,\"q\",z\nlast,\"\",field", " ".repeat(pad));
            identical::<3>(input.as_bytes());
        }
        identical::<2>(b"valid,a\nan excessively long valid,field\ninvalid\xff,y\nwide,\xc3\xa9");
        for pad in 0..64 {
            let input = format!(
                "{}a,b\né,x\nc,d\nbad,\u{fffd}\n\nend,last",
                " ".repeat(pad)
            );
            identical::<2>(input.as_bytes());
        }
        identical::<2>(b"normal,ok\nnonascii,\xff\nnormal,yes\n");
        identical::<2>("ascii,ok\naccént,yes\nascii,ok\n".as_bytes());
    }
}
