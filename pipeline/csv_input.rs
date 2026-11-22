use std::{
    fs, io,
    path::{Path, PathBuf},
};

pub fn digits(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let mut x: i64 = 0;
    if bytes.len() <= 18 {
        // An eighteen-digit decimal value cannot overflow a signed i64.
        // Avoid checked arithmetic on the hot 13-digit epoch timestamps.
        for &byte in bytes {
            let d = byte.wrapping_sub(b'0');
            if d > 9 {
                return None;
            }
            x = x * 10 + i64::from(d);
        }
        return Some(x);
    }
    for &byte in bytes {
        let d = byte.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        x = x.checked_mul(10)?.checked_add(i64::from(d))?;
    }
    Some(x)
}
fn dec2(s: &[u8]) -> Option<i64> {
    if s.len() != 2 { return None; }
    let a = s[0].wrapping_sub(b'0');
    let b = s[1].wrapping_sub(b'0');
    if (a | b) > 9 { return None; }
    Some(i64::from(a) * 10 + i64::from(b))
}
fn dec4(s: &[u8]) -> Option<i64> {
    let a = dec2(&s[..2])?;
    let b = dec2(&s[2..4])?;
    Some(a * 100 + b)
}
fn date_parts(s: &[u8]) -> Option<(i64, i64, i64, i64, i64, i64)> {
    if s.len() < 19
        || s[4] != b'-'
        || s[7] != b'-'
        || s[10] != b'T'
        || s[13] != b':'
        || s[16] != b':'
    {
        return None;
    }
    let y = dec4(&s[0..4])?;
    let m = dec2(&s[5..7])?;
    let d = dec2(&s[8..10])?;
    let hh = dec2(&s[11..13])?;
    let mm = dec2(&s[14..16])?;
    let ss = dec2(&s[17..19])?;
    if !(1..=9999).contains(&y) || !(1..=12).contains(&m) || hh > 23 || mm > 59 || ss > 59 {
        return None;
    }
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let days_in_month = match m {
        2 if leap => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if d < 1 || d > days_in_month {
        return None;
    }
    Some((y, m, d, hh, mm, ss))
}
pub fn stamp(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.get(4) != Some(&b'-') {
        return digits(b);
    }
    if !(b.len() == 20 && b[19] == b'Z'
        || b.len() == 25 && (b[19] == b'+' || b[19] == b'-') && b[22] == b':')
    {
        return None;
    }
    let (y, m, d, hh, mm, ss) = date_parts(b)?;
    let y2 = y - i64::from(m <= 2);
    let era = y2.div_euclid(400);
    let yoe = y2 - era * 400;
    let mp = m + if m > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    let mut ms = (((days * 24 + hh) * 60 + mm) * 60 + ss) * 1000;
    if b.len() == 25 {
        let offh = dec2(&b[20..22])?;
        let offm = dec2(&b[23..25])?;
        if offh > 23 || offm > 59 {
            return None;
        }
        let off = (offh * 60 + offm) * 60_000;
        ms += if b[19] == b'+' { -off } else { off };
    }
    Some(ms)
}
pub fn parse_line<const N: usize>(line: &[u8]) -> Option<[&str; N]> {
    let raw = line.strip_suffix(b"\n").unwrap_or(line);
    if raw.is_empty() {
        return None;
    }
    let strline = std::str::from_utf8(raw).ok()?;
    let mut parts = [""; N];
    let mut fields = strline.split(',');
    for part in &mut parts {
        let v = fields.next()?;
        let v = if v.starts_with('"') && v.ends_with('"') && v.len() >= 2 {
            let inner = &v[1..v.len() - 1];
            if inner.contains('"') {
                return None;
            }
            inner
        } else {
            if v.contains('"') {
                return None;
            }
            v
        };
        *part = v;
    }
    if fields.next().is_none() {
        Some(parts)
    } else {
        None
    }
}
pub fn input_files(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for p in fs::read_dir(dir)? {
        let p = p?.path();
        if p.extension().is_some_and(|x| x == "csv") && p.is_file() {
            files.push(p);
        }
    }
    files.sort();
    Ok(files)
}
