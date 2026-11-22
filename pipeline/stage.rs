//! High-throughput, dependency-free staging for Shopping Sessions.
//! Only replaces Python CSV preparation; the proven DuckDB SQL remains unchanged.
use std::collections::HashMap;
use std::env;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

fn digits(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() { return None; }
    let mut x: i64 = 0;
    for b in bytes {
        if !b.is_ascii_digit() { return None; }
        x = x.checked_mul(10)?.checked_add(i64::from(b - b'0'))?;
    }
    Some(x)
}
fn date_parts(s: &[u8]) -> Option<(i64,i64,i64,i64,i64,i64)> {
    if s.len() < 19 || s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' { return None; }
    let y = digits(&s[0..4])?;
    let m = digits(&s[5..7])?;
    let d = digits(&s[8..10])?;
    let hh = digits(&s[11..13])?;
    let mm = digits(&s[14..16])?;
    let ss = digits(&s[17..19])?;
    if !(1..=9999).contains(&y) || !(1..=12).contains(&m) || hh>23 || mm>59 || ss>59 { return None; }
    let leap = y%4==0 && (y%100!=0 || y%400==0);
    let days_in_month = match m {
        2 if leap => 29, 2 => 28, 4|6|9|11 => 30, _ => 31,
    };
    if d < 1 || d > days_in_month { return None; }
    Some((y,m,d,hh,mm,ss))
}
fn stamp(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.iter().all(u8::is_ascii_digit) && !b.is_empty() { return digits(b); }
    if !(b.len()==20 && b[19]==b'Z' || b.len()==25 && (b[19]==b'+' || b[19]==b'-') && b[22]==b':') { return None; }
    let (y,m,d,hh,mm,ss) = date_parts(b)?;
    let y2 = y - i64::from(m<=2);
    let era = y2.div_euclid(400);
    let yoe = y2-era*400;
    let mp = m + if m>2 { -3 } else { 9 };
    let doy = (153*mp+2)/5 + d-1;
    let doe = yoe*365 + yoe/4-yoe/100 + doy;
    let days = era*146097 + doe - 719468;
    let mut ms = (((days*24 + hh)*60 + mm)*60 + ss)*1000;
    if b.len()==25 {
        let offh = digits(&b[20..22])?;
        let offm = digits(&b[23..25])?;
        if offh>23 || offm>59 { return None; }
        let off = (offh*60 + offm)*60_000;
        ms += if b[19]==b'+' { -off } else { off };
    }
    Some(ms)
}
fn parse_line<const N: usize>(line: &[u8]) -> Option<[&str; N]> {
    let raw = line.strip_suffix(b"\n").unwrap_or(line);
    if raw.is_empty() { return None; }
    let strline = std::str::from_utf8(raw).ok()?;
    let mut parts = [""; N];
    let mut fields = strline.split(',');
    for part in &mut parts {
        let v = fields.next()?;
        let v = if v.starts_with('"') && v.ends_with('"') && v.len()>=2 {
            let inner = &v[1..v.len()-1];
            if inner.contains('"') { return None; }
            inner
        } else {
            if v.contains('"') { return None; }
            v
        };
        *part = v;
    }
    if fields.next().is_none() { Some(parts) } else { None }
}
fn input_files(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for p in fs::read_dir(dir)? {
        let p = p?.path();
        if p.extension().is_some_and(|x| x=="csv") && p.is_file() { files.push(p); }
    }
    files.sort();
    Ok(files)
}
fn read_rows<const N: usize>(path: &Path, mut f: impl FnMut(&[&str; N]) -> io::Result<()>) -> io::Result<()> {
    let mut reader = BufReader::with_capacity(1024 * 1024, File::open(path)?);
    let mut line = Vec::new();
    reader.read_until(b'\n', &mut line)?; // skip each shard's header
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 { break; }
        if let Some(cols) = parse_line(&line) { f(&cols)?; }
    }
    Ok(())
}

fn stage_shards<const N: usize>(
    data: &Path,
    scratch: &Path,
    kind: &str,
    workers: usize,
    write: impl Fn(&mut BufWriter<File>, &[&str; N]) -> io::Result<()> + Sync,
) -> io::Result<()> {
    let files = input_files(&data.join(kind))?;
    let next = AtomicUsize::new(0);
    thread::scope(|scope| {
        let jobs: Vec<_> = (0..workers.min(files.len())).map(|_| {
            scope.spawn(|| -> io::Result<()> {
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(path) = files.get(index) else { return Ok(()); };
                    let output = File::create(scratch.join(format!("{kind}-{index:012}.stage")))?;
                    let mut writer = BufWriter::with_capacity(1024 * 1024, output);
                    read_rows(path, |row| write(&mut writer, row))?;
                    writer.flush()?;
                }
            })
        }).collect();
        for job in jobs {
            job.join().expect("staging worker panicked")?;
        }
        Ok(())
    })
}
fn cell<W: Write>(w: &mut W, value: &str) -> io::Result<()> {
    if value.as_bytes().iter().any(|&x| x==0x1f || x==b'\n' || x==b'\r' || x==b'"') {
        w.write_all(b"\"")?;
        for byte in value.as_bytes() {
            if *byte==b'"' { w.write_all(b"\"\"")?; } else { w.write_all(&[*byte])?; }
        }
        w.write_all(b"\"")?;
    } else { w.write_all(value.as_bytes())?; }
    Ok(())
}
fn write_row<W: Write>(w: &mut W, cols: &[&str]) -> io::Result<()> {
    for (i, val) in cols.iter().enumerate() {
        if i>0 { w.write_all(&[0x1f])?; }
        cell(w, val)?;
    }
    w.write_all(b"\n")
}
struct DSU { parent: HashMap<String,String> }
impl DSU {
    fn new() -> Self { Self{parent:HashMap::new()} }
    fn root(&mut self, s: &str) -> String {
        if !self.parent.contains_key(s) {
            self.parent.insert(s.to_owned(), s.to_owned());
            return s.to_owned();
        }
        let mut cur = s.to_owned();
        loop {
            let next = self.parent.get(&cur).expect("DSU node");
            if next==&cur { break; }
            cur = next.to_owned();
        }
        let root = cur.clone();
        let mut cur = s.to_owned();
        loop {
            let next = self.parent.get(&cur).expect("DSU node").clone();
            if next==cur { break; }
            self.parent.insert(cur, root.clone());
            cur = next;
        }
        root
    }
    fn join(&mut self, a: &str, b: &str) {
        let a = self.root(a);
        let b = self.root(b);
        if a!=b {
            if a<b { self.parent.insert(b,a); }
            else { self.parent.insert(a,b); }
        }
    }
}
fn main() -> Result<(),Box<dyn std::error::Error>> {
    let data=PathBuf::from(env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let scratch=PathBuf::from(env::var("SCRATCH_DIR").unwrap_or_else(|_| "/scratch".into()));
    fs::create_dir_all(&scratch)?;

    let mut dsu=DSU::new();
    for path in input_files(&data.join("links"))? {
        read_rows::<2>(&path, |r| {
            if !r[0].is_empty() && !r[1].is_empty() { dsu.join(r[0],r[1]); }
            Ok(())
        })?;
    }
    let mapping=File::create(scratch.join("mapping.stage"))?;
    let mut w=BufWriter::with_capacity(1024*1024,mapping);
    let keys:Vec<String>=dsu.parent.keys().cloned().collect();
    for k in &keys { let root=dsu.root(k); write_row(&mut w,&[k,&root])?; }
    w.flush()?;
    drop(dsu);

    let workers = env::var("NCPU").ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(8).max(1);
    stage_shards::<7>(&data, &scratch, "events", workers, |w, r| {
        if r[0].is_empty() { return Ok(()); }
        let ms=stamp(r[2]).map(|x| x.to_string()).unwrap_or_default();
        let ing=stamp(r[3]).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData,"invalid ingest timestamp"))?.to_string();
        let price=digits(r[6].as_bytes()).filter(|x| (1..=10_000_000).contains(x)).map(|x| x.to_string()).unwrap_or_default();
        write_row(w,&[r[0],r[1],&ms,&ing,r[4],r[5],&price])
    })?;

    stage_shards::<4>(&data, &scratch, "clicks", workers, |w, r| {
        if r[0].is_empty() { return Ok(()); }
        let ms=stamp(r[3]).map(|x| x.to_string()).unwrap_or_default();
        write_row(w,&[r[0],r[1],r[2],&ms])
    })?;
    Ok(())
}
