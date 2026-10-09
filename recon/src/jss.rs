//! Decoder for Jetstream v2 archive segments (`.jss`) and bare block frames,
//! limited to what an identity/account replay needs.
//!
//! Layout, all little-endian:
//!   256-byte header | blocks, each `u64 len` + one zstd frame | footer.
//! The footer starts with the block index: `block_count` entries of 52 bytes.
//! A block body is columnar: `u32 n`, fixed-width arrays, then five blobs.

use std::io::Read;

use anyhow::{anyhow, bail, Result};

pub const HEADER_LEN: usize = 256;
pub const BLOCK_ENTRY_LEN: usize = 52;
pub const KIND_IDENTITY: u8 = 4;
pub const KIND_ACCOUNT: u8 = 5;
/// Decompressed block cap. The reference implementation uses 1 GiB.
pub const MAX_BLOCK_BYTES: u64 = 1 << 30;
const MAX_EVENTS_PER_BLOCK: u32 = 1 << 18;

fn u16_at(b: &[u8], o: usize) -> u16 {
    u16::from_le_bytes([b[o], b[o + 1]])
}
fn u32_at(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(b[o..o + 4].try_into().expect("4 bytes"))
}
fn u64_at(b: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(b[o..o + 8].try_into().expect("8 bytes"))
}

#[derive(Debug, Clone)]
pub struct Header {
    pub checksum: u64,
    pub version: u16,
    pub block_count: u32,
    pub event_count: u32,
    pub min_seq: u64,
    pub max_seq: u64,
    pub footer_offset: u64,
    pub block_index_offset: u64,
    pub collection_index_offset: u64,
}

pub fn parse_header(b: &[u8]) -> Result<Header> {
    if b.len() < HEADER_LEN {
        bail!("header needs {HEADER_LEN} bytes, got {}", b.len());
    }
    if &b[0..4] != b"jss0" {
        bail!("bad magic");
    }
    let h = Header {
        checksum: u64_at(b, 4),
        version: u16_at(b, 12),
        block_count: u32_at(b, 14),
        event_count: u32_at(b, 18),
        min_seq: u64_at(b, 26),
        max_seq: u64_at(b, 34),
        footer_offset: u64_at(b, 58),
        collection_index_offset: u64_at(b, 82),
        block_index_offset: u64_at(b, 90),
    };
    if h.checksum == 0 {
        bail!("segment is not sealed");
    }
    if h.version != 1 {
        bail!("unsupported segment version {}", h.version);
    }
    if h.block_count > (1 << 20) {
        bail!("implausible block count {}", h.block_count);
    }
    if h.footer_offset < HEADER_LEN as u64 || h.block_index_offset != h.footer_offset {
        bail!("inconsistent footer offsets");
    }
    Ok(h)
}

#[derive(Debug, Clone)]
pub struct BlockEntry {
    pub offset: u64,
    pub compressed_size: u32,
    pub uncompressed_size: u32,
    pub event_count: u32,
    pub min_seq: u64,
    pub max_seq: u64,
}

/// `b` starts at the block index.
pub fn parse_block_index(b: &[u8], count: u32) -> Result<Vec<BlockEntry>> {
    let need = count as usize * BLOCK_ENTRY_LEN;
    if b.len() < need {
        bail!("block index needs {need} bytes, got {}", b.len());
    }
    let mut out = Vec::with_capacity(count as usize);
    for i in 0..count as usize {
        let e = &b[i * BLOCK_ENTRY_LEN..(i + 1) * BLOCK_ENTRY_LEN];
        out.push(BlockEntry {
            offset: u64_at(e, 0),
            compressed_size: u32_at(e, 8),
            uncompressed_size: u32_at(e, 12),
            event_count: u32_at(e, 16),
            min_seq: u64_at(e, 20),
            max_seq: u64_at(e, 28),
        });
    }
    Ok(out)
}

/// Decompress one block's zstd frame. Frames may omit their content size, so
/// this streams with a cap instead of trusting the frame header.
pub fn decompress(frame: &[u8]) -> Result<Vec<u8>> {
    let dec = zstd::stream::read::Decoder::new(frame)?;
    let mut out = Vec::new();
    dec.take(MAX_BLOCK_BYTES + 1).read_to_end(&mut out)?;
    if out.len() as u64 > MAX_BLOCK_BYTES {
        bail!("block exceeds the decompressed size cap");
    }
    Ok(out)
}

pub struct Row<'a> {
    pub seq: u64,
    pub witnessed_at: i64,
    pub kind: u8,
    pub did: &'a [u8],
    pub payload: &'a [u8],
}

/// Walk the rows of a decompressed block body. Unknown kinds are passed
/// through for the caller to skip; a new kind must not reject the block.
pub fn for_each_row<'a>(body: &'a [u8], mut f: impl FnMut(Row<'a>)) -> Result<u32> {
    if body.len() < 4 {
        bail!("block body too short");
    }
    let n = u32_at(body, 0);
    if n > MAX_EVENTS_PER_BLOCK {
        bail!("implausible event count {n}");
    }
    let n = n as usize;
    let fixed = 4 + n * 34;
    if body.len() < fixed {
        bail!("block body shorter than its fixed part");
    }
    let (seq_o, wit_o, kind_o) = (4, 4 + 8 * n, 4 + 24 * n);
    let (coll_o, did_o, rkey_o, rev_o, pay_o) = (4 + 25 * n, 4 + 26 * n, 4 + 28 * n, 4 + 29 * n, 4 + 30 * n);
    let (mut coll_sum, mut did_sum, mut rkey_sum, mut rev_sum, mut pay_sum) = (0usize, 0usize, 0usize, 0usize, 0usize);
    for i in 0..n {
        coll_sum += body[coll_o + i] as usize;
        did_sum += u16_at(body, did_o + 2 * i) as usize;
        rkey_sum += body[rkey_o + i] as usize;
        rev_sum += body[rev_o + i] as usize;
        pay_sum += u32_at(body, pay_o + 4 * i) as usize;
    }
    let dids_start = fixed + coll_sum;
    let pays_start = dids_start + did_sum + rkey_sum + rev_sum;
    if pays_start + pay_sum != body.len() {
        bail!("block body length does not match its columns");
    }
    let (mut d, mut p) = (dids_start, pays_start);
    for i in 0..n {
        let dl = u16_at(body, did_o + 2 * i) as usize;
        let pl = u32_at(body, pay_o + 4 * i) as usize;
        f(Row {
            seq: u64_at(body, seq_o + 8 * i),
            witnessed_at: u64_at(body, wit_o + 8 * i) as i64,
            kind: body[kind_o + i],
            did: &body[d..d + dl],
            payload: &body[p..p + pl],
        });
        d += dl;
        p += pl;
    }
    Ok(n as u32)
}

/// The fields of an `#identity` or `#account` payload that lodestar could use.
#[derive(Debug, Default)]
pub struct Marker {
    pub handle: Option<String>,
    pub active: Option<bool>,
    pub status: Option<String>,
}

struct Cbor<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Cbor<'a> {
    fn head(&mut self) -> Result<(u8, u64)> {
        let first = *self.b.get(self.i).ok_or_else(|| anyhow!("cbor: truncated"))?;
        self.i += 1;
        let (major, info) = (first >> 5, first & 0x1f);
        let extra = match info {
            0..=23 => return Ok((major, info as u64)),
            24 => 1,
            25 => 2,
            26 => 4,
            27 => 8,
            _ => bail!("cbor: indefinite or reserved length"),
        };
        let s = self.b.get(self.i..self.i + extra).ok_or_else(|| anyhow!("cbor: truncated"))?;
        self.i += extra;
        let mut v = 0u64;
        for x in s {
            v = (v << 8) | *x as u64;
        }
        Ok((major, v))
    }

    fn text(&mut self, len: u64) -> Result<&'a str> {
        let s = self.b.get(self.i..self.i + len as usize).ok_or_else(|| anyhow!("cbor: truncated"))?;
        self.i += len as usize;
        Ok(std::str::from_utf8(s)?)
    }

    fn skip(&mut self, depth: u32) -> Result<()> {
        if depth > 32 {
            bail!("cbor: nested too deep");
        }
        let (major, v) = self.head()?;
        match major {
            0 | 1 | 7 => {}
            2 | 3 => {
                if self.i + v as usize > self.b.len() {
                    bail!("cbor: truncated");
                }
                self.i += v as usize;
            }
            4 => {
                for _ in 0..v {
                    self.skip(depth + 1)?;
                }
            }
            5 => {
                for _ in 0..v {
                    self.skip(depth + 1)?;
                    self.skip(depth + 1)?;
                }
            }
            6 => self.skip(depth + 1)?,
            _ => bail!("cbor: bad major type"),
        }
        Ok(())
    }
}

/// Read `handle`, `active` and `status` out of a marker payload, skipping
/// everything else. An empty payload is valid and yields nothing.
pub fn parse_marker(payload: &[u8]) -> Result<Marker> {
    let mut m = Marker::default();
    if payload.is_empty() {
        return Ok(m);
    }
    let mut c = Cbor { b: payload, i: 0 };
    let (major, n) = c.head()?;
    if major != 5 {
        bail!("cbor: payload is not a map");
    }
    for _ in 0..n {
        let (km, kl) = c.head()?;
        if km != 3 {
            bail!("cbor: non-text key");
        }
        let key = c.text(kl)?;
        match key {
            "handle" | "status" => {
                let save = c.i;
                let (vm, vl) = c.head()?;
                if vm == 3 {
                    let v = c.text(vl)?.to_owned();
                    if key == "handle" {
                        m.handle = Some(v);
                    } else {
                        m.status = Some(v);
                    }
                } else {
                    c.i = save;
                    c.skip(0)?;
                }
            }
            "active" => {
                let (vm, vl) = c.head()?;
                if vm == 7 && (vl == 20 || vl == 21) {
                    m.active = Some(vl == 21);
                }
            }
            _ => c.skip(0)?,
        }
    }
    Ok(m)
}

/// Summarise a local file: a sealed segment, or a bare block frame.
pub fn inspect(path: &str) -> Result<()> {
    let data = std::fs::read(path)?;
    let mut kinds = [0u64; 256];
    let mut shown = 0;
    let mut show = |r: &Row| {
        if shown < 8 {
            shown += 1;
            let marker = if r.kind == KIND_IDENTITY || r.kind == KIND_ACCOUNT {
                format!("{:?}", parse_marker(r.payload))
            } else {
                String::new()
            };
            println!(
                "  seq {} kind {} did {} payload {} bytes {}",
                r.seq,
                r.kind,
                String::from_utf8_lossy(r.did),
                r.payload.len(),
                marker
            );
        }
    };
    if data.len() >= 4 && &data[0..4] == b"jss0" {
        let h = parse_header(&data)?;
        println!("{h:?}");
        let idx = parse_block_index(&data[h.block_index_offset as usize..], h.block_count)?;
        let mut rows = 0u64;
        for e in &idx {
            let start = e.offset as usize + 8;
            let frame = &data[start..start + e.compressed_size as usize];
            let body = decompress(frame)?;
            if body.len() != e.uncompressed_size as usize {
                bail!("block decompressed to {} bytes, index says {}", body.len(), e.uncompressed_size);
            }
            rows += for_each_row(&body, |r| {
                kinds[r.kind as usize] += 1;
                show(&r);
            })? as u64;
        }
        println!("blocks {} rows {rows} (header says {})", idx.len(), h.event_count);
    } else {
        let body = decompress(&data)?;
        let n = for_each_row(&body, |r| {
            kinds[r.kind as usize] += 1;
            show(&r);
        })?;
        println!("bare frame: rows {n}");
    }
    for (k, n) in kinds.iter().enumerate() {
        if *n > 0 {
            println!("kind {k}: {n}");
        }
    }
    Ok(())
}
