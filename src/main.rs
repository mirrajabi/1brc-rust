#![cfg_attr(feature = "simd-index", feature(portable_simd))]
#[cfg(feature = "simd-index")]
use core::simd::{Simd, prelude::*};

use memchr::memchr;
use memmap2::Mmap;
use rustc_hash::{FxBuildHasher, FxHashMap as HashMap};
use std::{cmp::min, collections::BTreeMap, fs::File, thread, time::Instant};
#[cfg(feature = "enable-profiler")]
use tracy_client::{Client, span};

// 8873 total cities and 8893 is one of the prime numbers around that range
const MAX_CITIES: usize = 8893;

// Shortest temperature value is in the format of 0.0, so we can always skip 3 bytes when looking for the newline
const NEWLINE_SKIP: usize = 3;

const MULTI_THREADED: bool = true;

/// Returns the first position of `needle` in `haystack`, or `None` if not found.
#[cfg(feature = "simd-index")]
const LANES: usize = 16;

#[derive(Copy, Clone)]
struct CityStats {
    sum: i32,
    count: i32,
    min: i16,
    max: i16,
}

impl CityStats {
    fn from_temp(temp: i16) -> Self {
        Self {
            min: temp,
            max: temp,
            sum: temp as i32,
            count: 1,
        }
    }
}

const PATH: &str = "data/measurements_1000000000.txt";

fn index_of(ch: u8, arr: &[u8]) -> Option<usize> {
    // arr.iter().position(|&c| c == ch)
    #[cfg(not(feature = "simd-index"))]
    return memchr(ch, arr);
    #[cfg(feature = "simd-index")]
    return simd_find_byte(ch, arr);
}

#[cfg(feature = "simd-index")]
fn simd_find_byte(needle: u8, haystack: &[u8]) -> Option<usize> {
    let needle_simd = Simd::splat(needle);
    let mut i = 0;
    while i + LANES <= haystack.len() {
        // let chunk = Simd::<u8, LANES>::from_slice(&haystack[i..i + LANES]);
        let chunk = Simd::<u8, LANES>::from_array(unsafe {
            *haystack.get_unchecked(i..i + LANES).as_ptr().cast()
        });
        let mask = chunk.simd_eq(needle_simd);
        let bitmask = mask.to_bitmask();

        if bitmask != 0 {
            // find index of the first matching lane
            let pos_in_chunk = bitmask.trailing_zeros() as usize;
            return Some(i + pos_in_chunk);
        }

        i += LANES;
    }

    // Fallback to scalar search for the tail
    for (j, &b) in haystack[i..].iter().enumerate() {
        if b == needle_simd[0] {
            return Some(i + j);
        }
    }

    None
}

fn parse_temp(mut bytes: &[u8]) -> i16 {
    let negative = if unsafe { *bytes.get_unchecked(0) } == b'-' {
        bytes = &bytes[1..];
        true
    } else {
        false
    };

    let (b, c, d) = match bytes {
        [c, b'.', d] => (0, c - b'0', d - b'0'),
        [b, c, b'.', d] => (b - b'0', c - b'0', d - b'0'),
        _ => panic!("Unknown pattern: {}", std::str::from_utf8(bytes).unwrap()),
    };

    let res = b as i16 * 100 + c as i16 * 10 + d as i16;
    if negative { -res } else { res }
}

fn process_chunk(data: &[u8]) -> HashMap<&[u8], CityStats> {
    let mut data = data;
    let mut map = HashMap::with_capacity_and_hasher(MAX_CITIES, FxBuildHasher);
    loop {
        #[cfg(feature = "enable-profiler")]
        let _s = span!("separator_pos");
        let sep_idx = index_of(b';', data);
        let Some(sep_idx) = sep_idx else {
            break;
        };

        #[cfg(feature = "enable-profiler")]
        let _s = span!("newline_pos");
        let mut nl_idx = index_of(b'\n', &data[sep_idx + 1 + NEWLINE_SKIP..]).unwrap();
        // Because we start counting from the separator and skip a few bytes
        nl_idx += sep_idx + 1 + NEWLINE_SKIP;

        #[cfg(feature = "enable-profiler")]
        let _s = span!("temp_parse");
        let name = &data[..sep_idx];
        let temp_bytes = &data[sep_idx + 1..nl_idx];
        let temp: i16 = parse_temp(temp_bytes);

        #[cfg(feature = "enable-profiler")]
        let _s = span!("hashmap_add");
        map.entry(name)
            .and_modify(|item: &mut CityStats| {
                if temp < item.min {
                    item.min = temp;
                } else if temp > item.max {
                    item.max = temp;
                }
                item.sum += temp as i32;
                item.count += 1;
            })
            .or_insert(CityStats::from_temp(temp));
        data = &data[nl_idx + 1..];
    }
    map
}

fn get_chunks(data: &[u8], n_threads: usize) -> Vec<(usize, usize)> {
    let file_size = data.len();
    let chunk_size = file_size / n_threads;
    let mut remaining_bytes: usize = 0;
    let mut chunks = vec![(0, 0); n_threads + 1];
    for i in 0..n_threads + 1 {
        // Whatever is left from the previous iteration will be prepended to this chunk
        let chunk_start = i * chunk_size - remaining_bytes + 1;
        let chunk_end = min(file_size, (i + 1) * chunk_size);
        let initial_chunk = &data[chunk_start..chunk_end];
        let last_linebreak = initial_chunk.iter().rposition(|ch| *ch == b'\n').unwrap();
        // chunk = &chunk[..last_linebreak + 1];
        remaining_bytes = chunk_end - last_linebreak - chunk_start;
        chunks[i] = (chunk_start, chunk_start + last_linebreak + 1);
    }
    chunks
}

fn main() -> std::io::Result<()> {
    #[cfg(feature = "enable-profiler")]
    Client::start();

    let start_time = Instant::now();
    let mmap: Mmap;
    let data;
    {
        let file = File::open(PATH)?;
        mmap = unsafe { Mmap::map(&file)? };
        data = &*mmap;
    }

    let n_threads: usize = thread::available_parallelism().unwrap().into();
    let n_threads = n_threads * 2;

    let mut acc: BTreeMap<&[u8], CityStats> = BTreeMap::default();
    fn add<'a>(acc: &mut BTreeMap<&'a [u8], CityStats>, item: (&'a [u8], CityStats)) {
        acc.entry(item.0)
            .and_modify(|stats: &mut CityStats| {
                let new_stats = item.1;
                stats.min = min(new_stats.min, stats.min);
                stats.max = min(new_stats.max, stats.max);
                stats.count += new_stats.count;
                stats.sum += new_stats.sum;
            })
            .or_insert(item.1);
    }

    if !MULTI_THREADED {
        get_chunks(data, n_threads)
            .into_iter()
            .flat_map(|chunk| process_chunk(&data[chunk.0..chunk.1]))
            .for_each(|item| add(&mut acc, item));
    } else {
        thread::scope(|s| {
            let mut handles = Vec::with_capacity(n_threads);

            get_chunks(data, n_threads).into_iter().for_each(|chunk| {
                println!("Chunk {:?}", chunk);
                let chunk = &data[chunk.0..chunk.1];
                let handle = s.spawn(|| process_chunk(chunk));
                handles.push(handle);
            });

            for handle in handles {
                let map = handle.join().unwrap();
                map.into_iter().for_each(|item| {
                    add(&mut acc, item);
                });
            }
        });
    }
    let mut separator = "";
    print!("{{");
    for item in acc.iter() {
        print!(
            "{}{}: {:.1}/{:.1}/{:.1}/{}",
            separator,
            String::from_utf8(item.0.to_vec()).unwrap(),
            ((item.1).min as f32) / 10f32,
            ((item.1).max as f32) / 10f32,
            ((item.1).sum as f32) / ((item.1).count as f32) / 10f32,
            (item.1).count,
        );
        separator = ", ";
    }
    print!("}}");
    println!("\nTotal cities: {}", acc.len());

    println!("Took: {}", start_time.elapsed().as_millis());
    Ok(())
}
