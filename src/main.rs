use memchr::memchr;
use memmap2::Mmap;
use rustc_hash::{FxBuildHasher, FxHashMap};
use std::{cmp::min, collections::BTreeMap, fs::File, thread, time::Instant};

const MAX_CITIES: usize = 10000;

// Shortest temperature value is in the format of 0.0, so we can always skip 3 bytes when looking for the newline
const NEWLINE_SKIP: usize = 3;

const MULTI_THREADED: bool = true;

#[derive(Copy, Clone)]
struct CityStats {
    min: i16,
    max: i16,
    sum: i32,
    count: i32,
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
// const PATH: &str = "data/measurements_10000000.txt";
// const PATH: &str = "data/measurements_1000000.txt";
// const PATH: &str = "data/measurements_10000.txt";

fn index_of(ch: u8, arr: &[u8]) -> Option<usize> {
    // arr.iter().position(|&c| c == ch)
    memchr(ch, arr)
}

fn parse_temp(mut bytes: &[u8]) -> i16 {
    let negative = if bytes[0] == b'-' {
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

fn process_chunk(data: &[u8]) -> FxHashMap<&[u8], CityStats> {
    let mut data = data;
    let mut map = FxHashMap::with_capacity_and_hasher(MAX_CITIES, FxBuildHasher);
    loop {
        let sep_idx = index_of(b';', data);
        let Some(sep_idx) = sep_idx else {
            break;
        };

        let mut nl_idx = index_of(b'\n', &data[sep_idx + 1 + NEWLINE_SKIP..]).unwrap();
        // Because we start counting from the separator
        nl_idx += sep_idx + 1 + NEWLINE_SKIP;

        let name = &data[..sep_idx];
        let temp_bytes = &data[sep_idx + 1..nl_idx];
        let temp: i16 = parse_temp(temp_bytes);
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
        let chunk_start = i * chunk_size - remaining_bytes;
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
    let start_time = Instant::now();

    let file = File::open(PATH)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let data = &*mmap;

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
        let mut acc = BTreeMap::default();
        thread::scope(|s| {
            let mut handles = Vec::with_capacity(n_threads);

            get_chunks(data, n_threads).into_iter().for_each(|chunk| {
                let handle = s.spawn(move || process_chunk(&data[chunk.0..chunk.1]));
                handles.push(handle);
            });

            for handle in handles {
                let map = handle.join().unwrap();
                map.into_iter().for_each(|item| {
                    add(&mut acc, item);
                });
            }
        });

        for item in acc.iter() {
            print!(
                "{}: {}/{}/{}/{}, ",
                String::from_utf8(item.0.to_vec()).unwrap(),
                (item.1).min,
                (item.1).max,
                (item.1).sum / (item.1).count,
                (item.1).count,
            );
        }
        println!("Total: {}", acc.len());
    }
    println!("Took: {}", start_time.elapsed().as_millis());
    Ok(())
}
