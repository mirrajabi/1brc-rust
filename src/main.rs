use std::{
    fs::File,
    io::{Read, Seek},
    thread,
};

const PATH: &str = "data/measurements_1000000000.txt";

fn main() -> std::io::Result<()> {
    let file = File::open(PATH)?;
    let file_size = file.metadata()?.len();
    let cpu_count = num_cpus::get() / 2;
    let chunk_size = (file_size / (cpu_count as u64)) as usize;

    thread::scope(|s| {
        for i in 0..cpu_count {
            s.spawn(move || {
                let mut file = File::open(PATH).expect("File can't be openned.");
                let chunk_start = chunk_size * i;
                file.seek(std::io::SeekFrom::Start(chunk_start as u64))
                    .expect("Seek issue.");
                let mut buf = vec![0; chunk_size];
                let n = file.read(&mut buf).expect("Error trying to read file");
                // println!(
                //     "index: {}, chunk_start: {}, chunk_end: {} -- start: {:?} end: {:?}",
                //     { i },
                //     chunk_start,
                //     chunk_start + chunk_size,
                //     String::from_utf8(buf[..16].to_vec()).unwrap(),
                //     String::from_utf8(buf[n - 16..n].to_vec()).unwrap()
                // );
            });
        }
    });

    Ok(())
}
