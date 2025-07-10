use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;
use std::time::Instant;
use anyhow::Result;
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use indicatif::{ProgressBar, ProgressStyle};
use rand::prelude::*;
use serde::{Deserialize, Serialize};

const OUTPUT_DIR: &str = "mock_logs";
const NUM_FILES: usize = 10_000;

const FIXED_KEYS: [&str; 15] = [
    "timestamp", "level", "message", "source_ip", "user_id", "request_id",
    "http_method", "http_path", "http_status", "user_agent", "response_time_ms",
    "app_version", "service_name", "region", "payload"
];

#[derive(Serialize, Deserialize)]
struct LogEntry {
    timestamp: String,
    level: String,
    message: String,
    source_ip: String,
    user_id: String,
    request_id: String,
    http_method: String,
    http_path: String,
    http_status: u16,
    user_agent: String,
    response_time_ms: u32,
    app_version: String,
    service_name: String,
    region: String,
    payload: String,
}

fn random_string(length: usize) -> String {
    thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

fn random_value(key: &str) -> serde_json::Value {
    match key {
        "timestamp" => {
            let hour = thread_rng().gen_range(0..24);
            let minute = thread_rng().gen_range(0..60);
            let second = thread_rng().gen_range(0..60);
            let millisecond = thread_rng().gen_range(0..1000);
            serde_json::Value::String(format!("2025-07-09T{:02}:{:02}:{:02}.{:03}Z", hour, minute, second, millisecond))
        }
        "level" => {
            let levels = ["INFO", "WARN", "ERROR", "DEBUG"];
            serde_json::Value::String(levels[thread_rng().gen_range(0..levels.len())].to_string())
        }
        "message" => {
            serde_json::Value::String(random_string(thread_rng().gen_range(50..151)))
        }
        "source_ip" => {
            serde_json::Value::String(format!(
                "{}.{}.{}.{}",
                thread_rng().gen_range(1..255),
                thread_rng().gen_range(1..255),
                thread_rng().gen_range(1..255),
                thread_rng().gen_range(1..255)
            ))
        }
        "user_id" => {
            serde_json::Value::String(format!("user-{}", thread_rng().gen_range(1000..10000)))
        }
        "request_id" => {
            serde_json::Value::String(random_string(32))
        }
        "http_method" => {
            let methods = ["GET", "POST", "PUT", "DELETE"];
            serde_json::Value::String(methods[thread_rng().gen_range(0..methods.len())].to_string())
        }
        "http_path" => {
            let segments = thread_rng().gen_range(1..4);
            let path = (0..segments)
                .map(|_| random_string(thread_rng().gen_range(5..11)))
                .collect::<Vec<_>>()
                .join("/");
            serde_json::Value::String(format!("/{}", path))
        }
        "http_status" => {
            let statuses = [200, 201, 400, 404, 500];
            serde_json::Value::Number(statuses[thread_rng().gen_range(0..statuses.len())].into())
        }
        "user_agent" => {
            serde_json::Value::String("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36".to_string())
        }
        "response_time_ms" => {
            serde_json::Value::Number(thread_rng().gen_range(10..501).into())
        }
        "app_version" => {
            serde_json::Value::String(format!(
                "{}.{}.{}",
                thread_rng().gen_range(1..6),
                thread_rng().gen_range(0..10),
                thread_rng().gen_range(0..10)
            ))
        }
        "service_name" => {
            let services = ["auth-service", "product-service", "order-service"];
            serde_json::Value::String(services[thread_rng().gen_range(0..services.len())].to_string())
        }
        "region" => {
            let regions = ["us-east-1", "us-west-2", "eu-central-1"];
            serde_json::Value::String(regions[thread_rng().gen_range(0..regions.len())].to_string())
        }
        "payload" => {
            serde_json::Value::String(random_string(2500))
        }
        _ => serde_json::Value::String(random_string(10))
    }
}

fn generate_json() -> serde_json::Value {
    let mut data = serde_json::Map::new();
    for key in FIXED_KEYS {
        data.insert(key.to_string(), random_value(key));
    }
    serde_json::Value::Object(data)
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;
    
    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }
    
    format!("{:.2} {}", size, UNITS[unit_index])
}

fn get_directory_size(path: &Path) -> Result<u64> {
    let mut total_size = 0;
    
    if path.is_dir() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                total_size += get_directory_size(&path)?;
            } else {
                total_size += entry.metadata()?.len();
            }
        }
    } else {
        total_size = fs::metadata(path)?.len();
    }
    
    Ok(total_size)
}

fn main() -> Result<()> {
    println!("🚀 Starting compression comparison project");
    println!("Generating {} fake JSON files...", NUM_FILES);
    
    // Create output directory
    fs::create_dir_all(OUTPUT_DIR)?;
    
    // Step 1: Generate JSON files
    println!("\n📝 Step 1: Generating JSON files");
    let start = Instant::now();
    let pb = ProgressBar::new(NUM_FILES as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos:>7}/{len:7} {msg}")
        .unwrap()
        .progress_chars("=>-"));
    
    for i in 0..NUM_FILES {
        let filename = format!("log_{:04}.json", i);
        let filepath = Path::new(OUTPUT_DIR).join(&filename);
        let file = File::create(&filepath)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, &generate_json())?;
        pb.inc(1);
    }
    pb.finish_with_message("JSON files generated!");
    
    let json_generation_time = start.elapsed();
    let json_size = get_directory_size(Path::new(OUTPUT_DIR))?;
    
    // Step 2: Compress each file with gzip
    println!("\n🗜️  Step 2: Compressing individual files with gzip");
    let start = Instant::now();
    let pb = ProgressBar::new(NUM_FILES as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos:>7}/{len:7} {msg}")
        .unwrap()
        .progress_chars("=>-"));
    
    for i in 0..NUM_FILES {
        let json_filename = format!("log_{:04}.json", i);
        let gz_filename = format!("log_{:04}.json.gz", i);
        let json_path = Path::new(OUTPUT_DIR).join(&json_filename);
        let gz_path = Path::new(OUTPUT_DIR).join(&gz_filename);
        
        let input_file = File::open(&json_path)?;
        let output_file = File::create(&gz_path)?;
        let mut encoder = GzEncoder::new(output_file, Compression::default());
        
        std::io::copy(&mut BufReader::new(input_file), &mut encoder)?;
        encoder.finish()?;
        pb.inc(1);
    }
    pb.finish_with_message("Individual gzip compression complete!");
    
    let gzip_compression_time = start.elapsed();
    let _gzip_size = get_directory_size(Path::new(OUTPUT_DIR))?;
    
    // Step 3: Decompress each gzip file
    println!("\n📦 Step 3: Decompressing gzip files");
    let start = Instant::now();
    let pb = ProgressBar::new(NUM_FILES as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos:>7}/{len:7} {msg}")
        .unwrap()
        .progress_chars("=>-"));
    
    for i in 0..NUM_FILES {
        let gz_filename = format!("log_{:04}.json.gz", i);
        let decompressed_filename = format!("log_{:04}_decompressed.json", i);
        let gz_path = Path::new(OUTPUT_DIR).join(&gz_filename);
        let decompressed_path = Path::new(OUTPUT_DIR).join(&decompressed_filename);
        
        let input_file = File::open(&gz_path)?;
        let output_file = File::create(&decompressed_path)?;
        let mut decoder = GzDecoder::new(BufReader::new(input_file));
        
        std::io::copy(&mut decoder, &mut BufWriter::new(output_file))?;
        pb.inc(1);
    }
    pb.finish_with_message("Gzip decompression complete!");
    
    let gzip_decompression_time = start.elapsed();
    
    // Step 4: Compress all original JSON files with zstd
    println!("\n🗜️  Step 4: Compressing all files with zstd");
    let start = Instant::now();
    
    let zstd_archive_path = Path::new(OUTPUT_DIR).join("all_logs.zst");
    let mut zstd_encoder = zstd::Encoder::new(File::create(&zstd_archive_path)?, 3)?;
    
    let pb = ProgressBar::new(NUM_FILES as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos:>7}/{len:7} {msg}")
        .unwrap()
        .progress_chars("=>-"));
    
    for i in 0..NUM_FILES {
        let json_filename = format!("log_{:04}.json", i);
        let json_path = Path::new(OUTPUT_DIR).join(&json_filename);
        
        let mut input_file = File::open(&json_path)?;
        
        // Write filename header for the archive
        let filename_bytes = json_filename.as_bytes();
        zstd_encoder.write_all(&(filename_bytes.len() as u32).to_le_bytes())?;
        zstd_encoder.write_all(filename_bytes)?;
        
        // Write file content
        let file_size = input_file.metadata()?.len();
        zstd_encoder.write_all(&(file_size as u32).to_le_bytes())?;
        std::io::copy(&mut input_file, &mut zstd_encoder)?;
        
        pb.inc(1);
    }
    
    zstd_encoder.finish()?;
    pb.finish_with_message("Zstd compression complete!");
    
    let zstd_compression_time = start.elapsed();
    let zstd_size = fs::metadata(&zstd_archive_path)?.len();
    
    // Calculate sizes for comparison
    let individual_gz_size: u64 = (0..NUM_FILES)
        .map(|i| {
            let gz_filename = format!("log_{:04}.json.gz", i);
            let gz_path = Path::new(OUTPUT_DIR).join(&gz_filename);
            fs::metadata(&gz_path).map(|m| m.len()).unwrap_or(0)
        })
        .sum();
    
    // Display results
    println!("\n📊 COMPRESSION COMPARISON RESULTS");
    println!("=====================================");
    println!("Original JSON files:");
    println!("  Size: {}", format_bytes(json_size));
    println!("  Generation time: {:.2?}", json_generation_time);
    println!();
    println!("Individual gzip compression:");
    println!("  Size: {}", format_bytes(individual_gz_size));
    println!("  Compression time: {:.2?}", gzip_compression_time);
    println!("  Decompression time: {:.2?}", gzip_decompression_time);
    println!("  Compression ratio: {:.2}%", (individual_gz_size as f64 / json_size as f64) * 100.0);
    println!();
    println!("Multi-file zstd compression:");
    println!("  Size: {}", format_bytes(zstd_size));
    println!("  Compression time: {:.2?}", zstd_compression_time);
    println!("  Compression ratio: {:.2}%", (zstd_size as f64 / json_size as f64) * 100.0);
    println!();
    println!("🏆 WINNER:");
    if zstd_size < individual_gz_size {
        let savings = individual_gz_size - zstd_size;
        let savings_percent = (savings as f64 / individual_gz_size as f64) * 100.0;
        println!("  Zstd wins by {} ({:.2}% smaller)", format_bytes(savings), savings_percent);
    } else {
        let savings = zstd_size - individual_gz_size;
        let savings_percent = (savings as f64 / zstd_size as f64) * 100.0;
        println!("  Gzip wins by {} ({:.2}% smaller)", format_bytes(savings), savings_percent);
    }
    
    println!("\n✅ Compression comparison complete!");
    
    Ok(())
}
