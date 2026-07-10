pub fn format_bytes(bytes: i64) -> String {
    const UNIT: i64 = 1024;
    if bytes < UNIT {
        return format!("{} B", bytes);
    }
    let suffixes = b"KMGTPE";
    let mut div: i64 = UNIT;
    let mut exp: usize = 0;
    let mut n = bytes / UNIT;

    while n >= UNIT && exp < suffixes.len() - 1 {
        div *= UNIT;
        exp += 1;
        n /= UNIT;
    }

    format!("{:.1} {}iB", bytes as f64 / div as f64, suffixes[exp] as char)
}
