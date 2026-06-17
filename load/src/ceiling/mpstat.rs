pub struct CpuSample {
    pub per_core_busy: Vec<f64>,
    pub mean_busy: f64,
}

pub fn parse_mpstat(out: &str) -> Option<CpuSample> {
    let mut per_core_busy = Vec::new();
    let mut mean_busy = None;
    for line in out.lines().filter(|l| l.starts_with("Average:")) {
        let cols: Vec<&str> = line.split_whitespace().collect();
        // cols[0]="Average:", cols[1]=CPU label, last col = %idle
        if cols.len() < 3 { continue; }
        let label = cols[1];
        // `cols.len() >= 3` is guaranteed by the guard above, so the last column exists.
        let idle: f64 = match cols[cols.len() - 1].parse() { Ok(v) => v, Err(_) => continue };
        let busy = 100.0 - idle;
        if label == "all" {
            mean_busy = Some(busy);
        } else if label.parse::<usize>().is_ok() {
            per_core_busy.push(busy);
        }
    }
    let mean_busy = mean_busy?;
    if per_core_busy.is_empty() { return None; }
    Some(CpuSample { per_core_busy, mean_busy })
}

pub async fn sample(interval_s: u64, count: u64) -> Option<CpuSample> {
    let out = tokio::process::Command::new("mpstat")
        .args(["-P", "ALL", &interval_s.to_string(), &count.to_string()])
        .output()
        .await
        .ok()?;
    if !out.status.success() { return None; }
    parse_mpstat(&String::from_utf8_lossy(&out.stdout))
}

#[cfg(test)]
mod tests {
    use super::*;
    // mpstat -P ALL prints an "Average:" block; CPU "all" then each core. %idle is last.
    const SAMPLE: &str = "\
Average:     CPU    %usr   %nice    %sys %iowait    %irq   %soft  %steal  %guest  %gnice   %idle
Average:     all   20.00    0.00   10.00    0.00    0.00    2.00    0.00    0.00    0.00   68.00
Average:       0   40.00    0.00   20.00    0.00    0.00    5.00    0.00    0.00    0.00   35.00
Average:       1   10.00    0.00    5.00    0.00    0.00    1.00    0.00    0.00    0.00   84.00
";
    #[test]
    fn parses_per_core_and_mean() {
        let s = parse_mpstat(SAMPLE).unwrap();
        assert_eq!(s.per_core_busy.len(), 2);
        assert!((s.per_core_busy[0] - 65.0).abs() < 0.01); // 100-35
        assert!((s.per_core_busy[1] - 16.0).abs() < 0.01); // 100-84
        assert!((s.mean_busy - 32.0).abs() < 0.01);        // 100-68 from the 'all' row
    }
}
