use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::Path;
use std::sync::{Arc, Mutex};

pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

#[derive(Default)]
pub struct Metrics {
    counters: Mutex<BTreeMap<String, u64>>,
    gauges: Mutex<BTreeMap<String, f64>>,
}

impl Metrics {
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn inc(&self, name: &str, labels: &[(&str, &str)]) {
        self.add(name, labels, 1);
    }

    pub fn add(&self, name: &str, labels: &[(&str, &str)], amount: u64) {
        let mut counters = self.counters.lock().unwrap();
        *counters.entry(series(name, labels)).or_insert(0) += amount;
    }

    pub fn set(&self, name: &str, labels: &[(&str, &str)], value: f64) {
        let mut gauges = self.gauges.lock().unwrap();
        gauges.insert(series(name, labels), value);
    }

    pub fn set_flag(&self, name: &str, labels: &[(&str, &str)], flag: bool) {
        self.set(name, labels, if flag { 1.0 } else { 0.0 });
    }

    #[cfg(test)]
    pub fn counter(&self, name: &str, labels: &[(&str, &str)]) -> u64 {
        self.counters
            .lock()
            .unwrap()
            .get(&series(name, labels))
            .copied()
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub fn gauge(&self, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
        self.gauges
            .lock()
            .unwrap()
            .get(&series(name, labels))
            .copied()
    }

    pub fn render(&self) -> String {
        let counters = self.counters.lock().unwrap().clone();
        let gauges = self.gauges.lock().unwrap().clone();
        let mut out = String::new();
        let mut family_seen = String::new();
        for (key, value) in &counters {
            type_line(&mut out, &mut family_seen, key, "counter");
            let _ = writeln!(out, "{key} {value}");
        }
        family_seen.clear();
        for (key, value) in &gauges {
            type_line(&mut out, &mut family_seen, key, "gauge");
            let _ = writeln!(out, "{key} {value}");
        }
        out
    }
}

fn type_line(out: &mut String, family_seen: &mut String, key: &str, kind: &str) {
    let family = key.split('{').next().unwrap_or(key);
    if family != family_seen {
        let _ = writeln!(out, "# TYPE {family} {kind}");
        family_seen.clear();
        family_seen.push_str(family);
    }
}

fn series(name: &str, labels: &[(&str, &str)]) -> String {
    if labels.is_empty() {
        return name.to_owned();
    }
    let rendered: Vec<String> = labels
        .iter()
        .map(|(key, value)| format!("{key}=\"{}\"", escape(value)))
        .collect();
    format!("{name}{{{}}}", rendered.join(","))
}

fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

pub fn dir_bytes(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.metadata().ok())
        .filter(|metadata| metadata.is_file())
        .map(|metadata| metadata.len())
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_accumulate_and_gauges_overwrite() {
        let metrics = Metrics::shared();

        metrics.inc("tracer_tick_total", &[("result", "ok")]);
        metrics.inc("tracer_tick_total", &[("result", "ok")]);
        metrics.add("tracer_tick_total", &[("result", "error")], 3);
        metrics.set("tracer_ready", &[], 0.0);
        metrics.set_flag("tracer_ready", &[], true);

        assert_eq!(metrics.counter("tracer_tick_total", &[("result", "ok")]), 2);
        assert_eq!(
            metrics.counter("tracer_tick_total", &[("result", "error")]),
            3
        );
        assert_eq!(
            metrics.counter("tracer_tick_total", &[("result", "none")]),
            0
        );
        assert_eq!(metrics.gauge("tracer_ready", &[]), Some(1.0));
        assert_eq!(metrics.gauge("tracer_missing", &[]), None);
    }

    #[test]
    fn render_emits_one_type_line_per_family_in_sorted_order() {
        let metrics = Metrics::shared();
        metrics.inc("tracer_b_total", &[("route", "/x")]);
        metrics.inc("tracer_a_total", &[]);
        metrics.set("tracer_gauge", &[("stream", "trades")], 1.5);
        metrics.set("tracer_gauge", &[("stream", "books")], 2.0);

        assert_eq!(
            metrics.render(),
            "# TYPE tracer_a_total counter\n\
             tracer_a_total 1\n\
             # TYPE tracer_b_total counter\n\
             tracer_b_total{route=\"/x\"} 1\n\
             # TYPE tracer_gauge gauge\n\
             tracer_gauge{stream=\"books\"} 2\n\
             tracer_gauge{stream=\"trades\"} 1.5\n"
        );
    }

    #[test]
    fn label_values_escape_quotes_backslashes_and_newlines() {
        assert_eq!(
            series("m", &[("k", "a\"b\\c\nd")]),
            "m{k=\"a\\\"b\\\\c\\nd\"}"
        );
    }

    #[test]
    fn dir_bytes_sums_files_only_and_tolerates_a_missing_directory() {
        let dir = std::env::temp_dir().join("tracerd-metrics-dir-bytes");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("nested")).unwrap();
        std::fs::write(dir.join("a.jsonl"), b"12345").unwrap();
        std::fs::write(dir.join("b.jsonl"), b"678").unwrap();
        std::fs::write(dir.join("nested/c.jsonl"), b"ignored").unwrap();

        assert_eq!(dir_bytes(&dir), 8);
        assert_eq!(dir_bytes(Path::new("/nonexistent/tracerd-dir")), 0);
    }
}
