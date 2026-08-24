pub fn decile_thresholds(fit_scores: &mut [f64]) -> Vec<f64> {
    fit_scores.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (1..10)
        .map(|d| fit_scores[(fit_scores.len() * d / 10).min(fit_scores.len() - 1)])
        .collect()
}

pub fn decile_of(thresholds: &[f64], score: f64) -> usize {
    thresholds.partition_point(|t| *t <= score)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thresholds_split_a_uniform_sample_into_deciles() {
        let mut scores: Vec<f64> = (0..100).map(|i| i as f64 / 100.0).collect();
        let t = decile_thresholds(&mut scores);
        assert_eq!(t.len(), 9);
        assert_eq!(t[0], 0.10);
        assert_eq!(t[8], 0.90);
    }

    #[test]
    fn decile_of_boundaries() {
        let t = vec![-0.8, -0.6, -0.4, -0.2, 0.0, 0.2, 0.4, 0.6, 0.8];
        assert_eq!(decile_of(&t, -0.9), 0);
        assert_eq!(decile_of(&t, -0.8), 1);
        assert_eq!(decile_of(&t, 0.0), 5);
        assert_eq!(decile_of(&t, 0.79), 8);
        assert_eq!(decile_of(&t, 0.8), 9);
        assert_eq!(decile_of(&t, 1.0), 9);
    }

    #[test]
    fn single_score_sample_degenerates_to_one_value() {
        let mut scores = vec![0.5];
        let t = decile_thresholds(&mut scores);
        assert_eq!(t, vec![0.5; 9]);
    }
}
