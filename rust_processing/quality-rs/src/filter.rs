use crate::util;
use anyhow::Error;
use color_eyre::eyre::Result;
use counter::Counter;
use serde_json::Value;
use std::collections::HashSet;

// 想要添加规则，新建一个 Filter struct 实现这个 trait 就好
pub trait Filter: Send + Sync {
    fn filter(&self, data: &mut Value) -> Result<bool, Error>;
    #[allow(dead_code)]
    fn name(&self) -> &str;
}

#[allow(dead_code)]
pub struct LineRemovalModifier {
    pub max_removed_ratio: f64,
    pub max_uppercase_ratio: f64,
    pub min_word_cnt_per_line: usize,
    pub lang: String,
}

impl Filter for LineRemovalModifier {
    fn filter(&self, data: &mut Value) -> Result<bool, Error> {
        let text = match data
            .get(util::TEXT_KEY)
            .and_then(Value::as_str)
            .map(str::trim)
        {
            Some(s) if !s.is_empty() => s.to_string(),
            _ => return Ok(false),
        };

        let lines = util::split_lines(&text);
        let mut new_lines: Vec<&str> = Vec::new();
        let mut fraction_of_words_corrected_in_lines: usize = 0;

        for line in lines {
            let (is_filtered, removed_words_cnt) = self.line_filtering(line);
            if !is_filtered {
                new_lines.push(line);
            }

            fraction_of_words_corrected_in_lines += removed_words_cnt;
        }

        let new_text = new_lines.join("\n");
        if new_text.is_empty() {
            return Ok(false);
        }

        data[util::TEXT_KEY] = Value::String(new_text.into());

        if self.max_removed_ratio as usize > 0 {
            let total_words_cnt = text.split_whitespace().count() as f64;
            if total_words_cnt as usize > 0
                && fraction_of_words_corrected_in_lines as f64 / total_words_cnt
                    > self.max_removed_ratio
            {
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn name(&self) -> &str {
        "LineRemovalModifier"
    }
}

impl LineRemovalModifier {
    fn line_filtering(&self, line: &str) -> (bool, usize) {
        let line_norm = line.trim().to_lowercase();
        if line_norm.is_empty() {
            return (true, 0);
        }

        let word_cnt_line = line_norm.split(' ').count();
        if word_cnt_line < self.min_word_cnt_per_line {
            return (true, word_cnt_line);
        }

        if self.check_javascript(&line_norm) {
            return (true, word_cnt_line);
        }

        if self.check_cookie(&line_norm) {
            return (true, word_cnt_line);
        }

        let num_uppercase = line.chars().filter(|c| c.is_uppercase()).count();
        if num_uppercase as f64 / line.len() as f64 > self.max_uppercase_ratio {
            return (true, word_cnt_line);
        }

        if line_norm.chars().all(|c| c.is_numeric()) {
            return (true, word_cnt_line);
        }

        if util::is_counter(&line_norm) {
            return (true, word_cnt_line);
        }

        (false, 0)
    }

    fn check_javascript(&self, line: &str) -> bool {
        if !line.contains("javascript") {
            return false;
        }
        if line.contains("enable") {
            return true;
        }
        if line.contains("disable") {
            return true;
        }
        if line.contains("require") {
            return true;
        }
        if line.contains("activate") {
            return true;
        }
        if line.contains("browser") {
            return true;
        }
        false
    }

    fn check_cookie(&self, line: &str) -> bool {
        const POLICY_SUBSTRINGS: [&str; 6] = [
            "terms of use",
            "privacy policy",
            "cookie policy",
            "uses cookies",
            "use of cookies",
            "use cookies",
        ];

        POLICY_SUBSTRINGS.iter().any(|p| line.contains(p))
    }
}

#[allow(dead_code)]
pub struct CacheTokenFilter {
    pub lang: String,
}

impl Filter for CacheTokenFilter {
    fn filter(&self, data: &mut Value) -> Result<bool, Error> {
        let text = match data.get(util::TEXT_KEY).and_then(Value::as_str) {
            Some(s) if !s.trim().is_empty() => s.trim(),
            _ => return Ok(false),
        };

        let words_result = util::split_words(text, Some(data), &self.lang, false, true);
        match words_result {
            Ok(words) => {
                data[util::WORDS_KEY] =
                    Value::Array(words.into_iter().map(|w| Value::String(w)).collect());
            }
            Err(e) => {
                println!("split words failed, error: {}", e);
                if text.len() > 1000 {
                    data[util::WORDS_KEY] = Value::Array(vec![]);
                }
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn name(&self) -> &str {
        "CacheTokenFilter"
    }
}

#[allow(dead_code)]
pub struct UncacheTokenFilter;

impl Filter for UncacheTokenFilter {
    fn filter(&self, data: &mut Value) -> Result<bool, Error> {
        util::clear_key(data, util::WORDS_KEY);
        Ok(true)
    }

    fn name(&self) -> &str {
        "UncacheTokenFilter"
    }
}

// gopher repetition
#[allow(dead_code)]
pub struct GopherRepetitionFilter {
    pub dup_line_frac: f64,
    pub dup_para_frac: f64,
    pub dup_line_char_frac: f64,
    pub dup_para_char_frac: f64,
    pub top_n_grams: Vec<(i32, f64)>,
    pub dup_n_grams: Vec<(i32, f64)>,
    pub lang: String,
}

fn repetition_filter(
    segments: Vec<&str>,
    dup_para_frac: f64,
    dup_para_char_frac: f64,
) -> Result<bool, Error> {
    if !segments.is_empty() {
        let total_chars: usize = segments.iter().map(|p| p.len()).sum();
        let segment_counts: Counter<&str> = segments.into_iter().collect();

        let repeated_segs: usize = segment_counts
            .iter()
            .filter(|(_, &cnt)| cnt > 1)
            .map(|(_, &cnt)| cnt)
            .sum();

        if repeated_segs as f64 / segment_counts.len() as f64 > dup_para_frac {
            return Ok(false);
        }

        let repeated_chars: usize = segment_counts
            .iter()
            .filter(|(_seg, &cnt)| cnt > 1)
            .map(|(seg, &cnt)| seg.len() * cnt)
            .sum();
        if repeated_chars as f64 / total_chars as f64 > dup_para_char_frac {
            return Ok(false);
        }
    }

    Ok(true)
}

impl Filter for GopherRepetitionFilter {
    fn filter(&self, data: &mut Value) -> Result<bool, Error> {
        let text = match data.get(util::TEXT_KEY).and_then(Value::as_str) {
            Some(s) if !s.trim().is_empty() => s.trim(),
            _ => return Ok(false),
        };

        let paragraphs = util::split_paragraphs(text);
        if !repetition_filter(paragraphs, self.dup_para_frac, self.dup_para_char_frac)? {
            return Ok(false);
        }

        let lines = util::split_lines(text);
        if !repetition_filter(lines, self.dup_line_frac, self.dup_line_char_frac)? {
            return Ok(false);
        }

        let words_result = util::split_words(text, Some(data), &self.lang, true, true);
        match words_result {
            Ok(words) => {
                let total_chars: usize = words.iter().map(|w| w.len()).sum();
                for &(n, n_frac) in &self.top_n_grams {
                    let n_grams = util::get_n_grams(&words, n as usize);
                    if n_grams.is_empty() {
                        continue;
                    }

                    let ngram_counts: Counter<Vec<&str>> = n_grams.into_iter().collect();
                    let ordered = ngram_counts.most_common_ordered();

                    let max_count = match ordered.first().map(|&(_, cnt)| cnt) {
                        Some(cnt) if cnt > 1 => cnt,
                        _ => continue,
                    };

                    let max_len = ordered
                        .iter()
                        .filter(|&(_, cnt)| *cnt == max_count)
                        .map(|(ngram, _)| ngram.iter().map(|w| w.len()).sum::<usize>())
                        .max()
                        .unwrap_or(0);

                    let repeated_chars = max_len * max_count;
                    if repeated_chars as f64 / total_chars as f64 > n_frac {
                        return Ok(false);
                    }
                }

                for &(n, n_frac) in &self.dup_n_grams {
                    let n_grams = util::get_n_grams(&words, n as usize);
                    if n_grams.is_empty() {
                        continue;
                    }

                    let ngram_counts: Counter<Vec<&str>> =
                        n_grams.iter().map(|s| s.clone()).collect();

                    let mut repeated_word_indices: HashSet<usize> = HashSet::new();
                    for (idx, ngram) in n_grams.iter().enumerate() {
                        if ngram_counts.get(ngram).copied().unwrap_or(0) > 1 {
                            for i in idx..(idx + n as usize) {
                                repeated_word_indices.insert(i);
                            }
                        }
                    }

                    let repeated_word_char_count: usize =
                        repeated_word_indices.iter().map(|&i| words[i].len()).sum();

                    if repeated_word_char_count as f64 / total_chars as f64 > n_frac {
                        return Ok(false);
                    }
                }
            }
            Err(e) => {
                println!("split words failed, error: {}", e);
                let text_len = text.len() as f64;
                if text_len as usize > 1000 {
                    return Ok(true);
                }
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn name(&self) -> &str {
        "GopherRepetitionFilter"
    }
}

#[allow(dead_code)]
pub struct GopherQualityFilter {
    pub min_doc_words: usize,
    pub max_doc_words: usize,
    pub min_avg_word_length: usize,
    pub max_avg_word_length: usize,
    pub max_symbol_word_ratio: f64,
    pub max_bullet_lines_ratio: f64,
    pub max_ellipsis_lines_ratio: f64,
    pub max_non_alpha_words_ratio: f64,
    pub min_stop_words: usize,
    pub lang: String,
}

impl Filter for GopherQualityFilter {
    fn filter(&self, data: &mut Value) -> Result<bool, Error> {
        let text = match data.get(util::TEXT_KEY).and_then(Value::as_str) {
            Some(s) if !s.trim().is_empty() => s.trim(),
            _ => return Ok(false),
        };
        let words_result = util::split_words(text, Some(data), &self.lang, true, true);
        match words_result {
            Ok(words) => {
                let n_words = words.len() as f64;
                let non_symbol_words: Vec<&String> = words
                    .iter()
                    .filter(|w| w.chars().any(|ch| !util::PUNCTUATION_SET.contains(&ch)))
                    .collect();
                let n_non_symbol_words: usize = non_symbol_words.len();

                if n_non_symbol_words < self.min_doc_words {
                    return Ok(false);
                }
                if n_non_symbol_words > self.max_doc_words {
                    return Ok(false);
                }
                let avg_n_words: f64 = non_symbol_words.iter().map(|w| w.len() as f64).sum::<f64>()
                    / n_non_symbol_words as f64;
                if avg_n_words < self.min_avg_word_length as f64 {
                    return Ok(false);
                }
                if avg_n_words > self.max_avg_word_length as f64 {
                    return Ok(false);
                }

                if words
                    .iter()
                    .filter(|w| util::STOP_WORDS.contains(&w.as_str()))
                    .count()
                    < self.min_stop_words
                {
                    return Ok(false);
                }

                if text.matches("#").count() as f64 / n_words > self.max_symbol_word_ratio {
                    return Ok(false);
                }

                if (text.matches("...").count() + text.matches("…").count()) as f64 / n_words
                    > self.max_symbol_word_ratio
                {
                    return Ok(false);
                }

                let lines = util::split_lines(text);
                let max_bullet_count: f64 = self.max_bullet_lines_ratio * lines.len() as f64;
                let bullet_count: usize = lines
                    .iter()
                    .filter(|line| {
                        let line = line.trim_start();
                        util::BULLET_POINT_SYMBOLS
                            .iter()
                            .any(|sym| line.starts_with(sym))
                    })
                    .count();

                if bullet_count as f64 > max_bullet_count {
                    return Ok(false);
                }

                let ellipsis_lines_count = lines
                    .iter()
                    .filter(|line| {
                        let line = line.trim_end();
                        line.ends_with("...") || line.ends_with("…")
                    })
                    .count();
                if ellipsis_lines_count as f64 / lines.len() as f64 > self.max_ellipsis_lines_ratio
                {
                    return Ok(false);
                }

                let alpha_count = words
                    .iter()
                    .filter(|w| w.chars().any(|c| c.is_alphabetic()))
                    .count();

                if alpha_count as f64 / n_words < self.max_non_alpha_words_ratio {
                    return Ok(false);
                }
            }
            Err(e) => {
                println!("split words failed, error: {}", e);
                let text_len = text.len() as f64;
                if text_len as usize > 1000 {
                    return Ok(true);
                }
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn name(&self) -> &str {
        "GopherQualityFilter"
    }
}

// fineweb quality 规则
#[allow(dead_code)]
pub struct FinewebQualityFilter {
    pub line_punct_thr: f64,
    pub short_line_length: usize,
    pub short_line_thr: f64,
    pub char_duplicates_ratio: f64,
    pub new_line_ratio: f64,
}

impl Filter for FinewebQualityFilter {
    fn filter(&self, data: &mut Value) -> Result<bool, Error> {
        let text = match data.get(util::TEXT_KEY).and_then(Value::as_str) {
            Some(s) if !s.trim().is_empty() => s.trim(),
            _ => return Ok(false),
        };

        let lines: Vec<&str> = text
            .split("\n")
            .map(|l| l.trim())
            .filter(|l| !l.is_empty())
            .collect();
        if lines.len() == 0 {
            return Ok(false);
        }
        let stop_chars = util::TERMINAL_PUNCTUATION;
        let total = lines.len();
        if total == 0 {
            return Ok(false);
        }
        let count = lines
            .iter()
            .filter(|l| stop_chars.iter().any(|ch| l.ends_with(ch)))
            .count();

        if (count as f64 / total as f64) < self.line_punct_thr {
            return Ok(false);
        }

        if (lines
            .iter()
            .filter(|l| l.len() <= self.short_line_length)
            .count() as f64
            / total as f64)
            > self.short_line_thr
        {
            return Ok(false);
        }

        let (_, dup_chars) = util::find_duplicates(&lines);
        if (dup_chars as f64 / text.replace("\n", "").len() as f64) > self.char_duplicates_ratio {
            return Ok(false);
        }

        let result = util::split_words(text, Some(data), "en", false, true);
        match result {
            Ok(tokens) => {
                if text.matches('\n').count() as f64 / tokens.len() as f64 > self.new_line_ratio {
                    return Ok(false);
                }
            }
            Err(e) => {
                println!("split words failed: {}", e);
                return Err(e);
            }
        }

        Ok(true)
    }

    fn name(&self) -> &str {
        "FinewebQualityFilter"
    }
}
