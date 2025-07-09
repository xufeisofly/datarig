use crate::util;
use anyhow::{Error, Result};
use serde_json::Value;

// 想要添加规则，新建一个 Filter struct 实现这个 trait 就好
pub trait Filter {
    fn filter(&self, data: &mut Value) -> Result<bool, Error>;
}

// fineweb quality 规则
pub struct FinewebQualityFilter {
    pub line_punct_thr: f64,
    pub short_line_length: usize,
    pub short_line_thr: f64,
    pub char_duplicates_ratio: f64,
    pub new_line_ratio: f64,
}

impl Filter for FinewebQualityFilter {
    fn filter(&self, data: &mut Value) -> Result<bool, Error> {
        let text = data["text"].as_str().unwrap();
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

        let result = util::split_words(text, "en", false, true);
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
}
