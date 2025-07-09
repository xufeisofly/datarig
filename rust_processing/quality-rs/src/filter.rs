use crate::util;
use anyhow::{Error, Result};
use serde_json::Value;

// 想要添加规则，新建一个 Filter struct 实现这个 trait 就好
pub trait Filter {
    fn filter(&self, data: &mut Value) -> Result<bool, Error>;
    #[allow(dead_code)]
    fn name(&self) -> &str;
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

impl Filter for GopherRepetitionFilter {
    fn filter(&self, data: &mut Value) -> Result<bool, Error> {
        let text = match data.get(util::TEXT_KEY).and_then(Value::as_str) {
            Some(s) if !s.trim().is_empty() => s.trim(),
            _ => return Ok(false),
        };

        let text_len = text.len() as f64;

        let paragraphs = util::split_paragraphs(text);

        if !paragraphs.is_empty() {
            let (para_duplicates, para_char_duplicates) = util::find_duplicates(&paragraphs);
            if para_duplicates as f64 / paragraphs.len() as f64 > self.dup_para_frac {
                return Ok(false);
            }
            if para_char_duplicates as f64 / text_len > self.dup_para_char_frac {
                return Ok(false);
            }
        }

        let lines = util::split_lines(text);
        if !lines.is_empty() {
            let (line_duplicates, line_char_duplicates) = util::find_duplicates(&lines);
            if line_duplicates as f64 / lines.len() as f64 > self.dup_line_frac {
                return Ok(false);
            }
            if line_char_duplicates as f64 / text_len > self.dup_line_char_frac {
                return Ok(false);
            }
        }

        let words_result = util::split_words(text, Some(data), &self.lang, false, true);
        match words_result {
            Ok(words) => {
                for &(n, n_frac) in &self.top_n_grams {
                    let n_grams = util::get_n_grams(&words, n as usize);
                    if n_grams.is_empty() {
                        continue;
                    }
                    let top_char_length = util::find_top_duplicate(&n_grams);
                    if top_char_length as f64 / text_len > n_frac {
                        return Ok(false);
                    }
                }

                for &(n, n_frac) in &self.dup_n_grams {
                    let n_duplicates_char = util::find_all_duplicate_fast(&words, n as usize);
                    if n_duplicates_char as f64 / text_len > n_frac {
                        return Ok(false);
                    }
                }
            }
            Err(e) => {
                println!("split words failed, error: {}", e);
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
