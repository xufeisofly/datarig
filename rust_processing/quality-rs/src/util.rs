use anyhow::{Error, Result};
use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use vtext::tokenize::{Tokenizer, VTextTokenizerParams};

pub const WORDS_KEY: &str = "words";
pub const TEXT_KEY: &str = "text";
static PARAGRAPH_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\n{2,}").unwrap());
static LINE_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\n+").unwrap());
pub static TERMINAL_PUNCTUATION: [&str; 159] = [
    "᪩",
    "？",
    "⁈",
    "𑩂",
    "．",
    "꩞",
    "𑅃",
    "﹗",
    "𑂾",
    "\u{1B7D}",
    "፧",
    "𑅂",
    "꡶",
    "꘎",
    "⁉",
    "࠾",
    "᪨",
    "𑊩",
    "𑱂",
    "᱿",
    "𖩮",
    "᥅",
    "\u{11F43}",
    "\u{11F44}",
    "﹒",
    "𑈹",
    "𑈸",
    "።",
    "܂",
    "؞",
    "꛳",
    "\u{10F88}",
    "𑗍",
    "𐩖",
    "𑙂",
    "\u{061D}",
    "꩟",
    "᠉",
    "\u{1B7E}",
    "𑗗",
    "᰼",
    "𑻸",
    "？",
    "𑪜",
    "꧉",
    "𑗉",
    "𐽙",
    "𖫵",
    "𖬷",
    "܀",
    "꓿",
    "᜵",
    "𑗏",
    "𑁇",
    "𑗓",
    "𑥄",
    "៖",
    "𑥆",
    "𑗑",
    "𑗒",
    "꯫",
    "۔",
    "𐩗",
    "\u{10F86}",
    "꡷",
    "\u{2E54}",
    "｡",
    "៕",
    "߹",
    "⸮",
    ".",
    "𑇅",
    "࠹",
    "𛲟",
    "꫰",
    "꤯",
    "𐽗",
    "᭞",
    "𑜼",
    "፨",
    "𑃁",
    "꣏",
    "𑇟",
    "𖬸",
    "𑪛",
    "𑜾",
    "࠷",
    "𝪈",
    "?",
    "𑃀",
    "𑗃",
    "！",
    "։",
    "꣎",
    "॥",
    "𑗖",
    "᭛",
    "᠃",
    "!",
    "၊",
    "𖺘",
    "⁇",
    "𑗌",
    "𑑋",
    "𖭄",
    "᭟",
    "𑅁",
    "𑙁",
    "⸼",
    "꩝",
    "𑗋",
    "。",
    "꧈",
    "꫱",
    "𑜽",
    "𐽖",
    "𑂿",
    "᙮",
    "។",
    "꛷",
    "\u{10F89}",
    "៚",
    "᥄",
    "𑗕",
    "𑗎",
    "᪪",
    "᭚",
    "࠽",
    "𑇞",
    "𑗊",
    "𐽘",
    "\u{2E53}",
    "𑗔",
    "𖩯",
    "𑇍",
    "𑻷",
    "𐽕",
    "𑩃",
    "।",
    "𑗂",
    "𑇆",
    "𑁈",
    "။",
    "᱾",
    "𑱁",
    "꘏",
    "܁",
    "᜶",
    "‼",
    "𑈻",
    "‽",
    "᪫",
    "﹖",
    "𑑌",
    "𑈼",
    "\u{10F87}",
    "𑗐",
    "៙",
    "᰻",
];

pub fn find_duplicates(x: &[&str]) -> (usize, usize) {
    let mut unique_x = HashSet::new();
    let mut duplicate_elements = 0;
    let mut duplicate_chars = 0;

    for &element in x {
        if unique_x.contains(element) {
            duplicate_elements += 1;
            duplicate_chars += element.len();
        } else {
            unique_x.insert(element);
        }
    }

    (duplicate_elements, duplicate_chars)
}

pub fn find_top_duplicate(x: &[String]) -> usize {
    let mut counter: HashMap<&str, usize> = HashMap::new();

    for element in x {
        *counter.entry(element.as_str()).or_insert(0) += 1;
    }

    if let Some((word, count)) = counter.into_iter().max_by_key(|&(_, count)| count) {
        word.len() * count
    } else {
        0
    }
}

pub fn find_all_duplicate(words: &[String], n: usize) -> usize {
    let mut unique = HashSet::new();
    let mut repeated_chars = 0;
    let mut idx = 0;
    let n_words = words.len();

    while idx + n <= n_words {
        let n_gram = words[idx..idx + n].join("");

        if unique.contains(&n_gram) {
            repeated_chars += n_gram.len();
            idx += n;
        } else {
            unique.insert(n_gram);
            idx += 1;
        }
    }

    let total_chars: usize = words.iter().map(|w| w.len()).sum();
    assert!(repeated_chars <= total_chars);

    repeated_chars
}

pub fn split_words(
    text: &str,
    data: Option<&Value>,
    lang: &str,
    ignore_punctuation: bool,
    ignore_whitespace: bool,
) -> Result<Vec<String>, Error> {
    let mut tokens: Vec<String> = match data {
        Some(page) => page
            .get(WORDS_KEY)
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default(),
        None => {
            vec![]
        }
    };

    if tokens.is_empty() {
        let tok = VTextTokenizerParams::default().lang(lang).build()?;
        tokens = tok.tokenize(text).map(|s| s.to_string()).collect();
    }

    if ignore_whitespace {
        tokens = tokens.iter().map(|w| w.trim().to_string()).collect();
    }
    if ignore_punctuation {
        tokens = tokens
            .into_iter()
            .filter(|w| {
                w.chars()
                    .next()
                    .map(|c| c.is_alphanumeric() || (!ignore_whitespace && c.is_whitespace()))
                    .unwrap_or(false)
            })
            .collect();
    }

    Ok(tokens)
}

pub fn split_paragraphs(text: &str) -> Vec<&str> {
    PARAGRAPH_RE.split(text).collect()
}

pub fn split_lines(text: &str) -> Vec<&str> {
    LINE_RE.split(text).collect()
}

pub fn clear_key(data: &mut Value, key: &str) {
    if let Value::Object(ref mut map) = data {
        map.remove(key);
    }
}

pub fn get_n_grams(words: &[String], n: usize) -> Vec<String> {
    if words.len() < n {
        return vec![];
    }

    (0..=words.len() - n)
        .map(|i| words[i..i + n].join(" "))
        .collect()
}
