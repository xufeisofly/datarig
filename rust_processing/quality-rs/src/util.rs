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
    let mut unique = HashSet::with_capacity(x.len());
    let mut dup_cnt = 0;
    let mut dup_chars = 0;

    for &e in x {
        if !unique.insert(e) {
            dup_cnt += 1;
            dup_chars += e.len();
        }
    }
    (dup_cnt, dup_chars)
}

pub fn find_top_duplicate(x: &[String]) -> usize {
    let mut counter = HashMap::with_capacity(x.len());

    // 只做一次哈希查找
    for s in x {
        *counter.entry(s.as_str()).or_insert(0) += 1;
    }

    // 查找 max
    counter
        .into_iter()
        .map(|(word, cnt)| word.len() * cnt)
        .max()
        .unwrap_or(0)
}

pub fn find_all_duplicate_fast(words: &[String], n: usize) -> usize {
    let m = words.len();
    if m < n || n == 0 {
        return 0;
    }

    // 1. 先把每个 word 哈希成 u64
    let mut word_hashes: Vec<u64> = Vec::with_capacity(m);
    for w in words {
        let mut h: u64 = 0;
        for &b in w.as_bytes() {
            // 这里给常量加上 u64 后缀
            h = h.wrapping_mul(1315423911u64).wrapping_add(b as u64);
        }
        word_hashes.push(h);
    }

    // 2. 预计算 base^i，也都是 u64
    let mut pow: Vec<u64> = Vec::with_capacity(m + 1);
    pow.push(1u64); // 这里的 1u64
    for i in 1..=m {
        // 这里的 wrapping_mul 也是 u64
        pow.push(pow[i - 1].wrapping_mul(1315423911u64));
    }

    // 3. 前缀哈希，类型都是 u64
    let mut pref: Vec<u64> = Vec::with_capacity(m + 1);
    pref.push(0u64); // 明确是 u64
    for i in 0..m {
        pref.push(
            pref[i]
                .wrapping_mul(1315423911u64)
                .wrapping_add(word_hashes[i]),
        );
    }

    // 4. 预存每个 word 长度
    let lens: Vec<usize> = words.iter().map(|w| w.len()).collect();

    // 5. 滑窗检测
    let mut seen = HashSet::with_capacity(m);
    let mut dup_chars = 0;
    let mut i = 0;

    while i + n <= m {
        // 计算 window [i, i+n) 的哈希
        // 这里 wrapping_sub 也是 u64
        let h = pref[i + n].wrapping_sub(pref[i].wrapping_mul(pow[n]));

        if !seen.insert(h) {
            // 只有重复时才计算长度
            let window_len: usize = lens[i..i + n].iter().sum();
            dup_chars += window_len;
            i += n;
        } else {
            i += 1;
        }
    }

    dup_chars
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
