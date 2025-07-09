use anyhow::{Error, Result};
use serde_json::Value;
use std::collections::HashSet;
use vtext::tokenize::{Tokenizer, VTextTokenizerParams};

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

pub fn split_words(
    text: &str,
    lang: &str,
    ignore_punctuation: bool,
    ignore_whitespace: bool,
) -> Result<Vec<String>, Error> {
    let tok = VTextTokenizerParams::default().lang(lang).build()?;
    let mut tokens: Vec<String> = tok.tokenize(text).map(|s| s.to_string()).collect();

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

pub fn clear_text_key(data: &mut Value) {
    if let Value::Object(ref mut map) = data {
        map.remove("text");
    }
}
