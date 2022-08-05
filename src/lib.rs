#![allow(dead_code)]

/// Extracts last n characters from a &String.
/// If text is shorter than n, returns full text
pub fn slice_from_end(text: &String, n: usize) -> Option<&str> {
    text.char_indices().rev().nth(n).map(|(i, _)| &text[i..]).or(Some(text))
}

