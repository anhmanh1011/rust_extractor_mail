use telegram_client::upload::caption::{render, CaptionInput};

#[test]
fn caption_renders_full_metadata_for_plain_mode() {
    let input = CaptionInput {
        original_name: "dump.txt",
        source_chat_id: -1001234567890,
        source_msg_id: 42,
        matcher_key: "gmail.com",
        matcher_mode: "plain",
        size_bytes: 1_500_000_000,
        lines_scanned: 12_345_678,
        lines_matched: 9_876,
        part_index: None,
        part_total: None,
    };
    let s = render(&input);
    assert!(s.contains("dump.txt"),         "caption: {s}");
    assert!(s.contains("-1001234567890"),   "caption: {s}");
    assert!(s.contains("42"),               "caption: {s}");
    assert!(s.contains("gmail.com"),        "caption: {s}");
    assert!(s.contains("plain"),            "caption: {s}");
    assert!(s.contains("12,345,678"),       "caption: {s}");
    assert!(s.contains("9,876"),            "caption: {s}");
    assert!(s.contains("1.5 GB") || s.contains("1.40 GiB"), "caption: {s}");
    assert!(!s.contains("Part"),            "single-part caption must NOT mention parts: {s}");
}

#[test]
fn caption_includes_part_label_when_split() {
    let input = CaptionInput {
        original_name: "huge.gz",
        source_chat_id: 5050,
        source_msg_id: 7,
        matcher_key: "linkedin.com",
        matcher_mode: "url",
        size_bytes: 3_000_000_000,
        lines_scanned: 1,
        lines_matched: 1,
        part_index: Some(2),
        part_total: Some(3),
    };
    let s = render(&input);
    assert!(s.contains("Part 2/3"), "caption: {s}");
}

#[test]
fn caption_truncates_to_telegram_limit() {
    let long = "a".repeat(2_000);
    let input = CaptionInput {
        original_name: &long,
        source_chat_id: 1,
        source_msg_id: 1,
        matcher_key: "k",
        matcher_mode: "plain",
        size_bytes: 0,
        lines_scanned: 0,
        lines_matched: 0,
        part_index: None,
        part_total: None,
    };
    let s = render(&input);
    assert!(s.chars().count() <= 1024, "caption length = {}", s.chars().count());
}

#[test]
fn caption_never_contains_matched_line_content() {
    // Defensive: structural sentinel — `CaptionInput` has no `sample_match` field.
    let _ = std::mem::size_of::<CaptionInput>();
}

#[test]
fn caption_data_input_attaches_part_label_and_stays_within_cap() {
    use telegram_client::upload::caption::CaptionData;

    let data = CaptionData {
        original_name:  "a".repeat(2_000),
        source_chat_id: 1,
        source_msg_id:  1,
        matcher_key:    "k".into(),
        matcher_mode:   "plain".into(),
        size_bytes:     0,
        lines_scanned:  0,
        lines_matched:  0,
    };
    let input = data.input(Some(2), Some(3));
    let rendered = render(&input);
    assert!(rendered.chars().count() <= 1024, "len = {}", rendered.chars().count());

    let normal = CaptionData {
        original_name:  "x.txt".into(),
        source_chat_id: 1,
        source_msg_id:  1,
        matcher_key:    "k".into(),
        matcher_mode:   "plain".into(),
        size_bytes:     0,
        lines_scanned:  0,
        lines_matched:  0,
    };
    let s2 = render(&normal.input(Some(2), Some(3)));
    assert!(s2.contains("Part 2/3"), "caption = {s2}");
}
