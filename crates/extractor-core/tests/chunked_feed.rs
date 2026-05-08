use extractor_core::{Matcher, Mode, Scanner};
use proptest::prelude::*;

/// Generate realistic-ish input: a mix of matching and non-matching lines.
fn lines_strategy() -> impl Strategy<Value = Vec<u8>> {
    let line_strat = prop_oneof![
        // matching plain
        Just(b"gmail.com:user@x.com:pass1234".to_vec()),
        Just(b"mail.gmail.com:bob@y:p".to_vec()),
        Just(b"foo.bar.gmail.com:c:d".to_vec()),
        // non-matching
        Just(b"yahoo.com:u:p".to_vec()),
        Just(b"xgmail.com:u:p".to_vec()),
        Just(b"gmail.com.vn:u:p".to_vec()),
        Just(b"random garbage line".to_vec()),
        // empty
        Just(Vec::<u8>::new()),
    ];
    prop::collection::vec(line_strat, 0..50).prop_map(|lines| {
        let mut out = Vec::new();
        for l in lines {
            out.extend_from_slice(&l);
            out.push(b'\n');
        }
        out
    })
}

fn split_at_indices(buf: &[u8], indices: &[usize]) -> Vec<Vec<u8>> {
    let mut sorted: Vec<usize> = indices.iter().copied().collect();
    sorted.sort_unstable();
    sorted.dedup();
    sorted.retain(|&i| i <= buf.len());
    let mut chunks = Vec::new();
    let mut prev = 0;
    for i in sorted {
        chunks.push(buf[prev..i].to_vec());
        prev = i;
    }
    chunks.push(buf[prev..].to_vec());
    chunks
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]
    #[test]
    fn scan_all_equals_chunked_feed(
        buf in lines_strategy(),
        splits in prop::collection::vec(any::<usize>(), 0..16),
    ) {
        let m = Matcher::new("gmail.com", Mode::Plain).unwrap();

        // Reference: scan_all on the whole buffer
        let mut ref_out: Vec<u8> = Vec::new();
        let mut s1 = Scanner::new(&m);
        s1.scan_all(&buf, &mut ref_out).unwrap();

        // Subject: feed in chunks
        let chunks = split_at_indices(&buf, &splits);
        let mut sub_out: Vec<u8> = Vec::new();
        let mut s2 = Scanner::new(&m);
        for c in &chunks {
            s2.feed(c, &mut sub_out).unwrap();
        }
        s2.finish(&mut sub_out).unwrap();

        prop_assert_eq!(ref_out, sub_out);
    }
}
