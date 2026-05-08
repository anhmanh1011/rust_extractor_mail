use extractor_core::{Matcher, Mode, Scanner, ScanError, LineSink};

struct FailingSink;
impl LineSink for FailingSink {
    type Error = &'static str;
    fn emit(&mut self, _line: &[u8]) -> Result<(), Self::Error> {
        Err("nope")
    }
}

#[test]
fn sink_error_propagates() {
    let m = Matcher::new("gmail.com", Mode::Plain).unwrap();
    let mut s = Scanner::new(&m);
    let r = s.scan_all(b"gmail.com:a:b\n", &mut FailingSink);
    assert!(matches!(r, Err(ScanError::Sink("nope"))));
}
