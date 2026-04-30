use quiche::h3;
use sfv::{Dictionary, FieldType, ListEntry};

const DEFAULT_URGENCY: u8 = 3;
const DEFAULT_INCREMENTAL: bool = false;
const MIN_URGENCY: i64 = 0;
const MAX_URGENCY: i64 = 7;

/// Parses a `Vec<u8>`, returning a quiche `Priority`.
pub fn parse_raw_priority(raw_prio: Option<Vec<u8>>) -> Option<h3::Priority> {
    let mut urgency = DEFAULT_URGENCY;
    let mut incremental = DEFAULT_INCREMENTAL;

    let prio_bytes = raw_prio.unwrap_or_default();
    let parsed_prio = sfv::Parser::new(&prio_bytes);

    if let Ok(dict) = Dictionary::parse(parsed_prio) {
        urgency = dict
            .get("u")
            .and_then(|prio| match prio {
                ListEntry::Item(item) => item.bare_item.as_integer(),
                _ => None,
            })
            .map(|u| i64::from(u).clamp(MIN_URGENCY, MAX_URGENCY) as u8)
            .unwrap_or(DEFAULT_URGENCY);

        incremental = match dict.get("i") {
            Some(ListEntry::Item(item)) => {
                item.bare_item.as_boolean().unwrap_or(false)
            }
            _ => false,
        };
    }

    Some(h3::Priority::new(urgency, incremental))
}

/// Extracts the `sched` token from an EPS priority header, if present.
///
/// The `sched` parameter selects the multipath packet-scheduling algorithm
/// on the proxy for the outer QUIC connection.  It accepts both full names
/// and common abbreviations:
///
/// | `sched=` value | Algorithm       |
/// |----------------|-----------------|
/// | `lowrtt`       | Low-RTT (default) |
/// | `minrtt`       | Min-RTT         |
/// | `rr`           | Round-Robin     |
/// | `rand`         | Random          |
/// | `ll`           | Lowest-Latency  |
/// | `ecf`          | Earliest Completion First |
/// | `sa-ecf`       | Stream-Aware ECF |
///
/// Returns `None` when the header is absent, malformed, or has no `sched`
/// member.
pub fn parse_scheduler_hint(raw_prio: Option<Vec<u8>>) -> Option<String> {
    let prio_bytes = raw_prio?;
    let parsed_prio = sfv::Parser::new(&prio_bytes);
    let dict = Dictionary::parse(parsed_prio).ok()?;

    match dict.get("sched") {
        Some(ListEntry::Item(item)) => {
            item.bare_item.as_token().map(|t| t.as_str().to_owned())
        }
        _ => None,
    }
}

#[cfg(test)]
mod eps {
    use super::*;

    fn bytes(s: &str) -> Option<Vec<u8>> {
        Some(s.as_bytes().to_vec())
    }

    // --- parse_scheduler_hint tests ---

    #[test]
    fn scheduler_hint_absent_returns_none() {
        assert_eq!(parse_scheduler_hint(None), None);
    }

    #[test]
    fn scheduler_hint_empty_returns_none() {
        assert_eq!(parse_scheduler_hint(Some(Vec::new())), None);
    }

    #[test]
    fn scheduler_hint_no_sched_field_returns_none() {
        assert_eq!(parse_scheduler_hint(bytes("u=0")), None);
        assert_eq!(parse_scheduler_hint(bytes("i")), None);
        assert_eq!(parse_scheduler_hint(bytes("u=4, i")), None);
    }

    #[test]
    fn scheduler_hint() {
        assert_eq!(
            parse_scheduler_hint(bytes("u=4, i, sched=rr")),
            Some("rr".to_owned())
        );

        assert_eq!(
            parse_scheduler_hint(bytes("u=4, i, sched=roundrobin")),
            Some("roundrobin".to_owned())
        );
    }

    #[test]
    fn scheduler_hint_sched_only() {
        assert_eq!(
            parse_scheduler_hint(bytes("sched=ll")),
            Some("ll".to_owned())
        );
    }

    #[test]
    fn scheduler_hint_non_token_value_returns_none() {
        // SFV string values ("...") are not tokens; should return None.
        assert_eq!(parse_scheduler_hint(bytes("sched=\"rr\"")), None);
    }

    #[test]
    fn returns_defaults_on_none() {
        let prio = parse_raw_priority(None).expect("Default priority");
        assert_eq!(prio, h3::Priority::default());
    }

    #[test]
    fn returns_defaults_on_empty() {
        let prio =
            parse_raw_priority(Some(Vec::new())).expect("Default priority");
        assert_eq!(prio, h3::Priority::default());
    }

    #[test]
    fn parses_urgency_only() {
        let prio = parse_raw_priority(bytes("u=5")).expect("Urgency of 5");
        assert_eq!(prio, h3::Priority::new(5, false));
    }

    #[test]
    fn parses_incremental_only() {
        let prio = parse_raw_priority(bytes("i"))
            .expect("Default prio with incremental flag set");
        assert_eq!(prio, h3::Priority::new(3, true));
    }

    #[test]
    fn parses_both_fields() {
        let prio = parse_raw_priority(bytes("u=2, i"))
            .expect("Parses both EPS parameters");
        assert_eq!(prio, h3::Priority::new(2, true));
    }

    #[test]
    fn parses_both_fields_despite_sched() {
        let prio = parse_raw_priority(bytes("u=2, i, sched=ecf"))
            .expect("Parses both EPS parameters");
        assert_eq!(prio, h3::Priority::new(2, true));
    }

    #[test]
    fn parses_both_fields_starting_with_sched() {
        let prio = parse_raw_priority(bytes("sched=ecf,i, u=2"))
            .expect("Parses both EPS parameters");
        assert_eq!(prio, h3::Priority::new(2, true));
    }

    #[test]
    fn clamps_urgency_high() {
        let prio =
            parse_raw_priority(bytes("u=10, i")).expect("Clamps a high urgency");
        assert_eq!(prio, h3::Priority::new(7, true));
    }

    #[test]
    fn clamps_urgency_low() {
        let prio =
            parse_raw_priority(bytes("u=-5")).expect("Clamps a low urgency");
        assert_eq!(prio, h3::Priority::new(0, false));
    }

    #[test]
    fn invalid_types_fall_back_to_defaults() {
        let prio = parse_raw_priority(bytes("u=\"x\""))
            .expect("Non-integer urgency falls back to default");
        assert_eq!(prio, h3::Priority::default());

        let prio = parse_raw_priority(bytes("i=5"))
            .expect("Non-boolean incremental falls back to default");
        assert_eq!(prio, h3::Priority::default());
    }

    #[test]
    fn malformed_dict_falls_back_to_defaults() {
        let prio = parse_raw_priority(bytes("u"))
            .expect("Malformed structured dictionary falls back to default");
        assert_eq!(prio, h3::Priority::default());
    }

    #[test]
    fn parses_stream_aware_ecf_with_priority() {
        let raw = bytes("u=0,i,sched=sa-ecf");

        let prio = parse_raw_priority(raw.clone()).expect("Parses priority");
        assert_eq!(prio, h3::Priority::new(0, true));

        let sched = parse_scheduler_hint(raw);
        assert_eq!(sched, Some("sa-ecf".to_owned()));
    }
}
