use std::collections::BTreeMap;

use rpds::RedBlackTreeMapSync;
use rust_decimal::Decimal;

use super::super::{Fault, Mark, Product};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::binding::matcher::replay_transition::synthetic_risk) struct MarkHistory {
    by_product: BTreeMap<Product, RedBlackTreeMapSync<i64, Mark>>,
}

impl MarkHistory {
    pub(in crate::binding::matcher::replay_transition::synthetic_risk) fn try_from_vec(
        marks: Vec<Mark>,
    ) -> Result<Self, Fault> {
        let mut history = Self {
            by_product: BTreeMap::new(),
        };
        for mark in marks {
            validate_scalar(&mark)?;
            let tree = history.by_product.entry(mark.product.clone()).or_default();
            validate_neighbors(tree, &mark)?;
            *tree = tree.insert(mark.valid_from, mark);
        }
        Ok(history)
    }

    pub(in crate::binding::matcher::replay_transition::synthetic_risk) fn mark_at(
        &self,
        product: &Product,
        at: i64,
    ) -> Result<&Mark, Fault> {
        self.by_product
            .get(product)
            .and_then(|tree| tree.range(..=at).next_back().map(|(_, mark)| mark))
            .filter(|mark| mark.valid_from <= at && at < mark.valid_to)
            .ok_or("MARK_COVERAGE_MISSING")
    }

    pub(in crate::binding::matcher::replay_transition::synthetic_risk::scenario_account) fn stage_historical_update<
        'a,
    >(
        &self,
        at: i64,
        steps: impl IntoIterator<Item = (&'a Product, Decimal)>,
    ) -> Result<Self, Fault> {
        let mut candidate = self.clone();
        for (product, price) in steps {
            let tree = candidate.by_product.entry(product.clone()).or_default();
            if let Some((from, prior)) = tree.range(..=at).next_back() {
                if prior.valid_from < at && at < prior.valid_to {
                    *tree = tree.insert(
                        *from,
                        Mark {
                            valid_to: at,
                            ..prior.clone()
                        },
                    );
                }
            }
            *tree = tree.remove(&at);
            let mark = Mark {
                product: product.clone(),
                price,
                valid_from: at,
                valid_to: i64::MAX,
            };
            validate_scalar(&mark)?;
            validate_neighbors(tree, &mark)?;
            *tree = tree.insert(at, mark);
        }
        Ok(candidate)
    }

    #[cfg(test)]
    pub(super) fn to_vec_for_test(&self) -> Vec<Mark> {
        self.by_product
            .values()
            .flat_map(|tree| tree.iter().map(|(_, mark)| mark.clone()))
            .collect()
    }
}

fn validate_scalar(mark: &Mark) -> Result<(), Fault> {
    if mark.price <= Decimal::ZERO || mark.valid_from < 0 || mark.valid_to <= mark.valid_from {
        return Err("INVALID_MARK");
    }
    Ok(())
}

fn validate_neighbors(tree: &RedBlackTreeMapSync<i64, Mark>, mark: &Mark) -> Result<(), Fault> {
    if tree
        .range(..=mark.valid_from)
        .next_back()
        .is_some_and(|(_, prior)| {
            prior.valid_from < mark.valid_to && mark.valid_from < prior.valid_to
        })
        || tree
            .range(mark.valid_from..)
            .next()
            .is_some_and(|(_, next)| {
                next.valid_from < mark.valid_to && mark.valid_from < next.valid_to
            })
    {
        return Err("OVERLAPPING_MARKS");
    }
    Ok(())
}

fn assert_send<T: Send>() {}
const _: fn() = || {
    assert_send::<MarkHistory>();
    assert_send::<super::ScenarioAccount>();
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::matcher::replay_transition::synthetic_risk::scenario_account::mark_history::MarkHistory;

    fn row(product: Product, price: i64, from: i64, to: i64) -> Mark {
        Mark {
            product,
            price: Decimal::from(price),
            valid_from: from,
            valid_to: to,
        }
    }

    fn oracle_validate(rows: &[Mark]) -> Result<(), Fault> {
        super::super::super::validate_marks(rows)
    }

    fn oracle_at<'a>(rows: &'a [Mark], product: &Product, at: i64) -> Result<&'a Mark, Fault> {
        rows.iter()
            .find(|mark| mark.product == *product && mark.valid_from <= at && at < mark.valid_to)
            .ok_or("MARK_COVERAGE_MISSING")
    }

    fn oracle_stage(rows: &[Mark], at: i64, steps: &[(Product, i64)]) -> Result<Vec<Mark>, Fault> {
        let mut candidate = rows.to_vec();
        for (product, price) in steps {
            for mark in candidate.iter_mut().filter(|mark| {
                mark.product == *product && mark.valid_from < at && at < mark.valid_to
            }) {
                mark.valid_to = at;
            }
            candidate.retain(|mark| !(mark.product == *product && mark.valid_from == at));
            candidate.push(row(product.clone(), *price, at, i64::MAX));
        }
        oracle_validate(&candidate)?;
        Ok(candidate)
    }

    fn sort_rows(rows: &mut [Mark]) {
        rows.sort_by(|left, right| {
            left.product
                .cmp(&right.product)
                .then(left.valid_from.cmp(&right.valid_from))
        });
    }

    #[test]
    fn arbitrary_histories_match_source_order_oracle_faults() {
        let candidates = [
            vec![row(Product::Btc, 2, 0, 10), row(Product::Btc, 3, 5, 12)],
            vec![row(Product::Btc, 2, 0, 10), row(Product::Eth, 0, 0, 5)],
            vec![row(Product::Btc, 2, 0, 10), row(Product::Btc, 0, 3, 4)],
            vec![row(Product::Btc, 0, 0, 2), row(Product::Btc, 2, 0, 8)],
            vec![row(Product::Eth, 2, 10, 20), row(Product::Btc, 3, 0, 10)],
            vec![row(Product::Btc, 2, 10, 10)],
        ];
        for rows in candidates {
            assert_eq!(
                MarkHistory::try_from_vec(rows.clone()).map(|_| ()),
                oracle_validate(&rows)
            );
        }
    }

    #[test]
    fn exhaustive_short_histories_match_arbitrary_vec_oracle_faults() {
        let rows = [
            row(Product::Btc, 0, 0, 4),
            row(Product::Btc, 1, -1, 4),
            row(Product::Btc, 1, 0, 0),
            row(Product::Btc, 1, 0, 4),
            row(Product::Btc, 1, 2, 6),
            row(Product::Btc, 1, 4, 8),
            row(Product::Eth, 1, 0, 4),
        ];
        for length in 0..=4 {
            for mut encoded in 0..7_usize.pow(length as u32) {
                let mut candidate = Vec::with_capacity(length);
                for _ in 0..length {
                    candidate.push(rows[encoded % rows.len()].clone());
                    encoded /= rows.len();
                }
                assert_eq!(
                    MarkHistory::try_from_vec(candidate.clone()).map(|_| ()),
                    oracle_validate(&candidate),
                    "{candidate:?}"
                );
            }
        }
    }

    #[test]
    fn predecessor_lookup_matches_vec_for_valid_intervals_and_gaps() {
        let rows = vec![
            row(Product::Btc, 1, 0, 5),
            row(Product::Btc, 2, 8, 12),
            row(Product::Eth, 3, 2, 9),
        ];
        let history = MarkHistory::try_from_vec(rows.clone()).unwrap();
        for product in [Product::Btc, Product::Eth] {
            for at in -2..=15 {
                assert_eq!(
                    history.mark_at(&product, at),
                    oracle_at(&rows, &product, at)
                );
            }
        }
    }

    #[test]
    fn historical_updates_match_vec_oracle_including_future_seed_collisions() {
        let rows = vec![
            row(Product::Btc, 1, 0, 100),
            row(Product::Btc, 2, 150, 200),
            row(Product::Eth, 3, 0, 100),
        ];
        let history = MarkHistory::try_from_vec(rows.clone()).unwrap();
        for (at, steps) in [
            (50, vec![(Product::Btc, 4)]),
            (150, vec![(Product::Btc, 5)]),
            (120, vec![(Product::Btc, 6)]),
        ] {
            let mut actual = history
                .stage_historical_update(at, steps.iter().map(|(p, n)| (p, Decimal::from(*n))))
                .map(|next| next.to_vec_for_test());
            let mut expected = oracle_stage(&rows, at, &steps);
            if let Ok(actual) = &mut actual {
                sort_rows(actual);
            }
            if let Ok(expected) = &mut expected {
                sort_rows(expected);
            }
            assert_eq!(actual, expected);
        }
        assert_eq!(
            history.stage_historical_update(120, [(&Product::Btc, Decimal::from(6))]),
            Err("OVERLAPPING_MARKS")
        );
        assert_eq!(
            history.mark_at(&Product::Btc, 120),
            Err("MARK_COVERAGE_MISSING")
        );
    }

    #[test]
    fn persistent_clone_branches_are_isolated() {
        let root = MarkHistory::try_from_vec(vec![row(Product::Btc, 1, 0, 100)]).unwrap();
        let left = root
            .clone()
            .stage_historical_update(40, [(&Product::Btc, Decimal::from(2))])
            .unwrap();
        let right = root
            .clone()
            .stage_historical_update(60, [(&Product::Btc, Decimal::from(3))])
            .unwrap();
        assert_eq!(root.mark_at(&Product::Btc, 50).unwrap().price, Decimal::ONE);
        assert_eq!(
            left.mark_at(&Product::Btc, 50).unwrap().price,
            Decimal::from(2)
        );
        assert_eq!(
            right.mark_at(&Product::Btc, 50).unwrap().price,
            Decimal::ONE
        );
        assert_eq!(
            right.mark_at(&Product::Btc, 70).unwrap().price,
            Decimal::from(3)
        );
    }

    #[test]
    fn nested_clone_branch_keeps_all_ancestor_contents() {
        let root = MarkHistory::try_from_vec(vec![row(Product::Btc, 1, 0, 100)]).unwrap();
        let left = root
            .stage_historical_update(40, [(&Product::Btc, Decimal::from(2))])
            .unwrap();
        let nested = left
            .clone()
            .stage_historical_update(70, [(&Product::Btc, Decimal::from(3))])
            .unwrap();
        assert_eq!(root.to_vec_for_test(), vec![row(Product::Btc, 1, 0, 100)]);
        assert_eq!(
            left.to_vec_for_test(),
            vec![
                row(Product::Btc, 1, 0, 40),
                row(Product::Btc, 2, 40, i64::MAX)
            ]
        );
        assert_eq!(
            nested.to_vec_for_test(),
            vec![
                row(Product::Btc, 1, 0, 40),
                row(Product::Btc, 2, 40, 70),
                row(Product::Btc, 3, 70, i64::MAX)
            ]
        );
        assert_eq!(root.mark_at(&Product::Btc, 80).unwrap().price, Decimal::ONE);
        assert_eq!(
            left.mark_at(&Product::Btc, 80).unwrap().price,
            Decimal::from(2)
        );
    }

    #[test]
    fn source_order_builder_preserves_cross_product_fault_precedence() {
        let overlap_then_invalid = vec![
            row(Product::Btc, 1, 0, 10),
            row(Product::Btc, 2, 5, 8),
            row(Product::Eth, 0, 0, 2),
        ];
        let invalid_then_overlap = vec![
            row(Product::Eth, 0, 0, 2),
            row(Product::Btc, 1, 0, 10),
            row(Product::Btc, 2, 5, 8),
        ];
        assert_eq!(
            MarkHistory::try_from_vec(overlap_then_invalid).map(|_| ()),
            Err("OVERLAPPING_MARKS")
        );
        assert_eq!(
            MarkHistory::try_from_vec(invalid_then_overlap).map(|_| ()),
            Err("INVALID_MARK")
        );
    }

    #[test]
    fn rpds_backend_stays_private_to_history_map_adapters() {
        fn rust_files(path: &std::path::Path, output: &mut Vec<std::path::PathBuf>) {
            for entry in std::fs::read_dir(path).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    rust_files(&path, output);
                } else if path.extension().is_some_and(|extension| extension == "rs") {
                    output.push(path);
                }
            }
        }

        let crate_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let parent = crate_root.join("src/binding/matcher/replay_transition/synthetic_risk.rs");
        let account = crate_root
            .join("src/binding/matcher/replay_transition/synthetic_risk/scenario_account.rs");
        let subtree = crate_root
            .join("src/binding/matcher/replay_transition/synthetic_risk/scenario_account");
        let allowed = [
            subtree.join("mark_history.rs"),
            subtree.join("history_map.rs"),
        ];
        let mut files = vec![parent, account];
        rust_files(&subtree, &mut files);
        let forbidden = [
            "rpds",
            "archery",
            "RedBlackTreeMap",
            "SharedPointer",
            "ArcTK",
        ];
        for file in files.into_iter().filter(|file| !allowed.contains(file)) {
            let source = std::fs::read_to_string(&file).unwrap();
            for token in forbidden {
                assert!(
                    !source.contains(token),
                    "backend token {token:?} leaked into {}",
                    file.display()
                );
            }
        }

        let source = std::fs::read_to_string(&allowed[0]).unwrap();
        check_mark_history_source(&source, MARK_HISTORY_API).unwrap();
        let source = std::fs::read_to_string(&allowed[1]).unwrap();
        check_mark_history_source(&source, HISTORY_MAP_API).unwrap();
        for fixture in [
            "type Hidden = RedBlackTreeMapSync<i64, Mark>;\npub(super) fn leak() -> Hidden { todo!() }",
            "use rpds::RedBlackTreeMapSync as Backend;\npub(super) fn leak() -> Backend<i64, Mark> { todo!() }",
            "type First = usize;\ntype Second = First;\ntype Backend = RedBlackTreeMapSync<i64, Mark>;\npub fn leak() -> Second { todo!() }",
            "pub(super) fn leak(\n    value: Mark,\n) -> RedBlackTreeMapSync<i64, Mark> { todo!() }",
            "pub(super) fn new_api() -> usize { 1 }",
        ] {
            assert!(check_mark_history_source(fixture, &[]).is_err(), "checker accepted fixture: {fixture}");
        }
        for fixture in [
            "type Hidden =\n    RedBlackTreeMapSync<i64, Mark>;",
            "use rpds::{\n    RedBlackTreeMapSync as Backend,\n};",
            "type Hidden = rpds::RedBlackTreeMapSync<i64, Mark>;",
            "type First = RedBlackTreeMapSync<i64, Mark>;\ntype Second = First;",
        ] {
            assert!(
                check_mark_history_source(fixture, &[]).is_err(),
                "R2 accepted fixture: {fixture}"
            );
        }
        for fixture in [
            "pub(super) fn value() -> usize { let _ = RedBlackTreeMapSync::<i64, Mark>::new(); 1 }",
            "pub(super) trait Example { fn value() -> usize { let _ = RedBlackTreeMapSync::<i64, Mark>::new(); 1 } }",
            "// RedBlackTreeMapSync in a comment\npub(super) fn value() -> usize { let _ = \"rpds::RedBlackTreeMapSync\"; 1 }",
        ] {
            let allowed = extract_visible_api(fixture).unwrap();
            let allowlist: Vec<_> = allowed.iter().map(String::as_str).collect();
            assert!(check_mark_history_source(fixture, &allowlist).is_ok(), "checker rejected fixture: {fixture}");
        }
    }

    const MARK_HISTORY_API: &[&str] = &[
        "pub(in crate::binding::matcher::replay_transition::synthetic_risk) struct MarkHistory",
        "pub(in crate::binding::matcher::replay_transition::synthetic_risk) fn try_from_vec( marks: Vec<Mark>, ) -> Result<Self, Fault>",
        "pub(in crate::binding::matcher::replay_transition::synthetic_risk) fn mark_at( &self, product: &Product, at: i64, ) -> Result<&Mark, Fault>",
        "pub(in crate::binding::matcher::replay_transition::synthetic_risk::scenario_account) fn stage_historical_update< 'a, >( &self, at: i64, steps: impl IntoIterator<Item = (&'a Product, Decimal)>, ) -> Result<Self, Fault>",
        "pub(super) fn to_vec_for_test(&self) -> Vec<Mark>",
    ];

    const HISTORY_MAP_API: &[&str] = &[
        "pub(super) struct HistoryMap<K: Ord, V>(PersistentTree<K, V>)",
        "pub(super) fn new() -> Self",
        "pub(super) fn get<Q>(&self, key: &Q) -> Option<&V> where K: Borrow<Q>, Q: Ord + ?Sized,",
        "pub(super) fn contains_key<Q>(&self, key: &Q) -> bool where K: Borrow<Q>, Q: Ord + ?Sized,",
        "pub(super) fn insert(&mut self, key: K, value: V)",
        "pub(super) fn iter(&self) -> impl Iterator<Item = (&K, &V)>",
        "pub(super) fn values(&self) -> impl Iterator<Item = &V>",
        "pub(super) fn len(&self) -> usize",
        "pub(super) fn is_empty(&self) -> bool",
        "pub(super) fn clear(&mut self)",
    ];

    fn check_mark_history_source(source: &str, allowlist: &[&str]) -> Result<(), String> {
        let api = extract_visible_api(source)?;
        let forbidden = ["RedBlackTreeMap", "rpds", "archery"];
        for declaration in &api {
            if forbidden.iter().any(|token| declaration.contains(token)) {
                return Err(format!(
                    "backend token in visible declaration: {declaration}"
                ));
            }
        }
        let stripped = strip_comments_and_strings(source);
        for declaration in collect_type_and_use_declarations(&stripped) {
            let is_alias = declaration.starts_with("type ")
                || declaration.contains(" type ")
                || (declaration.trim_start().starts_with("use ") || declaration.contains(" use "))
                    && declaration.contains(" as ");
            if is_alias && forbidden.iter().any(|token| declaration.contains(token)) {
                return Err(format!("backend alias: {declaration}"));
            }
        }
        if api.iter().map(String::as_str).collect::<Vec<_>>() != allowlist {
            return Err(format!("visible API differs from allowlist: {api:?}"));
        }
        Ok(())
    }

    fn strip_comments_and_strings(source: &str) -> String {
        let mut output = String::with_capacity(source.len());
        let mut chars = source.chars().peekable();
        let mut in_string = false;
        let mut escaped = false;
        while let Some(ch) = chars.next() {
            if in_string {
                if ch == '\n' {
                    output.push('\n');
                }
                if escaped {
                    escaped = false;
                } else if ch == '\\' {
                    escaped = true;
                } else if ch == '"' {
                    in_string = false;
                }
                continue;
            }
            if ch == '"' {
                in_string = true;
                output.push(' ');
                continue;
            }
            if ch == '/' && chars.peek() == Some(&'/') {
                for comment_char in chars.by_ref() {
                    if comment_char == '\n' {
                        output.push('\n');
                        break;
                    }
                }
                continue;
            }
            output.push(ch);
        }
        output
    }

    fn collect_type_and_use_declarations(source: &str) -> Vec<String> {
        let mut declarations = Vec::new();
        let mut active: Option<String> = None;
        for line in source.lines() {
            let trimmed = line.trim_start();
            let starts = ["type ", "use ", "pub type ", "pub use "]
                .iter()
                .any(|prefix| trimmed.starts_with(prefix))
                || trimmed.starts_with("pub(")
                    && (trimmed.contains(") type ") || trimmed.contains(") use "));
            if active.is_none() && starts {
                active = Some(String::new());
            }
            if let Some(declaration) = active.as_mut() {
                declaration.push_str(line);
                declaration.push(' ');
                if line.contains(';') {
                    declarations.push(std::mem::take(declaration));
                    active = None;
                }
            }
        }
        declarations
    }

    fn strip_line_comments(source: &str) -> String {
        source
            .lines()
            .map(|line| line.split_once("//").map_or(line, |(head, _)| head))
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn extract_visible_api(source: &str) -> Result<Vec<String>, String> {
        let source = strip_line_comments(source);
        let lines: Vec<_> = source.lines().collect();
        let mut result = Vec::new();
        let mut i = 0;
        while i < lines.len() {
            let first = lines[i].trim_start();
            if !(first.starts_with("pub ")
                || first.starts_with("pub(")
                || first.starts_with("pub "))
            {
                i += 1;
                continue;
            }
            let mut decl = String::new();
            let mut nesting = 0_i32;
            loop {
                let line = lines[i];
                for ch in line.chars() {
                    match ch {
                        '(' | '[' => nesting += 1,
                        ')' | ']' => nesting -= 1,
                        _ => {}
                    }
                    if nesting == 0 && (ch == '{' || ch == ';') {
                        break;
                    }
                    decl.push(ch);
                }
                let ended = line.contains('{') || line.contains(';');
                if ended && nesting <= 0 {
                    break;
                }
                i += 1;
                if i == lines.len() {
                    return Err("unterminated public declaration".into());
                }
                decl.push(' ');
            }
            result.push(decl.split_whitespace().collect::<Vec<_>>().join(" "));
            i += 1;
        }
        Ok(result)
    }
}
