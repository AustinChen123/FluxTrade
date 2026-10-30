use super::super::Mark;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::binding::matcher::replay_transition::synthetic_risk) struct MarkHistory {
    marks: Vec<Mark>,
}

impl MarkHistory {
    pub(in crate::binding::matcher::replay_transition::synthetic_risk) fn from_vec(
        marks: Vec<Mark>,
    ) -> Self {
        Self { marks }
    }

    pub(in crate::binding::matcher::replay_transition::synthetic_risk) fn as_slice(
        &self,
    ) -> &[Mark] {
        &self.marks
    }

    pub(in crate::binding::matcher::replay_transition::synthetic_risk) fn mark_at(
        &self,
        product: &super::super::Product,
        at: i64,
    ) -> Result<&Mark, super::super::Fault> {
        self.marks
            .iter()
            .find(|mark| mark.product == *product && mark.valid_from <= at && at < mark.valid_to)
            .ok_or("MARK_COVERAGE_MISSING")
    }

    pub(in crate::binding::matcher::replay_transition::synthetic_risk::scenario_account) fn stage_historical_update<
        'a,
    >(
        &self,
        at: i64,
        steps: impl IntoIterator<Item = (&'a super::super::Product, super::super::Decimal)>,
    ) -> Result<Self, super::super::Fault> {
        let mut candidate = self.marks.clone();
        for (product, price) in steps {
            for mark in candidate.iter_mut().filter(|mark| {
                mark.product == *product && mark.valid_from < at && at < mark.valid_to
            }) {
                mark.valid_to = at;
            }
            candidate.retain(|mark| !(mark.product == *product && mark.valid_from == at));
            candidate.push(Mark {
                product: product.clone(),
                price,
                valid_from: at,
                valid_to: i64::MAX,
            });
        }
        super::super::validate_marks(&candidate)?;
        Ok(Self::from_vec(candidate))
    }

    #[cfg(test)]
    pub(super) fn into_vec_for_test(self) -> Vec<Mark> {
        self.marks
    }

    #[cfg(test)]
    pub(super) fn from_vec_unchecked_for_test(marks: Vec<Mark>) -> Self {
        Self { marks }
    }

    #[cfg(test)]
    pub(super) fn round_trip_for_test(self, mutate: impl FnOnce(&mut Vec<Mark>)) -> Self {
        let mut marks = self.into_vec_for_test();
        mutate(&mut marks);
        Self::from_vec_unchecked_for_test(marks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::matcher::replay_transition::synthetic_risk::Product;
    use rust_decimal::Decimal;

    fn row(product: Product, price: i64, from: i64, to: i64) -> Mark {
        Mark {
            product,
            price: Decimal::from(price),
            valid_from: from,
            valid_to: to,
        }
    }

    fn legacy_stage(
        rows: &[Mark],
        at: i64,
        steps: &[(Product, i64)],
    ) -> Result<Vec<Mark>, super::super::Fault> {
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
        super::super::validate_marks(&candidate)?;
        Ok(candidate)
    }

    fn facade_stage(
        rows: &[Mark],
        at: i64,
        steps: &[(Product, i64)],
    ) -> Result<Vec<Mark>, super::super::Fault> {
        let history = MarkHistory::from_vec(rows.to_vec());
        history
            .stage_historical_update(
                at,
                steps
                    .iter()
                    .map(|(product, price)| (product, Decimal::from(*price))),
            )
            .map(MarkHistory::into_vec_for_test)
    }

    #[test]
    fn staging_candidates_match_legacy_vec_procedure() {
        let finite = vec![row(Product::Btc, 10, 0, 20)];
        let cases = [
            (
                finite.clone(),
                5,
                vec![(Product::Btc, 11)],
                Ok(vec![
                    row(Product::Btc, 10, 0, 5),
                    row(Product::Btc, 11, 5, i64::MAX),
                ]),
            ),
            (
                finite.clone(),
                10,
                vec![(Product::Btc, 10)],
                Ok(vec![
                    row(Product::Btc, 10, 0, 10),
                    row(Product::Btc, 10, 10, i64::MAX),
                ]),
            ),
            (
                finite.clone(),
                10,
                vec![(Product::Btc, 11)],
                Ok(vec![
                    row(Product::Btc, 10, 0, 10),
                    row(Product::Btc, 11, 10, i64::MAX),
                ]),
            ),
            (
                finite.clone(),
                15,
                vec![(Product::Btc, 11)],
                Ok(vec![
                    row(Product::Btc, 10, 0, 15),
                    row(Product::Btc, 11, 15, i64::MAX),
                ]),
            ),
            (
                vec![
                    row(Product::Btc, 10, 0, 5),
                    row(Product::Btc, 11, 5, i64::MAX),
                ],
                5,
                vec![(Product::Btc, 11)],
                Ok(vec![
                    row(Product::Btc, 10, 0, 5),
                    row(Product::Btc, 11, 5, i64::MAX),
                ]),
            ),
            (
                vec![row(Product::Btc, 10, 0, 25), row(Product::Btc, 11, 25, 30)],
                10,
                vec![(Product::Btc, 10)],
                Err("OVERLAPPING_MARKS"),
            ),
        ];
        for (rows, at, steps, expected) in cases {
            let oracle = legacy_stage(&rows, at, &steps);
            assert_eq!(oracle, expected);
            assert_eq!(facade_stage(&rows, at, &steps), oracle);
        }
    }

    #[test]
    fn historical_staging_candidates_are_isolated_across_nested_clones() {
        let root = MarkHistory::from_vec(vec![row(Product::Btc, 10, 0, 20)]);
        let left = root
            .clone()
            .stage_historical_update(5, [(&Product::Btc, Decimal::from(11))])
            .unwrap();
        let left_clone = left.clone();
        let right = left_clone
            .stage_historical_update(8, [(&Product::Btc, Decimal::from(12))])
            .unwrap();
        assert_eq!(root.as_slice(), &[row(Product::Btc, 10, 0, 20)]);
        assert_eq!(
            left.as_slice(),
            &[
                row(Product::Btc, 10, 0, 5),
                row(Product::Btc, 11, 5, i64::MAX)
            ]
        );
        assert_eq!(
            right.as_slice(),
            &[
                row(Product::Btc, 10, 0, 5),
                row(Product::Btc, 11, 5, 8),
                row(Product::Btc, 12, 8, i64::MAX)
            ]
        );
    }

    #[test]
    fn facade_preserves_vec_content_and_order_and_clone_is_isolated() {
        let rows = vec![row(Product::Eth, 2, 10, 20), row(Product::Btc, 1, 0, 10)];
        let history = MarkHistory::from_vec(rows.clone());
        assert_eq!(history.as_slice(), rows.as_slice());
        let mut clone_rows = history.clone().into_vec_for_test();
        clone_rows.remove(0);
        let clone = MarkHistory::from_vec_unchecked_for_test(clone_rows);
        assert_eq!(history.as_slice(), rows.as_slice());
        assert_eq!(clone.as_slice(), &rows[1..]);
    }

    #[test]
    fn lookup_matches_first_vec_match_for_products_gaps_and_arbitrary_times() {
        let rows = vec![
            row(Product::Btc, 10, 0, 5),
            row(Product::Eth, 20, 2, 8),
            row(Product::Btc, 11, 5, 9),
            // Deliberate overlap confirms that lookup keeps Vec first-match semantics.
            row(Product::Btc, 12, 3, 7),
            row(Product::Eth, 21, 10, 12),
        ];
        let history = MarkHistory::from_vec(rows.clone());
        for product in [Product::Btc, Product::Eth] {
            for at in -3..=15 {
                let expected = rows
                    .iter()
                    .find(|mark| {
                        mark.product == product && mark.valid_from <= at && at < mark.valid_to
                    })
                    .ok_or("MARK_COVERAGE_MISSING");
                match (history.mark_at(&product, at), expected) {
                    (Ok(actual), Ok(expected)) => {
                        assert_eq!(actual, expected, "{product:?} at {at}")
                    }
                    (Err(actual), Err(expected)) => {
                        assert_eq!(actual, expected, "{product:?} at {at}")
                    }
                    (actual, expected) => panic!(
                        "lookup mismatch for {product:?} at {at}: {actual:?} vs {expected:?}"
                    ),
                }
            }
        }
        for at in [i64::MIN, i64::MAX] {
            assert_eq!(
                history.mark_at(&Product::Btc, at).cloned(),
                rows.iter()
                    .find(|m| m.product == Product::Btc && m.valid_from <= at && at < m.valid_to)
                    .cloned()
                    .ok_or("MARK_COVERAGE_MISSING")
            );
        }
    }
}
