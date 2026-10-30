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
