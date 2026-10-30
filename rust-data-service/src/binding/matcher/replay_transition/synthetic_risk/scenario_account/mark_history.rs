use super::super::Mark;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct MarkHistory {
    marks: Vec<Mark>,
}

impl MarkHistory {
    pub(super) fn from_vec(marks: Vec<Mark>) -> Self {
        Self { marks }
    }

    pub(super) fn as_slice(&self) -> &[Mark] {
        &self.marks
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
}
