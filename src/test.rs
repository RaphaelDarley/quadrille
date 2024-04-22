use crate::*;
use crate::stores::NaiveBTree;

#[test]
fn basic() {
    let kv = Quadrille::<NaiveBTree>::new();
    let mut tx_a = kv.transaction();
    let mut tx_b = kv.transaction();
    //
    let tmp = tx_a.get(&[0]);
    assert_eq!(tmp, None);
    //
    let tmp = tx_a.insert(vec![0], vec![1]);
    assert_eq!(tmp, false);
    //
    let tmp = tx_a.get(&[0]);
    assert_eq!(tmp, Some(vec![1]));
    //
    let tmp = tx_b.get(&[0]);
    assert_eq!(tmp, None);
    //
    let tmp = tx_a.commit();
    assert!(tmp.is_ok());
    //
    let mut tx_c = kv.transaction();
    //
    let tmp = tx_c.get(&[0]);
    assert_eq!(tmp, Some(vec![1]));
}
