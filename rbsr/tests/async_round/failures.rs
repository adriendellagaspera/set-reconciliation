use super::*;

fn query_count() -> usize {
    let view = View::new(root(), true);
    let (mut children, mut enumerations) = outputs();
    run(protocol_round_with_policy_async(
        &view,
        &SqrtFanOut,
        active(),
        &mut children,
        &mut enumerations,
        &mut StdRng::seed_from_u64(91),
    ))
    .unwrap();
    view.trace().len()
}

#[test]
fn error_at_every_query_preserves_existing_outputs_and_rng() {
    let total = query_count();
    assert!(total > 20);
    for ordinal in 0..total {
        let mut view = View::new(root(), true);
        view.fail = Some(ordinal);
        let (mut children, mut enumerations) = outputs();
        let before = (children.clone(), enumerations.clone());
        let mut rng = StdRng::seed_from_u64(91);
        let mut before_rng = rng.clone();
        assert_eq!(
            run(protocol_round_with_policy_async(
                &view,
                &SqrtFanOut,
                active(),
                &mut children,
                &mut enumerations,
                &mut rng
            )),
            Err(Error::Io)
        );
        assert_eq!(view.trace().len(), ordinal + 1);
        assert_eq!((children, enumerations), before);
        assert_eq!(rng.next_u64(), before_rng.next_u64());
    }
}

#[test]
fn cancel_at_every_suspension_preserves_existing_outputs_and_rng() {
    let total = query_count();
    let waker = waker();
    let mut cx = Context::from_waker(&waker);
    for ordinal in 0..total {
        let view = View::new(root(), true);
        let (mut children, mut enumerations) = outputs();
        let before = (children.clone(), enumerations.clone());
        let mut rng = StdRng::seed_from_u64(91);
        let mut before_rng = rng.clone();
        {
            let future = protocol_round_with_policy_async(
                &view,
                &SqrtFanOut,
                active(),
                &mut children,
                &mut enumerations,
                &mut rng,
            );
            let mut future = std::pin::pin!(future);
            for _ in 0..=ordinal {
                assert!(future.as_mut().poll(&mut cx).is_pending());
            }
        }
        assert_eq!(view.trace().len(), ordinal + 1);
        assert_eq!((children, enumerations), before);
        assert_eq!(rng.next_u64(), before_rng.next_u64());
    }
}
