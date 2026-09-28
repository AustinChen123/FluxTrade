use super::*;
use std::ffi::CString;

#[test]
fn python_surface_arguments_exceptions_and_privacy_are_closed() {
    Python::with_gil(|py| {
        let module = PyModule::new(py, "fluxtrade_core").unwrap();
        register(&module).unwrap();
        let globals = pyo3::types::PyDict::new(py);
        globals.set_item("native", &module).unwrap();
        py.run(&CString::new(r#"
import json
expected = {'_SyntheticScenarioReplaySession', 'ScenarioReplayInputError', 'ScenarioReplayLookupError', 'ScenarioReplayConflictError', 'ScenarioReplayInvariantError'}
assert {n for n in vars(native) if not n.startswith('__')} == expected
for name, base in [('Input', ValueError), ('Lookup', LookupError), ('Conflict', ValueError), ('Invariant', RuntimeError)]:
    cls = getattr(native, 'ScenarioReplay' + name + 'Error')
    assert cls.__bases__ == (base,)
def failure(call, cls, reason=None):
    try:
        call()
    except cls as exc:
        if reason is not None:
            assert exc.args == (reason,) and str(exc) == reason
    else:
        raise AssertionError('expected exception')
factory = native._SyntheticScenarioReplaySession
profile = 'SYNTHETIC_BTC_ETH_V1'
key = '{"venue":"okx-scenario","environment":"test","account":"A"}'
s = factory(profile_id=profile, account_key=key)
class Coerce:
    def __str__(self):
        raise AssertionError('coercion forbidden')
for invalid in [None, 1, True, {}, b'{}', Coerce(), '\ud800']:
    failure(lambda: factory(invalid, key), native.ScenarioReplayInputError, 'INVALID_SCHEMA')
    failure(lambda: factory(profile, invalid), native.ScenarioReplayInputError, 'INVALID_SCHEMA')
    for name in ['apply_group', 'capture_snapshot', 'build_delivery']:
        failure(lambda: getattr(s, name)(invalid), native.ScenarioReplayInputError, 'INVALID_SCHEMA')
for args, kwargs in [((), {}), ((profile,), {}), ((profile,key,key), {}), ((profile,key), {'other':1})]:
    failure(lambda: factory(*args, **kwargs), TypeError)
for name in ['apply_group', 'capture_snapshot', 'build_delivery']:
    method = getattr(s, name)
    for args, kwargs in [((), {}), (('{}','{}'), {}), ((), {'other':'{}'})]:
        failure(lambda: method(*args, **kwargs), TypeError)
    failure(lambda: method(request='{'), native.ScenarioReplayInputError, 'INVALID_JSON')
failure(lambda: s.inspect_state('{}'), TypeError)
failure(lambda: s.inspect_state(other=1), TypeError)
for field in ['owner', 'inner', 'snapshots', 'deliveries', 'completed', 'poisoned', 'request', '__dict__']:
    assert not hasattr(s, field)
failure(lambda: setattr(s, 'owner', 1), AttributeError)
group = {'schema_version':'scenario_group_v1','group_id':'G','account_key':json.loads(key),'ordering_contract_id':'S_order_v1','group_effective_at':500,'declared_member_count':1,'members':[{'kind':'INTENT','stamp':{'event_id':'G','effective_at':500,'causal_parent_ids':[],'ordering_contract_id':'S_order_v1','scenario_ordinal':60},'payload':{'intent_id':'G','client_order_id':'G','config_id':'scenario-v1','product_id':'BTC-USDT-SWAP','strategy_id':'S','side':'SHORT','order_type':'LIMIT','quantity_contracts':'1','limit_price':'50000.1','reduce_only':True,'requested_at':500}}]}
committed = s.apply_group(request=json.dumps(group))
assert json.loads(committed)['classification'] == 'COMMITTED'
assert s.apply_group(json.dumps(group)) == committed
raw = s.inspect_state()
assert isinstance(raw, str) and raw == s.inspect_state()
assert raw == json.dumps(json.loads(raw), ensure_ascii=False, sort_keys=True, separators=(',', ':'))
request = json.dumps({'schema_version':'snapshot_request_v1','account_key':json.loads(key),'snapshot_id':'S','snapshot_kind':'TRADING','capture_mode':'OWNER_CURRENT','captured_at':500})
fact = s.capture_snapshot(request=request)
assert fact == s.capture_snapshot(request)
conflict = json.loads(request); conflict['captured_at'] = 501
failure(lambda: s.capture_snapshot(json.dumps(conflict)), native.ScenarioReplayConflictError, 'SNAPSHOT_ID_CONFLICT')
projection = json.dumps({'schema_version':'delivery_projection_v1','reference':{'namespace':'SOURCE','fact_id':'missing'},'payload_kind':'EXECUTION_FACT','occurrence_index':0,'schedule_sequence':0,'visible_at':0})
failure(lambda: s.build_delivery(projection), native.ScenarioReplayLookupError, 'UNKNOWN_RECEIPT_REFERENCE')
projection = json.loads(projection); projection.update(reference={'namespace':'SNAPSHOT','fact_id':'S'}, payload_kind='TRADING_SNAPSHOT')
delivery = s.build_delivery(request=json.dumps(projection))
assert json.loads(delivery)['immutable_payload'] == json.loads(fact)['immutable_payload']
assert s.build_delivery(json.dumps(projection)) == delivery
assert s.inspect_state() == raw
"#).unwrap(), Some(&globals), None).unwrap();
    });
}

#[test]
fn poisoned_python_boundary_is_fixed_and_inspection_remains_guarded() {
    Python::with_gil(|py| {
        let key = r#"{"venue":"okx-scenario","environment":"test","account":"A"}"#;
        let mut inner = Session::new("SYNTHETIC_BTC_ETH_V1", key).unwrap();
        assert_eq!(
            inner.guarded(true, |_| panic!("private test")),
            Err(BoundaryError::Invariant)
        );
        let object = Py::new(py, PySession { inner }).unwrap();
        for name in ["apply_group", "capture_snapshot", "build_delivery"] {
            for argument in [
                py.None(),
                "{".into_pyobject(py).unwrap().unbind().into_any(),
            ] {
                let e = object.call_method1(py, name, (argument,)).unwrap_err();
                assert!(e.is_instance_of::<ScenarioReplayInvariantError>(py));
                assert_eq!(
                    e.value(py)
                        .getattr("args")
                        .unwrap()
                        .extract::<(String,)>()
                        .unwrap()
                        .0,
                    "NATIVE_INVARIANT"
                );
            }
        }
        assert!(object.call_method0(py, "inspect_state").is_ok());
        object.borrow_mut(py).inner.owner.state_version = u64::MAX;
        let e = object.call_method0(py, "inspect_state").unwrap_err();
        assert!(e.is_instance_of::<ScenarioReplayInvariantError>(py));
        assert_eq!(
            e.value(py).str().unwrap().to_str().unwrap(),
            "NATIVE_INVARIANT"
        );
    });
}
