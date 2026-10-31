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
assert factory(profile, key, None).inspect_state() == s.inspect_state()
class Coerce:
    def __str__(self):
        raise AssertionError('coercion forbidden')
def product(product_id, code):
    return {'product_id':product_id,'instrument_code':code,'taker_fee_rate':'0','liquidation_fee_rate':'0',
        'specs':[{'version':'s1','valid_from':0,'valid_to':None,'contract_value':'1','multiplier':'1','price_tick':'1','quantity_step':'1','minimum_quantity':'1'}],
        'tiers':[{'version':'t1','valid_from':0,'valid_to':None,'rows':[{'minimum_contracts':'0','maximum_contracts':'10','mmr':'0','imr':'0','max_leverage':'1'}]}],
        'marks':[{'valid_from':0,'valid_to':100,'mark':'1'}]}
configuration = json.dumps({'schema_version':'synthetic_multi_product_config_v1','config_id':'py-config',
    'seed_effective_at':50,'cash':'10','leverage':'1','products':[product('WIRE-Z',2),product('WIRE-A',1)],'positions':[],'orders':[]})
configured = 'SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1'
valid = factory(configured, key, configuration)
facts = json.loads(valid.inspect_state())
assert facts['profile_id'] == configured and facts['config_id'] == 'py-config'
assert factory(profile_id=configured, account_key=key, configuration=configuration).inspect_state() == valid.inspect_state()
gross_profile = 'SYNTHETIC_CONFIGURED_GROSS_ADMISSION_V1'
gross = factory(gross_profile, key, configuration)
gross_projection = json.loads(gross._gross_admission_projection())
assert set(gross_projection) == {'schema_version','risk_policy_id','bound_scope','account_key',
    'account_version','valuation_context_id','cap','effective_equity','gross','net','worst_gross',
    'order_loss_hold','fee_hold','equity_floor','gross_ratio','bound_reason'}
assert gross_projection['schema_version'] == 'spider_gross_admission_projection_v1'
assert gross_projection['risk_policy_id'] == 'CURRENT_MARK_GROSS_CAP_TWO_V1'
assert gross_projection['cap'] == '2' and gross_projection['gross_ratio'] == {'denominator':'10','numerator':'0'}
assert json.loads(gross.inspect_state())['profile_id'] == gross_profile
failure(lambda: valid._gross_admission_projection(), native.ScenarioReplayInputError, 'UNSUPPORTED_GROSS_PROFILE')
response_state = json.loads(gross._gross_response_projection())
assert set(response_state) == {'schema_version','account_key','profile_id','account_version',
    'valuation_context_id','valuation_effective_at','owner_state_digest','gate','lifecycle',
    'gross_admission','maintenance_margin','available_margin','current_admission_state_ready',
    'products','orders'}, 'gross response root fields'
assert response_state['schema_version'] == 'spider_gross_response_native_state_v1', 'gross response schema'
assert response_state['profile_id'] == gross_profile and response_state['gate'] == 'RUNNING', 'gross response identity'
assert [row['product_id'] for row in response_state['products']] == ['WIRE-A','WIRE-Z'], 'gross response product ordering'
assert response_state['products'][0]['lots'] == [] and response_state['orders'] == [], 'gross response empty state'
failure(lambda: valid._gross_response_projection(), native.ScenarioReplayInputError, 'UNSUPPORTED_GROSS_PROFILE')
control = json.dumps({'schema_version':'spider_gross_response_group_control_v1','restricted':True})
group = {'schema_version':'scenario_group_v1','group_id':'GROSS-GUARD','account_key':json.loads(key),
    'ordering_contract_id':'S_order_v1','group_effective_at':50,'declared_member_count':1,
    'members':[{'kind':'INTENT','stamp':{'event_id':'GROSS-GUARD-EVENT','effective_at':50,
        'causal_parent_ids':[],'ordering_contract_id':'S_order_v1','scenario_ordinal':60},
        'payload':{'intent_id':'GROSS-GUARD-INTENT','client_order_id':'GROSS-GUARD-CLIENT',
        'config_id':'py-config','product_id':'WIRE-A','strategy_id':'S','side':'LONG',
        'order_type':'LIMIT','quantity_contracts':'1','limit_price':'1','reduce_only':False,
        'requested_at':50}}]}
guard_before = json.loads(gross.inspect_state())
guard_result = json.loads(gross._apply_gross_response_group(json.dumps(group), control))
guard_after = json.loads(gross.inspect_state())
assert guard_result['classification'] == 'REJECTED', 'gross guard class'
assert guard_result['rejections'] == [{'event_id':'GROSS-GUARD-EVENT','reason':'GROSS_RESPONSE_PENDING'}], 'gross guard reason'
assert guard_result['account_version_before'] == guard_result['account_version_after'] == 0, 'gross guard version'
assert guard_before['orders_digest'] == guard_after['orders_digest'], 'gross guard orders'
assert guard_before['positions_digest'] == guard_after['positions_digest'], 'gross guard positions'
assert guard_before['total_fees'] == guard_after['total_fees'], 'gross guard fees'
failure(lambda: gross._apply_gross_response_group(json.dumps(group), '{}'), native.ScenarioReplayInputError, 'INVALID_SCHEMA')
failure(lambda: valid._apply_gross_response_group(json.dumps(group), control), native.ScenarioReplayInputError, 'UNSUPPORTED_GROSS_PROFILE')
assert valid.inspect_state() == valid.inspect_state()
invalid_fee = json.loads(configuration)
invalid_fee['products'][0]['taker_fee_rate'] = '2'
invalid_fee = json.dumps(invalid_fee)
assert factory(configured, key, invalid_fee).inspect_state()
failure(lambda: factory(gross_profile, key, invalid_fee), native.ScenarioReplayInputError, 'INVALID_SCHEMA')
class Text(str): pass
assert json.loads(factory(Text(configured), Text(key), Text(configuration)).inspect_state())['config_id'] == 'py-config'
for call in [lambda: factory(configured, key), lambda: factory(configured, key, None),
             lambda: factory(configured, key, 'null'), lambda: factory(configured, key, '{'),
             lambda: factory(configured, key, '{}'), lambda: factory(profile, key, configuration),
             lambda: factory('unknown', key), lambda: factory('unknown', key, configuration),
             lambda: factory(profile, key, key)]:
    failure(call, native.ScenarioReplayInputError, 'INVALID_SCHEMA')
for invalid in [None, 1, True, {}, b'{}', Coerce(), '\ud800']:
    failure(lambda: factory(invalid, key), native.ScenarioReplayInputError, 'INVALID_SCHEMA')
    failure(lambda: factory(profile, invalid), native.ScenarioReplayInputError, 'INVALID_SCHEMA')
    failure(lambda: factory(configured, key, invalid), native.ScenarioReplayInputError, 'INVALID_SCHEMA')
    for name in ['apply_group', 'capture_snapshot', 'build_delivery']:
        failure(lambda: getattr(s, name)(invalid), native.ScenarioReplayInputError, 'INVALID_SCHEMA')
for args, kwargs in [((), {}), ((profile,), {}), ((profile,key,key,key), {}), ((profile,key), {'other':1})]:
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
        let mut inner = Session::new("SYNTHETIC_BTC_ETH_V1", key, None).unwrap();
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
