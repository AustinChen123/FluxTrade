//! Exact, bounded Python wire boundary for the pure profile composition owner.
use crate::volume_profile::{Error, Grid, Volume, VolumeProfile, Window};
use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    types::{PyInt, PyList, PyString, PyTuple},
};
use rust_decimal::Decimal;

const BIN_CEILING: usize = 131_072; // Compute ceiling, not a source-completeness limit.
fn invalid() -> PyErr {
    PyValueError::new_err("PROFILE_MERGE_INVALID_INPUT")
}
fn resource() -> PyErr {
    PyValueError::new_err("PROFILE_MERGE_RESOURCE_LIMIT")
}
fn core(error: Error) -> PyErr {
    PyValueError::new_err(match error {
        Error::Product => "PROFILE_MERGE_INVALID_PRODUCT",
        Error::Grid => "PROFILE_MERGE_INVALID_GRID",
        Error::Window => "PROFILE_MERGE_INVALID_WINDOW",
        Error::Composition => "PROFILE_MERGE_INVALID_COMPOSITION",
        Error::BinValue => "PROFILE_MERGE_INVALID_BIN",
        Error::Arithmetic => "PROFILE_MERGE_ARITHMETIC",
        Error::TradeScope => "PROFILE_MERGE_INVALID_TRADE_SCOPE",
        Error::TradeValue => "PROFILE_MERGE_INVALID_TRADE_VALUE",
    })
}
fn sequence(value: &Bound<'_, PyAny>) -> PyResult<usize> {
    if !value.is_exact_instance_of::<PyList>() && !value.is_exact_instance_of::<PyTuple>() {
        return Err(invalid());
    }
    value.len().map_err(|_| invalid())
}
fn record(value: &Bound<'_, PyAny>, size: usize) -> PyResult<()> {
    if !value.is_exact_instance_of::<PyTuple>() || value.len()? != size {
        return Err(invalid());
    }
    Ok(())
}
fn integer(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    value.extract().map_err(|_| invalid()) // Exact PyInt shape was checked in pass one.
}
fn string(value: &Bound<'_, PyAny>, overflow: Error) -> PyResult<String> {
    let value = value.downcast_exact::<PyString>().map_err(|_| invalid())?;
    if value.len().map_err(|_| invalid())? > 64 {
        return Err(core(overflow));
    }
    let text = value.to_str().map_err(|_| invalid())?;
    if text.len() > 64 {
        return Err(core(overflow));
    }
    Ok(text.to_owned())
}
fn decimal(value: &Bound<'_, PyAny>) -> PyResult<Decimal> {
    let failure = || PyValueError::new_err("PROFILE_MERGE_INVALID_DECIMAL");
    let value = value.downcast_exact::<PyString>().map_err(|_| invalid())?;
    if value.len().map_err(|_| failure())? > 64 {
        return Err(failure());
    }
    let text = value.to_str().map_err(|_| failure())?;
    if text.len() > 64 {
        return Err(failure());
    }
    Decimal::from_str_exact(text).map_err(|_| failure())
}
fn canonical(value: Decimal) -> String {
    value.normalize().to_string()
}

#[pyclass(frozen, get_all)]
pub struct VolumeProfileMergeResult {
    product_id: String,
    window_start_ms: i64,
    window_end_ms: i64,
    bin_origin: String,
    bin_step: String,
    unit: String,
    bins: Vec<(i64, String, String, i64)>,
    base_volume: String,
    quote_volume: String,
    aggregate_count: i64,
    poc_index: Option<i64>,
    poc_low: Option<String>,
    poc_high_exclusive: Option<String>,
}

#[pyfunction]
pub fn merge_volume_profiles(
    product_id: &Bound<'_, PyAny>,
    bin_origin: &Bound<'_, PyAny>,
    bin_step: &Bound<'_, PyAny>,
    unit: &Bound<'_, PyAny>,
    days: &Bound<'_, PyAny>,
    output_step: &Bound<'_, PyAny>,
) -> PyResult<VolumeProfileMergeResult> {
    for value in [product_id, bin_origin, bin_step, unit, output_step] {
        if !value.is_exact_instance_of::<PyString>() {
            return Err(invalid());
        }
    }
    let count = sequence(days)?;
    if !(1..=90).contains(&count) {
        return Err(resource());
    }
    let mut bin_count = 0usize;
    for i in 0..count {
        let day = days.get_item(i)?;
        record(&day, 3)?;
        for j in [0, 1] {
            if !day.get_item(j)?.is_exact_instance_of::<PyInt>() {
                return Err(invalid());
            }
        }
        let bins = day.get_item(2)?;
        let length = sequence(&bins)?;
        bin_count = bin_count
            .checked_add(length)
            .filter(|n| *n <= BIN_CEILING)
            .ok_or_else(resource)?;
        for j in 0..length {
            let bin = bins.get_item(j)?;
            record(&bin, 4)?;
            for k in [0, 3] {
                if !bin.get_item(k)?.is_exact_instance_of::<PyInt>() {
                    return Err(invalid());
                }
            }
            for k in [1, 2] {
                if !bin.get_item(k)?.is_exact_instance_of::<PyString>() {
                    return Err(invalid());
                }
            }
        }
    }
    let product = string(product_id, Error::Product)?;
    let origin = decimal(bin_origin)?;
    let unit = string(unit, Error::Grid)?;
    let grid = Grid::new(origin, decimal(bin_step)?, unit.clone()).map_err(core)?;
    let output = Grid::new(origin, decimal(output_step)?, unit).map_err(core)?;
    let mut profiles = Vec::with_capacity(count);
    for i in 0..count {
        let day = days.get_item(i)?;
        let window =
            Window::new(integer(&day.get_item(0)?)?, integer(&day.get_item(1)?)?).map_err(core)?;
        let rows = day.get_item(2)?;
        let mut bins = Vec::with_capacity(rows.len()?);
        for j in 0..rows.len()? {
            let bin = rows.get_item(j)?;
            let count = bin
                .get_item(3)?
                .extract::<i64>()
                .map_err(|_| core(Error::BinValue))?;
            if count <= 0 {
                return Err(core(Error::BinValue));
            }
            bins.push((
                integer(&bin.get_item(0)?)?,
                Volume {
                    base_volume: decimal(&bin.get_item(1)?)?,
                    quote_volume: decimal(&bin.get_item(2)?)?,
                    aggregate_count: count as u64,
                },
            ));
        }
        profiles.push(
            VolumeProfile::from_bins(product.clone(), grid.clone(), window, bins).map_err(core)?,
        );
    }
    let result = VolumeProfile::merge_ordered(&profiles, output).map_err(core)?;
    let aggregate_count =
        i64::try_from(result.totals().aggregate_count).map_err(|_| core(Error::Arithmetic))?;
    // Positive bin counts cannot exceed this checked total; each bin cast is lossless.
    let poc = result.poc().map_err(core)?;
    Ok(VolumeProfileMergeResult {
        product_id: result.product_id().into(),
        window_start_ms: result.window().start_ms(),
        window_end_ms: result.window().end_ms(),
        bin_origin: canonical(result.grid().origin()),
        bin_step: canonical(result.grid().step()),
        unit: result.grid().unit().into(),
        bins: result
            .bins()
            .iter()
            .map(|(&index, v)| {
                (
                    index,
                    canonical(v.base_volume),
                    canonical(v.quote_volume),
                    v.aggregate_count as i64,
                )
            })
            .collect(),
        base_volume: canonical(result.totals().base_volume),
        quote_volume: canonical(result.totals().quote_volume),
        aggregate_count,
        poc_index: poc.as_ref().map(|p| p.index),
        poc_low: poc.as_ref().map(|p| canonical(p.low)),
        poc_high_exclusive: poc.map(|p| canonical(p.high_exclusive)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::types::PyDict;
    use std::ffi::CString;

    #[test]
    fn actual_python_boundary_smoke() {
        Python::with_gil(|py| {
            let module = PyModule::new(py, "fluxtrade_core").unwrap();
            crate::fluxtrade_core(&module).unwrap();
            let locals = PyDict::new(py);
            locals.set_item("m", module).unwrap();
            let code = CString::new(r#"
import copy, inspect
from decimal import Decimal
f = m.merge_volume_profiles
assert list(inspect.signature(f).parameters) == ['product_id','bin_origin','bin_step','unit','days','output_step']
args = ['BINANCE:BTCUSDT-SPOT','0','10','USDT',[(0,86400000,[(1,'1.00','10',1),(2,'2','30.0',1)]),(86400000,172800000,())],'50']
before = copy.deepcopy(args)
r = f(*args)
assert args == before and r.bins == [(0,'3','40',2)]
assert (r.product_id,r.window_start_ms,r.window_end_ms) == (args[0],0,172800000)
assert (r.bin_origin,r.bin_step,r.unit,r.base_volume,r.quote_volume,r.aggregate_count) == ('0','50','USDT','3','40',2)
assert (r.poc_index,r.poc_low,r.poc_high_exclusive) == (0,'0','50')
detached = r.bins
detached.clear()
assert r.bins == [(0,'3','40',2)]
empty = f(*args[:4],[(0,86400000,[])],'10')
assert empty.bins == [] and empty.base_volume == empty.quote_volume == '0'
assert (empty.poc_index,empty.poc_low,empty.poc_high_exclusive) == (None,None,None)
def error(values, code):
    try: f(*values)
    except ValueError as exc: assert str(exc) == 'PROFILE_MERGE_' + code
    else: raise AssertionError('accepted invalid input')
product64 = 'BINANCE:' + 'A'*51 + '-SPOT'
assert len(product64) == 64
assert f(product64,'0','10','U'*64,[(0,86400000,[])],'10').product_id == product64
error([product64+'A']+args[1:], 'INVALID_PRODUCT')
error(args[:3]+['U'*65]+args[4:], 'INVALID_GRID')
error(['\ud800'*65]+args[1:], 'INVALID_PRODUCT')
error(args[:3]+['\ud800'*65]+args[4:], 'INVALID_GRID')
class S(str): pass
class I(int): pass
class L(list): pass
class Indexed:
    def __index__(self): raise AssertionError('must not invoke custom index')
for position, value in [(0,S(args[0])),(4,L(args[4])),(4,iter(args[4])),
    (4,[(I(0),86400000,[])]),(4,[(Indexed(),86400000,[])]),(4,[[0,86400000,[]]]),
    (4,[(0,86400000,[[0,'1','1',1]])]),(4,[(0,86400000,[(0,'1','1',True)])])]:
    changed = args.copy(); changed[position] = value
    error(changed, 'INVALID_INPUT')
for position, value, code in [(0,1,'INVALID_INPUT'),(1,Decimal('0'),'INVALID_INPUT'),(2,1.0,'INVALID_INPUT'),
    (1,'NaN_SECRET','INVALID_DECIMAL'),(1,'0'*65,'INVALID_DECIMAL'),(4,[],'RESOURCE_LIMIT'),
    (4,[(True,86400000,[])],'INVALID_INPUT'),(4,[(0,86400000,[(0,'1','1',0)])],'INVALID_BIN'),
    (4,[(0,86400000,[()] * 131073)],'RESOURCE_LIMIT'),(4,[(0,86400000,[()] * 131072)],'INVALID_INPUT'),
    (1,'\ud800','INVALID_DECIMAL'),(2,'0.'+'0'*28+'1','INVALID_DECIMAL'),
    (4,[(0,86400000,[(0,'1','1',2**63)])],'INVALID_BIN')]:
    changed = args.copy(); changed[position] = value
    error(changed, code)
maximum = 2**63-1
assert f(*args[:4],[(0,86400000,[(0,'1','1',maximum)])],'10').aggregate_count == maximum
error(args[:4]+[[(0,86400000,[(0,'1','1',maximum),(1,'1','1',1)])],'10'], 'ARITHMETIC')
try: r.aggregate_count = 9
except AttributeError: pass
else: raise AssertionError('mutable result')
try: m.VolumeProfileMergeResult()
except TypeError: pass
else: raise AssertionError('public constructor')
try: type('Derived',(m.VolumeProfileMergeResult,),{})
except TypeError: pass
else: raise AssertionError('subclassable result')
"#).unwrap();
            py.run(&code, Some(&locals), Some(&locals)).unwrap();
        });
    }
}
