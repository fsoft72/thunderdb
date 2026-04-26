//! Hash-grouping aggregator used by `Database::aggregate`.
//!
//! Default code path. Indexed / cached fast paths dispatch in
//! `Database::aggregate` *before* falling through here.

use crate::error::{Error, Result};
use crate::query::direct::{Aggregate, AggRow};
use crate::storage::Value;
use std::collections::HashMap;

/// Per-aggregate accumulator. One slot per element of the input `aggs`.
#[derive(Debug, Clone)]
enum AggSlot {
    Count(u64),
    CountCol { non_null: u64 },
    Sum { acc: i128, has_value: bool },
    Avg { acc: i128, n: u64 },
    Min { v: Option<Value> },
    Max { v: Option<Value> },
}

impl AggSlot {
    /// Build a fresh slot from an aggregate spec (zero-initialised).
    fn from_spec(a: &Aggregate) -> Self {
        match a {
            Aggregate::Count => AggSlot::Count(0),
            Aggregate::CountCol(_) => AggSlot::CountCol { non_null: 0 },
            Aggregate::Sum(_) => AggSlot::Sum { acc: 0, has_value: false },
            Aggregate::Avg(_) => AggSlot::Avg { acc: 0, n: 0 },
            Aggregate::Min(_) => AggSlot::Min { v: None },
            Aggregate::Max(_) => AggSlot::Max { v: None },
        }
    }

    /// Convert the accumulated slot into its final SQL-style `Value`.
    /// Returns `Error::Query` for arithmetic overflow on Sum/Count.
    fn finalize(self) -> Result<Value> {
        Ok(match self {
            AggSlot::Count(n) => {
                let v = i64::try_from(n).map_err(|_| Error::Query(
                    "aggregate: COUNT exceeds i64::MAX".into()))?;
                Value::Int64(v)
            }
            AggSlot::CountCol { non_null } => {
                let v = i64::try_from(non_null).map_err(|_| Error::Query(
                    "aggregate: COUNT(col) exceeds i64::MAX".into()))?;
                Value::Int64(v)
            }
            AggSlot::Sum { has_value: false, .. } => Value::Null,
            AggSlot::Sum { acc, .. } => {
                let v = i64::try_from(acc).map_err(|_| Error::Query(
                    format!("aggregate: SUM overflow ({} exceeds i64 range)", acc)))?;
                Value::Int64(v)
            }
            AggSlot::Avg { n: 0, .. } => Value::Null,
            AggSlot::Avg { acc, n } => Value::Float64(acc as f64 / n as f64),
            AggSlot::Min { v: None } => Value::Null,
            AggSlot::Min { v: Some(x) } => x,
            AggSlot::Max { v: None } => Value::Null,
            AggSlot::Max { v: Some(x) } => x,
        })
    }
}

type GroupState = Vec<AggSlot>;

/// Pre-computed plan for an `aggregate()` execution: column resolution,
/// projection list, and reverse mappings from group-by/aggregate inputs
/// back to positions inside the projected row slice.
pub(crate) struct AggPlan {
    pub key_idxs: Vec<usize>,
    pub agg_specs: Vec<Aggregate>,
    pub agg_col_idxs: Vec<Option<usize>>,
    pub projection: Vec<usize>,
    pub key_proj_pos: Vec<usize>,
    pub agg_proj_pos: Vec<Option<usize>>,
}

/// Resolve column names to indices and build the minimal projection
/// covering all referenced columns.
pub(crate) fn plan(
    schema_cols: &[String],
    schema_types: &[String],
    group_by: &[String],
    aggs: &[Aggregate],
) -> Result<AggPlan> {
    let lookup = |name: &str| -> Result<usize> {
        schema_cols.iter().position(|c| c == name).ok_or_else(|| {
            Error::Query(format!("aggregate: unknown column `{}`", name))
        })
    };

    let key_idxs: Vec<usize> = group_by.iter()
        .map(|n| lookup(n)).collect::<Result<_>>()?;

    let agg_col_idxs: Vec<Option<usize>> = aggs.iter().map(|a| match a {
        Aggregate::Count => Ok(None),
        Aggregate::CountCol(c)
        | Aggregate::Sum(c)
        | Aggregate::Avg(c)
        | Aggregate::Min(c)
        | Aggregate::Max(c) => lookup(c).map(Some),
    }).collect::<Result<_>>()?;

    // Type-validate Sum/Avg: must reference an INT64 column. Fold-time
    // would otherwise silently drop non-Int64 values, producing wrong totals.
    for a in aggs {
        let col = match a {
            Aggregate::Sum(c) | Aggregate::Avg(c) => c.as_str(),
            _ => continue,
        };
        let idx = schema_cols.iter().position(|c| c == col).unwrap();
        if schema_types[idx] != "INT64" {
            return Err(Error::Query(format!(
                "aggregate: SUM/AVG requires INT64 column, `{}` is `{}`",
                col, schema_types[idx]
            )));
        }
    }

    let mut projection: Vec<usize> = Vec::new();
    for &k in &key_idxs {
        if !projection.contains(&k) { projection.push(k); }
    }
    for slot in &agg_col_idxs {
        if let Some(idx) = slot {
            if !projection.contains(idx) { projection.push(*idx); }
        }
    }

    let key_proj_pos: Vec<usize> = key_idxs.iter()
        .map(|k| projection.iter().position(|p| p == k).unwrap()).collect();
    let agg_proj_pos: Vec<Option<usize>> = agg_col_idxs.iter()
        .map(|opt| opt.map(|c| projection.iter().position(|p| *p == c).unwrap()))
        .collect();

    Ok(AggPlan {
        key_idxs,
        agg_specs: aggs.to_vec(),
        agg_col_idxs,
        projection,
        key_proj_pos,
        agg_proj_pos,
    })
}

/// Fold one projected row into the running group state.
pub(crate) fn fold_row(
    plan: &AggPlan,
    groups: &mut HashMap<Vec<Value>, GroupState>,
    row: &[Value],
) {
    let key: Vec<Value> = plan.key_proj_pos.iter()
        .map(|&p| row[p].clone()).collect();

    let entry = groups.entry(key).or_insert_with(|| {
        plan.agg_specs.iter().map(AggSlot::from_spec).collect()
    });

    for (i, spec) in plan.agg_specs.iter().enumerate() {
        let v: Option<&Value> = plan.agg_proj_pos[i].map(|p| &row[p]);
        match (&mut entry[i], spec, v) {
            (AggSlot::Count(n), Aggregate::Count, _) => { *n += 1; }
            (AggSlot::CountCol { non_null }, Aggregate::CountCol(_), Some(val)) => {
                if !matches!(val, Value::Null) { *non_null += 1; }
            }
            (AggSlot::Sum { acc, has_value }, Aggregate::Sum(_), Some(val)) => {
                if let Value::Int64(x) = val {
                    *acc += *x as i128;
                    *has_value = true;
                }
            }
            (AggSlot::Avg { acc, n }, Aggregate::Avg(_), Some(val)) => {
                if let Value::Int64(x) = val {
                    *acc += *x as i128;
                    *n += 1;
                }
            }
            (AggSlot::Min { v }, Aggregate::Min(_), Some(val)) => {
                if matches!(val, Value::Null) { /* skip */ }
                else if v.as_ref().map_or(true, |cur| val < cur) { *v = Some(val.clone()); }
            }
            (AggSlot::Max { v }, Aggregate::Max(_), Some(val)) => {
                if matches!(val, Value::Null) { /* skip */ }
                else if v.as_ref().map_or(true, |cur| val > cur) { *v = Some(val.clone()); }
            }
            _ => {}
        }
    }
}

/// Drain accumulated groups into the public `AggRow` form.
/// For a global aggregate (no group-by) over an empty input, emits
/// exactly one row with zero-initialised slots — matching SQL semantics.
pub(crate) fn finalize(
    plan: &AggPlan,
    groups: HashMap<Vec<Value>, GroupState>,
) -> Result<Vec<AggRow>> {
    if plan.key_idxs.is_empty() && groups.is_empty() {
        let zero_state: GroupState = plan.agg_specs.iter().map(AggSlot::from_spec).collect();
        let aggs: Vec<Value> = zero_state.into_iter()
            .map(AggSlot::finalize).collect::<Result<_>>()?;
        return Ok(vec![AggRow { keys: vec![], aggs }]);
    }
    groups.into_iter().map(|(keys, state)| {
        let aggs: Vec<Value> = state.into_iter()
            .map(AggSlot::finalize).collect::<Result<_>>()?;
        Ok(AggRow { keys, aggs })
    }).collect()
}

/// Stateful aggregator. Caller calls `feed` for each projected row,
/// then `into_rows` to drain.
pub(crate) struct Aggregator<'a> {
    plan: &'a AggPlan,
    groups: HashMap<Vec<Value>, GroupState>,
}

impl<'a> Aggregator<'a> {
    /// Build a new aggregator bound to the given plan.
    pub fn new(plan: &'a AggPlan) -> Self {
        Self { plan, groups: HashMap::new() }
    }

    /// Fold a single projected row into the running group state.
    pub fn feed(&mut self, row: &[Value]) {
        fold_row(self.plan, &mut self.groups, row);
    }

    /// Consume the aggregator and produce the final result rows.
    /// Propagates `Error::Query` on aggregate overflow at finalize time.
    pub fn into_rows(self) -> Result<Vec<AggRow>> {
        finalize(self.plan, self.groups)
    }
}
