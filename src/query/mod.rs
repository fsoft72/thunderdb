// Query layer - Phase 3
//
// Direct data access API with type-safe operations

pub mod filter;
pub mod direct;
pub mod builder;

pub use filter::{Filter, Operator};
pub use direct::{DirectDataAccess, QueryContext, apply_filters, choose_index};
pub use builder::{QueryBuilder, QueryPlan, OrderDirection};
