// Storage layer - Phase 1
//
// This module implements the foundational storage system:
// - Value: Enum for different data types with binary serialization
// - Row: Structure representing a database row
// - RecordAddressTable (RAT): In-memory index for fast row lookups
// - DataFile: Append-only data.bin file management
// - TableEngine: Coordinator for storage operations

pub mod value;
pub mod row;
pub mod rat;
pub mod data_file;
pub mod table_engine;
pub mod small_string;
pub mod page;
pub mod page_file;
pub mod toast;
pub mod paged_table;

pub use value::Value;
pub use row::Row;
pub use rat::RecordAddressTable;
pub use data_file::DataFile;
pub use table_engine::TableEngine;
pub use small_string::SmallString;
pub use page_file::PageFile;
pub use paged_table::PagedTable;
