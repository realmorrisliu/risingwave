// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::Statement;

use super::delete::BoundDelete;
use crate::binder::{Binder, BoundInsert, BoundQuery};

#[derive(Debug)]
pub enum BoundStatement {
    Insert(Box<BoundInsert>),
    Delete(Box<BoundDelete>),
    Query(Box<BoundQuery>),
}

impl Binder {
    pub(super) fn bind_statement(&mut self, stmt: Statement) -> Result<BoundStatement> {
        dbg!(&stmt);
        match stmt {
            Statement::Insert {
                table_name,
                columns,
                source,
            } => Ok(BoundStatement::Insert(
                self.bind_insert(table_name, columns, *source)?.into(),
            )),

            Statement::Delete {
                table_name,
                selection,
            } => Ok(BoundStatement::Delete(
                self.bind_delete(table_name, selection)?.into(),
            )),

            Statement::Query(q) => Ok(BoundStatement::Query(self.bind_query(*q)?.into())),

            _ => Err(
                ErrorCode::NotImplementedError(format!("unsupported statement {:?}", stmt)).into(),
            ),
        }
    }
}
