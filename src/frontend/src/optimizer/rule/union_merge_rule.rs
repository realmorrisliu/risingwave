// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::{BoxedRule, Result, Rule};
use crate::optimizer::plan_node::PlanTreeNode;
use crate::optimizer::PlanRef;

pub struct UnionMergeRule {}
impl Rule for UnionMergeRule {
    fn apply(&self, plan: PlanRef) -> Result<Option<PlanRef>> {
        let top_union = plan.as_logical_union();
        if top_union.is_none() {
            return Ok(None);
        }
        let top_union = top_union.unwrap();

        let top_all = top_union.all();
        let mut new_inputs = vec![];
        let mut has_merge = false;
        for input in top_union.inputs() {
            if let Some(bottom_union) = input.as_logical_union()
                && bottom_union.all() == top_all
            {
                new_inputs.extend(bottom_union.inputs());
                has_merge = true;
            } else {
                new_inputs.push(input);
            }
        }

        if has_merge {
            Ok(Some(top_union.clone_with_inputs(&new_inputs)))
        } else {
            Ok(None)
        }
    }
}

impl UnionMergeRule {
    pub fn create() -> BoxedRule {
        Box::new(UnionMergeRule {})
    }
}
