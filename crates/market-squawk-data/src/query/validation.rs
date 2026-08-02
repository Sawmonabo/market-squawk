//! SQL AST and relation confinement for the query boundary.

use std::collections::BTreeSet;
use std::ops::ControlFlow;

use datafusion::sql::parser::{DFParserBuilder, Statement as DataFusionStatement};
use datafusion::sql::sqlparser::ast::{
    Expr, ObjectName, Query, Statement, TableFactor, Visit, Visitor,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::tokenizer::Tokenizer;

use super::QueryError;

pub(super) fn validate_read_only_statement(
    statement: &DataFusionStatement,
) -> Result<usize, QueryError> {
    match statement {
        DataFusionStatement::Statement(statement) => match statement.as_ref() {
            Statement::Query(query) => validate_query(query),
            _ => Err(QueryError::ForbiddenStatement),
        },
        DataFusionStatement::Explain(explain) => validate_read_only_statement(&explain.statement),
        DataFusionStatement::CreateExternalTable(_)
        | DataFusionStatement::CopyTo(_)
        | DataFusionStatement::Reset(_) => Err(QueryError::ForbiddenStatement),
    }
}

fn validate_query(query: &Query) -> Result<usize, QueryError> {
    let mut visitor = ConfinementVisitor::default();
    match query.visit(&mut visitor) {
        ControlFlow::Continue(()) => Ok(visitor.nodes),
        ControlFlow::Break(error) => Err(error),
    }
}

pub(super) fn validate_relations(
    sql: &str,
    table_name: &str,
    max_nodes: usize,
) -> Result<(), QueryError> {
    let dialect = GenericDialect;
    let token_count = Tokenizer::new(&dialect, sql)
        .tokenize()
        .map_err(|error| QueryError::Parse(error.to_string()))?
        .len();
    if token_count > max_nodes {
        return Err(QueryError::AstLimitExceeded);
    }
    let mut parser = DFParserBuilder::new(sql)
        .with_dialect(&dialect)
        .with_recursion_limit(64)
        .build()
        .map_err(|error| QueryError::Parse(error.to_string()))?;
    let statement = parser
        .parse_statements()
        .map_err(|error| QueryError::Parse(error.to_string()))?
        .pop_front()
        .ok_or(QueryError::ForbiddenStatement)?;
    let mut visitor = RelationVisitor::new(table_name);
    match statement {
        DataFusionStatement::Statement(statement) => match statement.as_ref() {
            Statement::Query(query) => match query.visit(&mut visitor) {
                ControlFlow::Continue(()) => Ok(()),
                ControlFlow::Break(error) => Err(error),
            },
            _ => Err(QueryError::ForbiddenStatement),
        },
        DataFusionStatement::Explain(explain) => {
            validate_relations(&explain.statement.to_string(), table_name, max_nodes)
        }
        _ => Err(QueryError::ForbiddenStatement),
    }
}

#[derive(Default)]
struct ConfinementVisitor {
    nodes: usize,
}

impl Visitor for ConfinementVisitor {
    type Break = QueryError;

    fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
        self.nodes = self.nodes.saturating_add(1);
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<Self::Break> {
        self.nodes = self.nodes.saturating_add(1);
        match factor {
            TableFactor::Table { args: None, .. } | TableFactor::Derived { .. } => {
                ControlFlow::Continue(())
            }
            _ => ControlFlow::Break(QueryError::ForbiddenTableFunction),
        }
    }

    fn pre_visit_expr(&mut self, expression: &Expr) -> ControlFlow<Self::Break> {
        self.nodes = self.nodes.saturating_add(1);
        if let Expr::Function(function) = expression {
            let name = function.name.to_string().to_ascii_lowercase();
            if !matches!(
                name.as_str(),
                "abs"
                    | "avg"
                    | "coalesce"
                    | "count"
                    | "date_trunc"
                    | "lower"
                    | "max"
                    | "min"
                    | "round"
                    | "sum"
                    | "upper"
            ) {
                return ControlFlow::Break(QueryError::ForbiddenFunction);
            }
        }
        ControlFlow::Continue(())
    }
}

struct RelationVisitor {
    allowed: BTreeSet<String>,
}

impl RelationVisitor {
    fn new(table_name: &str) -> Self {
        Self {
            allowed: BTreeSet::from([table_name.to_ascii_lowercase()]),
        }
    }
}

impl Visitor for RelationVisitor {
    type Break = QueryError;

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                self.allowed
                    .insert(cte.alias.name.value.to_ascii_lowercase());
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
        let relation = relation.to_string().to_ascii_lowercase();
        if self.allowed.contains(&relation) {
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break(QueryError::ForbiddenRelation)
        }
    }
}
