//! Shared closed statement collector for SGML and XML token streams.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr as _;

use rust_decimal::Decimal;

use crate::{CellValue, FileAdapterError, ParseBudget, ParsedRow, ParserLimit};

const STATEMENT_TAGS: [&str; 2] = ["STMTRS", "CCSTMTRS"];

pub(super) struct Collector<'a, 'b> {
    account_id: &'a str,
    currency: &'a str,
    budget: &'a mut ParseBudget<'b>,
    stack: Vec<Frame>,
    statement: Option<Statement>,
    transaction: Option<Transaction>,
    rows: Vec<ParsedRow>,
    matched_statements: usize,
    root_seen: bool,
    root_closed: bool,
}

impl<'a, 'b> Collector<'a, 'b> {
    pub(super) fn new(
        account_id: &'a str,
        currency: &'a str,
        budget: &'a mut ParseBudget<'b>,
    ) -> Self {
        Self {
            account_id,
            currency,
            budget,
            stack: Vec::new(),
            statement: None,
            transaction: None,
            rows: Vec::new(),
            matched_statements: 0,
            root_seen: false,
            root_closed: false,
        }
    }

    pub(super) fn checkpoint(&self) -> Result<(), FileAdapterError> {
        self.budget.checkpoint()
    }

    pub(super) fn reserve_vec_slot<T>(
        &mut self,
        values: &mut Vec<T>,
    ) -> Result<(), FileAdapterError> {
        self.budget.reserve_vec_slot(values)
    }

    pub(super) fn start(&mut self, name: &str) -> Result<(), FileAdapterError> {
        validate_tag(name)?;
        if self.root_closed {
            return Err(FileAdapterError::UnsafeOfx);
        }
        if self.stack.is_empty() {
            if self.root_seen || name != "OFX" {
                return Err(FileAdapterError::UnsafeOfx);
            }
            self.root_seen = true;
        } else if !supported_child(
            self.stack
                .last()
                .map(|frame| frame.name.as_str())
                .ok_or(FileAdapterError::UnsafeOfx)?,
            name,
        ) {
            return Err(FileAdapterError::UnsafeOfx);
        }
        if let Some(parent) = self.stack.last_mut() {
            if !parent.text.trim().is_empty() {
                return Err(FileAdapterError::UnsafeOfx);
            }
            parent.has_child = true;
        }
        let depth = self
            .stack
            .len()
            .checked_add(1)
            .ok_or(FileAdapterError::LimitExceeded(ParserLimit::NestingDepth))?;
        self.budget.depth(depth)?;
        self.budget.text(name.len())?;
        let starts_statement = STATEMENT_TAGS.contains(&name);
        let starts_transaction = name == "STMTTRN";
        if starts_transaction {
            let statement = self.statement.as_ref().ok_or(FileAdapterError::UnsafeOfx)?;
            let statement_account = statement
                .account_id
                .as_deref()
                .ok_or(FileAdapterError::UnsafeOfx)?;
            let _ = statement
                .currency
                .as_deref()
                .ok_or(FileAdapterError::UnsafeOfx)?;
            if statement_account == self.account_id {
                self.budget.record()?;
            }
        }
        let name = name.to_owned();
        self.budget.reserve_vec_slot(&mut self.stack)?;
        self.stack.push(Frame {
            name,
            text: String::new(),
            has_child: false,
        });
        if starts_statement {
            if self.statement.is_some() {
                return Err(FileAdapterError::UnsafeOfx);
            }
            self.statement = Some(Statement::new());
        } else if starts_transaction {
            if self.statement.is_none() || self.transaction.is_some() {
                return Err(FileAdapterError::UnsafeOfx);
            }
            self.transaction = Some(Transaction::new());
        }
        Ok(())
    }

    pub(super) fn text(&mut self, value: &str) -> Result<(), FileAdapterError> {
        if value.is_empty() {
            return Ok(());
        }
        let frame = self.stack.last_mut().ok_or(FileAdapterError::UnsafeOfx)?;
        if frame.has_child && !value.chars().all(char::is_whitespace) {
            return Err(FileAdapterError::UnsafeOfx);
        }
        let length = frame
            .text
            .len()
            .checked_add(value.len())
            .ok_or(FileAdapterError::LimitExceeded(ParserLimit::TextBytes))?;
        self.budget.text(length)?;
        self.budget.append_string(&mut frame.text, value)?;
        Ok(())
    }

    pub(super) const fn root_closed(&self) -> bool {
        self.root_closed
    }

    pub(super) fn trailing_document_whitespace(&self, value: &str) -> Result<(), FileAdapterError> {
        if self.root_closed && value.chars().all(char::is_whitespace) {
            Ok(())
        } else {
            Err(FileAdapterError::UnsafeOfx)
        }
    }

    pub(super) fn end(&mut self, name: &str) -> Result<(), FileAdapterError> {
        let frame = self.stack.pop().ok_or(FileAdapterError::UnsafeOfx)?;
        if frame.name != name || frame.has_child && !frame.text.trim().is_empty() {
            return Err(FileAdapterError::UnsafeOfx);
        }
        let value = frame.text.trim();
        if !value.is_empty() {
            self.budget.cell()?;
            self.handle_leaf(name, value)?;
        }
        if name == "STMTTRN" {
            self.finish_transaction()?;
        } else if STATEMENT_TAGS.contains(&name) {
            self.finish_statement()?;
        } else if name == "OFX" {
            if !self.stack.is_empty() {
                return Err(FileAdapterError::UnsafeOfx);
            }
            self.root_closed = true;
        }
        Ok(())
    }

    pub(super) fn finish(self) -> Result<Vec<ParsedRow>, FileAdapterError> {
        if !self.stack.is_empty()
            || self.statement.is_some()
            || self.transaction.is_some()
            || !self.root_seen
            || !self.root_closed
            || self.matched_statements != 1
        {
            return Err(FileAdapterError::UnsafeOfx);
        }
        Ok(self.rows)
    }

    fn handle_leaf(&mut self, name: &str, value: &str) -> Result<(), FileAdapterError> {
        if self.transaction.is_some()
            && self.stack.last().map(|frame| frame.name.as_str()) == Some("STMTTRN")
        {
            let key = name.to_ascii_lowercase();
            let value = self.budget.owned_text(value)?;
            let transaction = self
                .transaction
                .as_mut()
                .ok_or(FileAdapterError::UnsafeOfx)?;
            self.budget.map_entry::<String, String>()?;
            if transaction.fields.insert(key, value).is_some() {
                return Err(FileAdapterError::UnsafeOfx);
            }
            return Ok(());
        }
        let Some(statement) = self.statement.as_mut() else {
            return Ok(());
        };
        let parent = self.stack.last().map(|frame| frame.name.as_str());
        match (name, parent) {
            ("CURDEF", Some(parent)) if STATEMENT_TAGS.contains(&parent) => {
                set_once(&mut statement.currency, value, self.budget)?
            }
            ("ACCTID", Some("BANKACCTFROM" | "CCACCTFROM")) => {
                set_once(&mut statement.account_id, value, self.budget)?
            }
            ("BANKID", Some("BANKACCTFROM")) => {
                set_once(&mut statement.bank_id, value, self.budget)?
            }
            ("ACCTTYPE", Some("BANKACCTFROM")) => {
                set_once(&mut statement.account_type, value, self.budget)?
            }
            ("BALAMT", Some("LEDGERBAL")) => {
                set_once(&mut statement.ledger_balance, value, self.budget)?
            }
            ("DTASOF", Some("LEDGERBAL")) => {
                set_once(&mut statement.ledger_as_of, value, self.budget)?
            }
            _ => {}
        }
        Ok(())
    }

    fn finish_transaction(&mut self) -> Result<(), FileAdapterError> {
        let mut transaction = self.transaction.take().ok_or(FileAdapterError::UnsafeOfx)?;
        let fitid = required(&transaction.fields, "fitid")?;
        let amount = required(&transaction.fields, "trnamt")?;
        let posted_at = required(&transaction.fields, "dtposted")?;
        if !valid_identifier(fitid)
            || Decimal::from_str(amount).is_err()
            || !valid_datetime(posted_at)
        {
            return Err(FileAdapterError::UnsafeOfx);
        }
        let fitid = self.budget.owned_text(fitid)?;
        let amount = self.budget.owned_text(amount)?;
        let posted_at = self.budget.owned_text(posted_at)?;
        let statement = self.statement.as_mut().ok_or(FileAdapterError::UnsafeOfx)?;
        let retained_fitid = self.budget.owned_text(&fitid)?;
        if statement.fitids.contains(&retained_fitid) {
            return Err(FileAdapterError::UnsafeOfx);
        }
        self.budget.set_entry::<String>()?;
        let _ = statement.fitids.insert(retained_fitid);
        insert_alias(&mut transaction.fields, "id", &fitid, self.budget)?;
        insert_alias(&mut transaction.fields, "value", &amount, self.budget)?;
        insert_alias(
            &mut transaction.fields,
            "posted_at",
            &posted_at,
            self.budget,
        )?;
        self.budget.reserve_vec_slot(&mut statement.transactions)?;
        statement.transactions.push(transaction.fields);
        Ok(())
    }

    fn finish_statement(&mut self) -> Result<(), FileAdapterError> {
        if self.transaction.is_some() {
            return Err(FileAdapterError::UnsafeOfx);
        }
        let statement = self.statement.take().ok_or(FileAdapterError::UnsafeOfx)?;
        let account_id = statement.account_id.ok_or(FileAdapterError::UnsafeOfx)?;
        let currency = statement.currency.ok_or(FileAdapterError::UnsafeOfx)?;
        let ledger_balance = statement
            .ledger_balance
            .ok_or(FileAdapterError::UnsafeOfx)?;
        let ledger_as_of = statement.ledger_as_of.ok_or(FileAdapterError::UnsafeOfx)?;
        if !valid_identifier(&account_id)
            || currency.len() != 3
            || !currency.bytes().all(|byte| byte.is_ascii_uppercase())
            || Decimal::from_str(&ledger_balance).is_err()
            || !valid_datetime(&ledger_as_of)
        {
            return Err(FileAdapterError::UnsafeOfx);
        }
        if account_id != self.account_id {
            return Ok(());
        }
        if currency != self.currency {
            return Err(FileAdapterError::UnsafeOfx);
        }
        self.matched_statements = self
            .matched_statements
            .checked_add(1)
            .ok_or(FileAdapterError::UnsafeOfx)?;
        if self.matched_statements != 1 {
            return Err(FileAdapterError::UnsafeOfx);
        }
        for mut fields in statement.transactions {
            insert_alias(&mut fields, "account_id", &account_id, self.budget)?;
            insert_alias(&mut fields, "currency", &currency, self.budget)?;
            insert_alias(&mut fields, "ledger_balance", &ledger_balance, self.budget)?;
            insert_alias(&mut fields, "ledger_balance_at", &ledger_as_of, self.budget)?;
            if let Some(bank_id) = &statement.bank_id {
                insert_alias(&mut fields, "bank_id", bank_id, self.budget)?;
            }
            if let Some(account_type) = &statement.account_type {
                insert_alias(&mut fields, "account_type", account_type, self.budget)?;
            }
            self.budget.fields(fields.len())?;
            let mut normalized = BTreeMap::new();
            for (name, value) in fields {
                self.budget.map_entry::<String, CellValue>()?;
                normalized.insert(name, CellValue::Text(value));
            }
            let row = ParsedRow::try_new(normalized, self.budget)?;
            self.budget.reserve_vec_slot(&mut self.rows)?;
            self.rows.push(row);
        }
        Ok(())
    }
}

#[derive(Debug)]
struct Frame {
    name: String,
    text: String,
    has_child: bool,
}

#[derive(Debug)]
struct Transaction {
    fields: BTreeMap<String, String>,
}

impl Transaction {
    fn new() -> Self {
        Self {
            fields: BTreeMap::new(),
        }
    }
}

#[derive(Debug)]
struct Statement {
    account_id: Option<String>,
    currency: Option<String>,
    bank_id: Option<String>,
    account_type: Option<String>,
    ledger_balance: Option<String>,
    ledger_as_of: Option<String>,
    fitids: BTreeSet<String>,
    transactions: Vec<BTreeMap<String, String>>,
}

impl Statement {
    fn new() -> Self {
        Self {
            account_id: None,
            currency: None,
            bank_id: None,
            account_type: None,
            ledger_balance: None,
            ledger_as_of: None,
            fitids: BTreeSet::new(),
            transactions: Vec::new(),
        }
    }
}

fn supported_child(parent: &str, child: &str) -> bool {
    match parent {
        "OFX" => matches!(
            child,
            "SIGNONMSGSRSV1" | "BANKMSGSRSV1" | "CREDITCARDMSGSRSV1"
        ),
        "SIGNONMSGSRSV1" => child == "SONRS",
        "SONRS" => matches!(
            child,
            "STATUS"
                | "DTSERVER"
                | "LANGUAGE"
                | "DTPROFUP"
                | "DTACCTUP"
                | "SESSCOOKIE"
                | "ACCESSKEY"
                | "OFXEXTENSION"
                | "FI"
        ),
        "FI" => matches!(child, "ORG" | "FID"),
        "STATUS" => matches!(child, "CODE" | "SEVERITY" | "MESSAGE"),
        "BANKMSGSRSV1" => child == "STMTTRNRS",
        "CREDITCARDMSGSRSV1" => child == "CCSTMTTRNRS",
        "STMTTRNRS" => matches!(child, "TRNUID" | "STATUS" | "STMTRS"),
        "CCSTMTTRNRS" => matches!(child, "TRNUID" | "STATUS" | "CCSTMTRS"),
        "STMTRS" => matches!(
            child,
            "CURDEF" | "BANKACCTFROM" | "BANKTRANLIST" | "LEDGERBAL" | "AVAILBAL"
        ),
        "CCSTMTRS" => matches!(
            child,
            "CURDEF" | "CCACCTFROM" | "BANKTRANLIST" | "LEDGERBAL" | "AVAILBAL"
        ),
        "BANKACCTFROM" => matches!(
            child,
            "BANKID" | "BRANCHID" | "ACCTID" | "ACCTTYPE" | "ACCTKEY"
        ),
        "CCACCTFROM" => matches!(child, "ACCTID" | "ACCTKEY"),
        "BANKTRANLIST" => matches!(child, "DTSTART" | "DTEND" | "STMTTRN"),
        "STMTTRN" => matches!(
            child,
            "TRNTYPE"
                | "DTPOSTED"
                | "DTUSER"
                | "DTAVAIL"
                | "TRNAMT"
                | "FITID"
                | "CORRECTFITID"
                | "CORRECTACTION"
                | "SRVRTID"
                | "CHECKNUM"
                | "REFNUM"
                | "SIC"
                | "PAYEEID"
                | "NAME"
                | "EXTDNAME"
                | "MEMO"
        ),
        "LEDGERBAL" | "AVAILBAL" => matches!(child, "BALAMT" | "DTASOF"),
        _ => false,
    }
}

fn validate_tag(name: &str) -> Result<(), FileAdapterError> {
    if name.is_empty()
        || name.len() > 64
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'.')
    {
        return Err(FileAdapterError::UnsafeOfx);
    }
    Ok(())
}

fn set_once(
    slot: &mut Option<String>,
    value: &str,
    budget: &mut ParseBudget<'_>,
) -> Result<(), FileAdapterError> {
    let value = budget.owned_text(value)?;
    if slot.replace(value).is_some() {
        return Err(FileAdapterError::UnsafeOfx);
    }
    Ok(())
}

fn required<'a>(
    fields: &'a BTreeMap<String, String>,
    name: &str,
) -> Result<&'a str, FileAdapterError> {
    fields
        .get(name)
        .map(String::as_str)
        .ok_or(FileAdapterError::UnsafeOfx)
}

fn insert_alias(
    fields: &mut BTreeMap<String, String>,
    name: &str,
    value: &str,
    budget: &mut ParseBudget<'_>,
) -> Result<(), FileAdapterError> {
    let name = budget.owned_text(name)?;
    let value = budget.owned_text(value)?;
    budget.map_entry::<String, String>()?;
    if fields.insert(name, value).is_some() {
        return Err(FileAdapterError::UnsafeOfx);
    }
    Ok(())
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty() && value.len() <= 255 && !value.chars().any(char::is_control)
}

fn valid_datetime(value: &str) -> bool {
    let (date, zone) = value
        .split_once('[')
        .map_or((value, None), |(date, zone)| (date, zone.strip_suffix(']')));
    if value.contains('[') && zone.is_none() {
        return false;
    }
    let (base, fraction) = date
        .split_once('.')
        .map_or((date, None), |(base, fraction)| (base, Some(fraction)));
    if !matches!(base.len(), 8 | 10 | 12 | 14)
        || !base.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.is_some_and(|fraction| {
            fraction.is_empty()
                || fraction.len() > 9
                || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        })
    {
        return false;
    }
    if let Some(zone) = zone {
        let offset = zone.split_once(':').map_or(zone, |(offset, _)| offset);
        let Ok(offset) = Decimal::from_str(offset) else {
            return false;
        };
        if offset < Decimal::from(-14) || offset > Decimal::from(14) {
            return false;
        }
    }
    valid_calendar_time(base)
}

fn valid_calendar_time(base: &str) -> bool {
    let Ok(year) = base.get(0..4).unwrap_or_default().parse::<i32>() else {
        return false;
    };
    let Ok(month) = base.get(4..6).unwrap_or_default().parse::<u32>() else {
        return false;
    };
    let Ok(day) = base.get(6..8).unwrap_or_default().parse::<u32>() else {
        return false;
    };
    let time = |start: usize| {
        base.get(start..start + 2)
            .unwrap_or("00")
            .parse::<u32>()
            .unwrap_or(u32::MAX)
    };
    let Some(date) = chrono::NaiveDate::from_ymd_opt(year, month, day) else {
        return false;
    };
    date.and_hms_opt(time(8), time(10), time(12)).is_some()
}
