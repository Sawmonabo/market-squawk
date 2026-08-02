//! Windows current-user, least-privilege Task Scheduler registration.

use std::ffi::OsString;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use quick_xml::Reader;
use quick_xml::events::Event;
use serde::Serialize;

use super::{
    NativeRegistrationSnapshot, PreparedRegistration, ServiceRegistrationError, native_document,
    run_bounded, run_bounded_raw, sha256_bytes, xml_escape,
};

pub(super) const REGISTRATION_IDENTITY: &str = r"\MarketSquawk\Service";
const OWNER_DESCRIPTION: &str =
    "Managed by Market Squawk installer; owner=market-squawk-installer-v1";

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TaskSemantics {
    user_sid: Box<str>,
    command: Box<str>,
    arguments: Box<str>,
    restart_interval: Box<str>,
    restart_count: u8,
}

pub(super) fn render_task_xml(
    service: &Path,
    release_root: &Path,
    user_sid: &str,
) -> Result<String, ServiceRegistrationError> {
    validate_sid(user_sid)?;
    let service = service.to_str().ok_or(ServiceRegistrationError::Identity)?;
    let release_root = release_root
        .to_str()
        .ok_or(ServiceRegistrationError::Identity)?;
    let arguments = format!(
        "--training-release-root {}",
        quote_windows_argument(release_root)?
    );
    Ok(format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <Task version=\"1.4\" xmlns=\"http://schemas.microsoft.com/windows/2004/02/mit/task\">\n\
           <RegistrationInfo>\n\
             <Description>{description}</Description>\n\
             <URI>{uri}</URI>\n\
           </RegistrationInfo>\n\
           <Triggers>\n\
             <LogonTrigger>\n\
               <Enabled>true</Enabled>\n\
               <UserId>{sid}</UserId>\n\
             </LogonTrigger>\n\
           </Triggers>\n\
           <Principals>\n\
             <Principal id=\"Author\">\n\
               <UserId>{sid}</UserId>\n\
               <LogonType>InteractiveToken</LogonType>\n\
               <RunLevel>LeastPrivilege</RunLevel>\n\
             </Principal>\n\
           </Principals>\n\
           <Settings>\n\
             <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>\n\
             <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>\n\
             <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>\n\
             <AllowHardTerminate>true</AllowHardTerminate>\n\
             <StartWhenAvailable>true</StartWhenAvailable>\n\
             <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>\n\
             <AllowStartOnDemand>true</AllowStartOnDemand>\n\
             <Enabled>true</Enabled>\n\
             <Hidden>false</Hidden>\n\
             <RunOnlyIfIdle>false</RunOnlyIfIdle>\n\
             <WakeToRun>false</WakeToRun>\n\
             <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>\n\
             <Priority>7</Priority>\n\
             <RestartOnFailure>\n\
               <Interval>PT5S</Interval>\n\
               <Count>3</Count>\n\
             </RestartOnFailure>\n\
           </Settings>\n\
           <Actions Context=\"Author\">\n\
             <Exec>\n\
               <Command>{service}</Command>\n\
               <Arguments>{arguments}</Arguments>\n\
             </Exec>\n\
           </Actions>\n\
         </Task>\n",
        description = xml_escape(OWNER_DESCRIPTION)?,
        uri = xml_escape(REGISTRATION_IDENTITY)?,
        sid = xml_escape(user_sid)?,
        service = xml_escape(service)?,
        arguments = xml_escape(&arguments)?,
    ))
}

pub(super) fn task_configuration_digest(
    document: &[u8],
) -> Result<Box<str>, ServiceRegistrationError> {
    let text = decode_task_xml(document)?;
    let semantics = parse_task_semantics(&text)?;
    let canonical =
        serde_json::to_vec(&semantics).map_err(|_| ServiceRegistrationError::NativeDocument)?;
    Ok(sha256_bytes(&canonical))
}

pub(super) fn prepare(
    service: &Path,
    release_root: &Path,
) -> Result<PreparedRegistration, ServiceRegistrationError> {
    let sid = current_user_sid()?;
    let document = native_document(render_task_xml(service, release_root, &sid)?.into_bytes())?;
    Ok(PreparedRegistration {
        identity: REGISTRATION_IDENTITY,
        configuration_sha256: task_configuration_digest(&document)?,
        document,
    })
}

pub(super) fn inspect() -> Result<Option<NativeRegistrationSnapshot>, ServiceRegistrationError> {
    let scheduler = system_program("schtasks.exe")?;
    let query = run_bounded_raw(
        &scheduler,
        ["/Query", "/TN", REGISTRATION_IDENTITY, "/XML"],
        true,
    )?;
    if !query.status.success() {
        let listing = run_bounded(&scheduler, ["/Query", "/FO", "CSV", "/NH"], true)?;
        let listing = String::from_utf8(listing.stdout)
            .map_err(|_| ServiceRegistrationError::CommandOutput)?;
        if listing.lines().any(|line| {
            line.trim_start()
                .starts_with(&format!("\"{REGISTRATION_IDENTITY}\","))
        }) {
            return Err(ServiceRegistrationError::CommandFailed(query.status.code()));
        }
        return Ok(None);
    }
    let document = native_document(query.stdout)?;
    let admitted = task_configuration_digest(&document);
    let (owned, configuration_sha256) = match admitted {
        Ok(digest) => (true, digest),
        Err(_) => (false, sha256_bytes(&document)),
    };
    Ok(Some(NativeRegistrationSnapshot {
        document,
        configuration_sha256,
        owned,
    }))
}

pub(super) fn apply(prepared: &PreparedRegistration) -> Result<(), ServiceRegistrationError> {
    if prepared.identity != REGISTRATION_IDENTITY {
        return Err(ServiceRegistrationError::Identity);
    }
    end_if_running()?;
    register_document(&prepared.document)
}

pub(super) fn start() -> Result<(), ServiceRegistrationError> {
    run_bounded(
        &system_program("schtasks.exe")?,
        ["/Run", "/TN", REGISTRATION_IDENTITY],
        false,
    )?;
    Ok(())
}

pub(super) fn restart() -> Result<(), ServiceRegistrationError> {
    end_if_running()?;
    start()
}

pub(super) fn ensure_active() -> Result<(), ServiceRegistrationError> {
    if inspect()?.is_none() {
        return Err(ServiceRegistrationError::RegistrationMissing);
    }
    Ok(())
}

pub(super) fn remove(
    expected: &NativeRegistrationSnapshot,
) -> Result<(), ServiceRegistrationError> {
    let current = inspect()?.ok_or(ServiceRegistrationError::RegistrationMissing)?;
    if !current.owned
        || current.configuration_sha256.as_ref() != expected.configuration_sha256.as_ref()
    {
        return Err(ServiceRegistrationError::Conflict);
    }
    end_if_running()?;
    delete_task()
}

pub(super) fn restore(
    prior: Option<&NativeRegistrationSnapshot>,
    attempted: &PreparedRegistration,
) -> Result<(), ServiceRegistrationError> {
    if let Some(current) = inspect()? {
        if !current.owned
            || current.configuration_sha256.as_ref() != attempted.configuration_sha256.as_ref()
        {
            return Err(ServiceRegistrationError::Conflict);
        }
        end_if_running()?;
        delete_task()?;
    }
    match prior {
        Some(prior) if prior.owned => {
            register_document(&prior.document)?;
            start()
        }
        Some(_) => Err(ServiceRegistrationError::Conflict),
        None => Ok(()),
    }
}

fn register_document(document: &[u8]) -> Result<(), ServiceRegistrationError> {
    native_document(document.to_vec())?;
    let mut temporary = tempfile::Builder::new()
        .prefix("market-squawk-task-")
        .suffix(".xml")
        .tempfile()
        .map_err(|source| ServiceRegistrationError::io("create scheduled-task document", source))?;
    temporary
        .write_all(document)
        .and_then(|()| temporary.as_file().sync_all())
        .map_err(|source| ServiceRegistrationError::io("write scheduled-task document", source))?;
    let path = temporary.path().as_os_str().to_owned();
    run_bounded(
        &system_program("schtasks.exe")?,
        [
            OsString::from("/Create"),
            OsString::from("/TN"),
            OsString::from(REGISTRATION_IDENTITY),
            OsString::from("/XML"),
            path,
            OsString::from("/F"),
        ],
        false,
    )?;
    Ok(())
}

fn end_if_running() -> Result<(), ServiceRegistrationError> {
    match run_bounded(
        &system_program("schtasks.exe")?,
        ["/End", "/TN", REGISTRATION_IDENTITY],
        false,
    ) {
        Ok(_) | Err(ServiceRegistrationError::CommandFailed(_)) => Ok(()),
        Err(error) => Err(error),
    }
}

fn delete_task() -> Result<(), ServiceRegistrationError> {
    run_bounded(
        &system_program("schtasks.exe")?,
        ["/Delete", "/TN", REGISTRATION_IDENTITY, "/F"],
        false,
    )?;
    Ok(())
}

fn current_user_sid() -> Result<String, ServiceRegistrationError> {
    let output = run_bounded(
        &system_program("whoami.exe")?,
        ["/user", "/fo", "csv", "/nh"],
        true,
    )?;
    let text =
        String::from_utf8(output.stdout).map_err(|_| ServiceRegistrationError::CommandOutput)?;
    let start = text
        .find("S-1-")
        .ok_or(ServiceRegistrationError::Identity)?;
    let sid: String = text[start..]
        .chars()
        .take_while(|character| {
            character.is_ascii_digit() || *character == '-' || *character == 'S'
        })
        .collect();
    validate_sid(&sid)?;
    Ok(sid)
}

fn system_program(name: &str) -> Result<PathBuf, ServiceRegistrationError> {
    let root = std::env::var_os("SystemRoot").ok_or(ServiceRegistrationError::UnsafePath)?;
    let root = PathBuf::from(root);
    if !root.is_absolute()
        || name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'-')
    {
        return Err(ServiceRegistrationError::UnsafePath);
    }
    Ok(root.join("System32").join(name))
}

fn parse_task_semantics(text: &str) -> Result<TaskSemantics, ServiceRegistrationError> {
    let mut reader = Reader::from_str(text);
    reader.config_mut().check_end_names = true;
    reader.config_mut().expand_empty_elements = true;
    reader.config_mut().trim_text(false);
    let mut stack = Vec::<String>::new();
    let mut values = TaskValues::default();
    let mut structures = TaskStructures::default();
    let mut root_closed = false;
    loop {
        match reader
            .read_event()
            .map_err(|_| ServiceRegistrationError::NativeDocument)?
        {
            Event::Start(start) => {
                if root_closed || stack.len() >= 32 {
                    return Err(ServiceRegistrationError::NativeDocument);
                }
                let qualified_name = start.name();
                let name = local_name(qualified_name.as_ref())?;
                let parent = stack.last().map(String::as_str);
                structures.observe(parent, name)?;
                values.begin(parent, name)?;
                stack.push(name.to_owned());
            }
            Event::End(end) => {
                let qualified_name = end.name();
                let name = local_name(qualified_name.as_ref())?;
                if stack.pop().as_deref() != Some(name) {
                    return Err(ServiceRegistrationError::NativeDocument);
                }
                if stack.is_empty() {
                    root_closed = true;
                }
            }
            Event::Text(text) => {
                let decoded = text
                    .decode()
                    .map_err(|_| ServiceRegistrationError::NativeDocument)?;
                let unescaped = quick_xml::escape::unescape(&decoded)
                    .map_err(|_| ServiceRegistrationError::NativeDocument)?;
                let active = stack.last().map(String::as_str);
                let parent = stack.iter().rev().nth(1).map(String::as_str);
                values.append(parent, active, unescaped.as_ref())?;
            }
            Event::GeneralRef(reference) => {
                let reference = reference
                    .decode()
                    .map_err(|_| ServiceRegistrationError::NativeDocument)?;
                let encoded = format!("&{reference};");
                let unescaped = quick_xml::escape::unescape(&encoded)
                    .map_err(|_| ServiceRegistrationError::NativeDocument)?;
                let active = stack.last().map(String::as_str);
                let parent = stack.iter().rev().nth(1).map(String::as_str);
                values.append(parent, active, unescaped.as_ref())?;
            }
            Event::Decl(_) | Event::Comment(_) => {}
            Event::Eof => break,
            Event::DocType(_) | Event::PI(_) | Event::CData(_) => {
                return Err(ServiceRegistrationError::NativeDocument);
            }
            Event::Empty(_) => return Err(ServiceRegistrationError::NativeDocument),
        }
    }
    if !root_closed || !stack.is_empty() {
        return Err(ServiceRegistrationError::NativeDocument);
    }
    structures.finish()?;
    values.finish()
}

#[derive(Default)]
struct TaskStructures {
    task: u8,
    registration_info: u8,
    triggers: u8,
    logon_trigger: u8,
    principals: u8,
    principal: u8,
    settings: u8,
    restart_on_failure: u8,
    actions: u8,
    exec: u8,
}

impl TaskStructures {
    fn observe(
        &mut self,
        parent: Option<&str>,
        name: &str,
    ) -> Result<(), ServiceRegistrationError> {
        match (parent, name) {
            (None, "Task") => increment(&mut self.task)?,
            (Some("Task"), "RegistrationInfo") => increment(&mut self.registration_info)?,
            (Some("Task"), "Triggers") => increment(&mut self.triggers)?,
            (Some("Triggers"), "LogonTrigger") => increment(&mut self.logon_trigger)?,
            (Some("Triggers"), _) => return Err(ServiceRegistrationError::NativeDocument),
            (Some("Task"), "Principals") => increment(&mut self.principals)?,
            (Some("Principals"), "Principal") => increment(&mut self.principal)?,
            (Some("Principals"), _) => return Err(ServiceRegistrationError::NativeDocument),
            (Some("Task"), "Settings") => increment(&mut self.settings)?,
            (Some("Settings"), "RestartOnFailure") => increment(&mut self.restart_on_failure)?,
            (Some("Task"), "Actions") => increment(&mut self.actions)?,
            (Some("Actions"), "Exec") => increment(&mut self.exec)?,
            (Some("Actions"), _) => return Err(ServiceRegistrationError::NativeDocument),
            (Some("Exec"), "Command" | "Arguments") => {}
            (Some("Exec"), _) => return Err(ServiceRegistrationError::NativeDocument),
            (None, _) => return Err(ServiceRegistrationError::NativeDocument),
            _ => {}
        }
        Ok(())
    }

    fn finish(self) -> Result<(), ServiceRegistrationError> {
        if [
            self.task,
            self.registration_info,
            self.triggers,
            self.logon_trigger,
            self.principals,
            self.principal,
            self.settings,
            self.restart_on_failure,
            self.actions,
            self.exec,
        ]
        .into_iter()
        .any(|count| count != 1)
        {
            return Err(ServiceRegistrationError::NativeDocument);
        }
        Ok(())
    }
}

#[derive(Default)]
struct TaskValues {
    description: Vec<String>,
    uri: Vec<String>,
    user_ids: Vec<String>,
    logon_type: Vec<String>,
    run_level: Vec<String>,
    multiple_instances: Vec<String>,
    logon_enabled: Vec<String>,
    settings_enabled: Vec<String>,
    allow_start_on_demand: Vec<String>,
    execution_time_limit: Vec<String>,
    restart_interval: Vec<String>,
    restart_count: Vec<String>,
    command: Vec<String>,
    arguments: Vec<String>,
}

impl TaskValues {
    fn begin(&mut self, parent: Option<&str>, name: &str) -> Result<(), ServiceRegistrationError> {
        let target = match (parent, name) {
            (Some("RegistrationInfo"), "Description") => Some(&mut self.description),
            (Some("RegistrationInfo"), "URI") => Some(&mut self.uri),
            (Some("LogonTrigger"), "UserId") | (Some("Principal"), "UserId") => {
                Some(&mut self.user_ids)
            }
            (Some("LogonTrigger"), "Enabled") => Some(&mut self.logon_enabled),
            (Some("Principal"), "LogonType") => Some(&mut self.logon_type),
            (Some("Principal"), "RunLevel") => Some(&mut self.run_level),
            (Some("Settings"), "MultipleInstancesPolicy") => Some(&mut self.multiple_instances),
            (Some("Settings"), "ExecutionTimeLimit") => Some(&mut self.execution_time_limit),
            (Some("Settings"), "Enabled") => Some(&mut self.settings_enabled),
            (Some("Settings"), "AllowStartOnDemand") => Some(&mut self.allow_start_on_demand),
            (Some("RestartOnFailure"), "Interval") => Some(&mut self.restart_interval),
            (Some("RestartOnFailure"), "Count") => Some(&mut self.restart_count),
            (Some("Exec"), "Command") => Some(&mut self.command),
            (Some("Exec"), "Arguments") => Some(&mut self.arguments),
            (
                _,
                "Description"
                | "URI"
                | "UserId"
                | "Enabled"
                | "LogonType"
                | "RunLevel"
                | "MultipleInstancesPolicy"
                | "ExecutionTimeLimit"
                | "AllowStartOnDemand"
                | "Interval"
                | "Count"
                | "Command"
                | "Arguments",
            ) => {
                return Err(ServiceRegistrationError::NativeDocument);
            }
            _ => None,
        };
        if let Some(target) = target {
            target.push(String::new());
            if target.len() > 2 {
                return Err(ServiceRegistrationError::NativeDocument);
            }
        }
        Ok(())
    }

    fn append(
        &mut self,
        parent: Option<&str>,
        active: Option<&str>,
        value: &str,
    ) -> Result<(), ServiceRegistrationError> {
        let target = match (parent, active) {
            (Some("RegistrationInfo"), Some("Description")) => Some(&mut self.description),
            (Some("RegistrationInfo"), Some("URI")) => Some(&mut self.uri),
            (Some("LogonTrigger"), Some("UserId")) | (Some("Principal"), Some("UserId")) => {
                Some(&mut self.user_ids)
            }
            (Some("LogonTrigger"), Some("Enabled")) => Some(&mut self.logon_enabled),
            (Some("Principal"), Some("LogonType")) => Some(&mut self.logon_type),
            (Some("Principal"), Some("RunLevel")) => Some(&mut self.run_level),
            (Some("Settings"), Some("MultipleInstancesPolicy")) => {
                Some(&mut self.multiple_instances)
            }
            (Some("Settings"), Some("ExecutionTimeLimit")) => Some(&mut self.execution_time_limit),
            (Some("Settings"), Some("Enabled")) => Some(&mut self.settings_enabled),
            (Some("Settings"), Some("AllowStartOnDemand")) => Some(&mut self.allow_start_on_demand),
            (Some("RestartOnFailure"), Some("Interval")) => Some(&mut self.restart_interval),
            (Some("RestartOnFailure"), Some("Count")) => Some(&mut self.restart_count),
            (Some("Exec"), Some("Command")) => Some(&mut self.command),
            (Some("Exec"), Some("Arguments")) => Some(&mut self.arguments),
            _ => None,
        };
        if let Some(target) = target {
            let current = target
                .last_mut()
                .ok_or(ServiceRegistrationError::NativeDocument)?;
            if current.len().saturating_add(value.len()) > 8 * 1024 {
                return Err(ServiceRegistrationError::NativeDocument);
            }
            current.push_str(value);
        }
        Ok(())
    }

    fn finish(self) -> Result<TaskSemantics, ServiceRegistrationError> {
        exact(&self.description, OWNER_DESCRIPTION)?;
        exact(&self.uri, REGISTRATION_IDENTITY)?;
        exact(&self.logon_type, "InteractiveToken")?;
        exact(&self.run_level, "LeastPrivilege")?;
        exact(&self.multiple_instances, "IgnoreNew")?;
        exact(&self.logon_enabled, "true")?;
        exact(&self.settings_enabled, "true")?;
        exact(&self.allow_start_on_demand, "true")?;
        exact(&self.execution_time_limit, "PT0S")?;
        if self.user_ids.len() != 2 || self.user_ids[0] != self.user_ids[1] {
            return Err(ServiceRegistrationError::NativeDocument);
        }
        validate_sid(&self.user_ids[0])?;
        let command = one(self.command)?;
        let arguments = one(self.arguments)?;
        if !is_windows_absolute(&command)
            || !arguments.starts_with("--training-release-root \"")
            || !arguments.ends_with('"')
        {
            return Err(ServiceRegistrationError::NativeDocument);
        }
        exact(&self.restart_interval, "PT5S")?;
        let restart_count = one(self.restart_count)?
            .parse::<u8>()
            .map_err(|_| ServiceRegistrationError::NativeDocument)?;
        if restart_count != 3 {
            return Err(ServiceRegistrationError::NativeDocument);
        }
        Ok(TaskSemantics {
            user_sid: self.user_ids[0].clone().into(),
            command: command.into(),
            arguments: arguments.into(),
            restart_interval: "PT5S".into(),
            restart_count,
        })
    }
}

fn increment(value: &mut u8) -> Result<(), ServiceRegistrationError> {
    *value = value
        .checked_add(1)
        .ok_or(ServiceRegistrationError::NativeDocument)?;
    Ok(())
}

fn local_name(name: &[u8]) -> Result<&str, ServiceRegistrationError> {
    let name = std::str::from_utf8(name).map_err(|_| ServiceRegistrationError::NativeDocument)?;
    Ok(name.rsplit(':').next().unwrap_or(name))
}

fn exact(values: &[String], expected: &str) -> Result<(), ServiceRegistrationError> {
    if values.len() != 1 || values[0] != expected {
        return Err(ServiceRegistrationError::NativeDocument);
    }
    Ok(())
}

fn one(values: Vec<String>) -> Result<String, ServiceRegistrationError> {
    if values.len() != 1 {
        return Err(ServiceRegistrationError::NativeDocument);
    }
    values
        .into_iter()
        .next()
        .ok_or(ServiceRegistrationError::NativeDocument)
}

fn is_windows_absolute(value: &str) -> bool {
    let bytes = value.as_bytes();
    (bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes[2], b'\\' | b'/'))
        || value.starts_with(r"\\")
}

fn decode_task_xml(document: &[u8]) -> Result<String, ServiceRegistrationError> {
    let text = if document.starts_with(&[0xff, 0xfe]) {
        let chunks = document[2..].chunks_exact(2);
        if !chunks.remainder().is_empty() {
            return Err(ServiceRegistrationError::NativeDocument);
        }
        let units = chunks
            .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
            .collect::<Vec<_>>();
        String::from_utf16(&units).map_err(|_| ServiceRegistrationError::NativeDocument)?
    } else if document.starts_with(&[0xfe, 0xff]) {
        let chunks = document[2..].chunks_exact(2);
        if !chunks.remainder().is_empty() {
            return Err(ServiceRegistrationError::NativeDocument);
        }
        let units = chunks
            .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
            .collect::<Vec<_>>();
        String::from_utf16(&units).map_err(|_| ServiceRegistrationError::NativeDocument)?
    } else {
        let document = document
            .strip_prefix(&[0xef, 0xbb, 0xbf])
            .unwrap_or(document);
        String::from_utf8(document.to_vec())
            .map_err(|_| ServiceRegistrationError::NativeDocument)?
    };
    Ok(text
        .replacen("encoding=\"UTF-16\"", "encoding=\"UTF-8\"", 1)
        .replacen("encoding='UTF-16'", "encoding='UTF-8'", 1))
}

fn quote_windows_argument(value: &str) -> Result<String, ServiceRegistrationError> {
    if value.chars().any(|character| character.is_control()) {
        return Err(ServiceRegistrationError::Identity);
    }
    let mut quoted = String::from("\"");
    let mut backslashes = 0_usize;
    for character in value.chars() {
        match character {
            '\\' => backslashes += 1,
            '"' => {
                quoted.extend(std::iter::repeat_n('\\', backslashes * 2 + 1));
                quoted.push('"');
                backslashes = 0;
            }
            _ => {
                quoted.extend(std::iter::repeat_n('\\', backslashes));
                quoted.push(character);
                backslashes = 0;
            }
        }
    }
    quoted.extend(std::iter::repeat_n('\\', backslashes * 2));
    quoted.push('"');
    Ok(quoted)
}

fn validate_sid(sid: &str) -> Result<(), ServiceRegistrationError> {
    if sid.len() < 7
        || sid.len() > 184
        || !sid.starts_with("S-1-")
        || sid[4..].split('-').any(|component| {
            component.is_empty() || !component.bytes().all(|byte| byte.is_ascii_digit())
        })
    {
        return Err(ServiceRegistrationError::Identity);
    }
    Ok(())
}
