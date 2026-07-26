use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(windows)]
use std::os::windows::process::CommandExt;

use serde_json::{json, Map, Value};

const MANIFEST_DIR: &str = env!("CARGO_MANIFEST_DIR");
const IMAGE_EXTS: [&str; 7] = ["jpg", "jpeg", "png", "webp", "bmp", "tif", "tiff"];
const NATIVE_METADATA_PROVIDER: &str = "native-exiftool";
const NATIVE_METADATA_VERSION: &str = "metadata-native-v2-nas-paths";
#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x08000000;

fn hide_command_window(command: &mut Command) {
    #[cfg(windows)]
    {
        command.creation_flags(CREATE_NO_WINDOW);
    }
    #[cfg(not(windows))]
    {
        let _ = command;
    }
}

fn desktop_root() -> PathBuf {
    Path::new(MANIFEST_DIR)
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from(MANIFEST_DIR))
}

fn project_root() -> PathBuf {
    let desktop = desktop_root();
    desktop.parent().map(Path::to_path_buf).unwrap_or(desktop)
}

fn scraper_backend_script_path(root: &Path) -> PathBuf {
    root.join("desktop-next")
        .join("scripts")
        .join("nativeScraperBackend.ts")
}

fn image_action_cli_path(root: &Path) -> PathBuf {
    root.join("scripts").join("image_action_cli.py")
}

fn metadata_ai_cli_path(root: &Path) -> PathBuf {
    root.join("scripts").join("metadata_ai_cli.py")
}

fn settings_cli_path(root: &Path) -> PathBuf {
    root.join("scripts").join("settings_cli.py")
}

fn exiftool_config_path(root: &Path) -> PathBuf {
    root.join("desktop-next")
        .join("config")
        .join("exiftool-titi.config")
}

fn resolve_exiftool_executable(root: &Path) -> PathBuf {
    let candidates = [
        root.join("desktop-next")
            .join("node_modules")
            .join("exiftool-vendored.exe")
            .join("bin")
            .join("exiftool.exe"),
        root.join("desktop-next")
            .join("node_modules")
            .join("exiftool-vendored.pl")
            .join("bin")
            .join("exiftool"),
    ];
    candidates
        .into_iter()
        .find(|candidate| candidate.exists())
        .unwrap_or_else(|| PathBuf::from("exiftool"))
}

fn resolve_node_executable(root: &Path) -> PathBuf {
    let candidates = [
        root.join("desktop-next")
            .join("node_modules")
            .join(".bin")
            .join("node"),
        root.join("desktop-next")
            .join("node_modules")
            .join(".bin")
            .join("node.exe"),
    ];
    candidates
        .into_iter()
        .find(|candidate| candidate.exists())
        .unwrap_or_else(|| PathBuf::from("node"))
}

fn resolve_python_executable(root: &Path) -> PathBuf {
    let candidates = [
        root.join(".venv").join("Scripts").join("python.exe"),
        root.join(".venv").join("bin").join("python"),
    ];
    candidates
        .into_iter()
        .find(|candidate| candidate.exists())
        .unwrap_or_else(|| {
            if cfg!(windows) {
                PathBuf::from("python")
            } else {
                PathBuf::from("python3")
            }
        })
}

fn sanitize_filename_stem(value: &str, fallback: &str) -> String {
    let mut out = String::new();
    for ch in value.trim().chars() {
        let invalid = matches!(ch, '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*')
            || (ch as u32) < 0x20;
        out.push(if invalid { '_' } else { ch });
    }
    let collapsed = out.split_whitespace().collect::<Vec<_>>().join(" ");
    let cleaned = collapsed
        .trim_matches(|ch| ch == '.' || ch == ' ')
        .to_string();
    if cleaned.is_empty() {
        fallback.to_string()
    } else {
        cleaned
    }
}

fn unique_sibling_path(source: &Path, new_name: &str) -> Result<PathBuf, String> {
    let parent = source
        .parent()
        .ok_or_else(|| String::from("source file has no parent directory"))?;
    let fallback = source
        .file_stem()
        .and_then(|v| v.to_str())
        .unwrap_or("image");
    let stem = sanitize_filename_stem(new_name, fallback);
    let extension = source
        .extension()
        .and_then(|v| v.to_str())
        .map(|v| format!(".{v}"))
        .unwrap_or_default();
    let first = parent.join(format!("{stem}{extension}"));
    if first == source || !first.exists() {
        return Ok(first);
    }
    for index in 2..10000 {
        let candidate = parent.join(format!("{stem}_{index}{extension}"));
        if !candidate.exists() {
            return Ok(candidate);
        }
    }
    Err(String::from("unable to create unique filename"))
}

fn normalize_windows_path_text(value: &str) -> String {
    let mut text = value.trim().trim_matches('"').replace('/', "\\");
    if cfg!(windows) {
        while text.starts_with("\\\\\\") && !text.starts_with("\\\\?\\") {
            text = text.replacen("\\\\\\", "\\\\", 1);
        }
        let lower = text.to_lowercase();
        if lower.starts_with("\\\\?\\unc\\") {
            text = format!("\\\\{}", &text[8..]);
        } else if lower.starts_with("\\?\\unc\\") {
            text = format!("\\\\{}", &text[7..]);
        } else if lower.starts_with("?\\unc\\") {
            text = format!("\\\\{}", &text[6..]);
        } else if lower.starts_with("\\\\?\\") {
            text = text[4..].to_string();
        } else if lower.starts_with("\\?\\") {
            text = text[3..].to_string();
        } else if lower.starts_with("?\\") {
            text = text[2..].to_string();
        }
        let lower = text.to_lowercase();
        if lower.starts_with("\\?\\unc\\") {
            text = format!("\\\\{}", &text[7..]);
        } else if lower.starts_with("?\\unc\\") {
            text = format!("\\\\{}", &text[6..]);
        } else if lower.starts_with("\\\\?\\") {
            text = text[4..].to_string();
        } else if lower.starts_with("\\?\\") {
            text = text[3..].to_string();
        } else if lower.starts_with("?\\") {
            text = text[2..].to_string();
        }
    }
    text
}

fn normalize_input_path(value: &str) -> PathBuf {
    PathBuf::from(normalize_windows_path_text(value))
}

fn normalize_existing_path(value: &str) -> PathBuf {
    normalize_input_path(value)
}

fn app_path_text(path: &Path) -> String {
    normalize_windows_path_text(&path.to_string_lossy())
}

fn require_existing_file_path(value: &str) -> Result<PathBuf, String> {
    let target = normalize_existing_path(value);
    if target.is_file() {
        Ok(target)
    } else {
        Err(format!(
            "image not found (raw={}, normalized={})",
            value.trim(),
            target.display()
        ))
    }
}

fn open_with_system(path: &Path, reveal: bool) -> Result<(), String> {
    let target = path.to_path_buf();
    if !target.exists() {
        return Err(format!("path not found ({})", target.display()));
    }

    let mut command = if cfg!(windows) {
        let mut cmd = Command::new("explorer.exe");
        if reveal {
            cmd.arg(format!("/select,{}", target.to_string_lossy()));
        } else {
            cmd.arg(target.to_string_lossy().to_string());
        }
        cmd
    } else if cfg!(target_os = "macos") {
        let mut cmd = Command::new("open");
        if reveal {
            cmd.arg("-R");
        }
        cmd.arg(target.to_string_lossy().to_string());
        cmd
    } else {
        let mut cmd = Command::new("xdg-open");
        if reveal {
            let parent = target.parent().unwrap_or_else(|| Path::new("."));
            cmd.arg(parent.to_string_lossy().to_string());
        } else {
            cmd.arg(target.to_string_lossy().to_string());
        }
        cmd
    };

    command
        .spawn()
        .map_err(|error| format!("failed to open path: {error}"))?;
    Ok(())
}

fn parse_bridge_error(payload: &Value) -> String {
    let error = payload
        .get("error")
        .and_then(Value::as_str)
        .unwrap_or("bridge command failed");
    let detail = payload.get("detail").and_then(Value::as_str).unwrap_or("");
    if detail.is_empty() {
        error.to_string()
    } else {
        format!("{error} ({detail})")
    }
}

fn run_python_json_cli(
    root: &Path,
    script_path: PathBuf,
    args: &[String],
    failure_context: &str,
) -> Result<Value, String> {
    let mut command = Command::new(resolve_python_executable(root));
    command.current_dir(root).arg(script_path);
    for arg in args {
        command.arg(arg);
    }
    hide_command_window(&mut command);
    let output = command
        .output()
        .map_err(|error| format!("failed to run {failure_context}: {error}"))?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let last_line = stdout
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .unwrap_or("");
    let payload = serde_json::from_str::<Value>(last_line)
        .map_err(|error| format!("failed to parse {failure_context} output: {error}; {stderr}"))?;
    if !output.status.success() || payload.get("ok").and_then(Value::as_bool) != Some(true) {
        return Err(parse_bridge_error(&payload));
    }
    Ok(payload)
}

#[cfg(windows)]
fn powershell_pick_result_path(kind: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "d2i-lite-{kind}-{}-{}.txt",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ))
}

#[cfg(windows)]
fn read_powershell_pick_result(path: &Path) -> Result<Option<String>, String> {
    if !path.exists() {
        return Ok(None);
    }
    let text = fs::read_to_string(path)
        .map_err(|error| format!("failed to read picker result: {error}"))?;
    let _ = fs::remove_file(path);
    let picked = normalize_windows_path_text(text.trim_start_matches('\u{feff}').trim());
    if picked.is_empty() {
        Ok(None)
    } else {
        Ok(Some(picked))
    }
}

#[cfg(windows)]
fn pick_folder_with_system_dialog(
    initial_folder: Option<String>,
) -> Result<Option<String>, String> {
    let result_path = powershell_pick_result_path("folder");
    let script = r#"
Add-Type -AssemblyName System.Windows.Forms
$dialog = New-Object System.Windows.Forms.FolderBrowserDialog
$dialog.Description = 'Select image folder'
$dialog.ShowNewFolderButton = $false
$initial = [string]$env:D2I_INITIAL_FOLDER
if ($initial -and (Test-Path -LiteralPath $initial -PathType Container)) {
  $dialog.SelectedPath = $initial
}
$result = $dialog.ShowDialog()
if ($result -eq [System.Windows.Forms.DialogResult]::OK) {
  [System.IO.File]::WriteAllText([string]$env:D2I_PICK_RESULT_FILE, $dialog.SelectedPath, [System.Text.UTF8Encoding]::new($false))
}
"#;
    let mut command = Command::new("powershell.exe");
    command
        .arg("-NoProfile")
        .arg("-Sta")
        .arg("-ExecutionPolicy")
        .arg("Bypass")
        .arg("-Command")
        .arg(script)
        .env(
            "D2I_INITIAL_FOLDER",
            initial_folder
                .map(|value| normalize_windows_path_text(&value))
                .unwrap_or_default(),
        )
        .env(
            "D2I_PICK_RESULT_FILE",
            result_path.to_string_lossy().to_string(),
        );
    hide_command_window(&mut command);
    let output = command
        .output()
        .map_err(|error| format!("failed to open folder picker: {error}"))?;
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if !output.status.success() {
        let _ = fs::remove_file(&result_path);
        return Err(if stderr.is_empty() {
            String::from("folder picker failed")
        } else {
            stderr
        });
    }
    read_powershell_pick_result(&result_path)
}

#[cfg(windows)]
fn pick_image_with_system_dialog(initial_folder: Option<String>) -> Result<Option<String>, String> {
    let result_path = powershell_pick_result_path("image");
    let script = r#"
Add-Type -AssemblyName System.Windows.Forms
$dialog = New-Object System.Windows.Forms.OpenFileDialog
$dialog.Title = 'Select image'
$dialog.Filter = 'Images|*.jpg;*.jpeg;*.png;*.webp;*.bmp;*.tif;*.tiff|All files|*.*'
$dialog.Multiselect = $false
$initial = [string]$env:D2I_INITIAL_FOLDER
if ($initial -and (Test-Path -LiteralPath $initial -PathType Container)) {
  $dialog.InitialDirectory = $initial
}
$result = $dialog.ShowDialog()
if ($result -eq [System.Windows.Forms.DialogResult]::OK) {
  [System.IO.File]::WriteAllText([string]$env:D2I_PICK_RESULT_FILE, $dialog.FileName, [System.Text.UTF8Encoding]::new($false))
}
"#;
    let mut command = Command::new("powershell.exe");
    command
        .arg("-NoProfile")
        .arg("-Sta")
        .arg("-ExecutionPolicy")
        .arg("Bypass")
        .arg("-Command")
        .arg(script)
        .env(
            "D2I_INITIAL_FOLDER",
            initial_folder
                .map(|value| normalize_windows_path_text(&value))
                .unwrap_or_default(),
        )
        .env(
            "D2I_PICK_RESULT_FILE",
            result_path.to_string_lossy().to_string(),
        );
    hide_command_window(&mut command);
    let output = command
        .output()
        .map_err(|error| format!("failed to open image picker: {error}"))?;
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if !output.status.success() {
        let _ = fs::remove_file(&result_path);
        return Err(if stderr.is_empty() {
            String::from("image picker failed")
        } else {
            stderr
        });
    }
    read_powershell_pick_result(&result_path)
}

#[cfg(not(windows))]
fn pick_image_with_system_dialog(
    _initial_folder: Option<String>,
) -> Result<Option<String>, String> {
    Ok(None)
}

#[cfg(not(windows))]
fn pick_folder_with_system_dialog(
    _initial_folder: Option<String>,
) -> Result<Option<String>, String> {
    Ok(None)
}

fn normalize_text(value: Option<&Value>) -> String {
    match value {
        Some(Value::Array(items)) => items
            .first()
            .map(|item| normalize_text(Some(item)))
            .unwrap_or_default(),
        Some(Value::String(text)) => text.replace('\0', "").trim().to_string(),
        Some(Value::Object(map)) => map
            .values()
            .next()
            .map(|item| normalize_text(Some(item)))
            .unwrap_or_default(),
        Some(Value::Null) | None => String::new(),
        Some(other) => other.to_string().replace('\0', "").trim().to_string(),
    }
}

fn normalize_list(value: Option<&Value>) -> Vec<String> {
    let mut output = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    let items: Vec<&Value> = match value {
        Some(Value::Array(values)) => values.iter().collect(),
        Some(other) => vec![other],
        None => Vec::new(),
    };

    for item in items {
        let text = normalize_text(Some(item));
        if text.is_empty() {
            continue;
        }
        let key = text.to_lowercase();
        if seen.insert(key) {
            output.push(text);
        }
    }
    output
}

fn parse_json_candidate(value: Option<&Value>) -> Option<Map<String, Value>> {
    let text = normalize_text(value);
    if text.is_empty() || !text.starts_with('{') || !text.ends_with('}') {
        return None;
    }
    serde_json::from_str::<Value>(&text)
        .ok()
        .and_then(|parsed| parsed.as_object().cloned())
}

fn normalize_gender_value(value: Option<&Value>) -> String {
    let raw = normalize_text(value);
    if raw.is_empty() {
        return String::new();
    }
    let lowered = raw.to_lowercase();
    let normalized = match lowered.as_str() {
        "男" | "male" | "m" | "man" | "男性" => "男",
        "女" | "female" | "f" | "woman" | "女性" => "女",
        _ => raw.as_str(),
    };
    normalized.to_string()
}

fn normalize_police_id_value(raw: &str) -> String {
    let text = raw.trim();
    if text.is_empty() {
        return String::new();
    }
    let lowered = text.to_lowercase();
    let unknown_tokens = [
        "unknown",
        "unkonw",
        "n/a",
        "na",
        "none",
        "null",
        "未知",
        "未详",
        "不详",
        "待补充",
        "-",
    ];
    if unknown_tokens.contains(&lowered.as_str()) || unknown_tokens.contains(&text) {
        String::new()
    } else {
        text.to_string()
    }
}

fn extract_police_id_from_profile(profile: Option<&Map<String, Value>>) -> String {
    let candidate_keys = [
        "police_id",
        "police_no",
        "police_number",
        "badge_no",
        "badge_id",
        "badge_number",
        "officer_id",
        "警号",
    ];

    if let Some(profile_map) = profile {
        for key in candidate_keys {
            let value = normalize_police_id_value(&normalize_text(profile_map.get(key)));
            if !value.is_empty() {
                return value;
            }
        }
        if let Some(extra_fields) = profile_map.get("extra_fields").and_then(Value::as_object) {
            for key in candidate_keys {
                let value = normalize_police_id_value(&normalize_text(extra_fields.get(key)));
                if !value.is_empty() {
                    return value;
                }
            }
        }
    }
    String::new()
}

fn extract_image_url_from_titi_json(titi_json: Option<&Map<String, Value>>) -> String {
    if let Some(data) = titi_json {
        let direct = normalize_text(data.get("source_image").or_else(|| data.get("image_url")));
        if !direct.is_empty() {
            return direct;
        }

        if let Some(items) = data.get("source_images").and_then(Value::as_array) {
            for item in items {
                let text = normalize_text(Some(item));
                if !text.is_empty() {
                    return text;
                }
            }
        }

        if let Some(items) = data.get("source_inputs").and_then(Value::as_array) {
            for item in items {
                if let Some(record) = item.as_object() {
                    for key in ["source_image", "url", "image_url", "filename", "path"] {
                        let text = normalize_text(record.get(key));
                        if !text.is_empty() {
                            return text;
                        }
                    }
                }
            }
        }

        if let Some(profile) = data.get("d2i_profile").and_then(Value::as_object) {
            let text = normalize_text(profile.get("image_url").or_else(|| profile.get("url")));
            if !text.is_empty() {
                return text;
            }
        }
    }
    String::new()
}

fn build_uuid_like_id() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:x}-{:x}", now, std::process::id())
}

fn filter_tag_group(tags: &Map<String, Value>, prefixes: &[&str]) -> Value {
    let mut output = Map::new();
    for (key, value) in tags {
        if prefixes.iter().any(|prefix| key.starts_with(prefix)) {
            output.insert(key.clone(), value.clone());
        }
    }
    Value::Object(output)
}

fn parse_titi_json(tags: &Map<String, Value>) -> Option<Map<String, Value>> {
    let candidates = [
        tags.get("XMP-titi:Meta"),
        tags.get("EXIF:UserComment"),
        tags.get("PNG:Titi"),
        tags.get("PNG:Comment"),
    ];
    for candidate in candidates {
        if let Some(parsed) = parse_json_candidate(candidate) {
            if parsed.contains_key("titi_asset_id")
                || parsed
                    .get("schema")
                    .and_then(Value::as_str)
                    .map(|item| item == "titi-meta")
                    .unwrap_or(false)
            {
                return Some(parsed);
            }
        }
    }
    None
}

fn is_supported_image(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| {
            IMAGE_EXTS
                .iter()
                .any(|allowed| ext.eq_ignore_ascii_case(allowed))
        })
        .unwrap_or(false)
}

fn list_images_in_folder(folder: &Path, limit: usize) -> Result<Vec<String>, String> {
    if !folder.exists() {
        return Err(format!("folder not found ({})", folder.display()));
    }
    if !folder.is_dir() {
        return Err(format!("folder is not a directory ({})", folder.display()));
    }

    let mut items: Vec<PathBuf> = fs::read_dir(folder)
        .map_err(|error| format!("failed to read folder {}: {error}", folder.display()))?
        .filter_map(|entry| entry.ok().map(|item| item.path()))
        .filter(|path| path.is_file() && is_supported_image(path))
        .collect();

    items.sort_by(|left, right| {
        let left_name = left
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or_default()
            .to_lowercase();
        let right_name = right
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or_default()
            .to_lowercase();
        left_name.cmp(&right_name)
    });

    if limit > 0 && items.len() > limit {
        items.truncate(limit);
    }

    Ok(items.into_iter().map(|path| app_path_text(&path)).collect())
}

fn run_scraper_backend(args: &[String]) -> Result<Value, String> {
    let root = project_root();
    let script_path = scraper_backend_script_path(&root);
    if !script_path.exists() {
        return Err(format!(
            "backend script not found: {}",
            script_path.display()
        ));
    }

    let mut command_args = vec![
        String::from("--experimental-strip-types"),
        String::from("--experimental-specifier-resolution=node"),
        script_path.to_string_lossy().to_string(),
    ];
    command_args.extend(args.iter().cloned());

    let mut command = Command::new(resolve_node_executable(&root));
    command.args(&command_args).current_dir(&root);
    hide_command_window(&mut command);
    let output = command
        .output()
        .map_err(|error| format!("failed to run scraper backend: {error}"))?;

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if stdout.is_empty() {
        return Err(if stderr.is_empty() {
            format!(
                "desktop bridge returned empty stdout (code={})",
                output.status.code().unwrap_or(-1)
            )
        } else {
            stderr
        });
    }

    let payload: Value = serde_json::from_str(&stdout)
        .map_err(|error| format!("invalid bridge json: {stdout}\n{error}"))?;
    if !output.status.success() {
        return Err(parse_bridge_error(&payload));
    }
    if payload.get("ok").and_then(Value::as_bool) != Some(true) {
        return Err(parse_bridge_error(&payload));
    }
    Ok(payload)
}

fn run_exiftool_raw(args: &[String]) -> Result<String, String> {
    let root = project_root();
    let executable = resolve_exiftool_executable(&root);
    let config_path = exiftool_config_path(&root);

    let mut command = Command::new(executable);
    command
        .arg("-config")
        .arg(config_path)
        .arg("-charset")
        .arg("ExifTool=UTF8")
        .args(args)
        .current_dir(&root);
    hide_command_window(&mut command);
    let output = command
        .output()
        .map_err(|error| format!("failed to run exiftool: {error}"))?;

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if !output.status.success() {
        return Err(if stderr.is_empty() { stdout } else { stderr });
    }
    if stdout.is_empty() {
        return Err(if stderr.is_empty() {
            String::from("exiftool returned empty output")
        } else {
            stderr
        });
    }
    Ok(stdout)
}

fn run_exiftool_with_args_file(args: &[String]) -> Result<String, String> {
    let temp_root = project_root().join(".tmp").join("desktop-next");
    fs::create_dir_all(&temp_root)
        .map_err(|error| format!("failed to create exiftool temp root: {error}"))?;
    let args_path = temp_root.join(format!(
        "exiftool-write-{}-{}.args",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let body = format!("{}\n", args.join("\n"));
    fs::write(&args_path, body)
        .map_err(|error| format!("failed to write exiftool args file: {error}"))?;
    let result = run_exiftool_raw(&[String::from("-@"), args_path.to_string_lossy().to_string()]);
    let _ = fs::remove_file(args_path);
    result
}

fn read_exiftool_tags(path: &str) -> Result<Map<String, Value>, String> {
    let args = vec![
        String::from("-json"),
        String::from("-G1"),
        String::from("-a"),
        String::from("-struct"),
        String::from(path),
    ];
    let payload = run_exiftool_with_args_file(&args)?;
    let parsed: Value = serde_json::from_str(&payload)
        .map_err(|error| format!("invalid exiftool json: {error}"))?;
    parsed
        .as_array()
        .and_then(|items| items.first())
        .and_then(Value::as_object)
        .cloned()
        .ok_or_else(|| format!("invalid exiftool payload: {payload}"))
}

fn metadata_keywords(payload: &Map<String, Value>) -> Vec<String> {
    payload
        .get("keywords")
        .map(|value| normalize_list(Some(value)))
        .unwrap_or_default()
}

fn build_merged_titi_json(
    payload: &Map<String, Value>,
    existing: Option<&Map<String, Value>>,
) -> Value {
    let mut base = existing.cloned().unwrap_or_default();
    if normalize_text(base.get("schema")).is_empty() {
        base.insert(
            String::from("schema"),
            Value::String(String::from("titi-meta")),
        );
    }
    if !matches!(base.get("schema_version"), Some(Value::Number(_))) {
        base.insert(String::from("schema_version"), Value::Number(1.into()));
    }
    let app = normalize_text(base.get("app"));
    if app.is_empty() || app == "D2I" {
        base.insert(String::from("app"), Value::String(String::from("PWI")));
    }
    if normalize_text(base.get("component")).is_empty() {
        base.insert(
            String::from("component"),
            Value::String(String::from("forge")),
        );
    }

    let requested_asset_id = normalize_text(payload.get("titi_asset_id"));
    let existing_asset_id = normalize_text(base.get("titi_asset_id"));
    let asset_id = if !requested_asset_id.is_empty() {
        requested_asset_id
    } else if !existing_asset_id.is_empty() {
        existing_asset_id
    } else {
        build_uuid_like_id()
    };
    base.insert(String::from("titi_asset_id"), Value::String(asset_id));

    let requested_world_id = normalize_text(payload.get("titi_world_id"));
    let existing_world_id = normalize_text(base.get("titi_world_id"));
    let world_id = if !requested_world_id.is_empty() {
        requested_world_id
    } else if !existing_world_id.is_empty() {
        existing_world_id
    } else {
        String::from("default")
    };
    base.insert(String::from("titi_world_id"), Value::String(world_id));

    let image_url = normalize_text(payload.get("image_url"));
    if !image_url.is_empty() {
        base.insert(
            String::from("source_image"),
            Value::String(image_url.clone()),
        );
    }

    let mut profile = base
        .get("d2i_profile")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let profile_payload = payload.get("d2i_profile").and_then(Value::as_object);
    if let Some(profile_input) = profile_payload {
        for (key, value) in profile_input {
            let should_remove = match value {
                Value::Null => true,
                Value::String(text) => text.trim().is_empty(),
                Value::Array(items) => items.is_empty(),
                Value::Object(map) => map.is_empty(),
                _ => false,
            };
            if should_remove {
                profile.remove(key);
            } else {
                profile.insert(key.clone(), value.clone());
            }
        }
    }

    let person = normalize_text(payload.get("person"));
    let title = normalize_text(payload.get("title"));
    let description = normalize_text(payload.get("description"));
    let keywords = metadata_keywords(payload);
    let source = normalize_text(payload.get("source"));
    let city = normalize_text(payload.get("city"));
    let gender = normalize_gender_value(
        payload
            .get("gender")
            .or_else(|| profile_payload.and_then(|data| data.get("gender"))),
    );
    let mut police_id = normalize_police_id_value(&normalize_text(payload.get("police_id")));
    if police_id.is_empty() {
        police_id = extract_police_id_from_profile(profile_payload);
    }

    if profile_payload.is_none() && !person.is_empty() {
        profile.insert(String::from("name"), Value::String(person.clone()));
    }
    if !description.is_empty() {
        profile.insert(String::from("description"), Value::String(description));
    }
    if !keywords.is_empty() {
        profile.insert(
            String::from("keywords"),
            Value::Array(keywords.into_iter().map(Value::String).collect()),
        );
    }
    if !source.is_empty() {
        profile.insert(String::from("source"), Value::String(source));
    }
    if !image_url.is_empty() {
        profile.insert(String::from("image_url"), Value::String(image_url));
    }
    if !city.is_empty() {
        profile.insert(String::from("city"), Value::String(city));
    }
    if !gender.is_empty() {
        profile.insert(String::from("gender"), Value::String(gender));
    } else if profile.contains_key("gender") {
        let existing_gender = normalize_gender_value(profile.get("gender"));
        if existing_gender.is_empty() {
            profile.remove("gender");
        } else {
            profile.insert(String::from("gender"), Value::String(existing_gender));
        }
    }
    if !police_id.is_empty() {
        profile.insert(String::from("police_id"), Value::String(police_id));
    } else if profile.contains_key("police_id") {
        let existing_police = normalize_police_id_value(&normalize_text(profile.get("police_id")));
        if existing_police.is_empty() {
            profile.remove("police_id");
        } else {
            profile.insert(String::from("police_id"), Value::String(existing_police));
        }
    }
    if profile_payload.is_none()
        && person.is_empty()
        && !title.is_empty()
        && normalize_text(profile.get("name")).is_empty()
    {
        let fallback = title
            .split(" - ")
            .next()
            .unwrap_or(title.as_str())
            .trim()
            .to_string();
        if !fallback.is_empty() {
            profile.insert(String::from("name"), Value::String(fallback));
        }
    }
    if !profile.is_empty() {
        let extracted_at = format!(
            "{}Z",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        );
        profile.insert(String::from("extracted_at"), Value::String(extracted_at));
        base.insert(String::from("d2i_profile"), Value::Object(profile));
    }

    if let Some(role_aliases) = payload.get("role_aliases") {
        if role_aliases
            .as_array()
            .map(|items| items.is_empty())
            .unwrap_or(false)
        {
            base.remove("role_aliases");
        } else if role_aliases.is_array() {
            base.insert(String::from("role_aliases"), role_aliases.clone());
        }
    }

    Value::Object(base)
}

fn build_metadata_item(path: &str, tags: &Map<String, Value>) -> Result<Value, String> {
    let metadata = fs::metadata(path).map_err(|error| format!("failed to stat file: {error}"))?;
    let titi_json = parse_titi_json(tags);
    let adaptive_profile = titi_json
        .as_ref()
        .and_then(|data| data.get("d2i_profile"))
        .and_then(Value::as_object)
        .cloned();
    let keywords = normalize_list(tags.get("XMP-dc:Subject"));
    let person_list = normalize_list(tags.get("XMP-iptcExt:PersonInImage"));
    let person = person_list.first().cloned().unwrap_or_else(|| {
        adaptive_profile
            .as_ref()
            .map(|profile| normalize_text(profile.get("name")))
            .unwrap_or_default()
    });
    let title = normalize_text(tags.get("XMP-dc:Title"));
    let description = {
        let direct = normalize_text(tags.get("XMP-dc:Description"));
        if direct.is_empty() {
            adaptive_profile
                .as_ref()
                .map(|profile| normalize_text(profile.get("description")))
                .unwrap_or_default()
        } else {
            direct
        }
    };
    let source = {
        let direct = normalize_text(tags.get("XMP-dc:Source"));
        if direct.is_empty() {
            adaptive_profile
                .as_ref()
                .map(|profile| normalize_text(profile.get("source")))
                .unwrap_or_default()
        } else {
            direct
        }
    };
    let image_url = {
        let direct = normalize_text(tags.get("XMP-titi:SourceImage"));
        if direct.is_empty() {
            extract_image_url_from_titi_json(titi_json.as_ref())
        } else {
            direct
        }
    };
    let city = {
        let direct = normalize_text(tags.get("XMP-photoshop:City"));
        if direct.is_empty() {
            adaptive_profile
                .as_ref()
                .map(|profile| normalize_text(profile.get("city")))
                .unwrap_or_default()
        } else {
            direct
        }
    };
    let gender = adaptive_profile
        .as_ref()
        .map(|profile| normalize_gender_value(profile.get("gender")))
        .unwrap_or_default();
    let position = normalize_text(tags.get("XMP-photoshop:AuthorsPosition"));
    let police_id = extract_police_id_from_profile(adaptive_profile.as_ref());
    let titi_asset_id = titi_json
        .as_ref()
        .map(|data| normalize_text(data.get("titi_asset_id")))
        .unwrap_or_default();
    let titi_world_id = titi_json
        .as_ref()
        .map(|data| normalize_text(data.get("titi_world_id")))
        .unwrap_or_default();
    let has_readable = !title.is_empty()
        || !description.is_empty()
        || !keywords.is_empty()
        || !source.is_empty()
        || !city.is_empty()
        || !person.is_empty()
        || !image_url.is_empty();
    let has_titi = titi_json.is_some() || !titi_asset_id.is_empty();
    let status = if !has_readable && !has_titi {
        "none"
    } else if !title.is_empty() && !description.is_empty() && !keywords.is_empty() && has_titi {
        "complete"
    } else {
        "partial"
    };

    Ok(json!({
        "filepath": path,
        "filename": Path::new(path).file_name().and_then(|value| value.to_str()).unwrap_or_default(),
        "title": title,
        "description": description,
        "keywords": keywords,
        "source": source,
        "image_url": image_url,
        "city": city,
        "person": person,
        "gender": gender,
        "position": position,
        "police_id": police_id,
        "titi_asset_id": titi_asset_id,
        "titi_world_id": titi_world_id,
        "filesize": metadata.len(),
        "modified_time": normalize_text(tags.get("System:FileModifyDate")),
        "titi_json": titi_json.map(Value::Object).unwrap_or(Value::Null),
        "other_xmp": filter_tag_group(tags, &["XMP-"]),
        "other_exif": filter_tag_group(tags, &["EXIF:", "JFIF:"]),
        "other_iptc": filter_tag_group(tags, &["IPTC:"]),
        "status": status,
        "matched_row": Value::Null,
    }))
}

fn build_fallback_metadata_item(path: &str, reason: &str) -> Result<Value, String> {
    let metadata = fs::metadata(path).map_err(|error| format!("failed to stat file: {error}"))?;
    let filename = Path::new(path)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    Ok(json!({
        "filepath": path,
        "filename": filename,
        "title": "",
        "description": "",
        "keywords": [],
        "source": "",
        "image_url": "",
        "city": "",
        "person": "",
        "gender": "",
        "position": "",
        "police_id": "",
        "titi_asset_id": "",
        "titi_world_id": "default",
        "filesize": metadata.len(),
        "modified_time": "",
        "titi_json": Value::Null,
        "other_xmp": json!({}),
        "other_exif": json!({
            "metadata_read_error": reason,
        }),
        "other_iptc": json!({}),
        "metadata_read_error": reason,
        "status": "none",
        "matched_row": Value::Null,
    }))
}

fn save_metadata_native(path: &str, payload: &Value) -> Result<(), String> {
    let tags = read_exiftool_tags(path)?;
    let payload_map = payload.as_object().cloned().unwrap_or_default();
    let merged_titi_json = build_merged_titi_json(&payload_map, parse_titi_json(&tags).as_ref());
    let merged_json_text = serde_json::to_string(&merged_titi_json)
        .map_err(|error| format!("failed to serialize titi json: {error}"))?;

    let title = normalize_text(payload_map.get("title"));
    let description = normalize_text(payload_map.get("description"));
    let keywords = metadata_keywords(&payload_map);
    let source = normalize_text(payload_map.get("source"));
    let image_url = normalize_text(payload_map.get("image_url"));
    let city = normalize_text(payload_map.get("city"));
    let person = normalize_text(payload_map.get("person"));
    let position = normalize_text(payload_map.get("position"));
    let user_comment_existing = normalize_text(tags.get("EXIF:UserComment")).to_lowercase();
    let can_update_user_comment = user_comment_existing.is_empty()
        || user_comment_existing.contains("titi_asset_id")
        || (user_comment_existing.contains("schema")
            && user_comment_existing.contains("titi-meta"));

    let mut args = vec![String::from("-overwrite_original")];
    if !title.is_empty() {
        args.push(format!("-XMP-dc:Title={title}"));
    }
    if !description.is_empty() {
        args.push(format!("-XMP-dc:Description={description}"));
        args.push(format!("-EXIF:XPComment={description}"));
    }
    if !keywords.is_empty() {
        args.push(format!("-XMP-dc:Subject={}", keywords[0]));
        for keyword in keywords.into_iter().skip(1) {
            args.push(format!("-XMP-dc:Subject+={keyword}"));
        }
    }
    if !source.is_empty() {
        args.push(format!("-XMP-dc:Source={source}"));
    }
    if !image_url.is_empty() {
        args.push(format!("-XMP-titi:SourceImage={image_url}"));
    }
    if !city.is_empty() {
        args.push(format!("-XMP-photoshop:City={city}"));
    }
    if !person.is_empty() {
        args.push(format!("-XMP-iptcExt:PersonInImage={person}"));
    }
    if !position.is_empty() {
        args.push(format!("-XMP-photoshop:AuthorsPosition={position}"));
    }
    args.push(format!("-XMP-titi:Meta={merged_json_text}"));
    if can_update_user_comment {
        args.push(format!("-EXIF:UserComment={merged_json_text}"));
    }
    args.push(String::from(path));
    run_exiftool_with_args_file(&args).map(|_| ())
}

fn write_payload_temp_file(payload: &Value) -> Result<PathBuf, String> {
    let root = project_root().join(".tmp").join("desktop-next");
    fs::create_dir_all(&root).map_err(|error| format!("failed to create temp root: {error}"))?;
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let path = root.join(format!("payload-{}-{ts}.json", std::process::id()));
    let body = serde_json::to_vec_pretty(payload)
        .map_err(|error| format!("failed to serialize payload: {error}"))?;
    fs::write(&path, body).map_err(|error| format!("failed to write temp payload: {error}"))?;
    Ok(path)
}

#[tauri::command]
fn bridge_ping() -> Result<Value, String> {
    Ok(json!({
        "ok": true,
        "provider": NATIVE_METADATA_PROVIDER,
        "version": NATIVE_METADATA_VERSION,
    }))
}

#[tauri::command]
fn bridge_pick_image(initial_folder: Option<String>) -> Result<Value, String> {
    let picked = pick_image_with_system_dialog(initial_folder)?;
    let canceled = picked.is_none();
    Ok(json!({
        "ok": true,
        "path": picked.unwrap_or_default(),
        "canceled": canceled,
    }))
}

#[tauri::command]
fn bridge_pick_folder(initial_folder: Option<String>) -> Result<Value, String> {
    let picked = pick_folder_with_system_dialog(initial_folder)?;
    let canceled = picked.is_none();
    Ok(json!({
        "ok": true,
        "path": picked.unwrap_or_default(),
        "canceled": canceled,
    }))
}

#[tauri::command]
fn bridge_path_info(path: String) -> Result<Value, String> {
    let target_path = normalize_existing_path(&path);
    Ok(json!({
        "ok": true,
        "path": app_path_text(&target_path),
        "exists": target_path.exists(),
        "is_file": target_path.is_file(),
        "is_dir": target_path.is_dir(),
    }))
}

#[tauri::command]
fn bridge_launch_path() -> Result<Value, String> {
    let picked = std::env::args()
        .skip(1)
        .find(|arg| {
            let text = arg.trim();
            !text.is_empty() && !text.starts_with('-')
        })
        .unwrap_or_default();
    let target_path = normalize_existing_path(&picked);
    Ok(json!({
        "ok": true,
        "path": if picked.trim().is_empty() { String::new() } else { app_path_text(&target_path) },
        "exists": !picked.trim().is_empty() && target_path.exists(),
        "is_file": !picked.trim().is_empty() && target_path.is_file(),
        "is_dir": !picked.trim().is_empty() && target_path.is_dir(),
    }))
}

#[tauri::command]
fn bridge_list_images(folder: String, limit: Option<i64>) -> Result<Value, String> {
    let folder_path = normalize_existing_path(&folder);
    let items = list_images_in_folder(&folder_path, limit.unwrap_or_default().max(0) as usize)?;
    Ok(json!({
        "ok": true,
        "folder": app_path_text(&folder_path),
        "count": items.len(),
        "items": items,
    }))
}

#[tauri::command]
fn bridge_read_metadata(path: String) -> Result<Value, String> {
    let target_path = require_existing_file_path(&path)?;
    let target_text = app_path_text(&target_path);
    let item = match read_exiftool_tags(&target_text) {
        Ok(tags) => build_metadata_item(&target_text, &tags)?,
        Err(error) => build_fallback_metadata_item(&target_text, &error)?,
    };
    Ok(json!({
        "ok": true,
        "item": item,
    }))
}

#[tauri::command]
fn bridge_get_default_scraper_base_root() -> Result<Value, String> {
    run_scraper_backend(&[String::from("default-root")])
}

#[tauri::command]
fn bridge_read_scraper_launch_state(
    source_hint: Option<String>,
    template_path: Option<String>,
) -> Result<Value, String> {
    run_scraper_backend(&[
        String::from("launch-state"),
        String::from("--source-hint"),
        source_hint.unwrap_or_default(),
        String::from("--template-path"),
        template_path.unwrap_or_default(),
    ])
}

#[tauri::command]
fn bridge_read_scraper_workspace(
    base_root: String,
    selected_root: Option<String>,
    progress_limit: Option<i64>,
    log_lines: Option<i64>,
) -> Result<Value, String> {
    run_scraper_backend(&[
        String::from("workspace"),
        String::from("--base-root"),
        base_root,
        String::from("--selected-root"),
        selected_root.unwrap_or_default(),
        String::from("--progress-limit"),
        progress_limit.unwrap_or(300).max(20).to_string(),
        String::from("--log-lines"),
        log_lines.unwrap_or(80).max(20).to_string(),
    ])
}

#[tauri::command]
fn bridge_start_scraper_task(base_root: Option<String>, values: Value) -> Result<Value, String> {
    let payload_file = write_payload_temp_file(&values)?;
    let result = run_scraper_backend(&[
        String::from("start"),
        String::from("--base-root"),
        base_root.unwrap_or_default(),
        String::from("--values-file"),
        payload_file.to_string_lossy().to_string(),
    ]);
    let _ = fs::remove_file(payload_file);
    result
}

#[tauri::command]
fn bridge_run_scraper_action(
    action: String,
    output_root: String,
    base_root: Option<String>,
    control: Option<Value>,
) -> Result<Value, String> {
    let payload_file = write_payload_temp_file(&control.unwrap_or_else(|| json!({})))?;
    let result = run_scraper_backend(&[
        String::from("action"),
        String::from("--action"),
        action,
        String::from("--output-root"),
        output_root,
        String::from("--base-root"),
        base_root.unwrap_or_default(),
        String::from("--options-file"),
        payload_file.to_string_lossy().to_string(),
    ]);
    let _ = fs::remove_file(payload_file);
    result
}

#[tauri::command]
fn bridge_clear_scraper_review_item(
    output_root: String,
    detail_url: String,
    base_root: Option<String>,
) -> Result<Value, String> {
    run_scraper_backend(&[
        String::from("review-clear"),
        String::from("--output-root"),
        output_root,
        String::from("--detail-url"),
        detail_url,
        String::from("--base-root"),
        base_root.unwrap_or_default(),
    ])
}

#[tauri::command]
fn bridge_save_metadata(path: String, payload: Value) -> Result<Value, String> {
    let target_path = require_existing_file_path(&path)?;
    let target_text = app_path_text(&target_path);
    save_metadata_native(&target_text, &payload)?;
    Ok(json!({
        "ok": true,
        "saved": true,
        "path": target_text,
    }))
}

#[tauri::command]
fn bridge_add_name_bar(path: String, options: Value) -> Result<Value, String> {
    let root = project_root();
    let target_path = require_existing_file_path(&path)?;
    let opts = options.as_object().cloned().unwrap_or_default();
    let name = normalize_text(opts.get("name"));
    if name.is_empty() {
        return Err(String::from("name is required"));
    }

    let mut command = Command::new(resolve_python_executable(&root));
    command
        .current_dir(&root)
        .arg(image_action_cli_path(&root))
        .arg("name-bar")
        .arg("--image")
        .arg(target_path.to_string_lossy().to_string())
        .arg("--name")
        .arg(name);

    let output_dir = normalize_text(opts.get("output_dir"));
    if !output_dir.is_empty() {
        command.arg("--output-dir").arg(output_dir);
    }
    let output_format = normalize_text(opts.get("output_format"));
    if !output_format.is_empty() {
        command.arg("--format").arg(output_format);
    }
    let output_name = normalize_text(opts.get("output_name"));
    if !output_name.is_empty() {
        command.arg("--output-name").arg(output_name);
    }

    hide_command_window(&mut command);
    let output = command
        .output()
        .map_err(|error| format!("failed to run image action: {error}"))?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let last_line = stdout
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .unwrap_or("");
    let payload = serde_json::from_str::<Value>(last_line)
        .map_err(|error| format!("failed to parse image action output: {error}; {stderr}"))?;
    if !output.status.success() || payload.get("ok").and_then(Value::as_bool) != Some(true) {
        return Err(parse_bridge_error(&payload));
    }
    let mut body = payload.as_object().cloned().unwrap_or_default();
    body.insert(String::from("ok"), Value::Bool(true));
    Ok(Value::Object(body))
}

#[tauri::command]
fn bridge_rename_image(path: String, new_name: String) -> Result<Value, String> {
    let target_path = require_existing_file_path(&path)?;
    let next_path = unique_sibling_path(&target_path, &new_name)?;
    if next_path != target_path {
        fs::rename(&target_path, &next_path)
            .map_err(|error| format!("failed to rename image: {error}"))?;
    }
    Ok(json!({
        "ok": true,
        "old_path": app_path_text(&target_path),
        "new_path": app_path_text(&next_path),
        "filename": next_path.file_name().and_then(|v| v.to_str()).unwrap_or("").to_string(),
    }))
}

#[tauri::command]
fn bridge_autofill_metadata(path: String, options: Value) -> Result<Value, String> {
    let root = project_root();
    let target_path = require_existing_file_path(&path)?;
    let opts = options.as_object().cloned().unwrap_or_default();
    let input_mode = normalize_text(opts.get("input_mode"));
    let input_mode = if input_mode.is_empty() {
        String::from("filename_metadata")
    } else {
        input_mode
    };

    let mut command = Command::new(resolve_python_executable(&root));
    command
        .current_dir(&root)
        .arg(metadata_ai_cli_path(&root))
        .arg("autofill")
        .arg("--image")
        .arg(target_path.to_string_lossy().to_string())
        .arg("--input-mode")
        .arg(input_mode.clone());

    if let Some(form) = opts.get("form") {
        command
            .arg("--form-json")
            .arg(serde_json::to_string(form).unwrap_or_else(|_| String::from("{}")));
    }

    hide_command_window(&mut command);
    let output = command
        .output()
        .map_err(|error| format!("failed to run metadata AI: {error}"))?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let last_line = stdout
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .unwrap_or("");
    let payload = serde_json::from_str::<Value>(last_line)
        .map_err(|error| format!("failed to parse metadata AI output: {error}; {stderr}"))?;
    if !output.status.success() || payload.get("ok").and_then(Value::as_bool) != Some(true) {
        return Err(parse_bridge_error(&payload));
    }
    Ok(json!({
        "ok": true,
        "result": payload.get("result").cloned().unwrap_or_else(|| json!({})),
        "input_mode": payload.get("input_mode").and_then(Value::as_str).unwrap_or(&input_mode),
    }))
}

#[tauri::command]
fn bridge_generate_biography(path: String, options: Value) -> Result<Value, String> {
    let root = project_root();
    let target_path = require_existing_file_path(&path)?;
    let opts = options.as_object().cloned().unwrap_or_default();

    let mut args = vec![
        String::from("biography"),
        String::from("--image"),
        target_path.to_string_lossy().to_string(),
    ];
    if let Some(form) = opts.get("form") {
        args.push(String::from("--form-json"));
        args.push(serde_json::to_string(form).unwrap_or_else(|_| String::from("{}")));
    }
    let payload = run_python_json_cli(
        &root,
        metadata_ai_cli_path(&root),
        &args,
        "metadata AI biography",
    )?;
    Ok(json!({
        "ok": true,
        "result": payload.get("result").cloned().unwrap_or_else(|| json!({})),
    }))
}

#[tauri::command]
fn bridge_read_app_settings() -> Result<Value, String> {
    let root = project_root();
    let payload = run_python_json_cli(
        &root,
        settings_cli_path(&root),
        &[String::from("get")],
        "settings CLI",
    )?;
    Ok(json!({
        "ok": true,
        "settings": payload.get("settings").cloned().unwrap_or_else(|| json!({})),
        "path": payload.get("path").and_then(Value::as_str).unwrap_or(""),
    }))
}

#[tauri::command]
fn bridge_save_app_settings(settings: Value) -> Result<Value, String> {
    let root = project_root();
    let payload_json = serde_json::to_string(&settings).unwrap_or_else(|_| String::from("{}"));
    let payload = run_python_json_cli(
        &root,
        settings_cli_path(&root),
        &[
            String::from("save"),
            String::from("--payload-json"),
            payload_json,
        ],
        "settings CLI",
    )?;
    Ok(json!({
        "ok": true,
        "settings": payload.get("settings").cloned().unwrap_or_else(|| json!({})),
        "path": payload.get("path").and_then(Value::as_str).unwrap_or(""),
    }))
}

#[tauri::command]
fn bridge_open_path(path: String) -> Result<Value, String> {
    let target_path = normalize_existing_path(&path);
    open_with_system(&target_path, false)?;
    Ok(json!({
        "ok": true,
        "opened": true,
    }))
}

#[tauri::command]
fn bridge_reveal_path(path: String) -> Result<Value, String> {
    let target_path = normalize_existing_path(&path);
    open_with_system(&target_path, true)?;
    Ok(json!({
        "ok": true,
        "revealed": true,
    }))
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![
            bridge_ping,
            bridge_pick_image,
            bridge_pick_folder,
            bridge_path_info,
            bridge_launch_path,
            bridge_list_images,
            bridge_read_metadata,
            bridge_get_default_scraper_base_root,
            bridge_read_scraper_launch_state,
            bridge_read_scraper_workspace,
            bridge_start_scraper_task,
            bridge_run_scraper_action,
            bridge_clear_scraper_review_item,
            bridge_save_metadata,
            bridge_add_name_bar,
            bridge_rename_image,
            bridge_autofill_metadata,
            bridge_generate_biography,
            bridge_read_app_settings,
            bridge_save_app_settings,
            bridge_open_path,
            bridge_reveal_path,
        ])
        .setup(|app| {
            if cfg!(debug_assertions) {
                let _ = app.handle().plugin(
                    tauri_plugin_log::Builder::default()
                        .level(log::LevelFilter::Info)
                        .build(),
                );
            }
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
