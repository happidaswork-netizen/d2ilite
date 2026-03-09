use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::Value;

const MANIFEST_DIR: &str = env!("CARGO_MANIFEST_DIR");

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

fn bridge_script_path(root: &Path) -> PathBuf {
    root.join("scripts").join("desktop_bridge_cli.py")
}

fn resolve_python_executable(root: &Path) -> PathBuf {
    let candidates = [
        root.join(".venv").join("Scripts").join("python.exe"),
        root.join(".venv").join("bin").join("python"),
    ];
    candidates
        .into_iter()
        .find(|candidate| candidate.exists())
        .unwrap_or_else(|| PathBuf::from("python"))
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

fn run_bridge_cli(args: &[String]) -> Result<Value, String> {
    let root = project_root();
    let script_path = bridge_script_path(&root);
    if !script_path.exists() {
        return Err(format!(
            "bridge script not found: {}",
            script_path.display()
        ));
    }

    let output = Command::new(resolve_python_executable(&root))
        .arg(&script_path)
        .args(args)
        .current_dir(&root)
        .env("PYTHONUTF8", "1")
        .output()
        .map_err(|error| format!("failed to run bridge cli: {error}"))?;

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
    run_bridge_cli(&[String::from("ping")])
}

#[tauri::command]
fn bridge_list_images(folder: String, limit: Option<i64>) -> Result<Value, String> {
    let limit_value = limit.unwrap_or_default().max(0).to_string();
    run_bridge_cli(&[
        String::from("list"),
        String::from("--folder"),
        folder,
        String::from("--limit"),
        limit_value,
    ])
}

#[tauri::command]
fn bridge_read_metadata(path: String) -> Result<Value, String> {
    run_bridge_cli(&[String::from("read"), String::from("--path"), path])
}

#[tauri::command]
fn bridge_save_metadata(path: String, payload: Value) -> Result<Value, String> {
    let payload_file = write_payload_temp_file(&payload)?;
    let result = run_bridge_cli(&[
        String::from("save"),
        String::from("--path"),
        path,
        String::from("--payload-file"),
        payload_file.to_string_lossy().to_string(),
    ]);
    let _ = fs::remove_file(payload_file);
    result
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![
            bridge_ping,
            bridge_list_images,
            bridge_read_metadata,
            bridge_save_metadata,
        ])
        .setup(|app| {
            if cfg!(debug_assertions) {
                app.handle().plugin(
                    tauri_plugin_log::Builder::default()
                        .level(log::LevelFilter::Info)
                        .build(),
                )?;
            }
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
