use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::Value;

const MANIFEST_DIR: &str = env!("CARGO_MANIFEST_DIR");
const IMAGE_EXTS: [&str; 7] = ["jpg", "jpeg", "png", "webp", "bmp", "tif", "tiff"];

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

fn metadata_backend_script_path(root: &Path) -> PathBuf {
    root.join("scripts").join("desktop_metadata_backend.py")
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

    Ok(items
        .into_iter()
        .map(|path| path.to_string_lossy().to_string())
        .collect())
}

fn run_metadata_backend(args: &[String]) -> Result<Value, String> {
    let root = project_root();
    let script_path = metadata_backend_script_path(&root);
    if !script_path.exists() {
        return Err(format!(
            "metadata backend script not found: {}",
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
    run_metadata_backend(&[String::from("ping")])
}

#[tauri::command]
fn bridge_list_images(folder: String, limit: Option<i64>) -> Result<Value, String> {
    let folder_path = PathBuf::from(folder.trim());
    let items = list_images_in_folder(&folder_path, limit.unwrap_or_default().max(0) as usize)?;
    Ok(serde_json::json!({
        "ok": true,
        "folder": folder_path.to_string_lossy().to_string(),
        "count": items.len(),
        "items": items,
    }))
}

#[tauri::command]
fn bridge_read_metadata(path: String) -> Result<Value, String> {
    run_metadata_backend(&[String::from("read"), String::from("--path"), path])
}

#[tauri::command]
fn bridge_save_metadata(path: String, payload: Value) -> Result<Value, String> {
    let payload_file = write_payload_temp_file(&payload)?;
    let result = run_metadata_backend(&[
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
