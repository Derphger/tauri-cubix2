#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod engine;

use engine::{Cb, S};
use serde_json::{json, Value};
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tauri::{AppHandle, Emitter};
use tauri_plugin_dialog::DialogExt;

static BUSY: AtomicBool = AtomicBool::new(false);

struct Guard;
impl Drop for Guard {
    fn drop(&mut self) {
        BUSY.store(false, Ordering::SeqCst);
    }
}
fn guard() -> Result<Guard, String> {
    if BUSY.swap(true, Ordering::SeqCst) {
        Err("Идёт другая операция, подожди".into())
    } else {
        Ok(Guard)
    }
}

fn mkcb(app: &AppHandle) -> Cb {
    let a = app.clone();
    Arc::new(move |t: &str, f: Option<f64>| {
        let _ = a.emit("progress", (t.to_string(), f));
    })
}

async fn blocking<T: Send + 'static>(f: impl FnOnce() -> Result<T, String> + Send + 'static) -> Result<T, String> {
    match tauri::async_runtime::spawn_blocking(f).await {
        Ok(r) => r,
        Err(x) => Err(x.to_string()),
    }
}

fn inst_of(iid: &str) -> Result<Value, String> {
    engine::get_instance(iid).ok_or_else(|| "Сборка не найдена".to_string())
}

fn mods_dir(iid: &str) -> Result<std::path::PathBuf, String> {
    Ok(engine::game_dir(&inst_of(iid)?).join("mods"))
}

fn clean_name(name: &str) -> Result<String, String> {
    Path::new(name)
        .file_name()
        .map(|x| x.to_string_lossy().to_string())
        .ok_or_else(|| "Плохое имя файла".to_string())
}

// ───────────── состояние ─────────────
#[tauri::command]
fn state() -> Value {
    let c = engine::load_config();
    json!({
        "nick": c["nick"],
        "java": c["java"],
        "ram_gb": c["ram_mb"].as_u64().unwrap_or(4096) / 1024,
        "has_key": !c["cf_key"].as_str().unwrap_or("").is_empty(),
        "last": c["last"],
        "instances": engine::list_instances()
    })
}

#[tauri::command]
async fn releases() -> Vec<String> {
    blocking(|| engine::release_ids())
        .await
        .unwrap_or_else(|_| ["1.21.1", "1.20.1", "1.19.4", "1.18.2", "1.16.5"].iter().map(|s| s.to_string()).collect())
}

// ───────────── сборки ─────────────
#[tauri::command]
async fn create(app: AppHandle, name: String, mc: String, loader: String) -> Result<Value, String> {
    let _g = guard()?;
    let cb = mkcb(&app);
    blocking(move || {
        let n = if name.trim().is_empty() { "Новая сборка".to_string() } else { name.trim().to_string() };
        engine::create_instance(&n, mc.trim(), &loader, None, &cb)
    })
    .await
}

#[tauri::command]
async fn import_pack(app: AppHandle) -> Result<Option<Value>, String> {
    let _g = guard()?;
    let cb = mkcb(&app);
    let a = app.clone();
    let key = engine::load_config()["cf_key"].as_str().unwrap_or("").to_string();
    blocking(move || {
        let picked = a.dialog().file().add_filter("Сборки", &["mrpack", "zip"]).blocking_pick_file();
        let Some(p) = picked else { return Ok(None) };
        let path = p.into_path().s()?;
        let (inst, miss) = engine::import_modpack(&path, &cb, &key)?;
        Ok(Some(json!([inst, miss])))
    })
    .await
}

#[tauri::command]
fn delete_instance(iid: String) -> Result<(), String> {
    engine::delete_instance(&inst_of(&iid)?);
    Ok(())
}

#[tauri::command]
fn open_dir(iid: Option<String>) -> Result<(), String> {
    let p = match iid {
        Some(i) if !i.is_empty() => engine::game_dir(&inst_of(&i)?),
        _ => engine::root(),
    };
    #[cfg(windows)]
    {
        let _ = Command::new("explorer").arg(&p).spawn();
    }
    #[cfg(not(windows))]
    {
        let _ = Command::new("xdg-open").arg(&p).spawn();
    }
    Ok(())
}

#[tauri::command]
fn set_ram(iid: String, ram_gb: u64) -> Result<(), String> {
    let mut i = inst_of(&iid)?;
    i["ram_mb"] = json!(ram_gb * 1024);
    engine::save_instance(&i)
}

#[tauri::command]
fn log(iid: String) -> Result<String, String> {
    let i = inst_of(&iid)?;
    let p = engine::dir("instances").join(i["id"].as_str().unwrap_or("")).join("launch.log");
    match std::fs::read_to_string(&p) {
        Ok(t) => {
            let n = t.chars().count();
            Ok(t.chars().skip(n.saturating_sub(20000)).collect())
        }
        Err(_) => Ok("Лог появится после первого запуска.".to_string()),
    }
}

// ───────────── запуск ─────────────
#[tauri::command]
async fn play(app: AppHandle, iid: String, nick: String) -> Result<(), String> {
    let nick = nick.trim().to_string();
    if nick.len() < 3 || nick.len() > 16 || !nick.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err("Ник: 3–16 символов, латиница, цифры и _".into());
    }
    let _g = guard()?;
    let inst = inst_of(&iid)?;
    let mut cfg = engine::load_config();
    cfg["nick"] = json!(nick);
    cfg["last"] = json!(iid);
    engine::save_config(&cfg)?;
    let cb = mkcb(&app);
    blocking(move || engine::launch(&inst, &nick, &cfg, &cb)).await
}

// ───────────── моды ─────────────
#[tauri::command]
fn mods(iid: String) -> Result<Value, String> {
    let d = mods_dir(&iid)?;
    let mut v: Vec<(String, bool)> = vec![];
    if let Ok(rd) = std::fs::read_dir(&d) {
        for e in rd.flatten() {
            let n = e.file_name().to_string_lossy().to_string();
            if n.ends_with(".jar") {
                v.push((n, true));
            } else if n.ends_with(".jar.disabled") {
                v.push((n.trim_end_matches(".disabled").to_string(), false));
            }
        }
    }
    v.sort_by_key(|x| x.0.to_lowercase());
    Ok(Value::Array(v.into_iter().map(|(n, on)| json!({"name": n, "on": on})).collect()))
}

#[tauri::command]
fn toggle_mod(iid: String, name: String) -> Result<(), String> {
    let d = mods_dir(&iid)?;
    let n = clean_name(&name)?;
    let on = d.join(&n);
    let off = d.join(format!("{n}.disabled"));
    if on.exists() {
        std::fs::rename(&on, &off).s()
    } else if off.exists() {
        std::fs::rename(&off, &on).s()
    } else {
        Ok(())
    }
}

#[tauri::command]
fn delete_mod(iid: String, name: String) -> Result<(), String> {
    let d = mods_dir(&iid)?;
    let n = clean_name(&name)?;
    let _ = std::fs::remove_file(d.join(&n));
    let _ = std::fs::remove_file(d.join(format!("{n}.disabled")));
    Ok(())
}

// ───────────── каталог ─────────────
#[tauri::command]
async fn search(q: String, src: String, kind: String, iid: Option<String>) -> Result<Value, String> {
    let key = engine::load_config()["cf_key"].as_str().unwrap_or("").to_string();
    blocking(move || {
        let inst = iid.as_deref().and_then(engine::get_instance);
        if src == "Modrinth" {
            engine::mr_search(&q, &kind, inst.as_ref())
        } else {
            if key.is_empty() {
                return Err("Для CurseForge нужен API-ключ: Настройки".to_string());
            }
            engine::cf_search(&key, &q, &kind, inst.as_ref())
        }
    })
    .await
}

#[tauri::command]
async fn install(app: AppHandle, src: String, pid: String, kind: String, iid: Option<String>) -> Result<Value, String> {
    let _g = guard()?;
    let key = engine::load_config()["cf_key"].as_str().unwrap_or("").to_string();
    let cb = mkcb(&app);
    blocking(move || {
        if kind == "modpack" {
            let (i, m) = engine::modpack_from_source(&src, &pid, &key, &cb)?;
            Ok(json!([i, m]))
        } else {
            let inst = iid
                .as_deref()
                .and_then(engine::get_instance)
                .ok_or_else(|| "Сначала выбери сборку".to_string())?;
            let g = engine::install_content(&src, &pid, &kind, &inst, &key, &cb)?;
            Ok(json!(g))
        }
    })
    .await
}

// ───────────── настройки ─────────────
#[tauri::command]
fn save_settings(key: String, java: String, ram_gb: u64) -> Result<(), String> {
    let mut c = engine::load_config();
    let k = key.trim().trim_matches(|ch| ch == '"' || ch == '\'').to_string();
    if !k.is_empty() {
        c["cf_key"] = json!(k);
    }
    c["java"] = json!(java.trim());
    c["ram_mb"] = json!(ram_gb.max(1) * 1024);
    engine::save_config(&c)
}

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .invoke_handler(tauri::generate_handler![
            state, releases, create, import_pack, delete_instance, open_dir, set_ram, log, play, mods,
            toggle_mod, delete_mod, search, install, save_settings
        ])
        .run(tauri::generate_context!())
        .expect("error while running Cubix");
}
