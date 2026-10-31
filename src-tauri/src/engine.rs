//! Cubix engine: версии Minecraft, Java, загрузчики, сборки, Modrinth / CurseForge.
use md5::Md5;
use rayon::prelude::*;
use serde_json::{json, Value};
use sha1::{Digest, Sha1};
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

pub type R<T> = Result<T, String>;
pub type Cb = Arc<dyn Fn(&str, Option<f64>) + Send + Sync>;

pub trait S<T> {
    fn s(self) -> R<T>;
}
impl<T, E: std::fmt::Display> S<T> for Result<T, E> {
    fn s(self) -> R<T> {
        self.map_err(|e| e.to_string())
    }
}

const NOWIN: u32 = 0x08000000;
const MANIFEST: &str = "https://piston-meta.mojang.com/mc/game/version_manifest_v2.json";
const MR: &str = "https://api.modrinth.com/v2";
const CF: &str = "https://api.curseforge.com/v1";

pub fn say(cb: &Cb, t: &str, f: Option<f64>) {
    (cb.as_ref())(t, f)
}
fn gs(v: &Value, k: &str) -> String {
    v[k].as_str().unwrap_or("").to_string()
}
fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{:02x}", x)).collect()
}

// ───────────── пути, конфиг ─────────────
pub fn root() -> PathBuf {
    static R0: OnceLock<PathBuf> = OnceLock::new();
    R0.get_or_init(|| {
        let p = match std::env::var("CUBIX_HOME") {
            Ok(x) => PathBuf::from(x),
            Err(_) => {
                let h = std::env::var("USERPROFILE").or_else(|_| std::env::var("HOME")).unwrap_or(".".into());
                PathBuf::from(h).join(".cubix")
            }
        };
        for d in ["libraries", "assets", "versions", "instances", "runtimes", "tmp"] {
            let _ = fs::create_dir_all(p.join(d));
        }
        p
    })
    .clone()
}
pub fn dir(n: &str) -> PathBuf {
    root().join(n)
}

pub fn load_config() -> Value {
    let mut c = json!({"nick": "Player", "cf_key": "", "java": "", "ram_mb": 4096});
    if let Ok(t) = fs::read_to_string(root().join("config.json")) {
        if let Ok(Value::Object(m)) = serde_json::from_str::<Value>(&t) {
            for (k, v) in m {
                c[k.as_str()] = v;
            }
        }
    }
    c
}
pub fn save_config(c: &Value) -> R<()> {
    fs::write(root().join("config.json"), serde_json::to_string_pretty(c).s()?).s()
}

// ───────────── сеть ─────────────
fn http() -> &'static reqwest::blocking::Client {
    static C: OnceLock<reqwest::blocking::Client> = OnceLock::new();
    C.get_or_init(|| {
        reqwest::blocking::Client::builder()
            .user_agent("Cubix/1.0 (personal launcher)")
            .timeout(Duration::from_secs(900))
            .build()
            .expect("http client")
    })
}
fn get_json(url: &str) -> R<Value> {
    http().get(url).send().s()?.error_for_status().s()?.json::<Value>().s()
}
fn get_q(url: &str, q: &[(&str, String)]) -> R<Value> {
    http().get(url).query(q).send().s()?.error_for_status().s()?.json::<Value>().s()
}
pub fn fetch_bytes(url: &str) -> R<Vec<u8>> {
    Ok(http().get(url).send().s()?.error_for_status().s()?.bytes().s()?.to_vec())
}

fn try_dl(url: &str, path: &Path, sha1: Option<&str>) -> R<()> {
    let mut r = http().get(url).send().s()?.error_for_status().s()?;
    let tmp = PathBuf::from(format!("{}.part", path.display()));
    let mut f = File::create(&tmp).s()?;
    let mut h = Sha1::new();
    let mut buf = [0u8; 65536];
    loop {
        let n = r.read(&mut buf).s()?;
        if n == 0 {
            break;
        }
        f.write_all(&buf[..n]).s()?;
        h.update(&buf[..n]);
    }
    drop(f);
    if let Some(s) = sha1 {
        if hex(&h.finalize()) != s {
            let _ = fs::remove_file(&tmp);
            return Err("sha1 не совпал".into());
        }
    }
    fs::rename(&tmp, path).s()
}

pub fn download(url: &str, path: &Path, sha1: Option<&str>) -> R<()> {
    if path.exists() {
        return Ok(());
    }
    if let Some(p) = path.parent() {
        fs::create_dir_all(p).s()?;
    }
    let mut last = String::new();
    for _ in 0..3 {
        match try_dl(url, path, sha1) {
            Ok(()) => return Ok(()),
            Err(e) => last = e,
        }
    }
    Err(format!("Не скачалось: {url} ({last})"))
}

pub struct Job {
    pub url: String,
    pub dst: PathBuf,
    pub sha: Option<String>,
}

pub fn fetch_all(jobs: Vec<Job>, cb: &Cb, label: &str, tolerant: bool) -> R<Vec<String>> {
    let n = jobs.len();
    if n == 0 {
        return Ok(vec![]);
    }
    let done = AtomicUsize::new(0);
    let failed: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let err: Mutex<Option<String>> = Mutex::new(None);
    let pool = rayon::ThreadPoolBuilder::new().num_threads(16).build().s()?;
    pool.install(|| {
        jobs.par_iter().for_each(|j| {
            if !j.url.is_empty() {
                if let Err(e) = download(&j.url, &j.dst, j.sha.as_deref()) {
                    if tolerant {
                        let nm = j.dst.file_name().map(|x| x.to_string_lossy().to_string()).unwrap_or_default();
                        failed.lock().unwrap().push(nm);
                    } else {
                        *err.lock().unwrap() = Some(e);
                    }
                }
            }
            let d = done.fetch_add(1, Ordering::SeqCst) + 1;
            if d % 10 == 0 || d == n {
                say(cb, &format!("{label} {d}/{n}"), Some(d as f64 / n as f64));
            }
        });
    });
    if let Some(e) = err.into_inner().unwrap() {
        return Err(e);
    }
    Ok(failed.into_inner().unwrap())
}

// ───────────── утилиты ─────────────
pub fn safe_join(base: &Path, rel: &str) -> R<PathBuf> {
    let r = Path::new(rel);
    if r.is_absolute() || r.components().any(|c| matches!(c, Component::ParentDir | Component::Prefix(_) | Component::RootDir)) {
        return Err(format!("Небезопасный путь в архиве: {rel}"));
    }
    Ok(base.join(r))
}

fn unzip(arc: &Path, dest: &Path) -> R<()> {
    let f = File::open(arc).s()?;
    let mut z = zip::ZipArchive::new(f).s()?;
    z.extract(dest).s()
}

fn read_entry(z: &mut zip::ZipArchive<File>, name: &str) -> R<Vec<u8>> {
    let mut e = z.by_name(name).s()?;
    let mut b = Vec::new();
    e.read_to_end(&mut b).s()?;
    Ok(b)
}

fn extract_overrides(z: &mut zip::ZipArchive<File>, prefix: &str, gd: &Path) -> R<()> {
    let names: Vec<String> = z.file_names().map(String::from).collect();
    for n in names {
        if n.starts_with(prefix) && !n.ends_with('/') {
            let dst = safe_join(gd, &n[prefix.len()..])?;
            if let Some(p) = dst.parent() {
                fs::create_dir_all(p).s()?;
            }
            let b = read_entry(z, &n)?;
            fs::write(&dst, b).s()?;
        }
    }
    Ok(())
}

pub fn offline_uuid(name: &str) -> String {
    let d = Md5::digest(format!("OfflinePlayer:{name}").as_bytes());
    let mut h = [0u8; 16];
    h.copy_from_slice(&d);
    h[6] = (h[6] & 0x0f) | 0x30;
    h[8] = (h[8] & 0x3f) | 0x80;
    let x = hex(&h);
    format!("{}-{}-{}-{}-{}", &x[0..8], &x[8..12], &x[12..16], &x[16..20], &x[20..])
}

// ───────────── версии ─────────────
pub fn release_ids() -> R<Vec<String>> {
    let m = get_json(MANIFEST)?;
    Ok(m["versions"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter(|v| v["type"] == "release")
        .map(|v| gs(v, "id"))
        .collect())
}

fn os_match(n: &str) -> bool {
    n == "windows"
        || (n == "windows-arm64" && cfg!(target_arch = "aarch64"))
        || (n == "windows-x86" && cfg!(target_pointer_width = "32"))
}

pub fn rules_ok(rules: Option<&Value>) -> bool {
    let arr = match rules.and_then(|r| r.as_array()) {
        Some(a) if !a.is_empty() => a,
        _ => return true,
    };
    let mut ok = false;
    for r in arr {
        let mut m = true;
        if let Some(o) = r.get("os") {
            if let Some(n) = o["name"].as_str() {
                if !os_match(n) {
                    m = false;
                }
            }
            match o["arch"].as_str() {
                Some("x86") if !cfg!(target_pointer_width = "32") => m = false,
                Some("arm64") if !cfg!(target_arch = "aarch64") => m = false,
                _ => {}
            }
        }
        if r.get("features").is_some() {
            m = false;
        }
        if m {
            ok = r["action"] == "allow";
        }
    }
    ok
}

fn lib_path(name: &str) -> String {
    let p: Vec<&str> = name.split(':').collect();
    let (g, a) = (p[0], p[1]);
    let mut v = p[2].to_string();
    let mut ext = "jar".to_string();
    if let Some((vv, e)) = p[2].split_once('@') {
        v = vv.to_string();
        ext = e.to_string();
    }
    let cls = if p.len() > 3 { format!("-{}", p[3]) } else { String::new() };
    format!("{}/{}/{}/{}-{}{}.{}", g.replace('.', "/"), a, v, a, v, cls, ext)
}

fn merge(p: &Value, c: &Value) -> Value {
    let mut m = p.clone();
    if let (Some(mo), Some(co)) = (m.as_object_mut(), c.as_object()) {
        for (k, v) in co {
            mo.insert(k.clone(), v.clone());
        }
    }
    let mut libs = c["libraries"].as_array().cloned().unwrap_or_default();
    libs.extend(p["libraries"].as_array().cloned().unwrap_or_default());
    m["libraries"] = Value::Array(libs);
    if p.get("arguments").is_some() || c.get("arguments").is_some() {
        let g = |k: &str| {
            let mut v = p["arguments"][k].as_array().cloned().unwrap_or_default();
            v.extend(c["arguments"][k].as_array().cloned().unwrap_or_default());
            Value::Array(v)
        };
        m["arguments"] = json!({"game": g("game"), "jvm": g("jvm")});
    }
    m["_vanilla"] = p["_vanilla"].clone();
    m
}

pub fn load_version(vid: &str) -> R<Value> {
    let local = dir("versions").join(vid).join(format!("{vid}.json"));
    let mut vj: Value = if local.exists() {
        serde_json::from_str(&fs::read_to_string(&local).s()?).s()?
    } else {
        let m = get_json(MANIFEST)?;
        let e = m["versions"]
            .as_array()
            .and_then(|a| a.iter().find(|v| v["id"] == vid))
            .ok_or(format!("Версия {vid} не найдена"))?
            .clone();
        let j = get_json(e["url"].as_str().unwrap_or(""))?;
        fs::create_dir_all(local.parent().unwrap()).s()?;
        fs::write(&local, j.to_string()).s()?;
        j
    };
    if let Some(p) = vj["inheritsFrom"].as_str() {
        let parent = load_version(p)?;
        return Ok(merge(&parent, &vj));
    }
    vj["_vanilla"] = json!(vid);
    Ok(vj)
}

// ───────────── Java ─────────────
fn find_java(d: &Path) -> Option<PathBuf> {
    let rd = fs::read_dir(d).ok()?;
    for e in rd.flatten() {
        let p = e.path();
        if p.is_dir() {
            if let Some(x) = find_java(&p) {
                return Some(x);
            }
        } else if p.file_name().map(|n| n == "java.exe").unwrap_or(false)
            && p.parent().and_then(|x| x.file_name()).map(|n| n == "bin").unwrap_or(false)
        {
            return Some(p);
        }
    }
    None
}

pub fn ensure_java(major: u64, cb: &Cb) -> R<PathBuf> {
    let d = dir("runtimes").join(major.to_string());
    if let Some(j) = find_java(&d) {
        return Ok(j);
    }
    say(cb, &format!("Скачиваю Java {major}..."), None);
    let arch = if cfg!(target_arch = "aarch64") { "aarch64" } else { "x64" };
    let url = format!("https://api.adoptium.net/v3/binary/latest/{major}/ga/windows/{arch}/jre/hotspot/normal/eclipse");
    let arc = dir("runtimes").join(format!("{major}.archive"));
    download(&url, &arc, None)?;
    fs::create_dir_all(&d).s()?;
    unzip(&arc, &d)?;
    let _ = fs::remove_file(&arc);
    find_java(&d).ok_or("Не удалось распаковать Java".to_string())
}

// ───────────── подготовка версии ─────────────
pub struct Prep {
    pub vj: Value,
    pub cp: Vec<String>,
    pub natives: PathBuf,
}

pub fn prepare(vid: &str, cb: &Cb) -> R<Prep> {
    let vj = load_version(vid)?;
    let base = gs(&vj, "_vanilla");
    let cl = &vj["downloads"]["client"];
    let client = dir("versions").join(&base).join(format!("{base}.jar"));
    say(cb, "Скачиваю клиент...", None);
    download(cl["url"].as_str().ok_or("нет ссылки на клиент")?, &client, cl["sha1"].as_str())?;

    let mut jobs: Vec<Job> = vec![];
    let mut cp: Vec<String> = vec![];
    let mut nat: Vec<(PathBuf, Vec<String>)> = vec![];
    let mut seen: HashSet<String> = HashSet::new();
    let nat_dir = dir("versions").join(vid).join("natives");
    fs::create_dir_all(&nat_dir).s()?;

    for lib in vj["libraries"].as_array().cloned().unwrap_or_default() {
        if !rules_ok(lib.get("rules")) {
            continue;
        }
        let name = gs(&lib, "name");
        let parts: Vec<&str> = name.split(':').collect();
        let key = parts.iter().enumerate().filter(|(i, _)| *i != 2).map(|(_, p)| *p).collect::<Vec<_>>().join(":");
        if !seen.insert(key) {
            continue;
        }
        let d = lib.get("downloads");
        let mut art: Option<(String, String, Option<String>)> = None;
        if let Some(a) = d.and_then(|d| d.get("artifact")) {
            if a.is_object() {
                art = Some((gs(a, "path"), gs(a, "url"), a["sha1"].as_str().map(|x| x.to_string())));
            }
        }
        if art.is_none() && lib.get("natives").is_none() {
            let p = lib_path(&name);
            let bu = lib["url"].as_str().unwrap_or("https://libraries.minecraft.net/");
            art = Some((p.clone(), format!("{bu}{p}"), None));
        }
        if let Some((p, u, sha)) = art {
            let dst = dir("libraries").join(&p);
            jobs.push(Job { url: u, dst: dst.clone(), sha });
            cp.push(dst.to_string_lossy().to_string());
        }
        if let Some(nk) = lib["natives"]["windows"].as_str() {
            let ck = nk.replace("${arch}", "64");
            if let Some(c) = d.and_then(|d| d["classifiers"].get(ck.as_str())) {
                let dst = dir("libraries").join(gs(c, "path"));
                jobs.push(Job { url: gs(c, "url"), dst: dst.clone(), sha: c["sha1"].as_str().map(|x| x.to_string()) });
                let excl: Vec<String> = lib["extract"]["exclude"]
                    .as_array()
                    .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())
                    .unwrap_or_default();
                nat.push((dst, excl));
            }
        }
    }
    fetch_all(jobs, cb, "Библиотеки", false)?;
    for (p, excl) in nat {
        let f = File::open(&p).s()?;
        let mut z = zip::ZipArchive::new(f).s()?;
        for i in 0..z.len() {
            let mut ent = z.by_index(i).s()?;
            let n = ent.name().to_string();
            if n.ends_with('/') || excl.iter().any(|e| n.starts_with(e.as_str())) {
                continue;
            }
            let out = safe_join(&nat_dir, &n)?;
            if out.exists() {
                continue;
            }
            if let Some(pp) = out.parent() {
                fs::create_dir_all(pp).s()?;
            }
            let mut o = File::create(&out).s()?;
            std::io::copy(&mut ent, &mut o).s()?;
        }
    }

    let ai = &vj["assetIndex"];
    let idx = dir("assets").join("indexes").join(format!("{}.json", gs(ai, "id")));
    download(ai["url"].as_str().unwrap_or(""), &idx, None)?;
    let iv: Value = serde_json::from_str(&fs::read_to_string(&idx).s()?).s()?;
    let ajobs: Vec<Job> = iv["objects"]
        .as_object()
        .map(|o| {
            o.values()
                .map(|x| {
                    let h = gs(x, "hash");
                    Job {
                        url: format!("https://resources.download.minecraft.net/{}/{}", &h[..2], h),
                        dst: dir("assets").join("objects").join(&h[..2]).join(&h),
                        sha: Some(h.clone()),
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    fetch_all(ajobs, cb, "Ассеты", false)?;
    cp.push(client.to_string_lossy().to_string());
    Ok(Prep { vj, cp, natives: nat_dir })
}

// ───────────── загрузчики ─────────────
fn install_fabriclike(mc: &str, loader: &str, lv: Option<&str>) -> R<String> {
    let base = if loader == "fabric" { "https://meta.fabricmc.net/v2" } else { "https://meta.quiltmc.org/v3" };
    let lv = match lv {
        Some(x) => x.to_string(),
        None => {
            let lst = get_json(&format!("{base}/versions/loader/{mc}"))?;
            let arr = lst.as_array().cloned().unwrap_or_default();
            if arr.is_empty() {
                return Err(format!("{loader} не поддерживает Minecraft {mc}"));
            }
            let pick = arr.iter().find(|x| x["loader"]["stable"] != false).unwrap_or(&arr[0]);
            gs(&pick["loader"], "version")
        }
    };
    let pj = get_json(&format!("{base}/versions/loader/{mc}/{lv}/profile/json"))?;
    let vid = gs(&pj, "id");
    let d = dir("versions").join(&vid);
    fs::create_dir_all(&d).s()?;
    fs::write(d.join(format!("{vid}.json")), pj.to_string()).s()?;
    Ok(vid)
}

fn install_forge(mc: &str, kind: &str, ver: Option<&str>, cb: &Cb) -> R<String> {
    let ver: String = match ver {
        Some(v) => v.to_string(),
        None if kind == "forge" => {
            let pr = get_json("https://files.minecraftforge.net/net/minecraftforge/forge/promotions_slim.json")?;
            pr["promos"][format!("{mc}-recommended")]
                .as_str()
                .or(pr["promos"][format!("{mc}-latest")].as_str())
                .map(String::from)
                .ok_or(format!("forge не найден для Minecraft {mc}"))?
        }
        None => {
            let parts: Vec<&str> = mc.split('.').collect();
            let prefix = format!("{}.{}.", parts.get(1).unwrap_or(&"0"), parts.get(2).unwrap_or(&"0"));
            let vs = get_json("https://maven.neoforged.net/api/maven/versions/releases/net/neoforged/neoforge")?;
            vs["versions"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .filter(|v| v.starts_with(&prefix) && !v.contains("beta"))
                .last()
                .ok_or(format!("neoforge не найден для Minecraft {mc}"))?
        }
    };
    let url = if kind == "forge" {
        format!("https://maven.minecraftforge.net/net/minecraftforge/forge/{mc}-{ver}/forge-{mc}-{ver}-installer.jar")
    } else {
        format!("https://maven.neoforged.net/releases/net/neoforged/neoforge/{ver}/neoforge-{ver}-installer.jar")
    };
    let cache_f = root().join("loaders.json");
    let mut cache: Value = fs::read_to_string(&cache_f).ok().and_then(|t| serde_json::from_str(&t).ok()).unwrap_or(json!({}));
    let key = format!("{kind}-{mc}-{ver}");
    if let Some(v) = cache[key.as_str()].as_str() {
        if dir("versions").join(v).exists() {
            return Ok(v.to_string());
        }
    }
    let vj = prepare(mc, cb)?.vj;
    let java = ensure_java(vj["javaVersion"]["majorVersion"].as_u64().unwrap_or(8), cb)?;
    let jar = dir("installers").join(format!("{key}.jar"));
    say(cb, &format!("Скачиваю установщик {kind}..."), None);
    download(&url, &jar, None)?;
    let prof = root().join("launcher_profiles.json");
    if !prof.exists() {
        fs::write(&prof, "{\"profiles\":{}}").s()?;
    }
    say(cb, &format!("Устанавливаю {kind} (это долго)..."), None);
    let mut cmd = Command::new(&java);
    cmd.arg("-jar").arg(&jar).arg("--installClient").arg(root()).current_dir(root()).stdin(Stdio::null());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(NOWIN);
    }
    let out = cmd.output().s()?;
    if !out.status.success() {
        let t = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
        let tail: String = t.chars().rev().take(800).collect::<Vec<_>>().into_iter().rev().collect();
        return Err(format!("Установщик упал:\n{tail}"));
    }
    let cands: Vec<String> = fs::read_dir(dir("versions"))
        .s()?
        .flatten()
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|d| d.contains(&ver) && d.to_lowercase().contains(kind))
        .collect();
    let vid = cands.first().ok_or("Установщик отработал, но версия не появилась")?.clone();
    cache[key.as_str()] = json!(vid);
    fs::write(&cache_f, cache.to_string()).s()?;
    Ok(vid)
}

pub fn install_loader(mc: &str, loader: &str, lv: Option<&str>, cb: &Cb) -> R<String> {
    load_version(mc)?;
    match loader {
        "vanilla" => Ok(mc.to_string()),
        "fabric" | "quilt" => install_fabriclike(mc, loader, lv),
        "forge" | "neoforge" => install_forge(mc, loader, lv, cb),
        _ => Err(format!("Неизвестный загрузчик: {loader}")),
    }
}

// ───────────── сборки (instances) ─────────────
fn slug(n: &str) -> String {
    let s: String = n.chars().map(|c| if "\\/:*?\"<>|".contains(c) { '_' } else { c }).collect();
    let s = s.trim_matches(|c: char| c == ' ' || c == '.').to_string();
    if s.is_empty() { "instance".into() } else { s }
}
fn unique_id(name: &str) -> String {
    let base = slug(name);
    let (mut n, mut k) = (base.clone(), 2);
    while dir("instances").join(&n).exists() {
        n = format!("{base} ({k})");
        k += 1;
    }
    n
}
pub fn list_instances() -> Vec<Value> {
    let mut ps: Vec<PathBuf> = fs::read_dir(dir("instances")).map(|r| r.flatten().map(|e| e.path()).collect()).unwrap_or_default();
    ps.sort();
    ps.iter()
        .filter_map(|p| fs::read_to_string(p.join("instance.json")).ok())
        .filter_map(|t| serde_json::from_str::<Value>(&t).ok())
        .collect()
}
pub fn get_instance(iid: &str) -> Option<Value> {
    let t = fs::read_to_string(dir("instances").join(Path::new(iid).file_name()?).join("instance.json")).ok()?;
    serde_json::from_str(&t).ok()
}
pub fn save_instance(i: &Value) -> R<()> {
    let d = dir("instances").join(gs(i, "id"));
    fs::create_dir_all(&d).s()?;
    fs::write(d.join("instance.json"), serde_json::to_string_pretty(i).s()?).s()
}
pub fn delete_instance(i: &Value) {
    let _ = fs::remove_dir_all(dir("instances").join(gs(i, "id")));
}
pub fn game_dir(i: &Value) -> PathBuf {
    let p = dir("instances").join(gs(i, "id")).join("minecraft");
    let _ = fs::create_dir_all(&p);
    p
}

pub fn create_instance(name: &str, mc: &str, loader: &str, lv: Option<&str>, cb: &Cb) -> R<Value> {
    let vid = install_loader(mc, loader, lv, cb)?;
    let inst = json!({"id": unique_id(name), "name": name, "mc": mc, "loader": loader, "version_id": vid});
    save_instance(&inst)?;
    game_dir(&inst);
    prepare(&vid, cb)?;
    Ok(inst)
}

// ───────────── запуск ─────────────
fn expand(args: &Value, vars: &[(&str, String)]) -> Vec<String> {
    let mut out = vec![];
    for a in args.as_array().cloned().unwrap_or_default() {
        let items: Vec<String> = if let Some(s) = a.as_str() {
            vec![s.to_string()]
        } else {
            if !rules_ok(a.get("rules")) {
                continue;
            }
            match &a["value"] {
                Value::Array(v) => v.iter().filter_map(|x| x.as_str().map(String::from)).collect(),
                Value::String(s) => vec![s.clone()],
                _ => vec![],
            }
        };
        for mut s in items {
            for (k, val) in vars {
                s = s.replace(&format!("${{{}}}", k), val);
            }
            out.push(s);
        }
    }
    out
}

pub fn launch(inst: &Value, nick: &str, cfg: &Value, cb: &Cb) -> R<()> {
    let prep = prepare(&gs(inst, "version_id"), cb)?;
    let vj = &prep.vj;
    let java = match cfg["java"].as_str().filter(|s| !s.is_empty()) {
        Some(j) => PathBuf::from(j),
        None => ensure_java(vj["javaVersion"]["majorVersion"].as_u64().unwrap_or(8), cb)?,
    };
    let gd = game_dir(inst);
    let sep = ";";
    let vars: Vec<(&str, String)> = vec![
        ("auth_player_name", nick.to_string()),
        ("auth_uuid", offline_uuid(nick)),
        ("auth_access_token", "0".into()),
        ("auth_session", "0".into()),
        ("auth_xuid", "0".into()),
        ("clientid", "0".into()),
        ("user_type", "legacy".into()),
        ("user_properties", "{}".into()),
        ("version_name", gs(inst, "version_id")),
        ("version_type", vj["type"].as_str().unwrap_or("release").to_string()),
        ("game_directory", gd.to_string_lossy().to_string()),
        ("assets_root", dir("assets").to_string_lossy().to_string()),
        ("game_assets", dir("assets").to_string_lossy().to_string()),
        ("assets_index_name", gs(&vj["assetIndex"], "id")),
        ("natives_directory", prep.natives.to_string_lossy().to_string()),
        ("launcher_name", "Cubix".into()),
        ("launcher_version", "1.0".into()),
        ("classpath", prep.cp.join(sep)),
        ("library_directory", dir("libraries").to_string_lossy().to_string()),
        ("classpath_separator", sep.into()),
    ];
    let (jvm, game) = if vj.get("arguments").is_some() {
        (expand(&vj["arguments"]["jvm"], &vars), expand(&vj["arguments"]["game"], &vars))
    } else {
        let jv = json!(["-Djava.library.path=${natives_directory}", "-cp", "${classpath}"]);
        let ga: Vec<Value> = gs(vj, "minecraftArguments").split_whitespace().map(|x| json!(x)).collect();
        (expand(&jv, &vars), expand(&Value::Array(ga), &vars))
    };
    let ram = inst["ram_mb"].as_u64().or(cfg["ram_mb"].as_u64()).unwrap_or(4096);
    let log = File::create(dir("instances").join(gs(inst, "id")).join("launch.log")).s()?;
    let log2 = log.try_clone().s()?;
    let mut cmd = Command::new(&java);
    cmd.arg(format!("-Xmx{ram}M")).args(&jvm).arg(gs(vj, "mainClass")).args(&game)
        .current_dir(&gd).stdin(Stdio::null()).stdout(log).stderr(log2);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(NOWIN);
    }
    say(cb, "Запускаю игру...", None);
    cmd.spawn().s()?;
    Ok(())
}

// ───────────── Modrinth ─────────────
fn loaders_for(l: &str) -> Vec<String> {
    if l == "quilt" { vec!["quilt".into(), "fabric".into()] } else { vec![l.to_string()] }
}
fn subdir(kind: &str) -> &'static str {
    match kind {
        "resourcepack" => "resourcepacks",
        "shader" => "shaderpacks",
        _ => "mods",
    }
}

pub fn mr_search(query: &str, kind: &str, inst: Option<&Value>) -> R<Value> {
    let mut facets = vec![json!([format!("project_type:{kind}")])];
    if let Some(i) = inst {
        if kind != "modpack" {
            facets.push(json!([format!("versions:{}", gs(i, "mc"))]));
            if kind == "mod" && gs(i, "loader") != "vanilla" {
                let cats: Vec<String> = loaders_for(&gs(i, "loader")).iter().map(|l| format!("categories:{l}")).collect();
                facets.push(json!(cats));
            }
        }
    }
    let r = get_q(&format!("{MR}/search"), &[("query", query.to_string()), ("facets", Value::Array(facets).to_string()), ("limit", "30".into())])?;
    let hits = r["hits"].as_array().cloned().unwrap_or_default();
    Ok(Value::Array(
        hits.iter()
            .map(|h| json!({"id": h["project_id"], "title": h["title"], "desc": h["description"], "downloads": h["downloads"], "icon": h["icon_url"]}))
            .collect(),
    ))
}

pub fn mr_install(pid: &str, kind: &str, inst: &Value, cb: &Cb, seen: &mut HashSet<String>) -> R<Vec<String>> {
    if !seen.insert(pid.to_string()) {
        return Ok(vec![]);
    }
    let mut q = vec![("game_versions", json!([gs(inst, "mc")]).to_string())];
    if kind == "mod" && gs(inst, "loader") != "vanilla" {
        q.push(("loaders", json!(loaders_for(&gs(inst, "loader"))).to_string()));
    }
    let vs = get_q(&format!("{MR}/project/{pid}/version"), &q)?;
    let v = vs
        .as_array()
        .and_then(|a| a.first())
        .ok_or(format!("Нет версии под Minecraft {} / {}", gs(inst, "mc"), gs(inst, "loader")))?
        .clone();
    let files = v["files"].as_array().cloned().unwrap_or_default();
    let f = files.iter().find(|x| x["primary"] == true).or(files.first()).ok_or("У версии нет файлов")?.clone();
    let fname = gs(&f, "filename");
    let safe = Path::new(&fname).file_name().map(|x| x.to_owned()).ok_or("Плохое имя файла")?;
    say(cb, &format!("Скачиваю {fname}..."), None);
    download(&gs(&f, "url"), &game_dir(inst).join(subdir(kind)).join(safe), f["hashes"]["sha1"].as_str())?;
    let mut got = vec![fname];
    if kind == "mod" {
        for d in v["dependencies"].as_array().cloned().unwrap_or_default() {
            if d["dependency_type"] == "required" {
                if let Some(p) = d["project_id"].as_str() {
                    if let Ok(mut g) = mr_install(p, kind, inst, cb, seen) {
                        got.append(&mut g);
                    }
                }
            }
        }
    }
    Ok(got)
}

pub fn mr_modpack(pid: &str, cb: &Cb) -> R<(Value, Vec<String>)> {
    let vs = get_json(&format!("{MR}/project/{pid}/version"))?;
    let v = vs.as_array().and_then(|a| a.first()).ok_or("У сборки нет версий")?.clone();
    let files = v["files"].as_array().cloned().unwrap_or_default();
    let f = files.iter().find(|x| x["primary"] == true).or(files.first()).ok_or("У версии нет файлов")?.clone();
    say(cb, "Скачиваю сборку...", None);
    let name = Path::new(&gs(&f, "filename")).file_name().map(|x| x.to_owned()).ok_or("Плохое имя файла")?;
    let p = dir("tmp").join(name);
    download(&gs(&f, "url"), &p, f["hashes"]["sha1"].as_str())?;
    let r = import_modpack(&p, cb, "");
    let _ = fs::remove_file(&p);
    r
}

// ───────────── CurseForge ─────────────
fn cf_get(key: &str, path: &str, q: &[(&str, String)]) -> R<Value> {
    http().get(format!("{CF}{path}")).header("x-api-key", key).header("Accept", "application/json").query(q).send().s()?.error_for_status().s()?.json::<Value>().s()
}
fn cf_post(key: &str, path: &str, body: &Value) -> R<Value> {
    http().post(format!("{CF}{path}")).header("x-api-key", key).header("Accept", "application/json").json(body).send().s()?.error_for_status().s()?.json::<Value>().s()
}
fn cf_url(f: &Value) -> String {
    match f["downloadUrl"].as_str() {
        Some(u) if !u.is_empty() => u.to_string(),
        _ => {
            let id = f["id"].as_u64().unwrap_or(0);
            format!("https://mediafilez.forgecdn.net/files/{}/{}/{}", id / 1000, id % 1000, urlencoding::encode(&gs(f, "fileName")))
        }
    }
}
fn cf_loader(l: &str) -> Option<&'static str> {
    match l { "forge" => Some("1"), "fabric" => Some("4"), "quilt" => Some("5"), "neoforge" => Some("6"), _ => None }
}

pub fn cf_search(key: &str, query: &str, kind: &str, inst: Option<&Value>) -> R<Value> {
    let class = match kind { "modpack" => "4471", "resourcepack" => "12", "shader" => "6552", _ => "6" };
    let mut q = vec![
        ("gameId", "432".to_string()), ("classId", class.to_string()), ("searchFilter", query.to_string()),
        ("sortField", "2".into()), ("sortOrder", "desc".into()), ("pageSize", "30".into()),
    ];
    if let Some(i) = inst {
        if kind != "modpack" {
            q.push(("gameVersion", gs(i, "mc")));
            if kind == "mod" {
                if let Some(l) = cf_loader(&gs(i, "loader")) {
                    q.push(("modLoaderType", l.to_string()));
                }
            }
        }
    }
    let r = cf_get(key, "/mods/search", &q)?;
    Ok(Value::Array(
        r["data"].as_array().cloned().unwrap_or_default().iter()
            .map(|m| json!({"id": m["id"], "title": m["name"], "desc": m["summary"], "downloads": m["downloadCount"], "icon": m["logo"]["thumbnailUrl"]}))
            .collect(),
    ))
}

pub fn cf_install(key: &str, mid: u64, kind: &str, inst: &Value, cb: &Cb, seen: &mut HashSet<u64>) -> R<Vec<String>> {
    if !seen.insert(mid) {
        return Ok(vec![]);
    }
    let mut q = vec![("pageSize", "1".to_string()), ("gameVersion", gs(inst, "mc"))];
    if kind == "mod" {
        if let Some(l) = cf_loader(&gs(inst, "loader")) {
            q.push(("modLoaderType", l.to_string()));
        }
    }
    let r = cf_get(key, &format!("/mods/{mid}/files"), &q)?;
    let f = r["data"].as_array().and_then(|a| a.first()).ok_or(format!("Нет файла под Minecraft {} / {}", gs(inst, "mc"), gs(inst, "loader")))?.clone();
    let fname = gs(&f, "fileName");
    let safe = Path::new(&fname).file_name().map(|x| x.to_owned()).ok_or("Плохое имя файла")?;
    say(cb, &format!("Скачиваю {fname}..."), None);
    download(&cf_url(&f), &game_dir(inst).join(subdir(kind)).join(safe), None)?;
    let mut got = vec![fname];
    if kind == "mod" {
        for d in f["dependencies"].as_array().cloned().unwrap_or_default() {
            if d["relationType"] == 3 {
                if let Some(m) = d["modId"].as_u64() {
                    if let Ok(mut g) = cf_install(key, m, kind, inst, cb, seen) {
                        got.append(&mut g);
                    }
                }
            }
        }
    }
    Ok(got)
}

pub fn cf_modpack(key: &str, mid: u64, cb: &Cb) -> R<(Value, Vec<String>)> {
    let r = cf_get(key, &format!("/mods/{mid}/files"), &[("pageSize", "1".to_string())])?;
    let f = r["data"].as_array().and_then(|a| a.first()).ok_or("У сборки нет файлов")?.clone();
    say(cb, "Скачиваю сборку...", None);
    let name = Path::new(&gs(&f, "fileName")).file_name().map(|x| x.to_owned()).ok_or("Плохое имя файла")?;
    let p = dir("tmp").join(name);
    download(&cf_url(&f), &p, None)?;
    let res = import_modpack(&p, cb, key);
    let _ = fs::remove_file(&p);
    res
}

pub fn install_content(src: &str, pid: &str, kind: &str, inst: &Value, key: &str, cb: &Cb) -> R<Vec<String>> {
    if src == "Modrinth" {
        mr_install(pid, kind, inst, cb, &mut HashSet::new())
    } else {
        cf_install(key, pid.parse::<u64>().s()?, kind, inst, cb, &mut HashSet::new())
    }
}
pub fn modpack_from_source(src: &str, pid: &str, key: &str, cb: &Cb) -> R<(Value, Vec<String>)> {
    if src == "Modrinth" { mr_modpack(pid, cb) } else { cf_modpack(key, pid.parse::<u64>().s()?, cb) }
}

// ───────────── импорт сборок ─────────────
pub fn import_modpack(path: &Path, cb: &Cb, key: &str) -> R<(Value, Vec<String>)> {
    let f = File::open(path).s()?;
    let mut z = zip::ZipArchive::new(f).s()?;
    let is_mr = z.file_names().any(|x| x == "modrinth.index.json");
    let is_cf = z.file_names().any(|x| x == "manifest.json");
    if is_mr {
        import_mrpack(&mut z, cb)
    } else if is_cf {
        import_cf(&mut z, cb, key)
    } else {
        Err("Неизвестный формат: нужен .mrpack (Modrinth) или .zip (CurseForge)".into())
    }
}

fn import_mrpack(z: &mut zip::ZipArchive<File>, cb: &Cb) -> R<(Value, Vec<String>)> {
    let idx: Value = serde_json::from_slice(&read_entry(z, "modrinth.index.json")?).s()?;
    let deps = &idx["dependencies"];
    let mc = gs(deps, "minecraft");
    let (mut loader, mut lv) = ("vanilla", None);
    for (k, n) in [("fabric-loader", "fabric"), ("quilt-loader", "quilt"), ("neoforge", "neoforge"), ("forge", "forge")] {
        if let Some(v) = deps[k].as_str() {
            loader = n;
            lv = Some(v.to_string());
            break;
        }
    }
    let inst = create_instance(&gs(&idx, "name"), &mc, loader, lv.as_deref(), cb)?;
    let gd = game_dir(&inst);
    let mut jobs = vec![];
    for f in idx["files"].as_array().cloned().unwrap_or_default() {
        if f["env"]["client"] == "unsupported" {
            continue;
        }
        jobs.push(Job {
            url: f["downloads"][0].as_str().unwrap_or("").to_string(),
            dst: safe_join(&gd, &gs(&f, "path"))?,
            sha: f["hashes"]["sha1"].as_str().map(String::from),
        });
    }
    let missing = fetch_all(jobs, cb, "Моды", true)?;
    for pre in ["overrides/", "client-overrides/"] {
        extract_overrides(z, pre, &gd)?;
    }
    Ok((inst, missing))
}

fn import_cf(z: &mut zip::ZipArchive<File>, cb: &Cb, key: &str) -> R<(Value, Vec<String>)> {
    if key.is_empty() {
        return Err("Для сборок CurseForge нужен API-ключ (Настройки)".into());
    }
    let m: Value = serde_json::from_slice(&read_entry(z, "manifest.json")?).s()?;
    let mc = gs(&m["minecraft"], "version");
    let (mut loader, mut lv) = ("vanilla".to_string(), None);
    if let Some(mls) = m["minecraft"]["modLoaders"].as_array() {
        if let Some(ml) = mls.iter().find(|x| x["primary"] == true).or(mls.first()) {
            let id = gs(ml, "id");
            if let Some((a, b)) = id.split_once('-') {
                loader = a.to_string();
                lv = Some(b.to_string());
            }
        }
    }
    let name = m["name"].as_str().unwrap_or("CurseForge сборка").to_string();
    let inst = create_instance(&name, &mc, &loader, lv.as_deref(), cb)?;
    let gd = game_dir(&inst);
    let entries = m["files"].as_array().cloned().unwrap_or_default();
    let fids: Vec<u64> = entries.iter().filter_map(|f| f["fileID"].as_u64()).collect();
    let mut pids: Vec<u64> = entries.iter().filter_map(|f| f["projectID"].as_u64()).collect();
    pids.sort();
    pids.dedup();
    say(cb, "Получаю список модов...", None);
    let mut files: Vec<Value> = vec![];
    for ch in fids.chunks(200) {
        let r = cf_post(key, "/mods/files", &json!({"fileIds": ch}))?;
        files.extend(r["data"].as_array().cloned().unwrap_or_default());
    }
    let mut classes = std::collections::HashMap::new();
    for ch in pids.chunks(200) {
        let r = cf_post(key, "/mods", &json!({"modIds": ch}))?;
        for md in r["data"].as_array().cloned().unwrap_or_default() {
            classes.insert(md["id"].as_u64().unwrap_or(0), md["classId"].as_u64().unwrap_or(6));
        }
    }
    let mut jobs = vec![];
    for f in &files {
        let sub = match classes.get(&f["modId"].as_u64().unwrap_or(0)) { Some(12) => "resourcepacks", Some(6552) => "shaderpacks", _ => "mods" };
        let fname = gs(f, "fileName");
        if let Some(safe) = Path::new(&fname).file_name() {
            jobs.push(Job { url: cf_url(f), dst: gd.join(sub).join(safe), sha: None });
        }
    }
    let mut missing = fetch_all(jobs, cb, "Моды", true)?;
    let got: HashSet<u64> = files.iter().filter_map(|f| f["id"].as_u64()).collect();
    for i in &fids {
        if !got.contains(i) {
            missing.push(format!("fileID {i}"));
        }
    }
    let ov = m["overrides"].as_str().unwrap_or("overrides");
    extract_overrides(z, &format!("{ov}/"), &gd)?;
    Ok((inst, missing))
}
