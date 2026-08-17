# -*- coding: utf-8 -*-
"""
master.py — Sıralı Script Çalıştırıcı (Dinamik Klasör Algılamalı)
"""

import argparse
import subprocess
import sys
import os
import time
from datetime import datetime
from typing import List, Tuple, Optional

# ============ KULLANICI BÖLÜMÜ (Varsayılan Liste) ============
FILES_TO_RUN = [
    "ptfsmfsdf.py",
    "kur.py",
    "uzl.py",
    "daily_artvin.py",
    "aktar.py"
]

LOAD_ENV = True
ENV_FILE = ".env"

# master.py'ın fiziksel olarak bulunduğu klasörü bul (Dinamik Yol Çözümü)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ============ Yardımcılar ============

def load_env_file(path: str) -> None:
    env_path = os.path.join(BASE_DIR, path)
    if not os.path.isfile(env_path):
        return
    try:
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, val = line.split("=", 1)
                os.environ[key.strip()] = val.strip().strip('"').strip("'")
    except Exception as e:
        print(f"[ENV] Uyarı: {env_path} okunamadı: {e}", file=sys.stderr)

def timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def make_log_path(template: Optional[str]) -> Optional[str]:
    if not template: return None
    log_name = template.replace("{ts}", datetime.now().strftime("%Y%m%d_%H%M%S"))
    return os.path.join(BASE_DIR, log_name)

def write_log(log_fp, msg: str) -> None:
    if log_fp:
        log_fp.write(msg + "\n")
        log_fp.flush()

def build_child_env(force_utf8: bool) -> dict:
    child_env = os.environ.copy()
    if force_utf8:
        child_env.setdefault("PYTHONIOENCODING", "utf-8")
        child_env.setdefault("PYTHONUTF8", "1")
    return child_env

def run_script(py_exe: str, script_path: str, extra_args: Optional[List[str]] = None,
               env=None, encoding: str = "utf-8", errors: str = "replace") -> Tuple[int, str, str, float]:
    cmd = [py_exe, script_path]
    if extra_args:
        cmd.extend(extra_args)

    started = time.time()
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding=encoding,
            errors=errors,
            env=env
        )
        out, err = proc.communicate()
        rc = proc.returncode
    except FileNotFoundError:
        out, err, rc = "", f"Script bulunamadı: {script_path}", 127
    except Exception as e:
        out, err, rc = "", f"Çalıştırma hatası: {e}", 1

    return rc, out, err, time.time() - started

def file_exists_or_warn(path: str) -> bool:
    if os.path.isfile(path): return True
    print(f"[UYARI] Dosya bulunamadı: {path}")
    return False

# ============ Ana Çalıştırıcı ============

def main():
    parser = argparse.ArgumentParser(description="Sıralı Python script çalıştırıcı (master.py)")
    parser.add_argument("--files", nargs="+", help="Sırayla çalıştırılacak .py dosyaları.")
    parser.add_argument("--continue-on-error", action="store_true", help="Hata olsa da sonraki dosyalara devam et.")
    parser.add_argument("--log", default=None, help="Log dosyası yolu.")
    parser.add_argument("--arg", nargs="*", help="Ortak argümanlar.")
    parser.add_argument("--no-utf8", action="store_true", help="Alt süreçlerde UTF-8 zorlamasını kapat.")
    args = parser.parse_args()

    if LOAD_ENV:
        load_env_file(ENV_FILE)

    files = args.files if args.files else FILES_TO_RUN
    files = [f.strip() for f in files if f and f.strip()]

    log_path = make_log_path(args.log)
    log_fp = open(log_path, "w", encoding="utf-8") if log_path else None
    if log_fp: write_log(log_fp, f"[{timestamp()}] master.py başladı. Dosyalar: {files}")

    py_exe = sys.executable or "python"
    overall_ok, summary = True, []
    child_env = build_child_env(force_utf8=(not args.no_utf8))

    for idx, script_name in enumerate(files, start=1):
        # Dosya yolunu dinamik olarak master.py'ın olduğu klasörle birleştiriyoruz
        full_script_path = os.path.join(BASE_DIR, script_name)
        
        print(f"\n[{idx}/{len(files)}] Çalıştırılıyor -> {script_name}")
        write_log(log_fp, f"[{timestamp()}] RUN -> {script_name}")

        if not file_exists_or_warn(full_script_path):
            overall_ok = False
            summary.append((script_name, "NOT FOUND", 127, 0.0))
            if not args.continue_on_error: break
            continue

        rc, out, err, sec = run_script(py_exe, full_script_path, extra_args=args.arg, env=child_env)
        print(f"  Süre: {sec:.2f}s | Çıkış kodu: {rc}")
        
        if out.strip():
            print("  --- STDOUT ---")
            print(out.strip())
        if err.strip():
            print("  --- STDERR ---")
            print(err.strip())

        summary.append((script_name, "OK" if rc == 0 else "FAIL", rc, sec))
        if rc != 0:
            overall_ok = False
            if not args.continue_on_error:
                print("Hata: Akış durduruldu (continue-on-error kapalı).")
                break

    print("\n==== ÇALIŞTIRMA ÖZETİ ====")
    for script_name, status, rc, sec in summary:
        print(f"{script_name:35s} | {status:4s} | rc={rc:3d} | t={sec:.2f}s")

    if log_fp: log_fp.close()
    sys.exit(0 if overall_ok else 1)

if __name__ == "__main__":
    main()