import subprocess
from pathlib import Path

VERSION_FILE = Path("version.txt")

def read_version():
    return VERSION_FILE.read_text().strip()

def bump_version(ver: str) -> str:
    major, minor = ver.split(".")
    return f"{major}.{int(minor) + 1}"

def write_version(ver: str):
    VERSION_FILE.write_text(ver)

def main():
    old_ver = read_version()
    new_ver = bump_version(old_ver)

    print(f"[BUILD] Old version: v{old_ver}")
    print(f"[BUILD] New version: v{new_ver}")

    write_version(new_ver)

    exe_name = f"l50wialon-v{new_ver}"
    print(f"[BUILD] Building exe: {exe_name}.exe")

    cmd = [
        "pyinstaller",
        "--onefile",
        f"--name={exe_name}",
        "l50_to_wialon.py"
    ]

    subprocess.run(cmd)

    print("\n[BUILD] Done.")
    print("EXE output located under /dist/")
    print(f"→ dist/{exe_name}.exe")

if __name__ == "__main__":
    main()
