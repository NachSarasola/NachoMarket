import subprocess, sys, os
os.chdir(r"C:\Users\Usuario\Desktop\NachoMarket")
p = r"venv\Scripts\python.exe"
args = [p, "-m", "pytest", "tests/", "-x", "-q",
        "--ignore=tests/test_ab_ tester. py",
        "--ignore=tests/test_ strategy. py",
        "--ignore=tests/test_ copy_ trade. py"]
r = subprocess.run(args, capture_output=True, text=True, timeout=120)
print(r.stdout[:5000])
if r.returncode:
    print("ERR:", r.stderr[-1000:])
sys.exit(r.returncode)