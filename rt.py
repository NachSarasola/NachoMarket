import subprocess, sys, os
os.chdir("C:\\Users\\Usuario\\Desktop\\NachoMarket")
p = "venv\\Scripts\\python. exe"
r = subprocess.run([p, "-m", "pytest", "tests/", "-x", "-q", "--ignore=tests/ test_ ab_ tester. py", "--ignore=tests/ test_ strategy. py", "--ignore=tests/ test_ copy_ trade. py"], capture_output= True, text= True, timeout=120)
print(r. stdout[: 5000])
if r. returncode: print("ERR:", r. stderr[-1000:])
sys. exit(r. returncode)