import subprocess, sys, os
os.chdir(r"C:\Users\Usuario\Desktop\NachoMarket")
venv = os.path.join(os.getcwd(), "venv", "Scripts", "python.exe")
result = subprocess.run(
    [venv, "-m", "pytest",
     "tests/test_rewards_farmer.py",
     "tests/test_risk.py",
     "tests/test_markets.py",
     "tests/test_market_filter.py",
     "-x", "-q",
     "--ignore=tests/test_ab_tester.py",
     "--ignore=tests/test_strategy.py",
     "--ignore=tests/test_copy_trade.py"],
    capture_output=True, text=True, timeout=120
)
print(result.stdout[:5000])
if result.returncode != 0:
    print("STDERR:", result.stderr[-1000:])
sys.exit(result.returncode)