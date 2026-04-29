import subprocess, sys, os
os. chdir("C:/Users/Usuario/Desktop/NachoMarket")
r = subprocess.run(["git", "add", "src/polymarket/client.py"])
r = subprocess.run(["git", "commit", "-m", "fix: postOnly=True in post_batch_orders batch"], capture_output= True, text= True)
print(r.stdout, r.stderr)
r = subprocess.run(["git", "push", "origin", "master"], capture_output= True, text= True)
print(r.stdout, r.stderr)
sys.exit(r.returncode)