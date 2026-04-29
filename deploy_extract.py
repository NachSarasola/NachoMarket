import zipfile, shutil, os, glob

zip_path = "/tmp/nachomarket_deploy.zip"
extract_dir = "/tmp/nm_new"
shutil.rmtree(extract_dir, ignore_errors=True)
with zipfile.ZipFile(zip_path, "r") as z:
    z.extractall(extract_dir)
print("Extracted to", extract_dir)

dest = os.path.expanduser("~/nachomarket")
for root, dirs, files in os.walk(extract_dir):
    for f in files:
        src = os.path.join(root, f)
        rel = os.path.relpath(src, extract_dir)
        dst = os.path.join(dest, rel)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copy2(src, dst)
        print("Copied", rel)

for bad in glob.glob(dest + "/**/*\\*", recursive=True):
    if os.path.isfile(bad):
        os.remove(bad)
        print("Removed bad file", bad)

shutil.rmtree(extract_dir, ignore_errors=True)
os.remove(zip_path)
print("Deploy OK")
