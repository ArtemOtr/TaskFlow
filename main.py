import subprocess
import sys
import time

scripts = ["app.py", "bot.py"]
processes = []


for script in scripts:
    print(f"Запускаю {script}...")
    p = subprocess.Popen([sys.executable, script])
    processes.append(p)
    time.sleep(1)

for p in processes:
    p.wait()

