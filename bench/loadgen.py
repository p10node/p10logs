#!/usr/bin/env python3
"""Write CRI-format log lines into a fake /var/log/pods tree at a target rate.

usage: loadgen.py <podsDir> <pods> <lines_per_sec_total> <seconds> [avg_line_bytes]
Rotates each file at 10 MiB the way kubelet does (rename + recreate)."""
import os, sys, time, random, string

pods_dir, npods, rate, secs = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
avg = int(sys.argv[5]) if len(sys.argv) > 5 else 180
files = []
for i in range(npods):
    d = os.path.join(pods_dir, f"bench_pod-{i}_uid-{i:04d}", "app")
    os.makedirs(d, exist_ok=True)
    files.append(open(os.path.join(d, "0.log"), "a", buffering=1 << 16))
words = ["GET", "POST", "/v1/charges", "/v1/refunds", "200", "502", "user=42", "dur_ms=17", "req_id=abc123", "upstream", "timeout", "ok"]
def line(i):
    body = " ".join(random.choices(words, k=max(3, avg // 8)))
    t = time.time_ns()
    return f'{time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(t // 10**9))}.{t % 10**9:09d}Z stdout F {{"level":"info","i":{i},"msg":"{body}"}}\n'
per_tick = max(1, rate // 20)          # 20 ticks per second
end = time.time() + secs
i = 0
written = 0
while time.time() < end:
    t0 = time.time()
    for _ in range(per_tick):
        f = files[i % npods]
        f.write(line(i))
        written += 1
        i += 1
        if i % 2000 == 0 and f.tell() > 10 << 20:   # kubelet-style rotation
            f.flush(); p = f.name; f.close()
            os.rename(p, p + time.strftime(".%Y%m%d-%H%M%S"))
            files[(i - 1) % npods] = open(p, "a", buffering=1 << 16)
    for f in files:
        f.flush()
    time.sleep(max(0, 0.05 - (time.time() - t0)))
print(f"wrote {written} lines")
