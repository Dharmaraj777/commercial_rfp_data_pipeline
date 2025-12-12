Here you go — a “human + tech” friendly mini-guide you can literally share as a document.

---

# Making Code Faster & Scalable with Concurrency

*(Threads, Async, Processes — explained for tech & non-tech)*

---

## 1. What are we trying to improve?

There are two different “speed” questions:

1. **Latency** – “How long does ONE task take?”

   > Example: How long to translate one PDF?

2. **Throughput / total runtime** – “How long to finish MANY tasks?”

   > Example: How long to translate 100 PDFs?

Most concurrency tools (threads, async, processes) mainly improve **#2 (throughput)**.
A single PDF might still take 10 seconds, but 100 PDFs can finish much faster if you work on several at the same time.

---

## 2. Two types of work: Waiting vs Working

### 2.1 I/O-bound work (waiting on something)

These tasks spend most of their time **waiting**:

* Calling an API (LLM, REST, database)
* Downloading/uploading files (Blob, SharePoint, S3, etc.)
* Reading/writing large files on disk

**CPU is mostly idle while waiting.**

👉 Best tools: **Threads** (`ThreadPoolExecutor`) or **async/await**.

---

### 2.2 CPU-bound work (heavy computation)

These tasks spend most of their time **actually calculating**:

* Running ML models, time series forecasting
* Image processing, video processing
* Large numeric calculations, statistics, simulations

**CPU is busy 100% of the time.**

👉 Best tool: **Processes** (`ProcessPoolExecutor` / multiprocessing) to use multiple CPU cores.

---

## 3. The toolbox (high-level)

| Tool                        | Good For                               | Not Good For                      |
| --------------------------- | -------------------------------------- | --------------------------------- |
| Plain loop (no concurrency) | Simple tasks, low volume               | High volume, long multi-item jobs |
| **ThreadPoolExecutor**      | I/O-bound, blocking calls (HTTP, disk) | Heavy CPU work (limited by GIL)   |
| **async/await**             | Lots of small I/O calls efficiently    | CPU-heavy work                    |
| **ProcessPoolExecutor**     | CPU-bound heavy computations           | Shared mutable state, big globals |

> **Non-tech analogy:**
>
> * Threads/async = many people *waiting on calls/emails* at once.
> * Processes = many people *actually doing calculations* in parallel on different computers.

---

## 4. Core idea: break work into independent tasks

All of this relies on one simple pattern:

```python
for item in items:
    handle(item)  # tasks must not depend on each other
```

If `handle(item)`:

* doesn’t depend on other items, and
* doesn’t share fragile global state,

…you can run many of them *at the same time*.

---

## 5. Pattern 1 – Threads with `ThreadPoolExecutor` (I/O-bound)

### When to use

Use **threads** when:

* You are doing many network / file operations.
* Each task:

  * calls an API,
  * reads or writes files,
  * or waits on network responses.

**Goal:** While one thread is waiting, another can work.

---

### Example: Download multiple URLs in parallel

**Non-tech description:**
Imagine you have a list of 20 web pages to download.
Sequentially: one after another → very slow.
With threads: start several downloads at once.

**Code example:**

```python
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

URLS = [
    "https://example.com",
    "https://httpbin.org/delay/2",  # responds after 2 seconds
    "https://www.python.org",
    # ...more URLs
]

def download_url(url: str) -> tuple[str, int]:
    """Download one URL and return (url, status_code)."""
    resp = requests.get(url)
    return url, resp.status_code

def download_all(urls, max_workers: int = 5):
    start = time.time()
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {
            executor.submit(download_url, url): url
            for url in urls
        }

        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"Error downloading {url}: {e}")

    elapsed = time.time() - start
    print(f"Finished {len(urls)} downloads in {elapsed:.2f} seconds")
    return results

if __name__ == "__main__":
    download_all(URLS, max_workers=5)
```

**What happens:**

* Without threads:
  Each request waits for network → total time ≈ sum of all waits.

* With threads (5 workers):
  Several requests happen at once → total time ≈ max(wait times per batch).

**Effect:**
Total runtime for 20 URLs might drop from 40 seconds to ~8–10 seconds.

---

## 6. Pattern 2 – Async/await (`asyncio`) for many small I/O tasks

### When to use

Use **async/await** when:

* You have many small network operations (HTTP, DB, APIs),
* Libraries support async (`async def`, `await`),
* You want better scalability with lower overhead than threads.

---

### Example: Call an API for many items concurrently

Non-tech story:
You’re sending 50 requests to a web service. Instead of calling one, waiting, then calling the next, you send many requests and collect responses as they come.

**Code example:**

```python
import asyncio
import time
import aiohttp  # async HTTP client

URLS = [
    "https://httpbin.org/delay/2",
    "https://httpbin.org/delay/3",
    "https://httpbin.org/delay/1",
    # ...more URLs
]

async def fetch(session, url):
    async with session.get(url) as resp:
        status = resp.status
        text = await resp.text()
        return url, status, len(text)

async def fetch_all(urls, max_concurrent: int = 5):
    start = time.time()
    semaphore = asyncio.Semaphore(max_concurrent)

    async with aiohttp.ClientSession() as session:
        async def bound_fetch(url):
            async with semaphore:
                return await fetch(session, url)

        tasks = [asyncio.create_task(bound_fetch(url)) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    elapsed = time.time() - start
    print(f"Fetched {len(urls)} URLs in {elapsed:.2f} seconds")
    return results

if __name__ == "__main__":
    asyncio.run(fetch_all(URLS))
```

**What happens:**

* All HTTP calls are non-blocking and awaited.
* The semaphore ensures you don’t exceed a safe concurrency limit (`max_concurrent`).
* Network waiting time is overlapped, so total runtime drops sharply.

---

## 7. Pattern 3 – Processes with `ProcessPoolExecutor` (CPU-bound)

### When to use

Use **processes** when:

* Tasks are heavy CPU work: ML training, time series forecasting, big SHAP jobs, image processing.
* You want to use multiple CPU cores.

Python’s GIL means **threads** cannot run CPU-heavy Python code in true parallel.
Separate **processes** can.

---

### Example: CPU-heavy computation per item

Non-tech story:
You have 8 huge Excel sheets, each needing heavy calculations. You give each sheet to a different worker computer instead of one worker doing everything.

**Code example:**

```python
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
import math

def heavy_compute(n: int) -> float:
    """Fake CPU-heavy task: compute sum of square roots."""
    total = 0.0
    for i in range(1, n):
        total += math.sqrt(i)
    return total

def run_sequential(tasks):
    start = time.time()
    results = [heavy_compute(n) for n in tasks]
    elapsed = time.time() - start
    print(f"Sequential: {elapsed:.2f} seconds")
    return results

def run_parallel(tasks, max_workers: int = 4):
    start = time.time()
    results = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_n = {executor.submit(heavy_compute, n): n for n in tasks}
        for future in as_completed(future_to_n):
            n = future_to_n[future]
            try:
                res = future.result()
                results.append(res)
            except Exception as e:
                print(f"Error on task {n}: {e}")
    elapsed = time.time() - start
    print(f"Parallel (processes): {elapsed:.2f} seconds")
    return results

if __name__ == "__main__":
    tasks = [5_000_00, 6_000_00, 7_000_00, 8_000_00]  # heavy work items

    run_sequential(tasks)
    run_parallel(tasks, max_workers=4)
```

**What happens:**

* **Sequential**: tasks run one after another on a single core.
* **Parallel (processes)**: up to 4 tasks run at the same time on 4 cores.
* Total runtime drops roughly by a factor close to the number of cores (minus overhead).

---

## 8. How this reduces runtime and scales in practice

### For non-tech audience

* If **one task** takes 10 minutes and you have 10 tasks:

  * One worker sequentially = ~100 minutes.
  * 5 workers doing tasks in parallel = ~20–25 minutes.

* The task itself is not faster, but you **finish the batch sooner**.

* This is exactly how cloud systems scale: more workers handling more work in parallel.

---

### For tech audience – design checklist

1. **Identify independent units**

   * files, users, groups, policies, rows, etc.
   * make sure they don’t depend on shared mutable state.

2. **Classify workload**

   * I/O-bound → threads or async
   * CPU-bound → processes

3. **Wrap work into a pure function**

   ```python
   def handle_item(item):
       # uses only its inputs + config, no global mutation
       ...
   ```

4. **Replace the for-loop with a pool**

   * `ThreadPoolExecutor` for I/O
   * `ProcessPoolExecutor` for CPU
   * or `asyncio` where libraries support it

5. **Control concurrency**

   * `max_workers` or `Semaphore`
   * respect API rate limits & external system constraints

6. **Add resilient logging & error handling**

   * capture exceptions per task
   * don’t let one failure kill the entire batch

---

## 9. Summary – When to use what

* **Just a few tasks, not slow?**
  → Keep it simple, no concurrency needed.

* **Many network/file/API operations?**
  → Use **threads** or **async** to overlap waiting time and reduce total runtime.

* **Heavy numerical/ML computations across many items?**
  → Use **processes** to exploit multiple CPU cores.

* **Need to “scale” the system?**

  * Break work into independent units.
  * Use the right concurrency tool.
  * Increase workers carefully while monitoring errors and resource usage.

---

If you want, next step I can take a *real* loop from your code (e.g. “for each file in blob container, process it”) and turn it into:

* a threaded version (for I/O), and
* a process-based version (for CPU),

with comments that you could show to your team as a teaching example.
