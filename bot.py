import asyncio
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

import os, re, shutil, tempfile, time, itertools, json, math, glob
from pathlib import Path
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
ALLOWED = {int(x) for x in os.environ.get("ALLOWED_USERS", "").split(",") if x.strip()}
MAX_SIZE = 2000 * 1024 * 1024
TARGET_PART = 1800 * 1024 * 1024
PARALLEL_DL = 4
PARALLEL_UL = 3
VIDEO_EXT = {".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm", ".m4v", ".ts", ".mpg", ".mpeg", ".3gp"}
LINK_RE = re.compile(r"https?://mega\.nz/(?:folder/[\w-]+#[\w-]+|#F![\w-]+![\w-]+)")

app = Client(
    "mega_bot_pyro", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN,
    workdir=".", max_concurrent_transmissions=PARALLEL_UL)
queue = asyncio.Queue()
state = {"busy": False}
jobs = {}
counter = itertools.count(1)


class Job:
    def __init__(self, message, link):
        self.id = next(counter)
        self.message = message
        self.link = link
        self.status = None
        self.task = None
        self.cancelled = False
        self.done = False
        self.start_time = time.time()
        self.user_name = message.from_user.first_name if message.from_user and message.from_user.first_name else "User"


def fmt_size(n):
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or u == "TB":
            return f"{n:.2f} {u}"
        n /= 1024


def fmt_time(seconds):
    if seconds < 0 or math.isinf(seconds) or math.isnan(seconds):
        return "0s"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h}h {m}m {s}s"
    elif m > 0:
        return f"{m}m {s}s"
    else:
        return f"{s}s"


def progress_bar(current, total):
    if total <= 0:
        pct = 0.0
        filled = 0
    else:
        pct = min(current / total, 1.0)
        filled = int(round(pct * 20))
    bar = "⬢" * filled + "⬡" * (20 - filled)
    return f"({bar}) {pct * 100:.2f}%"


def build_status_text(job, status_str, speed, done_bytes, total_bytes, files_done, files_total):
    bar = progress_bar(done_bytes, total_bytes)
    elapsed = time.time() - job.start_time
    eta = (total_bytes - done_bytes) / speed if speed > 0 and total_bytes > done_bytes else 0
    total_est = elapsed + eta
    
    return (
        f"[{job.id}] Task\n\n"
        f"Task By {job.user_name}\n"
        f"{bar}\n"
        f"Status » {status_str} » {fmt_size(speed)}/s\n"
        f"Done » {fmt_size(done_bytes)}\n"
        f"Total » {fmt_size(total_bytes)}\n"
        f"Files » ( {files_done} / {files_total} )[span_1](start_span)[span_1](end_span)\n"
        f"ETA » ⏳ {fmt_time(eta)}\n"
        f"Past » {fmt_time(elapsed)} (Total ~{fmt_time(total_est)})\n"
        f"Engine » MegaBot v3.1.5\n"
        f"Mode » #Leech"
    )


def dir_stats(path):
    size = files = 0
    for p in path.rglob("*"):
        if p.is_file():
            try:
                size += p.stat().st_size
            except OSError:
                pass
            files += 1
    return size, files


def cancel_markup(job):
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("Cancel Task", callback_data=f"cancel:{job.id}")]])


async def safe_edit(job, text, final=False):
    try:
        await job.status.edit_text(
            text, reply_markup=None if final else cancel_markup(job))
    except Exception:
        pass


async def run_cmd(*args):
    proc = await asyncio.create_subprocess_exec(
        *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
    try:
        out, _ = await proc.communicate()
    except asyncio.CancelledError:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        raise
    return proc.returncode, out


async def probe(video):
    try:
        rc, out = await run_cmd(
            "ffprobe", "-v", "error", "-select_streams", "v:0",
            "-show_entries", "stream=width,height:format=duration",
            "-of", "json", str(video))
        data = json.loads(out)
        s = data["streams"][0]
        return float(data["format"].get("duration", 0)), int(s["width"]), int(s["height"])
    except Exception:
        return 0, 0, 0


async def make_thumb(video, duration):
    t = min(max(duration * 0.1, 1), 30) if duration else 1
    thumb = video.with_name(video.stem + "_thumb.jpg")
    try:
        rc, _ = await run_cmd(
            "ffmpeg", "-y", "-ss", f"{t:.1f}", "-i", str(video),
            "-frames:v", "1", "-vf", "scale=320:-2", "-q:v", "5", str(thumb))
        if rc == 0 and thumb.exists() and thumb.stat().st_size > 0:
            return thumb
    except Exception:
        pass
    return None


async def split_video(v, duration):
    if duration <= 0:
        return []
    n = math.ceil(v.stat().st_size / TARGET_PART)
    pattern = v.with_name(f"{v.stem}_part%03d{v.suffix}")
    glob_pat = f"{glob.escape(v.stem)}_part*{glob.escape(v.suffix)}"
    for _ in range(4):
        seg = duration / n
        rc, _ = await run_cmd(
            "ffmpeg", "-y", "-i", str(v),
            "-map", "0:v:0", "-map", "0:a?", "-c", "copy",
            "-f", "segment", "-segment_time", f"{seg:.2f}",
            "-reset_timestamps", "1", str(pattern))
        parts = sorted(v.parent.glob(glob_pat))
        if rc == 0 and parts and all(p.stat().st_size <= MAX_SIZE for p in parts):
            return parts
        for p in parts:
            p.unlink(missing_ok=True)
        n = math.ceil(n * 1.3) + 1
    return []


# ---------------- دانلود ----------------

async def list_videos(link):
    try:
        proc = await asyncio.create_subprocess_exec(
            "megals", "-R", link,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=90)
        if proc.returncode != 0:
            return []
    except Exception:
        return []
    paths = []
    for line in out.decode(errors="ignore").splitlines():
        line = line.strip()
        if line and Path(line).suffix.lower() in VIDEO_EXT:
            paths.append(line)
    return paths


async def download_parallel(job, paths, dest):
    sem = asyncio.Semaphore(PARALLEL_DL)
    res = {"ok": 0, "fail": 0}
    total_files = len(paths)

    async def one(i, p):
        async with sem:
            d = dest / f"{i:04d}"
            d.mkdir(parents=True, exist_ok=True)
            proc = await asyncio.create_subprocess_exec(
                "megadl", "--path", str(d), f"{job.link}:{p}",
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
            try:
                await proc.wait()
            finally:
                if proc.returncode is None:
                    try:
                        proc.kill()
                    except ProcessLookupError:
                        pass
            good = proc.returncode == 0 and any(f.is_file() for f in d.iterdir())
            res["ok" if good else "fail"] += 1

    tasks = [asyncio.create_task(one(i, p)) for i, p in enumerate(paths)]

    async def reporter():
        last_t, last_size = time.time(), 0
        while True:
            await asyncio.sleep(3)
            now = time.time()
            size, _ = dir_stats(dest)
            speed = (size - last_size) / max(now - last_t, 0.1)
            last_t, last_size = now, size
            files_done = res["ok"] + res["fail"]
            text = build_status_text(job, "Download", speed, size, size * 1.2 if size > 0 else 1024*1024, files_done, total_files)
            await safe_edit(job, text)

    rep = asyncio.create_task(reporter())
    try:
        await asyncio.gather(*tasks)
    finally:
        rep.cancel()
        for t in tasks:
            t.cancel()
    return res["ok"], res["fail"]


# ---------------- آپلود ----------------

async def send_one(job, path, active, st):
    thumb = None
    try:
        size = path.stat().st_size

        if path.suffix.lower() in (".m4v", ".mov"):
            path = path.rename(path.with_suffix(".mp4"))
        duration, w, h = await probe(path)
        thumb = await make_thumb(path, duration)

        def cb(cur, tot):
            active[path.name] = (cur, tot)

        await app.send_video(
            job.message.chat.id, str(path),
            caption=path.name[:1000],
            duration=int(duration), width=w, height=h,
            thumb=str(thumb) if thumb else None,
            supports_streaming=True,
            progress=cb)
        st["fin"] += size
        return True
    except asyncio.CancelledError:
        raise
    except Exception as e:
        await job.message.reply_text(f"❌ خطا در ارسال {path.name}: {e}")
        return False
    finally:
        active.pop(path.name, None)
        path.unlink(missing_ok=True)
        if thumb:
            thumb.unlink(missing_ok=True)


async def upload_all(job, videos):
    sem = asyncio.Semaphore(PARALLEL_UL)
    active = {}
    st = {"done": 0, "sent": 0, "fin": 0}
    total_files = len(videos)
    total_bytes = sum(v.stat().st_size for v in videos)

    async def handle(v):
        async with sem:
            try:
                if v.stat().st_size > MAX_SIZE:
                    active[v.name] = (0, 1)
                    duration, _, _ = await probe(v)
                    parts = await split_video(v, duration)
                    active.pop(v.name, None)
                    v.unlink(missing_ok=True)
                    if not parts:
                        await job.message.reply_text(f"❌ تقسیم {v.name} ناموفق بود و رد شد.")
                        return
                else:
                    parts = [v]
                all_ok = True
                for part in parts:
                    if not await send_one(job, part, active, st):
                        all_ok = False
                if all_ok:
                    st["sent"] += 1
            except asyncio.CancelledError:
                raise
            except Exception as e:
                await job.message.reply_text(f"❌ خطا برای {v.name}: {e}")
            finally:
                st["done"] += 1

    async def ui():
        last_t, last_b = time.time(), 0
        while True:
            await asyncio.sleep(3)
            now = time.time()
            cur_b = st["fin"] + sum(c for c, t in list(active.values()) if t > 1)
            speed = max(cur_b - last_b, 0) / max(now - last_t, 0.1)
            last_t, last_b = now, cur_b
            
            text = build_status_text(job, "Upload", speed, cur_b, total_bytes if total_bytes > 0 else 1, st["done"], total_files)
            await safe_edit(job, text)

    tasks = [asyncio.create_task(handle(v)) for v in videos]
    ui_task = asyncio.create_task(ui())
    try:
        await asyncio.gather(*tasks)
    finally:
        ui_task.cancel()
        for t in tasks:
            t.cancel()
    return st["sent"]


# ---------------- پردازش ----------------

async def process(job):
    await safe_edit(job, f"[{job.id}] Task\n\nTask By {job.user_name}\n⏳ در حال گرفتن فهرست فایل‌ها ...")
    tmp = Path(tempfile.mkdtemp(prefix="mega_"))
    try:
        paths = await list_videos(job.link)
        got = 0
        if paths:
            got, failed = await download_parallel(job, paths, tmp)
            if got and failed:
                await job.message.reply_text(f"⚠️ {failed} فایل دانلود نشد.")
        if not got:
            await safe_edit(job, "دانلود موازی ممکن نشد؛ دانلود پوشه...")
            ok, log = await download_folder_simple(job, tmp / "all")
            if not ok:
                return await safe_edit(job, f"❌ دانلود ناموفق بود:\n{log}", final=True)

        videos = sorted(p for p in tmp.rglob("*") if p.is_file() and p.suffix.lower() in VIDEO_EXT)
        if not videos:
            return await safe_edit(job, "هیچ ویدئویی در این پوشه پیدا نشد.", final=True)
        
        sent = await upload_all(job, videos)
        await safe_edit(job, f"🏁 پایان کار.\n{sent} از {len(videos)} ویدئو با موفقیت ارسال شد.", final=True)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


async def download_folder_simple(job, dest):
    dest.mkdir(parents=True, exist_ok=True)
    proc = await asyncio.create_subprocess_exec(
        "megadl", "--path", str(dest), job.link,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    await proc.wait()
    return proc.returncode == 0, ""


async def worker():
    while True:
        job = await queue.get()
        try:
            if job.cancelled:
                continue
            state["busy"] = True
            job.task = asyncio.create_task(process(job))
            try:
                await job.task
            except asyncio.CancelledError:
                if job.cancelled:
                    await safe_edit(job, f"🚫 کار شماره {job.id} لغو شد.", final=True)
                else:
                    raise
            except Exception as e:
                await safe_edit(job, f"❌ خطای غیرمنتظره: {e}", final=True)
        finally:
            job.done = True
            jobs.pop(job.id, None)
            state["busy"] = False
            queue.task_done()


# ---------------- هندلرهای تلگرام ----------------

@app.on_message(filters.command("start"))
async def start(client, message):
    await message.reply_text("سلام! لینک پوشه mega.nz را بفرستید تا با ظاهر پیشرفته فایل‌ها دانلود و آپلود شوند.")


@app.on_message(filters.text & ~filters.regex(r"^/"))
async def handler(client, message):
    uid = message.from_user.id if message.from_user else None
    if ALLOWED and uid not in ALLOWED:
        return
    m = LINK_RE.search(message.text or "")
    if not m:
        return await message.reply_text("لینک پوشه معتبر mega.nz پیدا نشد.", quote=True)
    job = Job(message, m.group(0))
    pos = queue.qsize() + (1 if state["busy"] else 0)
    text = f"📥 در صف قرار گرفت. جایگاه: {pos + 1}" if pos else "⏳ آماده‌سازی..."
    job.status = await message.reply_text(text, quote=True, reply_markup=cancel_markup(job))
    jobs[job.id] = job
    await queue.put(job)


@app.on_callback_query(filters.regex(r"^cancel:(\d+)$"))
async def on_cancel(client, cq):
    if ALLOWED and cq.from_user.id not in ALLOWED:
        return await cq.answer("اجازه ندارید.", show_alert=True)
    job = jobs.get(int(cq.matches[0].group(1)))
    if not job or job.done:
        return await cq.answer("این کار قبلاً تمام یا لغو شده است.")
    job.cancelled = True
    if job.task:
        job.task.cancel()
    else:
        await safe_edit(job, f"🚫 کار شماره {job.id} لغو شد.", final=True)
    await cq.answer("لغو شد")


async def main():
    await app.start()
    asyncio.create_task(worker())
    print("Bot is running...")
    await idle()
    await app.stop()

loop.run_until_complete(main())
