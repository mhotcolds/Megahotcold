# bot.py - MEGA folder -> Telegram leech bot (Pyrofork), built for Railway/Docker.
#
# How it works (streaming pipeline):
#   1. list the video files of the MEGA link (megals)
#   2. download several files in parallel (megadl)  --\
#   3. every finished file goes into a small queue      >-- running at the same time
#   4. several uploaders send them to Telegram         --/
#   Files are deleted right after upload, so the disk never holds the whole folder.
#
# Required variables (Railway -> Variables): API_ID, API_HASH, BOT_TOKEN, ALLOWED_USERS
# Optional: PARALLEL_DL (3), PARALLEL_UL (2), DISK_LIMIT_GB (8), BUFFER (3), WORK_DIR (/tmp/megabot)
import asyncio

# Pyrogram needs an event loop to exist at import time (Python 3.10+).
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

import glob
import itertools
import json
import logging
import math
import os
import re
import shutil
import signal
import sys
import tempfile
import time
from collections import namedtuple
from pathlib import Path

from pyrogram import Client, filters, idle
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, MessageNotModified
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

# ------------------------------------------------------------------ config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("pyrogram").setLevel(logging.WARNING)
log = logging.getLogger("megabot")


def env_number(name, default, lo, hi, cast=int):
    try:
        value = cast(os.environ.get(name, default))
    except (TypeError, ValueError):
        value = default
    return max(lo, min(hi, value))


for _key in ("API_ID", "API_HASH", "BOT_TOKEN"):
    if not os.environ.get(_key, "").strip():
        sys.exit(f"ERROR: environment variable {_key} is not set (Railway -> Variables).")
try:
    API_ID = int(os.environ["API_ID"].strip())
except ValueError:
    sys.exit("ERROR: API_ID must be a number.")
API_HASH = os.environ["API_HASH"].strip()
BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
ALLOWED = {int(x) for x in re.findall(r"-?\d+", os.environ.get("ALLOWED_USERS", ""))}

PARALLEL_DL = env_number("PARALLEL_DL", 3, 1, 8)
PARALLEL_UL = env_number("PARALLEL_UL", 2, 1, 6)
BUFFER = env_number("BUFFER", 3, 1, 20)
DISK_LIMIT = int(env_number("DISK_LIMIT_GB", 8.0, 0.5, 500.0, float) * 1024 ** 3)
WORK_DIR = Path(os.environ.get("WORK_DIR", "/tmp/megabot"))

MAX_SIZE = 2000 * 1024 * 1024      # Telegram limit per file
TARGET_PART = 1800 * 1024 * 1024   # size of each part when a file must be split
GUESS_SIZE = 1024 ** 3             # used for disk planning when MEGA does not tell the size
RETRIES = 3
EDIT_INTERVAL = 4.0

VIDEO_EXT = {".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm", ".m4v", ".ts", ".mpg", ".mpeg", ".3gp"}
FOLDER_RE = re.compile(r"https?://(?:www\.)?mega\.(?:nz|co\.nz)/(?:folder/[\w-]+#[\w-]+|#F![\w-]+![\w-]+)")
FILE_RE = re.compile(r"https?://(?:www\.)?mega\.(?:nz|co\.nz)/(?:file/[\w-]+#[\w-]+|#![\w-]+![\w-]+)")
# megals -l line: HANDLE [PARENT] TYPE SIZE DATE TIME /path
LS_RE = re.compile(r"^(\S+)\s+(?:\S+\s+)?(\d)\s+(\d+)\s+\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s+(/.*)$")

WORK_DIR.mkdir(parents=True, exist_ok=True)
for _old in WORK_DIR.glob("job_*"):
    shutil.rmtree(_old, ignore_errors=True)

app = Client(
    "megabot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True,
    parse_mode=ParseMode.DISABLED,
    sleep_threshold=60,
    max_concurrent_transmissions=PARALLEL_UL,
)

Remote = namedtuple("Remote", "path size handle")
QItem = namedtuple("QItem", "path stem reserved")

queue = asyncio.Queue()
jobs = {}
state = {"current": None}
_job_ids = itertools.count(1)
_file_ids = itertools.count(1)

# ------------------------------------------------------------------ helpers


def fmt_size(n):
    n = float(n or 0)
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{int(n)} B" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.2f} TB"


def fmt_time(seconds):
    if seconds is None or seconds < 0 or math.isinf(seconds) or math.isnan(seconds):
        return "—"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def make_bar(pct):
    if pct is None:
        return "(" + "⬡" * 20 + ") —"
    filled = int(pct * 20)
    return "(" + "⬢" * filled + "⬡" * (20 - filled) + f") {pct * 100:.1f}%"


def short(text, n=120):
    text = " ".join(str(text).split())
    return text if len(text) <= n else text[: n - 1] + "…"


def is_quota(text):
    t = (text or "").lower()
    return "quota" in t or "bandwidth limit" in t


def free_bytes(path):
    try:
        return shutil.disk_usage(path).free
    except OSError:
        return 0


def dir_size(path):
    total = 0
    try:
        for p in path.rglob("*"):
            try:
                if p.is_file():
                    total += p.stat().st_size
            except OSError:
                pass
    except OSError:
        pass
    return total


def is_allowed(user_id):
    return bool(user_id) and user_id in ALLOWED


def kill_proc(proc):
    try:
        proc.kill()
    except (ProcessLookupError, OSError):
        pass


# ------------------------------------------------------------------ job state


class Job:
    def __init__(self, message, link, single):
        user = message.from_user
        self.id = next(_job_ids)
        self.message = message
        self.link = link
        self.single = single            # True for a single-file link
        self.user = user.first_name if user and user.first_name else "User"
        self.status = None              # the status message shown in Telegram
        self.task = None
        self.cancelled = False
        self.done = False
        self.t0 = time.time()
        self.mode = "starting"
        self.note = ""
        self.n_files = 0
        self.total = 0                  # total bytes of all videos (0 = unknown)
        self.dl_files = 0
        self.ul_files = 0
        self.dl_done_bytes = 0
        self.ul_done_bytes = 0
        self.dl_now = 0
        self.ul_now = 0
        self.dl_speed = 0.0
        self.ul_speed = 0.0
        self.active_up = {}             # display name -> {"cur": int, "tot": int}
        self.active_dirs = set()        # per-file download folders (files mode)
        self.folder_dest = None         # download folder (whole-folder mode)
        self.registered = set()
        self.inflight = 0               # files on disk waiting for / doing upload
        self.procs = set()
        self.failed = []
        self.skipped = 0
        self.quota_hit = False
        self.fatal = ""
        self.kinds = ["handle", "path"]
        self.good_kind = None
        self.budget = None
        self.last_text = ""
        self.flood_until = 0.0


class Budget:
    """Limits how much disk the files in flight may reserve."""

    def __init__(self, limit):
        self.limit = limit
        self.used = 0
        self.cond = asyncio.Condition()

    async def acquire(self, n):
        async with self.cond:
            while self.used > 0 and self.used + n > self.limit:
                await self.cond.wait()
            self.used += n

    async def release(self, n):
        async with self.cond:
            self.used = max(0, self.used - n)
            self.cond.notify_all()


# ------------------------------------------------------------------ status message


def markup(job):
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("🔄 Refresh", callback_data=f"refresh:{job.id}"),
            InlineKeyboardButton("❌ Cancel", callback_data=f"cancel:{job.id}"),
        ]]
    )


def eta_seconds(job):
    if job.total <= 0:
        return None
    eta = 0.0
    rem_dl = max(job.total - job.dl_now, 0)
    rem_ul = max(job.total - job.ul_now, 0)
    if rem_dl > 0:
        if job.dl_speed < 1024:
            return None
        eta = max(eta, rem_dl / job.dl_speed)
    if rem_ul > 0:
        if job.ul_speed < 1024:
            return None
        eta = max(eta, rem_ul / job.ul_speed)
    return eta


def build_status(job):
    total_txt = fmt_size(job.total) if job.total else "?"
    pct = None
    if job.total:
        pct = min((job.dl_now + job.ul_now) / (2.0 * job.total), 1.0)
    files_total = job.n_files or "?"
    lines = [
        f"[{job.id}] Task by {job.user}",
        make_bar(pct),
        f"⬇ Download » {fmt_size(job.dl_now)} / {total_txt} » {fmt_size(job.dl_speed)}/s",
        f"⬆ Upload » {fmt_size(job.ul_now)} / {total_txt} » {fmt_size(job.ul_speed)}/s",
        f"Files » ⬇ {job.dl_files}/{files_total}   ⬆ {job.ul_files}/{files_total}",
    ]
    for name, st in list(job.active_up.items())[:3]:
        lines.append(f"  ▸ {short(name, 34)} {st['cur'] * 100 / max(st['tot'], 1):.0f}%")
    if job.note:
        lines.append(job.note)
    lines.append(f"ETA » ⏳ {fmt_time(eta_seconds(job))}")
    lines.append(f"Past » {fmt_time(time.time() - job.t0)}")
    lines.append(f"Mode » #Leech · {job.mode}")
    return "\n".join(lines)


def final_text(job):
    n = job.n_files or job.dl_files
    lines = [
        f"{'❌' if job.fatal else '🏁'} Task [{job.id}] finished",
        f"✅ Sent » {job.ul_files}/{n} files ({fmt_size(job.ul_done_bytes)})",
        f"⏱ Time » {fmt_time(time.time() - job.t0)}",
    ]
    if job.fatal:
        lines.append(f"❌ {job.fatal}")
    if job.quota_hit:
        lines.append("🚫 MEGA transfer quota exceeded. The limit is per IP address; try again in a few hours.")
    if job.skipped:
        lines.append(f"⏭ {job.skipped} files were skipped because of the quota.")
    if job.failed:
        lines.append(f"⚠️ Failed ({len(job.failed)}):")
        for name, why in job.failed[:10]:
            lines.append(f"• {short(name, 40)} — {short(why, 60)}")
        if len(job.failed) > 10:
            lines.append(f"… and {len(job.failed) - 10} more")
    if not job.fatal and not job.dl_files and not job.failed and not job.quota_hit:
        lines.append("No video files were found in this link.")
    return "\n".join(lines)


async def safe_edit(job, text, final=False):
    if not final:
        if text == job.last_text or time.time() < job.flood_until:
            return
    for _ in range(3 if final else 1):
        try:
            await job.status.edit_text(text, reply_markup=None if final else markup(job))
            job.last_text = text
            return
        except FloodWait as e:
            wait = float(getattr(e, "value", None) or getattr(e, "x", None) or 5)
            if not final:
                job.flood_until = time.time() + wait
                return
            await asyncio.sleep(wait + 1)
        except MessageNotModified:
            return
        except Exception as e:  # never let a status edit break the job
            log.warning("status edit failed: %s", e)
            return


# ------------------------------------------------------------------ subprocess helper


async def run_proc(job, *args, timeout=None, holder=None):
    """Runs a command. Returns (returncode, stdout bytes, stderr tail). Killed on cancel/timeout."""
    proc = await asyncio.create_subprocess_exec(
        *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    job.procs.add(proc)
    if holder is not None:
        holder.append(proc)
    out, err = bytearray(), bytearray()

    async def drain_out():
        while True:
            chunk = await proc.stdout.read(8192)
            if not chunk:
                return
            if len(out) < 5_000_000:
                out.extend(chunk)

    async def drain_err():
        while True:
            chunk = await proc.stderr.read(8192)
            if not chunk:
                return
            err.extend(chunk)
            if len(err) > 2000:
                del err[:-2000]

    readers = [asyncio.create_task(drain_out()), asyncio.create_task(drain_err())]
    try:
        await asyncio.wait_for(proc.wait(), timeout)
        await asyncio.gather(*readers)
    except BaseException:
        kill_proc(proc)
        raise
    finally:
        job.procs.discard(proc)
        for r in readers:
            r.cancel()
    return proc.returncode, bytes(out), err.decode(errors="ignore")


# ------------------------------------------------------------------ ffmpeg helpers


async def probe(job, path):
    """Returns (duration seconds, width, height); zeros when unknown."""
    try:
        rc, out, _ = await run_proc(
            job, "ffprobe", "-v", "error", "-select_streams", "v:0",
            "-show_entries",
            "stream=width,height:stream_tags=rotate:stream_side_data=rotation:format=duration",
            "-of", "json", str(path), timeout=120,
        )
        data = json.loads(out or b"{}")
        stream = (data.get("streams") or [{}])[0]
        width, height = int(stream.get("width") or 0), int(stream.get("height") or 0)
        rotation = 0
        try:
            rotation = int(float((stream.get("tags") or {}).get("rotate", 0)))
        except (TypeError, ValueError):
            pass
        for side in stream.get("side_data_list") or []:
            if "rotation" in side:
                rotation = int(float(side["rotation"]))
        if abs(rotation) % 180 == 90:
            width, height = height, width
        duration = float((data.get("format") or {}).get("duration") or 0)
        return duration, width, height
    except asyncio.CancelledError:
        raise
    except Exception:
        return 0.0, 0, 0


async def make_thumb(job, path, duration):
    """Grabs one frame of the video as a JPEG thumbnail (max 320 px)."""
    thumb = path.with_name(path.stem + "__thumb.jpg")
    scale = "scale=w='if(gt(iw,ih),320,-2)':h='if(gt(iw,ih),-2,320)'"
    first = min(max(duration * 0.1, 1.0), 30.0) if duration else 1.0
    for seek in (first, 0.0):
        try:
            rc, _, _ = await run_proc(
                job, "ffmpeg", "-y", "-v", "error", "-ss", f"{seek:.1f}", "-i", str(path),
                "-frames:v", "1", "-vf", scale, "-q:v", "5", str(thumb), timeout=120,
            )
            if rc == 0 and thumb.exists() and thumb.stat().st_size > 0:
                return thumb
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
    thumb.unlink(missing_ok=True)
    return None


async def remux_mp4(job, src):
    """Copies the streams into an mp4 container (no quality loss) so Telegram shows it as a video."""
    out = src.with_name(src.stem + "__tg.mp4")
    try:
        rc, _, _ = await run_proc(
            job, "ffmpeg", "-y", "-v", "error", "-i", str(src),
            "-map", "0:v:0", "-map", "0:a?", "-c", "copy", "-movflags", "+faststart", str(out),
            timeout=3600,
        )
        if rc == 0 and out.exists() and 0 < out.stat().st_size <= MAX_SIZE:
            src.unlink(missing_ok=True)
            return out
    except asyncio.CancelledError:
        out.unlink(missing_ok=True)
        raise
    except Exception as e:
        log.warning("remux failed: %s", e)
    out.unlink(missing_ok=True)
    return None


async def split_big(job, src, stem):
    """Splits a file bigger than 2 GB into parts. Returns [(path, as_video, display_name)]."""
    duration, _, _ = await probe(job, src)
    if duration <= 0:
        return []
    size = src.stat().st_size
    if free_bytes(src.parent) < size * 1.05:
        return []
    n = max(2, math.ceil(size / TARGET_PART))
    for as_mp4 in (True, False):
        ext = ".mp4" if as_mp4 else src.suffix
        for _ in range(4):
            pattern = src.with_name(f"{src.stem}__p%03d{ext}")
            found = f"{glob.escape(src.stem)}__p[0-9][0-9][0-9]{glob.escape(ext)}"
            cmd = ["ffmpeg", "-y", "-v", "error", "-i", str(src),
                   "-map", "0:v:0", "-map", "0:a?", "-c", "copy", "-f", "segment"]
            if as_mp4:
                cmd += ["-segment_format", "mp4"]
            cmd += ["-segment_time", f"{duration / n:.2f}", "-reset_timestamps", "1", str(pattern)]
            rc, _, _ = await run_proc(job, *cmd, timeout=3600)
            parts = sorted(src.parent.glob(found))
            if rc == 0 and parts and all(p.stat().st_size <= MAX_SIZE for p in parts):
                src.unlink(missing_ok=True)
                return [(p, ext == ".mp4", f"{stem}_part{i:03d}{ext}") for i, p in enumerate(parts, 1)]
            for p in parts:
                p.unlink(missing_ok=True)
            if rc != 0:
                break  # codecs not allowed in mp4 -> retry with the original container
            n = math.ceil(n * 1.3) + 1  # parts were still too big -> more, smaller parts
    return []


async def prepare(job, src, stem, orig_name):
    """Returns the list of [(path, as_video, display_name)] to upload for one downloaded file."""
    size = src.stat().st_size
    if size > MAX_SIZE:
        return await split_big(job, src, stem)
    if src.suffix == ".mp4":
        return [(src, True, stem + ".mp4")]
    if free_bytes(src.parent) > size * 1.05 + 200 * 1024 * 1024:
        out = await remux_mp4(job, src)
        if out:
            return [(out, True, stem + ".mp4")]
    return [(src, False, orig_name)]  # could not convert: send as a normal file


# ------------------------------------------------------------------ MEGA: listing & download


async def list_remote(job):
    """Lists the videos of a folder link. Tries the detailed listing first, then the plain one."""
    for args in (("megals", "-l", "-R", job.link), ("megals", "-R", job.link)):
        try:
            rc, out, err = await run_proc(job, *args, timeout=180)
        except asyncio.TimeoutError:
            continue
        found = []
        for line in out.decode("utf-8", "ignore").splitlines():
            line = line.rstrip()
            m = LS_RE.match(line)
            if m:
                if m.group(2) == "0" and Path(m.group(4)).suffix.lower() in VIDEO_EXT:
                    found.append(Remote(m.group(4), int(m.group(3)), m.group(1)))
            elif line.startswith("/") and Path(line).suffix.lower() in VIDEO_EXT:
                found.append(Remote(line, 0, ""))
        if found:
            return sorted(found, key=lambda r: r.path)
        if is_quota(err):
            job.quota_hit = True
    return []


def video_files(folder):
    return sorted(
        p for p in folder.rglob("*")
        if p.is_file() and p.suffix.lower() in VIDEO_EXT and not p.name.startswith(".megatmp")
    )


async def download_one(job, item, folder):
    """Downloads one file of the link. Returns (Path or None, error text)."""
    last_err = ""
    for attempt in range(RETRIES):
        kinds = [job.good_kind] if job.good_kind else list(job.kinds)
        for kind in kinds:
            if kind == "handle" and not item.handle:
                continue
            link = f"{job.link}/file/{item.handle}" if kind == "handle" else f"{job.link}:{item.path}"
            shutil.rmtree(folder, ignore_errors=True)
            folder.mkdir(parents=True, exist_ok=True)
            rc, _, err = await run_proc(job, "megadl", "--path", str(folder), link)
            files = video_files(folder)
            if rc == 0 and len(files) == 1:
                job.good_kind = kind
                return files[0], ""
            if rc == 0 and len(files) > 1:
                # this link style downloaded the whole folder, so it is not usable per file
                if kind in job.kinds:
                    job.kinds.remove(kind)
                continue
            if is_quota(err):
                job.quota_hit = True
                return None, "MEGA transfer quota exceeded"
            last_err = short(err.strip().splitlines()[-1] if err.strip() else f"exit code {rc}")
        if attempt < RETRIES - 1:
            await asyncio.sleep(2 * (attempt + 1))
    return None, last_err or "download failed"


async def produce_files(job, items, tmp, upq):
    """Parallel per-file download. Returns "ok", or "fallback" when per-file mode does not work."""
    sem = asyncio.Semaphore(PARALLEL_DL)
    stats = {"ok": 0, "fail": 0}
    fallback = asyncio.Event()

    async def one(index, item):
        async with sem:
            if fallback.is_set() and stats["ok"] == 0:
                return
            if job.quota_hit:
                job.skipped += 1
                return
            size = item.size or GUESS_SIZE
            needs_work = Path(item.path).suffix.lower() != ".mp4" or size > MAX_SIZE
            reserve = size * (2 if needs_work else 1)
            await job.budget.acquire(reserve)
            folder = tmp / f"{index:05d}"
            folder.mkdir(parents=True, exist_ok=True)
            job.active_dirs.add(folder)
            path, reason = None, "unknown error"
            try:
                path, reason = await download_one(job, item, folder)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                reason = short(str(e))
            finally:
                job.active_dirs.discard(folder)
            if path is None:
                await job.budget.release(reserve)
                shutil.rmtree(folder, ignore_errors=True)
                stats["fail"] += 1
                if not job.quota_hit:
                    job.failed.append((Path(item.path).name, reason))
                    if stats["ok"] == 0 and stats["fail"] >= min(3, len(items)):
                        fallback.set()
                return
            stats["ok"] += 1
            job.dl_done_bytes += path.stat().st_size
            job.dl_files += 1
            job.inflight += 1
            await upq.put(QItem(path, Path(item.path).stem, reserve))

    tasks = [asyncio.create_task(one(i, it)) for i, it in enumerate(items)]
    everything = asyncio.ensure_future(asyncio.gather(*tasks))
    everything.add_done_callback(lambda f: f.cancelled() or f.exception())  # silence 'never retrieved'
    waiter = asyncio.create_task(fallback.wait())
    try:
        await asyncio.wait({everything, waiter}, return_when=asyncio.FIRST_COMPLETED)
        if fallback.is_set() and stats["ok"] == 0:
            for t in tasks:
                t.cancel()
            everything.cancel()
            await asyncio.gather(everything, *tasks, return_exceptions=True)
            return "fallback"
        await everything
        return "ok"
    except asyncio.CancelledError:
        for t in tasks:
            t.cancel()
        everything.cancel()
        raise
    finally:
        waiter.cancel()


async def throttle(job, dest, holder):
    """Pauses the whole-folder download while the disk is full and uploads can still free space."""
    paused = False
    try:
        while True:
            await asyncio.sleep(3)
            if not holder:
                continue
            proc = holder[0]
            if proc.returncode is not None:
                return
            used = dir_size(dest)
            if not paused and used > DISK_LIMIT and job.inflight > 0:
                proc.send_signal(signal.SIGSTOP)
                paused = True
                job.note = "⏸ Waiting for uploads to free disk space"
            elif paused and (used < DISK_LIMIT * 0.6 or job.inflight == 0):
                proc.send_signal(signal.SIGCONT)
                paused = False
                job.note = ""
    except ProcessLookupError:
        pass
    finally:
        job.note = ""
        if paused:
            try:
                holder[0].send_signal(signal.SIGCONT)
            except Exception:
                pass


async def produce_folder(job, tmp, upq):
    """Fallback: download the whole link with one megadl and queue every finished video."""
    dest = tmp / "all"
    dest.mkdir(parents=True, exist_ok=True)
    job.folder_dest = dest
    job.registered = set()
    holder = []
    proc_task = asyncio.create_task(
        run_proc(job, "megadl", "--path", str(dest), job.link, holder=holder)
    )
    guard = asyncio.create_task(throttle(job, dest, holder))
    seen = {}
    try:
        while True:
            finished = proc_task.done()
            for p in video_files(dest):
                if p in job.registered:
                    continue
                try:
                    size = p.stat().st_size
                except OSError:
                    continue
                # megadl writes ".megatmp" files and renames when complete; also require a stable size
                if finished or seen.get(p) == size:
                    job.registered.add(p)
                    job.dl_done_bytes += size
                    job.dl_files += 1
                    job.inflight += 1
                    await upq.put(QItem(p, p.stem, 0))
                else:
                    seen[p] = size
            if finished:
                break
            await asyncio.sleep(3)
        rc, _, err = proc_task.result()
        if is_quota(err):
            job.quota_hit = True
        elif rc != 0:
            reason = short(err.strip().splitlines()[-1] if err.strip() else f"exit code {rc}")
            if job.dl_files == 0:
                job.fatal = f"MEGA download failed: {reason}"
            else:
                job.failed.append(("(some files)", f"MEGA download ended with an error: {reason}"))
    finally:
        guard.cancel()
        if not proc_task.done():
            proc_task.cancel()
        job.folder_dest = None


# ------------------------------------------------------------------ Telegram upload


async def send_part(job, path, as_video, display):
    thumb = None
    state_ = {"cur": 0, "tot": 1}
    try:
        size = path.stat().st_size
        state_["tot"] = max(size, 1)
        duration, width, height = await probe(job, path)
        thumb = await make_thumb(job, path, duration)
        job.active_up[display] = state_

        def progress(current, total):
            state_["cur"], state_["tot"] = current, max(total, 1)

        last = ""
        for _ in range(2):
            try:
                state_["cur"] = 0
                if as_video:
                    await app.send_video(
                        job.message.chat.id, str(path), caption=display[:1000], file_name=display,
                        duration=int(duration), width=width, height=height,
                        thumb=str(thumb) if thumb else None, supports_streaming=True,
                        progress=progress,
                    )
                else:
                    await app.send_document(
                        job.message.chat.id, str(path), caption=display[:1000], file_name=display,
                        thumb=str(thumb) if thumb else None, progress=progress,
                    )
                job.ul_done_bytes += size
                return True
            except asyncio.CancelledError:
                raise
            except Exception as e:
                last = short(f"{type(e).__name__}: {e}")
                log.warning("upload of %s failed: %s", display, last)
                await asyncio.sleep(3)
        job.failed.append((display, f"upload failed: {last}"))
        return False
    finally:
        job.active_up.pop(display, None)
        path.unlink(missing_ok=True)
        if thumb:
            thumb.unlink(missing_ok=True)


async def handle_file(job, qi):
    safe, parts = None, []
    try:
        orig_name = qi.path.name
        safe = qi.path.with_name(f"src{next(_file_ids)}{qi.path.suffix.lower()}")
        qi.path.rename(safe)
        parts = await prepare(job, safe, qi.stem, orig_name)
        if not parts:
            job.failed.append((orig_name, "could not convert or split the file"))
            return
        ok = True
        for path, as_video, display in list(parts):
            if job.cancelled:
                ok = False
                break
            if not await send_part(job, path, as_video, display):
                ok = False
        if ok:
            job.ul_files += 1
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.exception("file handling failed")
        job.failed.append((qi.stem, short(str(e))))
    finally:
        for path, _, _ in parts:
            path.unlink(missing_ok=True)
        if safe is not None:
            safe.unlink(missing_ok=True)
        job.inflight -= 1
        if qi.reserved and job.budget:
            await job.budget.release(qi.reserved)


async def uploader(job, upq):
    while True:
        qi = await upq.get()
        try:
            if qi is None:
                return
            await handle_file(job, qi)
        finally:
            upq.task_done()


# ------------------------------------------------------------------ job pipeline


async def monitor(job):
    """Every 2 s: measure progress and speed, refresh the Telegram status message."""
    last_t, last_dl, last_ul, last_edit = time.time(), 0, 0, 0.0
    while True:
        await asyncio.sleep(2)
        now = time.time()
        partial = sum(dir_size(d) for d in list(job.active_dirs))
        if job.folder_dest:
            for p in job.folder_dest.rglob("*"):
                try:
                    if p.is_file() and p not in job.registered:
                        partial += p.stat().st_size
                except OSError:
                    pass
        dl = job.dl_done_bytes + partial
        ul = job.ul_done_bytes + sum(s["cur"] for s in list(job.active_up.values()))
        dt = max(now - last_t, 0.1)
        inst_dl, inst_ul = max(dl - last_dl, 0) / dt, max(ul - last_ul, 0) / dt
        job.dl_speed = inst_dl if job.dl_speed == 0 else 0.7 * job.dl_speed + 0.3 * inst_dl
        job.ul_speed = inst_ul if job.ul_speed == 0 else 0.7 * job.ul_speed + 0.3 * inst_ul
        job.dl_now, job.ul_now = dl, ul
        last_t, last_dl, last_ul = now, dl, ul
        if now - last_edit >= EDIT_INTERVAL:
            last_edit = now
            await safe_edit(job, build_status(job))


async def process(job):
    job.t0 = time.time()
    tmp = Path(tempfile.mkdtemp(prefix="job_", dir=WORK_DIR))
    watcher = asyncio.create_task(monitor(job))
    uploaders = []
    try:
        job.mode = "listing files"
        await safe_edit(job, build_status(job))
        items = [] if job.single else await list_remote(job)
        job.n_files = len(items)
        job.total = sum(i.size for i in items) if items and all(i.size for i in items) else 0
        if not any(i.handle for i in items):
            job.kinds = ["path"]
        job.budget = Budget(DISK_LIMIT)
        upq = asyncio.Queue(maxsize=BUFFER)
        uploaders = [asyncio.create_task(uploader(job, upq)) for _ in range(PARALLEL_UL)]

        if items:
            job.mode = f"parallel ×{PARALLEL_DL}"
            result = await produce_files(job, items, tmp, upq)
        else:
            result = "fallback"
        if result == "fallback":
            if items:
                log.warning("per-file download did not work; switching to whole-folder mode")
            job.mode = "whole folder"
            job.failed.clear()
            for sub in tmp.iterdir():
                shutil.rmtree(sub, ignore_errors=True)
            await produce_folder(job, tmp, upq)

        for _ in uploaders:
            await upq.put(None)
        await asyncio.gather(*uploaders)
        await safe_edit(job, final_text(job), final=True)
        try:
            await job.message.reply_text(f"✅ Task [{job.id}] finished: {job.ul_files} file(s) sent.")
        except Exception:
            pass
    finally:
        watcher.cancel()
        for u in uploaders:
            u.cancel()
        for proc in list(job.procs):
            kill_proc(proc)
        shutil.rmtree(tmp, ignore_errors=True)


async def worker():
    while True:
        job = await queue.get()
        try:
            if job.cancelled:
                continue
            state["current"] = job
            job.task = asyncio.create_task(process(job))
            try:
                await job.task
            except asyncio.CancelledError:
                if job.cancelled:
                    await safe_edit(job, f"🚫 Task [{job.id}] cancelled.", final=True)
                else:
                    raise
            except Exception as e:
                log.exception("job failed")
                msg = str(e)
                if isinstance(e, FileNotFoundError):
                    msg = f"a required tool is missing ({e})"
                await safe_edit(job, f"❌ Task [{job.id}] failed: {short(msg)}", final=True)
        finally:
            job.done = True
            jobs.pop(job.id, None)
            state["current"] = None
            queue.task_done()


def cancel_job(job):
    job.cancelled = True
    for proc in list(job.procs):
        kill_proc(proc)
    if job.task and not job.task.done():
        job.task.cancel()


# ------------------------------------------------------------------ Telegram handlers

NOT_ALLOWED = (
    "⛔ You are not authorized to use this bot.\n"
    "Your user id: {uid}\n"
    "The owner must add it to ALLOWED_USERS on the server."
)


@app.on_message(filters.command(["start", "help", "id"]))
async def cmd_start(client, message):
    uid = message.from_user.id if message.from_user else 0
    if not is_allowed(uid):
        return await message.reply_text(NOT_ALLOWED.format(uid=uid))
    await message.reply_text(
        "Send me a MEGA folder link (mega.nz/folder/...) and I will upload all videos inside it here.\n"
        "You can send several links; they run one after another, and each has Cancel / Refresh buttons.\n"
        f"Your user id: {uid}"
    )


@app.on_message(filters.text & ~filters.regex(r"^/"))
async def on_text(client, message):
    uid = message.from_user.id if message.from_user else 0
    if not is_allowed(uid):
        return await message.reply_text(NOT_ALLOWED.format(uid=uid))
    text = message.text or ""
    links = [(m.group(0), False) for m in FOLDER_RE.finditer(text)]
    links += [(m.group(0), True) for m in FILE_RE.finditer(text)]
    unique, seen = [], set()
    for link, single in links:
        if link not in seen:
            seen.add(link)
            unique.append((link, single))
    if not unique:
        return await message.reply_text("Send a MEGA folder link, like https://mega.nz/folder/XXXX#YYYY", quote=True)
    for link, single in unique:
        if queue.qsize() >= 30:
            await message.reply_text("The queue is full. Try again later.", quote=True)
            break
        job = Job(message, link, single)
        position = queue.qsize() + (1 if state["current"] else 0)
        label = f"📥 [{job.id}] Queued. Position: {position + 1}" if position else f"⏳ [{job.id}] Starting..."
        job.status = await message.reply_text(label, quote=True, reply_markup=markup(job))
        jobs[job.id] = job
        await queue.put(job)


@app.on_callback_query(filters.regex(r"^refresh:(\d+)$"))
async def on_refresh(client, cq):
    if not is_allowed(cq.from_user.id):
        return await cq.answer("Not allowed.", show_alert=True)
    job = jobs.get(int(cq.matches[0].group(1)))
    if not job or job.done:
        return await cq.answer("This task has already finished.", show_alert=True)
    job.flood_until = 0.0
    await safe_edit(job, build_status(job))
    await cq.answer("Updated")


@app.on_callback_query(filters.regex(r"^cancel:(\d+)$"))
async def on_cancel(client, cq):
    if not is_allowed(cq.from_user.id):
        return await cq.answer("Not allowed.", show_alert=True)
    job = jobs.get(int(cq.matches[0].group(1)))
    if not job or job.done:
        return await cq.answer("This task has already finished or was cancelled.")
    cancel_job(job)
    if job.task is None:  # still waiting in the queue
        job.done = True
        jobs.pop(job.id, None)
        await safe_edit(job, f"🚫 Task [{job.id}] cancelled.", final=True)
    await cq.answer("Cancelled")


# ------------------------------------------------------------------ main


async def main():
    await app.start()
    me = await app.get_me()
    log.info("Bot @%s is running. Allowed users: %s", me.username, sorted(ALLOWED) or "NONE (set ALLOWED_USERS!)")
    for tool in ("megadl", "megals", "ffmpeg", "ffprobe"):
        if not shutil.which(tool):
            log.error("Required tool not found in PATH: %s", tool)
    worker_task = asyncio.create_task(worker())
    await idle()
    worker_task.cancel()
    current = state["current"]
    if current:
        cancel_job(current)
    await app.stop()


loop.run_until_complete(main())
