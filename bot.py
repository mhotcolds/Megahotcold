# bot.py - MEGA folder -> Telegram leech bot (Pyrofork), built for Railway/Docker.
#
# How it works (streaming pipeline):
#   1. read the folder info (names + sizes) with mega-public-client  -> real total size / file count
#   2. download the whole folder with megadl (megatools)             --\
#   3. every finished video is moved to its own work folder and queued >-- at the same time
#   4. several uploaders send the queued videos to Telegram          --/
#   Files are deleted right after upload; if the disk gets full, megadl is paused until uploads free space.
#
# Why not one megadl call per file?  Files inside a public folder can only be fetched together with the
# folder context, so a stand-alone file link gives "ENOENT" from MEGA (verified with the library docs).
#
# Required variables (Railway -> Variables): API_ID, API_HASH, BOT_TOKEN, ALLOWED_USERS
# Optional: PARALLEL_UL (2), DISK_LIMIT_GB (2), BUFFER (3), THUMB_SIZE (320), WORK_DIR (/tmp/megabot)
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

_URL_RE = re.compile(r"https?://[^\s'\"<>]+")
_OLD_LINK_RE = re.compile(r"#F?![\w-]+![\w-]+")
_KEY_RE = re.compile(r"#[\w-]{16,}")


def redact(text):
    """Removes MEGA links and keys from any text that is logged or shown to the user."""
    text = _URL_RE.sub("<link>", str(text))
    text = _OLD_LINK_RE.sub("<link>", text)
    return _KEY_RE.sub("#<key>", text)


class RedactFormatter(logging.Formatter):
    def format(self, record):
        return redact(super().format(record))


_handler = logging.StreamHandler()
_handler.setFormatter(RedactFormatter("%(asctime)s %(levelname)s %(message)s"))
logging.basicConfig(level=logging.INFO, handlers=[_handler])
for _noisy in ("pyrogram", "httpx", "httpcore", "mega_client"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)
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

PARALLEL_UL = env_number("PARALLEL_UL", 2, 1, 6)
BUFFER = env_number("BUFFER", 3, 1, 20)
DISK_LIMIT = int(env_number("DISK_LIMIT_GB", 2.0, 0.3, 500.0, float) * 1024 ** 3)
MIN_FREE = 300 * 1024 ** 2           # always keep this much disk free
WORK_DIR = Path(os.environ.get("WORK_DIR", "/tmp/megabot"))

MAX_SIZE = 2000 * 1024 * 1024      # Telegram limit per file
TARGET_PART = 1800 * 1024 * 1024   # size of each part when a file must be split
THUMB_SIZE = env_number("THUMB_SIZE", 320, 90, 1280)   # longest side of the thumbnail in pixels
THUMB_MAX_BYTES = 195_000                                # Telegram wants thumbnails under 200 KB
EDIT_INTERVAL = 4.0

VIDEO_EXT = {".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm", ".m4v", ".ts", ".mpg", ".mpeg", ".3gp"}
FOLDER_RE = re.compile(r"https?://(?:www\.)?mega\.(?:nz|co\.nz)/(?:folder/[\w-]+#[\w-]+|#F![\w-]+![\w-]+)")
FILE_RE = re.compile(r"https?://(?:www\.)?mega\.(?:nz|co\.nz)/(?:file/[\w-]+#[\w-]+|#![\w-]+![\w-]+)")
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

Meta = namedtuple("Meta", "count total unknown expected largest unnamed")
QItem = namedtuple("QItem", "path stem")

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
    text = " ".join(redact(text).split())
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
        self.folder_dest = None         # download folder (whole-folder mode)
        self.registered = set()
        self.inflight = 0               # files on disk waiting for / doing upload
        self.procs = set()
        self.failed = []
        self.skipped = 0
        self.quota_hit = False
        self.fatal = ""
        self.expected = {}              # video file name -> [sizes] from the folder info
        self.meta_error = ""            # why the folder info could not be read (empty = ok)
        self.last_text = ""
        self.flood_until = 0.0


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
    dl, ul = job.dl_now, job.ul_now
    if job.total:  # non-video files can make the download counter exceed the video total
        dl, ul = min(dl, job.total), min(ul, job.total)
    pct = None
    if job.total:
        pct = min((dl + ul) / (2.0 * job.total), 1.0)
    files_total = job.n_files or "?"
    lines = [
        f"[{job.id}] Task by {job.user}",
        make_bar(pct),
        f"⬇ Download » {fmt_size(dl)} / {total_txt} » {fmt_size(job.dl_speed)}/s",
        f"⬆ Upload » {fmt_size(ul)} / {total_txt} » {fmt_size(job.ul_speed)}/s",
        f"Files » ⬇ {job.dl_files}/{files_total}   ⬆ {job.ul_files}/{files_total}",
    ]
    for name, st in list(job.active_up.items())[:3]:
        lines.append(f"  ▸ {short(name, 34)} {st['cur'] * 100 / max(st['tot'], 1):.0f}%")
    if job.note:
        lines.append(job.note)
    if not job.total and job.meta_error:
        lines.append(f"ℹ️ Total size unknown ({job.meta_error})")
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
    elif job.n_files and job.dl_files < job.n_files and not job.fatal and not job.quota_hit and not job.cancelled:
        lines.append(f"⚠️ MEGA lists {job.n_files} videos but only {job.dl_files} were downloaded.")
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
    """High-quality JPEG thumbnail: picks a representative frame (not a black one), Lanczos scaling,
    best JPEG quality that still fits Telegram's 200 KB limit."""
    thumb = path.with_name(path.stem + "__thumb.jpg")
    side = THUMB_SIZE
    scale = f"scale=w='if(gt(iw,ih),{side},-2)':h='if(gt(iw,ih),-2,{side})':flags=lanczos"
    first = min(max(duration * 0.1, 1.0), 120.0) if duration else 1.0
    for seek, pick in ((first, "thumbnail=90,"), (0.0, "")):
        for quality in (2, 4, 7, 12):
            try:
                rc, _, _ = await run_proc(
                    job, "ffmpeg", "-y", "-v", "error", "-ss", f"{seek:.1f}", "-i", str(path),
                    "-vf", pick + scale, "-frames:v", "1", "-q:v", str(quality),
                    "-pix_fmt", "yuvj420p", str(thumb), timeout=180,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                break
            if rc != 0 or not thumb.exists():
                break
            if 0 < thumb.stat().st_size <= THUMB_MAX_BYTES:
                return thumb
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


def explain_error(err, rc):
    """Short, link-free explanation of a megadl failure."""
    low = (err or "").lower()
    if is_quota(low):
        return "MEGA transfer quota exceeded"
    if "no space" in low:
        return "the server disk is full"
    lines = [ln for ln in (err or "").strip().splitlines() if ln.strip()]
    return short(lines[-1]) if lines else f"exit code {rc}"


def _meta_from_info(info):
    """Counts the videos in the folder info. Files whose name could not be read are counted as 'unnamed'
    (they may or may not be videos), so we never claim 'no videos' when we simply could not tell."""
    expected, total, count, unknown, largest, unnamed = {}, 0, 0, 0, 0, 0
    for node in getattr(info, "nodes", None) or []:
        try:
            if not getattr(node, "is_file", False):
                continue
            name = str(getattr(node, "name", "") or "")
            size = int(getattr(node, "size", 0) or 0)
        except (TypeError, ValueError):
            continue
        if not name:
            unnamed += 1
            continue
        if Path(name).suffix.lower() not in VIDEO_EXT:
            continue
        count += 1
        largest = max(largest, size)
        if size > 0:
            total += size
            expected.setdefault(name, []).append(size)
        else:
            unknown += 1
    return Meta(count, total, unknown, expected, largest, unnamed)


async def fetch_metadata(job):
    """Reads names and sizes of the videos in a public link (nothing is downloaded).
    Returns Meta, or None when it fails - the transfer still works, only the total size stays unknown."""
    try:
        from mega_client import MegaClient
    except Exception as e:
        job.meta_error = "mega-public-client is not installed"
        log.warning("mega-public-client is not available, sizes stay unknown: %s", e)
        return None

    def work():
        with MegaClient() as client:
            if job.single:
                return client.get_file_info(job.link)
            return client.get_folder_info(job.link)

    try:
        info = await asyncio.wait_for(asyncio.to_thread(work), 180)
        if job.single:
            node = type("N", (), {"is_file": True, "name": getattr(info, "name", ""), "size": getattr(info, "size", 0)})
            info = type("I", (), {"nodes": [node]})
        meta = _meta_from_info(info)
        log.info("folder info: videos=%d total=%s unknown_sizes=%d unnamed_files=%d",
                 meta.count, fmt_size(meta.total), meta.unknown, meta.unnamed)
        return meta
    except asyncio.CancelledError:
        raise
    except Exception as e:
        job.meta_error = short(f"{type(e).__name__}: {e}", 80)
        log.warning("could not read folder info (%s): %s", type(e).__name__, e)
        return None


def other_files(folder):
    """Finished files that are not videos (megadl names unfinished ones '.megatmp...')."""
    return sorted(
        p for p in folder.rglob("*")
        if p.is_file() and p.suffix.lower() not in VIDEO_EXT and not p.name.startswith(".megatmp")
    )


def video_files(folder):
    return sorted(
        p for p in folder.rglob("*")
        if p.is_file() and p.suffix.lower() in VIDEO_EXT and not p.name.startswith(".megatmp")
    )


async def throttle(job, root, holder):
    """Pauses megadl while the disk is (nearly) full and uploads can still free space."""
    paused = False
    try:
        while True:
            await asyncio.sleep(3)
            if not holder:
                continue
            proc = holder[0]
            if proc.returncode is not None:
                return
            used, free = dir_size(root), free_bytes(root)
            full = used > DISK_LIMIT or free < MIN_FREE
            roomy = used < DISK_LIMIT * 0.6 and free > MIN_FREE * 2
            if not paused and full and job.inflight > 0:
                proc.send_signal(signal.SIGSTOP)
                paused = True
                job.note = "⏸ Disk is full - waiting for uploads to free space"
            elif paused and (roomy or job.inflight == 0):
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
    """Downloads the whole link with one megadl and queues every finished video."""
    dest = tmp / "all"
    dest.mkdir(parents=True, exist_ok=True)
    job.folder_dest = dest
    job.registered = set()
    holder = []
    proc_task = asyncio.create_task(
        run_proc(job, "megadl", "--path", str(dest), job.link, holder=holder)
    )
    guard = asyncio.create_task(throttle(job, tmp, holder))
    seen = {}
    try:
        while True:
            finished = proc_task.done()
            for p in other_files(dest):
                # a finished non-video file is useless for us: delete it so it does not fill the disk
                try:
                    size = p.stat().st_size
                    prev = seen.get(p)
                    stable = prev[1] + 1 if prev and prev[0] == size else 0
                    seen[p] = (size, stable)
                    if finished or stable >= 1:
                        p.unlink(missing_ok=True)
                except OSError:
                    pass
            for p in video_files(dest):
                if p in job.registered:
                    continue
                try:
                    size = p.stat().st_size
                except OSError:
                    continue
                sizes = job.expected.get(p.name)
                prev = seen.get(p)
                stable = prev[1] + 1 if prev and prev[0] == size else 0   # scans with an unchanged size
                seen[p] = (size, stable)
                # ready when: megadl is done, or the size equals the one from the folder info, or the
                # size stopped changing (longer wait when the folder info disagrees with the file size)
                ready = finished or bool(sizes and size in sizes) or stable >= (4 if sizes else 1)
                if not ready:
                    continue
                # move it out of the watched folder, so the files we create while converting or
                # splitting it are never mistaken for new downloads
                work = tmp / f"p{next(_file_ids):05d}"
                try:
                    work.mkdir(parents=True, exist_ok=True)
                    moved = work / p.name
                    p.rename(moved)
                except OSError as e:
                    log.warning("could not move a finished file: %s", e)
                    continue
                job.registered.add(p)
                if sizes and size in sizes:
                    sizes.remove(size)
                job.dl_done_bytes += size
                job.dl_files += 1
                job.inflight += 1
                await upq.put(QItem(moved, p.stem))
            if finished:
                break
            await asyncio.sleep(3)
        rc, _, err = proc_task.result()
        if is_quota(err):
            job.quota_hit = True
        elif rc != 0:
            reason = explain_error(err, rc)
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
        partial = 0
        if job.folder_dest:
            for p in job.folder_dest.rglob("*"):
                try:
                    wanted = p.suffix.lower() in VIDEO_EXT or p.name.startswith(".megatmp")
                    if wanted and p not in job.registered and p.is_file():
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
        job.mode = "reading folder info"
        await safe_edit(job, build_status(job))
        meta = await fetch_metadata(job)
        if meta is not None:
            job.n_files = meta.count
            job.total = meta.total if meta.count and not meta.unknown else 0
            job.expected = meta.expected
            if meta.count == 0 and not meta.unnamed:
                await safe_edit(job, final_text(job), final=True)  # the folder has no videos
                return
            need = meta.largest * (2 if meta.largest > MAX_SIZE else 1) + MIN_FREE
            free = free_bytes(tmp)
            if meta.largest and free and need > free:
                job.fatal = (f"The largest video ({fmt_size(meta.largest)}) does not fit on the server disk "
                             f"(free: {fmt_size(free)}). A plan with more disk space is needed for this file.")
                await safe_edit(job, final_text(job), final=True)
                return
        upq = asyncio.Queue(maxsize=BUFFER)
        uploaders = [asyncio.create_task(uploader(job, upq)) for _ in range(PARALLEL_UL)]
        job.mode = "whole folder (megadl)"
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


@app.on_message(filters.command("disk"))
async def cmd_disk(client, message):
    uid = message.from_user.id if message.from_user else 0
    if not is_allowed(uid):
        return await message.reply_text(NOT_ALLOWED.format(uid=uid))
    try:
        usage = shutil.disk_usage(WORK_DIR)
        text = (
            "💾 Disk of the work folder\n"
            f"Total » {fmt_size(usage.total)}\n"
            f"Used » {fmt_size(usage.used)}\n"
            f"Free » {fmt_size(usage.free)}\n"
            f"Used by the bot now » {fmt_size(dir_size(WORK_DIR))}\n"
            f"Bot limit » {fmt_size(DISK_LIMIT)} (DISK_LIMIT_GB)\n"
            "Note: the host may report more space than your Railway plan really allows."
        )
    except OSError as e:
        text = f"Could not read the disk status: {short(e)}"
    await message.reply_text(text)


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
    for tool in ("megadl", "ffmpeg", "ffprobe"):
        if not shutil.which(tool):
            log.error("Required tool not found in PATH: %s", tool)
    try:
        import mega_client  # noqa: F401
    except Exception as e:
        log.warning("mega-public-client is not installed - total sizes will show '?' (%s)", e)
    try:
        usage = shutil.disk_usage(WORK_DIR)
        log.info("Disk: total=%s free=%s | bot limit=%s, keep free >= %s",
                 fmt_size(usage.total), fmt_size(usage.free), fmt_size(DISK_LIMIT), fmt_size(MIN_FREE))
    except OSError:
        pass
    worker_task = asyncio.create_task(worker())
    await idle()
    worker_task.cancel()
    current = state["current"]
    if current:
        cancel_job(current)
    await app.stop()


loop.run_until_complete(main())
