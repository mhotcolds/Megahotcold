# bot.py - MEGA folder -> Telegram leech bot (Pyrofork), built for Railway/Docker.
#
# How it works (streaming pipeline):
#   1. list public MEGA folder metadata through mega-public-client (no account login)
#   2. download several files in parallel (megatools megadl)          --\
#   3. every finished file goes into a small queue      >-- running at the same time
#   4. several uploaders send them to Telegram         --/
#   Files are deleted right after upload, so the disk never holds the whole folder.
#
# Required variables (Railway -> Variables): API_ID, API_HASH, BOT_TOKEN, ALLOWED_USERS
# Optional: PARALLEL_DL (2), PARALLEL_UL (1), DISK_LIMIT_GB (500; logical ceiling only), BUFFER (2), WORK_DIR (/tmp/megabot)
import asyncio
import base64
import struct

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

try:
    from mega_client import MegaClient
except ImportError as exc:
    MegaClient = None
    MEGA_CLIENT_IMPORT_ERROR = exc
else:
    MEGA_CLIENT_IMPORT_ERROR = None

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

# Railway Free is resource-constrained (0.5 GB RAM / 1 GB ephemeral disk).
# Keep defaults conservative; raise them only after measuring real throughput.
PARALLEL_DL = env_number("PARALLEL_DL", 2, 1, 8)
PARALLEL_UL = env_number("PARALLEL_UL", 1, 1, 6)
BUFFER = env_number("BUFFER", 2, 1, 20)
DISK_LIMIT = int(env_number("DISK_LIMIT_GB", 500.0, 0.2, 500.0, float) * 1024 ** 3)
DISK_SAFETY = 64 * 1024 ** 2  # keep 64 MiB free for the OS/container
WORK_DIR = Path(os.environ.get("WORK_DIR", "/tmp/megabot"))

MAX_SIZE = 2000 * 1024 * 1024      # Telegram limit per file
TARGET_PART = 1800 * 1024 * 1024   # size of each part when a file must be split
GUESS_SIZE = 1024 ** 3             # used for disk planning when MEGA does not tell the size
RETRIES = 3
EDIT_INTERVAL = 4.0

VIDEO_EXT = {".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm", ".m4v", ".ts", ".mpg", ".mpeg", ".3gp"}
FOLDER_RE = re.compile(r"https?://(?:www\.)?mega\.(?:nz|co\.nz)/(?:folder/[\w-]+#[\w-]+|#F![\w-]+![\w-]+)")
FILE_RE = re.compile(r"https?://(?:www\.)?mega\.(?:nz|co\.nz)/(?:file/[\w-]+#[\w-]+|#![\w-]+![\w-]+)")
# Folder metadata is read with mega-public-client. Actual file transfer uses
# megadl (megatools); megals is not used because it may require account credentials.

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

Remote = namedtuple("Remote", "path size handle url")
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


def make_bar(pct, width=18):
    """Compact Telegram-friendly progress bar."""
    if pct is None:
        return "▱" * width + "  --"
    pct = max(0.0, min(float(pct), 1.0))
    filled = min(width, int(pct * width + 0.5))
    return "▰" * filled + "▱" * (width - filled) + f"  {pct * 100:.1f}%"


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
    """Total on-disk bytes, used only for disk-pressure throttling."""
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


def video_dir_size(path):
    """Bytes of eligible video files only, matching the folder's MEGA total."""
    total = 0
    try:
        for p in path.rglob("*"):
            try:
                if p.is_file() and p.suffix.lower() in VIDEO_EXT and not p.name.startswith(".megatmp"):
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
        self.folder_seen_bytes = 0  # monotonic high-water mark for whole-folder fallback
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
        self.budget = None
        self.last_text = ""
        self.flood_until = 0.0


class Budget:
    """Queue files until both the logical budget and real free disk allow them."""

    def __init__(self, limit, disk_path):
        self.limit = limit
        self.disk_path = Path(disk_path)
        self.used = 0
        self.cond = asyncio.Condition()

    async def acquire(self, n):
        # A larger configured budget is only a scheduling ceiling; it cannot
        # create storage. Reject a file that can never fit on this filesystem.
        try:
            total = shutil.disk_usage(self.disk_path).total
        except OSError:
            total = 0
        max_file = max(0, total - DISK_SAFETY)
        if n > max_file:
            raise RuntimeError(
                f"This file needs about {fmt_size(n)} temporary disk space, but "
                f"the entire available filesystem is only {fmt_size(total)}. "
                "A larger DISK_LIMIT_GB value cannot increase Railway storage."
            )

        while True:
            async with self.cond:
                free = free_bytes(self.disk_path)
                logical_ok = self.used + n <= self.limit
                physical_ok = free >= n + DISK_SAFETY
                if logical_ok and physical_ok:
                    self.used += n
                    return
                # Wake on an upload cleanup, or periodically re-check real disk
                # space in case another process/container operation freed bytes.
                try:
                    await asyncio.wait_for(self.cond.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass

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
    """Estimated time until both download and upload stages finish."""
    if job.total <= 0:
        return None
    remaining = []
    rem_dl = max(job.total - job.dl_now, 0)
    rem_ul = max(job.total - job.ul_now, 0)
    if rem_dl > 0:
        if job.dl_speed < 1024:
            return None
        remaining.append(rem_dl / job.dl_speed)
    if rem_ul > 0:
        if job.ul_speed < 1024:
            return None
        remaining.append(rem_ul / job.ul_speed)
    return max(remaining, default=0.0)


def build_status(job):
    total_txt = fmt_size(job.total) if job.total else "در حال محاسبه"
    dl_pct = min(job.dl_now / job.total, 1.0) if job.total else None
    ul_pct = min(job.ul_now / job.total, 1.0) if job.total else None
    files_total = job.n_files if job.n_files else "?"
    overall = min(job.ul_now / job.total, 1.0) if job.total else None
    lines = [
        f"🚀 MEGA → TELEGRAM  |  #{job.id}",
        f"📁 حجم کل ویدئوها: {total_txt}   •   تعداد: {files_total}",
        "",
        f"⬇️ دانلود  {make_bar(dl_pct)}",
        f"   {fmt_size(job.dl_now)} / {total_txt}   •   {fmt_size(job.dl_speed)}/s",
        f"⬆️ آپلود   {make_bar(ul_pct)}",
        f"   {fmt_size(job.ul_now)} / {total_txt}   •   {fmt_size(job.ul_speed)}/s",
        "",
        f"📦 فایل‌ها: دانلود {job.dl_files}/{files_total}  |  ارسال {job.ul_files}/{files_total}",
    ]
    for name, st in list(job.active_up.items())[:2]:
        lines.append(f"   ↳ {short(name, 28)} · {st['cur'] * 100 / max(st['tot'], 1):.0f}%")
    if job.note:
        lines.append(job.note)
    lines += [
        f"⏱ زمان باقی‌مانده: {fmt_time(eta_seconds(job))}",
        f"⌛ زمان سپری‌شده: {fmt_time(time.time() - job.t0)}",
        f"⚙️ حالت: {job.mode}",
    ]
    if overall is not None:
        lines.insert(2, f"🎯 پیشرفت ارسال: {make_bar(overall, 14)}")
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
    """Create the sharpest Telegram-compatible JPEG thumbnail (<=320x320, <200 KB)."""
    thumb = path.with_name(path.stem + "__thumb.jpg")
    scale = (
        "scale=w='min(320,iw)':h='min(320,ih)':"
        "force_original_aspect_ratio=decrease:flags=lanczos"
    )
    first = min(max(duration * 0.12, 1.0), 30.0) if duration else 1.0

    for seek in (first, 0.0):
        for quality in (2, 4, 6, 8):  # lower q:v is higher JPEG quality
            try:
                rc, _, _ = await run_proc(
                    job, "ffmpeg", "-y", "-v", "error", "-ss", f"{seek:.1f}",
                    "-i", str(path), "-frames:v", "1", "-an", "-sn",
                    "-vf", scale, "-q:v", str(quality), "-f", "image2", str(thumb),
                    timeout=120,
                )
                if rc == 0 and thumb.exists():
                    size = thumb.stat().st_size
                    if 0 < size < 200 * 1024:
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


def _node_value(node, name, default=None):
    """Read a field from either a dataclass-like node or a mapping."""
    if isinstance(node, dict):
        return node.get(name, default)
    return getattr(node, name, default)


def _object_field_names(value):
    """Return declared dataclass/slot field names without exposing their values."""
    names = set()
    try:
        from dataclasses import fields, is_dataclass
        if is_dataclass(value):
            names.update(field.name for field in fields(value))
    except Exception:
        pass
    slots = getattr(type(value), "__slots__", ())
    if isinstance(slots, str):
        slots = (slots,)
    names.update(name for name in slots if isinstance(name, str))
    # Some SDK value objects keep their payload in private instance attributes.
    try:
        names.update(vars(value).keys())
    except (TypeError, AttributeError):
        pass
    return sorted(names)


def _key_shape(value):
    """Return safe diagnostics about a key without logging key material."""
    if value is None:
        return "None"
    if isinstance(value, str):
        return f"str(len={len(value)})"
    if isinstance(value, (bytes, bytearray, memoryview)):
        return f"{type(value).__name__}(len={len(value)})"
    if isinstance(value, (list, tuple)):
        sample_types = sorted({type(x).__name__ for x in value})
        return f"{type(value).__name__}(len={len(value)}, item_types={sample_types})"
    if isinstance(value, dict):
        return f"dict(keys={sorted(map(str, value.keys()))[:12]})"
    names = _object_field_names(value)
    return f"{type(value).__name__}(fields={names[:16]})"


def _node_key_to_b64(key, _depth=0):
    """Convert SDK FileKey/NodeInfo key representations to MEGA's 32-byte URL key.

    mega-public-client 0.1.1 exposes NodeInfo.key as a FileKey value object,
    not as bytes/string. Handle its dataclass/slot payload as well as common
    a32 (eight 32-bit words) and raw 32-byte representations.
    """
    if key is None or _depth > 5:
        return ""

    if isinstance(key, dict):
        for field in ("key", "file_key", "node_key", "value", "raw", "raw_words", "data", "bytes", "words", "a32", "material"):
            if field in key:
                result = _node_key_to_b64(key[field], _depth + 1)
                if result:
                    return result
        return ""

    if isinstance(key, str):
        value = key.strip()
        if re.fullmatch(r"[A-Za-z0-9_-]{43}={0,1}", value):
            try:
                raw = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
            except Exception:
                raw = b""
        else:
            try:
                raw = bytes.fromhex(value.removeprefix("0x"))
            except ValueError:
                raw = b""
    elif isinstance(key, (bytes, bytearray, memoryview)):
        raw = bytes(key)
        if len(raw) in (64, 43, 44):
            try:
                text_value = raw.decode("ascii").strip()
                if re.fullmatch(r"[0-9a-fA-F]{64}", text_value):
                    raw = bytes.fromhex(text_value)
                elif re.fullmatch(r"[A-Za-z0-9_-]{43}={0,1}", text_value):
                    raw = base64.urlsafe_b64decode(text_value + "=" * (-len(text_value) % 4))
            except (UnicodeDecodeError, ValueError):
                pass
    elif isinstance(key, (list, tuple)):
        try:
            values = [int(x) for x in key]
            if len(values) == 8 and all(0 <= x <= 0xFFFFFFFF for x in values):
                raw = struct.pack(">8I", *values)
            elif len(values) == 32 and all(0 <= x <= 255 for x in values):
                raw = bytes(values)
            else:
                raw = b""
        except (TypeError, ValueError, OverflowError):
            raw = b""
    else:
        raw = b""
        # Value objects sometimes provide an explicit byte conversion.
        for method_name in ("to_bytes", "as_bytes", "to_raw_bytes", "to_byte_array"):
            method = getattr(key, method_name, None)
            if callable(method):
                try:
                    candidate = method()
                    result = _node_key_to_b64(candidate, _depth + 1)
                    if result:
                        return result
                except Exception:
                    pass
        # FileKey is a typed SDK object; inspect only known payload fields.
        for field in (
            "key", "file_key", "node_key", "value", "raw", "data", "bytes",
            "raw_words", "words", "a32", "material", "_key", "_value", "_raw", "_data",
        ):
            try:
                nested = getattr(key, field, None)
            except Exception:
                nested = None
            if nested is not None and nested is not key and not callable(nested):
                result = _node_key_to_b64(nested, _depth + 1)
                if result:
                    return result
        # Last resort for iterable FileKey wrappers (never stringify/repr key data).
        try:
            values = list(key)
        except Exception:
            values = []
        if values:
            return _node_key_to_b64(values, _depth + 1)

    if len(raw) != 32:
        return ""
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _build_node_path(node, by_handle, root_handle, cache=None, visiting=None):
    """Build a stable relative path from MEGA's parent-handle relationships."""
    cache = cache if cache is not None else {}
    visiting = visiting if visiting is not None else set()
    handle = str(_node_value(node, "handle", "") or "")
    if handle in cache:
        return cache[handle]
    if handle in visiting:
        return str(_node_value(node, "name", "") or handle)
    visiting.add(handle)
    name = str(_node_value(node, "name", "") or handle or "unnamed")
    parent = _node_value(node, "parent", None)
    parent = str(parent) if parent is not None else ""
    if parent and parent != str(root_handle) and parent in by_handle:
        prefix = _build_node_path(by_handle[parent], by_handle, root_handle, cache, visiting)
        result = f"{prefix}/{name}"
    else:
        result = name
    visiting.discard(handle)
    cache[handle] = result
    return result


def _folder_nodes_to_remote(folder_info):
    """Convert mega-public-client FolderInfo nodes to Remote video entries."""
    nodes = list(_node_value(folder_info, "nodes", []) or [])
    root_handle = _node_value(folder_info, "handle", None)
    if root_handle is None:
        root_handle = _node_value(folder_info, "root_handle", "")
    by_handle = {
        str(_node_value(node, "handle", "")): node
        for node in nodes if _node_value(node, "handle", None) is not None
    }
    path_cache = {}
    result, seen = [], set()
    for node in nodes:
        is_file = _node_value(node, "is_file", None)
        if is_file is None:
            kind = _node_value(node, "kind", None)
            is_file = kind in (0, "file", "FILE")
        if not is_file:
            continue
        name = str(_node_value(node, "name", "") or "")
        path = _build_node_path(node, by_handle, root_handle, path_cache)
        if Path(name or path).suffix.lower() not in VIDEO_EXT:
            continue
        try:
            size = int(_node_value(node, "size", 0) or 0)
        except (TypeError, ValueError):
            size = 0
        handle = str(_node_value(node, "handle", "") or "")
        if size < 0 or not handle or handle in seen:
            continue
        seen.add(handle)
        raw_key = _node_value(node, "key", None)
        key = _node_key_to_b64(raw_key)
        if not key and len(result) < 3:
            # Never log key bytes/text: only type and length are safe diagnostics.
            log.warning(
                "MEGA node key unavailable for %s: key_shape=%s node_fields=%s",
                name, _key_shape(raw_key),
                sorted(list(vars(node).keys()))[:20] if hasattr(node, "__dict__") else type(node).__name__,
            )
        url = f"https://mega.nz/file/{handle}#{key}" if key else ""
        # Keep zero/unknown sizes in the manifest. The caller will refuse to
        # publish a misleading total instead of silently dropping these files.
        result.append(Remote(path, size, handle, url))
    return result


def _get_public_folder_info(link):
    """Synchronous SDK call, executed in a worker thread by list_remote."""
    if MegaClient is None:
        raise RuntimeError(
            "Missing dependency mega-public-client. Add mega-public-client==0.1.1 "
            "to requirements.txt and redeploy."
        ) from MEGA_CLIENT_IMPORT_ERROR
    with MegaClient() as client:
        return client.get_folder_info(link)


async def list_remote(job):
    """List all video files in a public MEGA folder without logging into an account."""
    try:
        folder_info = await asyncio.wait_for(
            asyncio.to_thread(_get_public_folder_info, job.link), timeout=180
        )
        items = _folder_nodes_to_remote(folder_info)
    except asyncio.TimeoutError as exc:
        raise RuntimeError("MEGA folder metadata request timed out") from exc
    except Exception as exc:
        log.exception("public MEGA folder metadata failed")
        raise RuntimeError(f"Could not read public MEGA folder metadata: {short(exc, 220)}") from exc

    unknown_links = sum(1 for item in items if not item.url)
    unknown_sizes = sum(1 for item in items if item.size <= 0)
    log.info(
        "Public MEGA folder metadata: nodes=%s videos=%d total_video_bytes=%d "
        "missing_file_keys=%d unknown_sizes=%d",
        len(_node_value(folder_info, "nodes", []) or []), len(items),
        sum(item.size for item in items), unknown_links, unknown_sizes,
    )
    if not items:
        log.warning("Public MEGA folder contains no supported video files")
    return items


def video_files(folder):
    return sorted(
        p for p in folder.rglob("*")
        if p.is_file() and p.suffix.lower() in VIDEO_EXT and not p.name.startswith(".megatmp")
    )


async def download_one(job, item, folder):
    """Download one public MEGA file with megatools; metadata still comes from SDK."""
    if not item.url:
        return None, "MEGA did not provide a usable per-file URL/key"

    last_err = ""
    for attempt in range(RETRIES):
        shutil.rmtree(folder, ignore_errors=True)
        folder.mkdir(parents=True, exist_ok=True)
        try:
            rc, out, err = await run_proc(
                job, "megadl", "--path", str(folder), item.url, timeout=None
            )
            files = video_files(folder)
            if rc == 0:
                if len(files) == 1 and files[0].stat().st_size > 0:
                    return files[0], ""
                # megadl can preserve a non-video extension or a remote filename.
                candidates = [p for p in folder.rglob("*") if p.is_file() and not p.name.startswith(".megatmp")]
                if len(candidates) == 1 and candidates[0].stat().st_size > 0:
                    return candidates[0], ""
                raise RuntimeError("megadl exited successfully but the downloaded file was not found")
            last_err = short(err.strip().splitlines()[-1] if err.strip() else f"megadl exited with code {rc}")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_err = short(str(exc))

        if is_quota(last_err):
            job.quota_hit = True
            return None, "MEGA transfer quota exceeded"
        if attempt < RETRIES - 1:
            await asyncio.sleep(2 * (attempt + 1))

    return None, last_err or "megadl download failed"


async def produce_files(job, items, tmp, upq):
    """Download files in parallel; cleanly release reservations on every failure/cancel path."""
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
            transferred = False
            path = None
            try:
                path, reason = await download_one(job, item, folder)
                if path is None:
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
                transferred = True  # uploader now owns the disk reservation and file
            except asyncio.CancelledError:
                raise
            except Exception as e:
                stats["fail"] += 1
                if not job.quota_hit:
                    job.failed.append((Path(item.path).name, short(str(e))))
                    if stats["ok"] == 0 and stats["fail"] >= min(3, len(items)):
                        fallback.set()
            finally:
                job.active_dirs.discard(folder)
                if not transferred:
                    shutil.rmtree(folder, ignore_errors=True)
                    await job.budget.release(reserve)

    tasks = [asyncio.create_task(one(i, it)) for i, it in enumerate(items)]
    # Keep the gather operation in a task so fallback can interrupt it.
    async def wait_all():
        await asyncio.gather(*tasks)
    everything = asyncio.create_task(wait_all())
    waiter = asyncio.create_task(fallback.wait())
    try:
        await asyncio.wait({everything, waiter}, return_when=asyncio.FIRST_COMPLETED)
        if fallback.is_set() and stats["ok"] == 0:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            return "failed"
        await everything
        return "ok"
    except asyncio.CancelledError:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    finally:
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)


async def produce_folder(job, tmp, upq):
    """Whole-folder downloads are intentionally disabled on Railway.

    A folder may be much larger than the ephemeral disk. Each video must be
    fetched independently with megadl and handed to the uploader immediately.
    """
    job.fatal = (
        "Per-file download failed. Whole-folder fallback is disabled because "
        "it can exceed Railway's temporary disk capacity. Check the megadl error in logs."
    )
    log.error("Whole-folder fallback skipped: per-file megadl is the only enabled download path")



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
    """Process one queued file outside the watched MEGA download tree.

    IMPORTANT: produce_folder() continuously scans folder_dest for video files.
    Keeping srcN / __tg temporary files beside the downloaded originals makes
    the scanner mistake our own processing files for new MEGA downloads, which
    causes duplicate uploads. Move each source into a private processing folder
    under WORK_DIR before remuxing/splitting it.
    """
    processing_dir = None
    safe, parts = None, []
    try:
        orig_name = qi.path.name
        processing_dir = Path(tempfile.mkdtemp(prefix=f"job_{job.id}_processing_", dir=WORK_DIR))
        safe = processing_dir / f"src{next(_file_ids)}{qi.path.suffix.lower()}"
        qi.path.replace(safe)

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
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
        if safe is not None:
            try:
                safe.unlink(missing_ok=True)
            except OSError:
                pass
        if processing_dir is not None:
            shutil.rmtree(processing_dir, ignore_errors=True)
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
        partial = 0
        for folder in list(job.active_dirs):
            try:
                partial += sum(p.stat().st_size for p in folder.iterdir() if p.is_file())
            except OSError:
                pass
        # In whole-folder fallback, scan only for byte accounting (not enqueueing).
        # Keep a high-water mark because completed files may be moved to upload processing.
        if job.folder_dest is not None:
            job.folder_seen_bytes = max(job.folder_seen_bytes, video_dir_size(job.folder_dest))
        dl = max(job.dl_done_bytes + partial, job.folder_seen_bytes)
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
        job.mode = "بررسی فایل‌ها و حجم پوشه"
        await safe_edit(job, build_status(job))
        items = [] if job.single else await list_remote(job)
        # Keep listing totals even when per-file export links are unavailable and
        # the downloader must use whole-folder fallback.
        job.n_files = len(items)
        unknown_sizes = sum(1 for i in items if i.size <= 0)
        job.total = sum(i.size for i in items if i.size > 0) if items and unknown_sizes == 0 else 0
        if items:
            log.info("Folder summary: videos=%d, total_bytes=%d, unknown_sizes=%d",
                     job.n_files, sum(max(0, i.size) for i in items), unknown_sizes)
        blocked = False
        if items and any(not i.url for i in items):
            missing = [i for i in items if not i.url]
            log.warning(
                "MEGA per-file key diagnostic: missing=%d/%d; first_missing=%s",
                len(missing), len(items),
                [(Path(i.path).name, i.handle) for i in missing[:3]],
            )
            # Do not attempt a whole-folder download on Railway: it needs the
            # entire folder on disk and commonly exceeds the ephemeral disk.
            # Fail before transferring anything; the safe-key diagnostics above
            # let the next iteration target the actual SDK value representation.
            job.fatal = (
                f"MEGA returned metadata for {len(items)} videos, but did not expose "
                f"usable per-file keys for {len(missing)} of them. No files were "
                "downloaded; whole-folder fallback is disabled to protect Railway disk."
            )
            log.error("Per-file download blocked: SDK key material unavailable for %d files", len(missing))
            blocked = True
        job.budget = Budget(DISK_LIMIT, WORK_DIR)
        upq = asyncio.Queue(maxsize=BUFFER)
        if not blocked:
            uploaders = [asyncio.create_task(uploader(job, upq)) for _ in range(PARALLEL_UL)]

        if blocked:
            result = "blocked"
        elif items:
            job.mode = f"تکی ×{PARALLEL_DL}"
            result = await produce_files(job, items, tmp, upq)
        else:
            result = "failed"
        if result == "failed" and items and job.ul_files == 0:
            job.fatal = (
                "دانلود فایل‌ها با megadl ناموفق بود. برای جلوگیری از دانلود کل پوشه "
                "و پر شدن دیسک Railway، مسیر جایگزین پوشه‌ای اجرا نشد. جزئیات خطا در لاگ موجود است."
            )

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
    for tool in ("ffmpeg", "ffprobe"):
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
