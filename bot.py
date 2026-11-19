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
