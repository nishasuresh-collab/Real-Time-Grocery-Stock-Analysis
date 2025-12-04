import os
import time
import random
import threading
from datetime import datetime

# =====================================================================
# CONFIGURATION
# =====================================================================

STREAM_INTERVAL = 0.5               # seconds between frames per feed
MAX_FRAMES = 40                     # frames to send per feed
RANDOM_DELAY = True                 # jitter for realism
SHUFFLE_FRAMES = True               # shuffle frame order
USE_MULTIPROCESS = True             # run all feeds in parallel

OUTPUT_DIR = "stream_input"
LOG_FILE = "feed_log.csv"

# =====================================================================
# CHOOSE 10 DISTINCT RANDOM FEEDS (NO REPEAT)
# =====================================================================

FEEDS = {
    "feed_1":  "inSitu/3/video",
    "feed_2":  "inSitu/12/video",
    "feed_3":  "inSitu/27/video",
    "feed_4":  "inSitu/41/video",
    "feed_5":  "inSitu/56/video",
    "feed_6":  "inSitu/63/video",
    "feed_7":  "inSitu/78/video",
    "feed_8":  "inSitu/95/video",
    "feed_9":  "inSitu/107/video",
    "feed_10": "inSitu/118/video",
}
