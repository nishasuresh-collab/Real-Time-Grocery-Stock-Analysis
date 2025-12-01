import os, time, random, shutil, multiprocessing as mp
from datetime import datetime
from src.constants import *
from src.helper import clean_dir, reset_log, write_log, timestamp_str

def simulate_feed(feed_id, src_folder):
    # one feed is one camera
    imgs = []
    for f in os.listdir(src_folder):
        if f.lower().endswith((".png", ".jpg")):
            imgs.append(f)

    if not imgs:
        print(feed_id, " no images found")
        return

    if SHUFFLE_FRAMES:
        random.shuffle(imgs)

    print(feed_id, " starting stream with ", len(imgs), " frames")
    count = 0

    for img in imgs:
        if count >= MAX_FRAMES:
            break

        time_stamp = timestamp_str()
        new_name = f"{feed_id}_{time_stamp}.png"
        shutil.copy(os.path.join(src_folder, img), os.path.join(OUTPUT_DIR, new_name))
        write_log(LOG_FILE, time_stamp, feed_id, img, new_name)

        count += 1
        delay = STREAM_INTERVAL + (random.uniform(-1, 1) if RANDOM_DELAY else 0)
        time.sleep(max(0.5, delay))

    print(feed_id, " finished sending", count, "frames")

def run_all_feeds(FEEDS):
    # running feeds in parallel
    if USE_MULTIPROCESS:
        processes = []
        for fid, folder in FEEDS.items():
            p = mp.Process(target=simulate_feed, args=(fid, folder))
            p.start()
            processes.append(p)
        for p in processes:
            p.join()
    else: # running feeds one by one
        for fid, folder in FEEDS.items():
            simulate_feed(fid, folder)

if __name__ == "__main__":
    print("starting feed simulator...")
    clean_dir(OUTPUT_DIR)
    reset_log(LOG_FILE)
    run_all_feeds()
    print("done streaming")
