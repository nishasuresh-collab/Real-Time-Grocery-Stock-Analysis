import os, csv, shutil
from datetime import datetime

def clean_dir(directory):
    # delete everything inside a folder but keep the folder itself
    if not os.path.exists(directory):
        os.makedirs(directory)
        print("created folder:", directory)
        return

    removed = 0
    for fileDir in os.listdir(directory):
        path = os.path.join(directory, fileDir)
        try:
            if os.path.isfile(path):
                os.remove(path)
                removed += 1
        except Exception as e:
            print("could not remove", path, ": ", e)
    print(f"cleaned {removed} files from {directory}")

def reset_log(log_file):
    # creat csv log
    if os.path.exists(log_file):
        os.remove(log_file)
        print("removed old log file")

    with open(log_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "feed_id", "original_fileName", "new_fileName"])
    print("created new log: ", log_file)

def write_log(log_file, time_stamp, feed_id, old_name, new_name):
    # append data to csv
    with open(log_file, "a", newline="") as f:
        csv.writer(f).writerow([time_stamp, feed_id, old_name, new_name])
    print(f"{feed_id} | {old_name} : {new_name}")

def timestamp_str():
    # returns current timestamp for filename
    return datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
