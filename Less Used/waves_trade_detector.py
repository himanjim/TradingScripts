from __future__ import annotations

import requests
import re
import time
from urllib.parse import urlparse

# Dailymotion video IDs usually start with "x"
DM_ID_RE = re.compile(r'^x[0-9a-zA-Z]+$')

def extract_dailymotion_id(url: str) -> str | None:
    """
    Extracts video ID from:
    - http(s)://www.dailymotion.com/video/<id>_<slug>
    - http(s)://www.dailymotion.com/video/<id>
    - http(s)://www.dailymotion.com/embed/video/<id>
    - http(s)://dai.ly/<id>
    """
    url = url.strip()
    if not url:
        return None

    # Line might already be just an ID
    if DM_ID_RE.match(url):
        return url

    try:
        p = urlparse(url)
    except Exception:
        return None

    host = (p.netloc or "").lower()
    path = (p.path or "").strip("/")
    parts = [x for x in path.split("/") if x]

    # dai.ly/<id>
    if host.endswith("dai.ly") and parts:
        vid = parts[0].split("_", 1)[0]
        return vid if DM_ID_RE.match(vid) else None

    # dailymotion.com/video/<id>_<slug>
    if "dailymotion.com" in host:
        for i, seg in enumerate(parts[:-1]):
            if seg == "video":
                vid = parts[i + 1].split("_", 1)[0]
                return vid if DM_ID_RE.match(vid) else None

    return None


def dailymotion_video_exists(video_id: str, timeout: int = 15) -> bool:
    """
    Checks video existence using Dailymotion API.
    Returns True only if the video exists.
    """
    api_url = f"https://api.dailymotion.com/video/{video_id}?fields=id"
    try:
        r = requests.get(api_url, timeout=timeout)
        if r.status_code != 200:
            return False

        data = r.json()
        if isinstance(data, dict) and data.get("id") == video_id:
            return True

        return False

    except requests.RequestException:
        return False


def process_dailymotion_links(input_file: str, output_file: str, delay=0.05):
    seen_ids = set()
    valid_ids = []

    with open(input_file, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            video_id = extract_dailymotion_id(line)
            if not video_id:
                continue

            if video_id in seen_ids:
                continue
            seen_ids.add(video_id)

            if dailymotion_video_exists(video_id):
                valid_ids.append(video_id)

            time.sleep(delay)  # be polite to API

    with open(output_file, "w", encoding="utf-8") as out:
        for vid in valid_ids:
            out.write(f"https://www.dailymotion.com/video/{vid}\n")

    print("===================================")
    print(f"Total unique IDs checked : {len(seen_ids)}")
    print(f"Valid existing videos   : {len(valid_ids)}")
    print(f"Output saved to         : {output_file}")
    print("===================================")


if __name__ == "__main__":
    input_file_path = r"C:\Users\himan\Downloads\daily_motion_links.txt"
    output_file_path = r"C:\Users\himan\Downloads\dailymotion_links_valid.txt"

    process_dailymotion_links(input_file_path, output_file_path)
