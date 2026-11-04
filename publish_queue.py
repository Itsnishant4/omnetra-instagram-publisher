#!/usr/bin/env python3
"""
Omnetra Instagram Autonomous Cloud Queue Publisher
Executes via GitHub Actions to publish scheduled posts even when local machines are powered off.
"""

import os
import sys
import json
import time
import requests
from datetime import datetime, timezone, timedelta
from pathlib import Path

IST = timezone(timedelta(hours=5, minutes=30))

# Paths
BASE_DIR = Path(__file__).resolve().parent
QUEUE_DIR = BASE_DIR / "queue"
PUBLISHED_DIR = BASE_DIR / "published"

# Secrets from Environment
META_ACCESS_TOKEN = os.environ.get("META_ACCESS_TOKEN", "").strip()
IG_USER_ID = os.environ.get("IG_USER_ID", "").strip()
X02_API_KEY = os.environ.get("X02_API_KEY", "").strip()

def log(msg):
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{now_str}] {msg}", flush=True)

def parse_iso_time(iso_str):
    """Parses ISO timestamp into timezone-aware datetime (defaulting naive timestamps to IST +05:30)."""
    dt = datetime.fromisoformat(iso_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)
    return dt

def create_container(media_url, media_type, caption, cover_url=None, ig_user_id=None, access_token=None):
    """Creates Instagram Media Container via Meta Graph API."""
    target_ig = ig_user_id or IG_USER_ID
    target_token = access_token or META_ACCESS_TOKEN
    url = f"https://graph.facebook.com/v19.0/{target_ig}/media"
    payload = {
        "access_token": target_token,
        "caption": caption
    }

    mtype = media_type.upper()
    if mtype in ("REELS", "VIDEO"):
        payload["media_type"] = "REELS"
        payload["video_url"] = media_url
        payload["share_to_feed"] = "true"
        if cover_url:
            payload["cover_url"] = cover_url
    else:
        payload["image_url"] = media_url

    log(f"Creating {mtype} container for media: {media_url[:60]} on IG {target_ig}...")
    res = requests.post(url, data=payload)
    data = res.json()

    if "id" not in data:
        raise RuntimeError(f"Failed to create container: {json.dumps(data)}")

    return data["id"]

def wait_for_container(container_id, max_wait_seconds=600, access_token=None):
    """Polls container status until FINISHED or ERROR."""
    target_token = access_token or META_ACCESS_TOKEN
    url = f"https://graph.facebook.com/v19.0/{container_id}?fields=status_code,status&access_token={target_token}"
    start = time.time()

    while time.time() - start < max_wait_seconds:
        res = requests.get(url).json()
        status_code = res.get("status_code")
        log(f"Container {container_id} status: {status_code} ({res.get('status', '')})")

        if status_code == "FINISHED":
            return True
        elif status_code == "ERROR" or status_code == "EXPIRED":
            raise RuntimeError(f"Container failed with status: {status_code} - {res}")

        time.sleep(10)

    raise TimeoutError(f"Container {container_id} did not finish within {max_wait_seconds}s")

def publish_container(container_id, ig_user_id=None, access_token=None):
    """Publishes the finished container to Instagram."""
    target_ig = ig_user_id or IG_USER_ID
    target_token = access_token or META_ACCESS_TOKEN
    url = f"https://graph.facebook.com/v19.0/{target_ig}/media_publish"
    payload = {
        "creation_id": container_id,
        "access_token": target_token
    }

    log(f"Publishing container {container_id} to IG {target_ig}...")
    res = requests.post(url, data=payload)
    data = res.json()

    if "id" not in data:
        raise RuntimeError(f"Failed to publish container: {json.dumps(data)}")

    post_id = data["id"]

    # Fetch permalink
    info_url = f"https://graph.facebook.com/v19.0/{post_id}?fields=id,permalink,media_type,timestamp&access_token={target_token}"
    info = requests.get(info_url).json()
    permalink = info.get("permalink", f"https://www.instagram.com/p/{post_id}/")

    return post_id, permalink

def check_existing_published_media(caption, ig_user_id=None, access_token=None):
    """Checks whether this post has already been published to Instagram to prevent double posting."""
    target_ig = ig_user_id or IG_USER_ID
    target_token = access_token or META_ACCESS_TOKEN
    if not target_ig or not target_token:
        return None
    url = f"https://graph.facebook.com/v19.0/{target_ig}/media"
    try:
        res = requests.get(url, params={
            "access_token": target_token,
            "fields": "id,caption,media_type,timestamp,permalink",
            "limit": 15
        }, timeout=15)
        if res.status_code == 200:
            posts = res.json().get("data", [])
            clean_needle = (caption or "").strip()[:40].lower()
            if not clean_needle:
                return None
            for p in posts:
                p_cap = (p.get("caption") or "").strip().lower()
                if clean_needle in p_cap or (p_cap and p_cap[:40] in clean_needle):
                    return p
    except Exception as e:
        log(f"Notice: Failed to check existing IG media: {e}")
    return None

def cleanup_x02(filename):
    """Deletes temporary file from X02 storage."""
    if not filename or not X02_API_KEY:
        return
    url = f"https://up.x02.me/api/delete/{filename}?apiKey={X02_API_KEY}"
    try:
        res = requests.get(url).json()
        log(f"X02 cleanup for {filename}: {res.get('message', 'ok')}")
    except Exception as e:
        log(f"Notice: X02 cleanup failed for {filename}: {e}")

def process_queue():
    if not META_ACCESS_TOKEN or not IG_USER_ID:
        log("❌ ERROR: Missing META_ACCESS_TOKEN or IG_USER_ID environment variables!")
        sys.exit(1)

    now = datetime.now(timezone.utc)
    log(f"🔍 Checking queue at {now.isoformat()}...")

    queue_files = sorted(QUEUE_DIR.glob("*.json"))
    if not queue_files:
        log("📭 Queue is empty. Nothing to publish.")
        return 0

    log(f"Found {len(queue_files)} items in queue.")
    published_count = 0

    for qfile in queue_files:
        try:
            with open(qfile, "r", encoding="utf-8") as f:
                item = json.load(f)

            publish_at_str = item.get("publish_at")
            if not publish_at_str:
                log(f"⚠️ Skipping {qfile.name}: Missing 'publish_at' field.")
                continue

            scheduled_dt = parse_iso_time(publish_at_str)
            if scheduled_dt > now:
                log(f"⏳ {qfile.name} scheduled for {scheduled_dt.isoformat()} (future, skipping).")
                continue

            log(f"🚀 Processing {qfile.name} (scheduled for {scheduled_dt.isoformat()})...")

            media_url = item["media_url"]
            media_type = item.get("media_type", "IMAGE")
            caption = item.get("caption", "")
            cover_url = item.get("cover_url")
            x02_filename = item.get("x02_filename")
            item_ig = item.get("ig_user_id") or IG_USER_ID
            item_token = item.get("access_token") or META_ACCESS_TOKEN

            # 0. Deduplication Guard: Check if post already exists on Instagram
            existing = check_existing_published_media(caption, ig_user_id=item_ig, access_token=item_token)
            if existing:
                post_id = existing["id"]
                permalink = existing.get("permalink", f"https://www.instagram.com/p/{post_id}/")
                log(f"⚡ Duplicate Guard: Reel is ALREADY published on Instagram! ID: {post_id} | Link: {permalink}")
                if x02_filename:
                    cleanup_x02(x02_filename)
                item["post_id"] = post_id
                item["permalink"] = permalink
                item["published_at"] = existing.get("timestamp", datetime.now(timezone.utc).isoformat())
                item["status"] = "PUBLISHED"
                dest_file = PUBLISHED_DIR / qfile.name
                with open(dest_file, "w", encoding="utf-8") as f:
                    json.dump(item, f, indent=2)
                qfile.unlink()
                log(f"✅ Moved {qfile.name} -> published/{dest_file.name} (duplicate publishing prevented)")
                published_count += 1
                continue

            # 1. Create container
            container_id = create_container(media_url, media_type, caption, cover_url, ig_user_id=item_ig, access_token=item_token)

            # 2. Wait for container to be ready
            wait_for_container(container_id, access_token=item_token)

            # 3. Publish
            post_id, permalink = publish_container(container_id, ig_user_id=item_ig, access_token=item_token)
            log(f"🎉 Post is LIVE on Instagram! ID: {post_id} | Link: {permalink}")

            # 4. Cleanup X02 storage
            if x02_filename:
                cleanup_x02(x02_filename)

            # 5. Move from queue to published
            item["post_id"] = post_id
            item["permalink"] = permalink
            item["container_id"] = container_id
            item["published_at"] = datetime.now(timezone.utc).isoformat()
            item["status"] = "PUBLISHED"

            dest_file = PUBLISHED_DIR / qfile.name
            with open(dest_file, "w", encoding="utf-8") as f:
                json.dump(item, f, indent=2)

            qfile.unlink()
            log(f"✅ Moved {qfile.name} -> published/{dest_file.name}")
            published_count += 1

        except Exception as e:
            log(f"❌ Error processing {qfile.name}: {e}")

    log(f"Finished processing. Successfully published: {published_count} post(s).")
    return published_count

if __name__ == "__main__":
    process_queue()
