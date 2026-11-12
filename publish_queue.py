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
META_ACCESS_TOKEN_OMNETRA = os.environ.get("META_ACCESS_TOKEN_OMNETRA", "").strip()
IG_USER_ID_OMNETRA = os.environ.get("IG_USER_ID_OMNETRA", "").strip()
META_ACCESS_TOKEN_VIDLINKS = os.environ.get("META_ACCESS_TOKEN_VIDLINKS", "").strip()
IG_USER_ID_VIDLINKS = os.environ.get("IG_USER_ID_VIDLINKS", "").strip()
X02_API_KEY = os.environ.get("X02_API_KEY", "").strip()
CRONJOB_API_KEY = os.environ.get("CRONJOB_API_KEY", "").strip()
FLIQ_API_KEY = os.environ.get("FLIQ_API_KEY", "").strip()


def resolve_account_credentials(item):
    """Resolves (ig_user_id, access_token) securely from environment secrets or legacy queue fields."""
    target_acc = str(item.get("target_account") or "").lower()
    item_ig = str(item.get("ig_user_id") or "").strip()
    caption = str(item.get("caption") or "").lower()
    x02_fn = str(item.get("x02_filename") or "").lower()

    is_vidlinks = (
        "vidlinks" in target_acc
        or (IG_USER_ID_VIDLINKS and item_ig == IG_USER_ID_VIDLINKS)
        or "vidlinks" in x02_fn
        or "vidlinks" in caption
    )
    if is_vidlinks:
        resolved_ig = item_ig or IG_USER_ID_VIDLINKS or IG_USER_ID
        resolved_tok = META_ACCESS_TOKEN_VIDLINKS or item.get("access_token") or META_ACCESS_TOKEN
        return resolved_ig, resolved_tok

    resolved_ig = item_ig or IG_USER_ID_OMNETRA or IG_USER_ID
    resolved_tok = META_ACCESS_TOKEN_OMNETRA or item.get("access_token") or META_ACCESS_TOKEN
    return resolved_ig, resolved_tok

def delete_cronjob(job_id):
    if not job_id or not CRONJOB_API_KEY:
        return
    try:
        r = requests.delete(f"https://api.cron-job.org/jobs/{job_id}", headers={"Authorization": f"Bearer {CRONJOB_API_KEY}"}, timeout=10)
        log(f"🗑️ Deleted one-shot cron-job #{job_id} after publish (status {r.status_code})")
    except Exception as e:
        log(f"Notice: Failed to delete cron-job #{job_id}: {e}")

def delete_fliq_job(job_id):
    if not job_id or not FLIQ_API_KEY:
        return
    try:
        r = requests.delete(f"https://api.fliq.sh/jobs/{job_id}", headers={"Authorization": f"Bearer {FLIQ_API_KEY}"}, timeout=10)
        log(f"🗑️ Deleted fliq.sh job {job_id} after publish (status {r.status_code})")
    except Exception as e:
        log(f"Notice: Failed to delete fliq job {job_id}: {e}")

def log(msg):
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{now_str}] {msg}", flush=True)

def parse_iso_time(iso_str):
    """Parses ISO timestamp into timezone-aware datetime (defaulting naive timestamps to IST +05:30)."""
    dt = datetime.fromisoformat(iso_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)
    return dt

def _http_json_with_retry(method, url, *, data=None, timeout=60, max_attempts=3):
    """Performs HTTP request with timeout, exponential backoff retries, and safe JSON parsing."""
    last_err = None
    for attempt in range(max_attempts):
        try:
            res = requests.request(method, url, data=data, timeout=timeout)
            try:
                body = res.json()
            except ValueError:
                body = {"raw_text": res.text[:400], "status_code": res.status_code}
            if res.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts - 1:
                wait_s = 2 ** (attempt + 1)
                log(f"⏳ Transient HTTP {res.status_code} from {url.split('?')[0]}; retrying in {wait_s}s...")
                time.sleep(wait_s)
                continue
            if res.status_code >= 400 and "error" not in body and "id" not in body:
                raise RuntimeError(f"HTTP {res.status_code}: {body}")
            return body
        except requests.RequestException as req_err:
            last_err = req_err
            if attempt < max_attempts - 1:
                wait_s = 2 ** (attempt + 1)
                log(f"⏳ Network error ({req_err}); retrying in {wait_s}s...")
                time.sleep(wait_s)
                continue
            raise RuntimeError(f"Request failed after {max_attempts} attempts: {last_err}") from req_err
    raise RuntimeError(f"Request failed: {last_err}")

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
    data = _http_json_with_retry("POST", url, data=payload, timeout=60)

    if "id" not in data:
        raise RuntimeError(f"Failed to create container: {json.dumps(data)}")

    return data["id"]

def wait_for_container(container_id, max_wait_seconds=600, access_token=None):
    """Polls container status until FINISHED or ERROR."""
    target_token = access_token or META_ACCESS_TOKEN
    url = f"https://graph.facebook.com/v19.0/{container_id}?fields=status_code,status&access_token={target_token}"
    start = time.time()

    while time.time() - start < max_wait_seconds:
        res = _http_json_with_retry("GET", url, timeout=30)
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
    data = _http_json_with_retry("POST", url, data=payload, timeout=60)

    if "id" not in data:
        raise RuntimeError(f"Failed to publish container: {json.dumps(data)}")

    post_id = data["id"]

    # Fetch permalink cleanly — never fail publish once post_id exists (prevents duplicate posting!)
    permalink = f"https://www.instagram.com/p/{post_id}/"
    try:
        info_url = f"https://graph.facebook.com/v19.0/{post_id}?fields=id,permalink,media_type,timestamp&access_token={target_token}"
        info = _http_json_with_retry("GET", info_url, timeout=25, max_attempts=2)
        permalink = info.get("permalink") or permalink
    except Exception as perm_err:
        log(f"Notice: Could not fetch permalink for published post {post_id} ({perm_err}); using fallback URL.")

    return post_id, permalink

def cleanup_x02(filename):
    """Deletes temporary file from X02 storage."""
    if not filename or not X02_API_KEY:
        return
    url = f"https://up.x02.me/api/delete/{filename}?apiKey={X02_API_KEY}"
    try:
        res = _http_json_with_retry("GET", url, timeout=15, max_attempts=2)
        log(f"X02 cleanup for {filename}: {res.get('message', 'ok')}")
    except Exception as e:
        log(f"Notice: X02 cleanup failed for {filename}: {e}")

def process_queue(target_file_name=None):
    now = datetime.now(timezone.utc)
    log(f"🔍 Checking queue at {now.isoformat()}...")

    # Recover any stale .processing files older than 15 minutes
    for proc_f in QUEUE_DIR.glob("*.processing"):
        try:
            if (now.timestamp() - proc_f.stat().st_mtime) > 900:
                recovered_name = proc_f.with_name(f"{proc_f.stem}.json")
                proc_f.rename(recovered_name)
                log(f"♻️ Recovered stale processing file: {proc_f.name} -> {recovered_name.name}")
        except Exception:
            pass

    if target_file_name:
        t_clean = Path(target_file_name).name
        target_f = QUEUE_DIR / t_clean
        queue_files = [target_f] if target_f.exists() else []
    else:
        queue_files = sorted(QUEUE_DIR.glob("*.json"))

    if not queue_files:
        log("📭 Queue is empty. Nothing to publish.")
        return 0

    log(f"Found {len(queue_files)} items in queue.")
    published_count = 0

    for qfile in queue_files:
        processing_file = None
        try:
            with open(qfile, "r", encoding="utf-8") as f:
                item = json.load(f)

            publish_at_str = item.get("publish_at")
            if not publish_at_str:
                log(f"⚠️ Skipping {qfile.name}: Missing 'publish_at' field.")
                continue

            scheduled_dt = parse_iso_time(publish_at_str)
            # If target_file_name was NOT explicitly given, check schedule time
            if not target_file_name and scheduled_dt > now:
                log(f"⏳ {qfile.name} scheduled for {scheduled_dt.isoformat()} (future, skipping).")
                continue

            log(f"🚀 Processing {qfile.name} (scheduled for {scheduled_dt.isoformat()})...")

            item_ig, item_token = resolve_account_credentials(item)
            if not item_ig or not item_token:
                raise RuntimeError(f"Missing IG credentials for target_account={item.get('target_account')}")
            caption = item.get("caption", "")
            media_url = item["media_url"]
            media_type = item.get("media_type", "IMAGE")
            cover_url = item.get("cover_url")
            x02_filename = item.get("x02_filename")
            cover_x02_filename = item.get("cover_x02_filename")

            if item.get("status") == "FAILED" and not target_file_name:
                log(f"⚠️ Skipping permanently failed {qfile.name} (retry_count={item.get('retry_count')}). Trigger manually or reschedule.")
                continue

            # 1. ATOMIC CLAIM: Rename .json to .processing so no concurrent runner can EVER pick it up!
            processing_file = qfile.with_name(f"{qfile.stem}.processing")
            try:
                qfile.rename(processing_file)
            except Exception as claim_err:
                log(f"🔒 {qfile.name} was already claimed by another runner ({claim_err}). Skipping.")
                continue

            # 2. Create container
            container_id = create_container(media_url, media_type, caption, cover_url, ig_user_id=item_ig, access_token=item_token)

            # 3. Wait for container to be ready
            wait_for_container(container_id, access_token=item_token)

            # 4. Publish
            post_id, permalink = publish_container(container_id, ig_user_id=item_ig, access_token=item_token)
            log(f"🎉 Post is LIVE on Instagram! ID: {post_id} | Link: {permalink}")

            # 5. Record published item FIRST (guarantees no duplicate IG post even if cleanup hits error)
            item.pop("access_token", None)
            item["post_id"] = post_id
            item["permalink"] = permalink
            item["container_id"] = container_id
            item["published_at"] = datetime.now(timezone.utc).isoformat()
            item["status"] = "PUBLISHED"

            dest_file = PUBLISHED_DIR / qfile.name
            with open(dest_file, "w", encoding="utf-8") as f:
                json.dump(item, f, indent=2)

            processing_file.unlink(missing_ok=True)
            log(f"✅ Successfully published {qfile.name} -> published/{dest_file.name}")
            published_count += 1

            # 6. Post-publish cleanups (non-fatal)
            if x02_filename:
                cleanup_x02(x02_filename)
            if cover_x02_filename:
                cleanup_x02(cover_x02_filename)
            if item.get("cron_job_id"):
                delete_cronjob(item["cron_job_id"])
            if item.get("fliq_job_id"):
                delete_fliq_job(item["fliq_job_id"])

        except Exception as e:
            try:
                if processing_file and processing_file.exists():
                    with open(processing_file, "r", encoding="utf-8") as f:
                        failed_item = json.load(f)
                    failed_item.pop("access_token", None)
                    if failed_item.get("cron_job_id"):
                        delete_cronjob(failed_item.pop("cron_job_id"))
                    if failed_item.get("fliq_job_id"):
                        delete_fliq_job(failed_item.pop("fliq_job_id"))
                    retries = int(failed_item.get("retry_count") or 0) + 1
                    failed_item["retry_count"] = retries
                    failed_item["last_error"] = str(e)
                    failed_item["last_failed_at"] = datetime.now(timezone.utc).isoformat()
                    failed_item["status"] = "FAILED" if retries >= 3 else "QUEUED"
                    with open(processing_file, "w", encoding="utf-8") as f:
                        json.dump(failed_item, f, indent=2)
                    # Restore .json file so backup/manual retry can recover
                    processing_file.rename(qfile)
            except Exception:
                if processing_file and processing_file.exists():
                    try:
                        processing_file.rename(qfile)
                    except Exception:
                        pass
            log(f"❌ Error processing {qfile.name}: {e}")

    log(f"Finished processing. Successfully published: {published_count} post(s).")
    return published_count

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Omnetra Instagram Queue Publisher")
    parser.add_argument("--target", help="Specific queue file to publish (optional)")
    args = parser.parse_args()
    process_queue(target_file_name=args.target)
