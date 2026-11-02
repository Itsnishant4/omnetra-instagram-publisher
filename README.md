# Omnetra Instagram Cloud Queue Publisher

Autonomous, 100% serverless scheduler for publishing Instagram Reels and Images directly to `@omnetra.infotech` via Meta Graph API.

Runs on **GitHub Actions** with **unlimited free runner minutes** in this public repository.

---

## 🔒 Security Architecture

* **Zero Credentials in Code**: Meta Access Tokens, Instagram Account IDs, and X02 API Keys are stored exclusively in **Encrypted GitHub Secrets** (`META_ACCESS_TOKEN`, `IG_USER_ID`, `X02_API_KEY`).
* **Zero Secrets in Queue Files**: Files in `queue/` only specify the temporary media URL on X02, caption, and the target publish timestamp.
* **Automatic Storage Cleanup**: Upon publication, the temporary image/video file is deleted from X02 storage.

---

## 📁 Repository Structure

* `queue/`: Pending posts scheduled for future dates/times (`.json`).
* `published/`: Archival log of published posts with live Instagram URLs and IDs (`.json`).
* `publish_queue.py`: Core publisher engine executed by GitHub Actions.
* `.github/workflows/publisher.yml`: GitHub Actions workflow running on a 15-minute cron schedule and `workflow_dispatch`.

---

## 🕒 How Scheduling Works

Posts can be scheduled **days, weeks, or months in advance**. Your local Mac does not need to be on:

1. A post JSON is pushed to `queue/`.
2. The author can turn off their computer completely.
3. Every 15 minutes, GitHub Actions wakes up, checks if any post's `publish_at` timestamp is due.
4. When due, Meta's API ingests the media, encodes the container, and publishes it live to Instagram.
5. The post is moved to `published/` with its permanent Instagram link.
