import os
import json
from datetime import timedelta, datetime, timezone
from flask import Flask, request, jsonify
from google.cloud import storage, firestore
import firebase_admin
from firebase_admin import auth, credentials

app = Flask(__name__)

# Configuration via environment variables
BUCKET = os.environ.get("BUCKET_NAME", "ingest-pipeline")
UPLOAD_PREFIX = os.environ.get("UPLOAD_PREFIX", "incoming/")
SIGNED_URL_EXP_MIN = int(os.environ.get("SIGNED_URL_EXP_MIN", "15"))
PROJECT = os.environ.get("PROJECT", None)

# Initialize Firebase Admin SDK (uses Application Default Credentials on Cloud Run)
if not firebase_admin._apps:
    firebase_admin.initialize_app()

# Clients
firestore_client = firestore.Client(project=PROJECT)
storage_client = storage.Client(project=PROJECT)

def verify_id_token_from_header():
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return None
    id_token = auth_header.split(" ", 1)[1].strip()
    try:
        decoded = auth.verify_id_token(id_token)
        return decoded  # contains uid, email, etc.
    except Exception as e:
        app.logger.warning("Token verification failed: %s", e)
        return None

def is_user_approved(uid):
    try:
        doc_ref = firestore_client.collection("approvals").document(uid)
        doc = doc_ref.get()
        if not doc.exists:
            return False
        data = doc.to_dict()
        return bool(data.get("approved", False))
    except Exception as e:
        app.logger.exception("Firestore check failed: %s", e)
        return False

def record_upload_request(uid, email, object_name, expires_at_iso):
    try:
        col = firestore_client.collection("upload_requests")
        doc = {
            "uid": uid,
            "email": email,
            "object_name": object_name,
            "requested_at": firestore.SERVER_TIMESTAMP,
            "expires_at": expires_at_iso,
            "status": "issued"
        }
        col.add(doc)
    except Exception as e:
        app.logger.exception("Failed to record upload request: %s", e)

@app.route("/get-signed-url", methods=["POST"])
def get_signed_url():
    # Verify identity
    decoded = verify_id_token_from_header()
    if not decoded:
        return jsonify({"error": "unauthenticated"}), 401
    uid = decoded.get("uid")
    email = decoded.get("email")

    # Check approval
    if not is_user_approved(uid):
        return jsonify({"error": "user_not_approved"}), 403

    body = request.get_json(force=True)
    filename = body.get("filename")
    content_type = body.get("content_type", "application/octet-stream")
    if not filename:
        return jsonify({"error": "missing filename"}), 400

    # Sanitize filename
    safe_name = filename.replace("..", "_").replace("/", "_")
    object_name = f"{UPLOAD_PREFIX}{safe_name}"

    # Generate signed URL (V4 PUT)
    try:
        bucket = storage_client.bucket(BUCKET)
        blob = bucket.blob(object_name)
        expiration = timedelta(minutes=SIGNED_URL_EXP_MIN)
        url = blob.generate_signed_url(
            version="v4",
            expiration=expiration,
            method="PUT",
            content_type=content_type,
        )
    except Exception as e:
        app.logger.exception("Signed URL generation failed: %s", e)
        return jsonify({"error": "signed_url_failed", "detail": str(e)}), 500

    # Record request in Firestore for audit and linking
    expires_at = (datetime.now(timezone.utc) + timedelta(minutes=SIGNED_URL_EXP_MIN)).isoformat()
    record_upload_request(uid, email, object_name, expires_at)

    return jsonify({"url": url, "object": object_name, "expires_at": expires_at}), 200

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
