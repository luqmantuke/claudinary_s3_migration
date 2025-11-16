import os
import json
from typing import Dict, Any

import firebase_admin
from firebase_admin import credentials, firestore


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FIREBASE_CREDENTIALS_PATH = os.path.join(BASE_DIR, "firebase_admin.json")
URL_MAPPING_PATH = os.path.join(BASE_DIR, "url_mapping.json")


def load_url_mapping() -> Dict[str, str]:
    """Load Cloudinary → Linode URL mapping from JSON file."""
    if not os.path.exists(URL_MAPPING_PATH):
        raise FileNotFoundError(f"Mapping file not found at {URL_MAPPING_PATH}")

    with open(URL_MAPPING_PATH, "r") as f:
        mapping = json.load(f)

    if not isinstance(mapping, dict):
        raise ValueError("url_mapping.json must be a JSON object of {old_url: new_url}")

    return mapping


def init_firestore():
    """Initialize Firebase Admin SDK and return Firestore client."""
    if not os.path.exists(FIREBASE_CREDENTIALS_PATH):
        raise FileNotFoundError(f"Firebase credentials file not found at {FIREBASE_CREDENTIALS_PATH}")

    if not firebase_admin._apps:
        cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
        firebase_admin.initialize_app(cred)

    return firestore.client()


def replace_urls_in_string(text: str, mapping: Dict[str, str]) -> str:
    """Replace all Cloudinary URLs in a string using the mapping dict."""
    if not text:
        return text

    updated = text
    for old_url, new_url in mapping.items():
        if old_url in updated:
            updated = updated.replace(old_url, new_url)
    return updated


def update_news_documents(db, mapping: Dict[str, str]) -> None:
    """
    Go through the 'news' collection and update:
      - 'image' field (string)
      - 'content' field (HTML string with <img src="..."> tags)
    replacing Cloudinary URLs with Linode URLs.
    """
    collection_ref = db.collection("news")
    docs = collection_ref.stream()

    total_docs = 0
    updated_docs = 0

    print("Starting Firestore migration for 'news' collection...")

    for doc in docs:
        total_docs += 1
        data: Dict[str, Any] = doc.to_dict() or {}

        original_image = data.get("image")
        original_content = data.get("content")

        new_image = original_image
        new_content = original_content

        # Replace in simple image field
        if isinstance(original_image, str):
            new_image = replace_urls_in_string(original_image, mapping)

        # Replace in HTML content string
        if isinstance(original_content, str):
            new_content = replace_urls_in_string(original_content, mapping)

        # Only write back if something actually changed
        update_payload: Dict[str, Any] = {}
        if new_image != original_image:
            update_payload["image"] = new_image
        if new_content != original_content:
            update_payload["content"] = new_content

        if update_payload:
            print(f"- Updating doc '{doc.id}' with fields: {list(update_payload.keys())}")
            doc.reference.update(update_payload)
            updated_docs += 1

    print(f"\nFinished updating 'news' collection.")
    print(f"  Total docs scanned : {total_docs}")
    print(f"  Docs updated       : {updated_docs}")


def main():
    print("=" * 60)
    print("Cloudinary → Linode URL Migration for Firestore")
    print("=" * 60)

    # Load mapping
    mapping = load_url_mapping()
    print(f"Loaded {len(mapping)} URL mappings from {URL_MAPPING_PATH}")

    # Init Firestore
    db = init_firestore()
    print("Connected to Firestore.")

    # Update 'news' collection
    update_news_documents(db, mapping)

    print("\nMigration complete.")


if __name__ == "__main__":
    main()


