# webhook_handler.py - Webhook endpoint for Pipedream
from flask import Flask, request, jsonify
import requests
import json
import os
from datetime import datetime

app = Flask(__name__)

# Your AstraDB credentials
ASTRA_DB_API_ENDPOINT = "https://4c83898c-ce8b-4e6d-b64b-65a1c883cdef-us-east1.apps.astra.datastax.com/api/rest/v2"
ASTRA_DB_TOKEN = "AstraCS:MEkOZlYiCGUgbiQrjHcsCDfq:4bab7b4cd28a1919937fa5594d7b59411598791fbf7d1d1f317db5e60f92d185"
ASTRA_DB_NAMESPACE = "demo"

# Webhook secret for security
WEBHOOK_SECRET = "your-secure-webhook-secret-key"

# AstraDB headers
headers = {
    "X-Cassandra-Token": ASTRA_DB_TOKEN,
    "Content-Type": "application/json"
}

base_url = f"{ASTRA_DB_API_ENDPOINT}/namespaces/{ASTRA_DB_NAMESPACE}"

@app.route('/webhook/gdrive', methods=['POST'])
def handle_gdrive_webhook():
    """Handle incoming Google Drive data from Pipedream"""
    
    try:
        # Verify webhook secret
        auth_header = request.headers.get('Authorization', '')
        if not auth_header.startswith('Bearer '):
            return jsonify({"error": "Missing Authorization header"}), 401
        
        webhook_secret = auth_header.replace('Bearer ', '')
        if webhook_secret != WEBHOOK_SECRET:
            return jsonify({"error": "Invalid webhook secret"}), 401
        
        # Get the data from Pipedream
        gdrive_data = request.json
        
        if not gdrive_data:
            return jsonify({"error": "No data provided"}), 400
        
        # Process the Google Drive document
        success, message = store_gdrive_document(gdrive_data)
        
        return jsonify({
            "success": success,
            "message": message,
            "timestamp": datetime.now().isoformat(),
            "document_id": f"gdrive_{gdrive_data.get('fileId', 'unknown')}"
        })
        
    except Exception as e:
        print(f"Webhook error: {e}")
        return jsonify({"error": str(e)}), 500

def store_gdrive_document(gdrive_data):
    """Store Google Drive document in AstraDB"""
    try:
        # Prepare document for AstraDB
        document = {
            "document_id": f"gdrive_{gdrive_data['fileId']}",
            "company_id": gdrive_data["companyId"],
            "filename": gdrive_data["fileName"],
            "content": gdrive_data.get("content", "")[:2000],
            "full_content": gdrive_data.get("content", "")[:10000],
            "source": "google_drive",
            "file_type": gdrive_data.get("mimeType", "unknown"),
            "file_size": gdrive_data.get("size", 0),
            "uploaded_by": "Google Drive Sync",
            "user_role": "System",
            "created_at": datetime.now().isoformat(),
            "gdrive_link": gdrive_data.get("webViewLink", ""),
            "synced_at": gdrive_data.get("syncedAt", datetime.now().isoformat()),
            "embedding": "[]"  # Empty embedding for now
        }
        
        # Insert into AstraDB
        response = requests.post(
            f"{base_url}/tables/documents/rows",
            headers=headers,
            json=document
        )
        
        if response.status_code in [200, 201]:
            return True, "Google Drive document synced successfully!"
        else:
            print(f"AstraDB insert failed: {response.status_code} - {response.text}")
            return False, f"Database insert failed: {response.status_code}"
        
    except Exception as e:
        print(f"Store document error: {e}")
        return False, f"Document storage failed: {str(e)}"

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "astra_endpoint": ASTRA_DB_API_ENDPOINT,
        "namespace": ASTRA_DB_NAMESPACE
    })

@app.route('/', methods=['GET'])
def home():
    """Home endpoint"""
    return jsonify({
        "service": "Multi-Tenant Document Management Webhook",
        "status": "active",
        "endpoints": {
            "webhook": "/webhook/gdrive",
            "health": "/health"
        }
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
