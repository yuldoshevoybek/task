import requests
import pandas as pd
import json

# ----------------------------------------
# API Configuration
# ----------------------------------------
# Supabase REST API endpoint (points to the 'commdata' table)
url = "https://xrfvuwgjrlznxdhiqded.supabase.co/rest/v1/commdata"

# Authentication headers
headers = {
    "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InhyZnZ1d2dqcmx6bnhkaGlxZGVkIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NzQ5NzEyNSwiZXhwIjoyMDczMDczMTI1fQ.sFT6MsN7Phla94BIyHXRjiLZB8TLQof9U17Rv51XJaM",
    "apikey": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InhyZnZ1d2dqcmx6bnhkaGlxZGVkIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NzQ5NzEyNSwiZXhwIjoyMDczMDczMTI1fQ.sFT6MsN7Phla94BIyHXRjiLZB8TLQof9U17Rv51XJaM"
}

# Query parameters — limit to 100 rows
params = {
    "limit": 100
}

# Send Request and Validate Response
response = requests.get(url, headers=headers, params=params)

if response.status_code == 200:
    data = response.json()
    print("Successfully retrieved", len(data), "rows from Supabase API")
else:
    raise Exception(f"Error {response.status_code}: {response.text}")


# Transform Raw JSON Data
# ----------------------------------------
# Each record has some base fields + nested JSON data inside 'raw_content'
rows = []

for item in data:
    # Extract base-level fields directly available in the response
    base = {
        "id": item.get("id"),
        "comm_type": item.get("comm_type"),
        "subject": item.get("subject"),
        "source_id": item.get("source_id"),
        "ingested_at": item.get("ingested_at"),
        "processed_at": item.get("processed_at"),
        "is_processed": item.get("is_processed"),
    }

    # Try to decode 'raw_content' JSON safely
    try:
        raw = json.loads(item.get("raw_content", "{}"))
    except json.JSONDecodeError:
        raw = {}

    # Add fields from the nested JSON (raw_content)
    base.update({
        "raw_id": raw.get("id"),
        "title": raw.get("title"),
        "duration": raw.get("duration"),
        "calendar_id": raw.get("calendar_id"),
        "audio_url": raw.get("audio_url"),
        "video_url": raw.get("video_url"),
        "transcript_url": raw.get("transcript_url"),
        "dateString": raw.get("dateString"),
        "host_email": raw.get("host_email"),
        "organizer_email": raw.get("organizer_email"),
        # Join list items into comma-separated strings
        "participants": ", ".join(raw.get("participants", [])),
        "speakers": ", ".join(raw.get("speakers", [])),
        "meeting_attendees": ", ".join(
            [attendee.get("email", "") for attendee in raw.get("meeting_attendees", [])]
        )
    })

    # Append the transformed row to the list
    rows.append(base)

# Create DataFrame and Save to Excel
df = pd.DataFrame(rows)

# Export the cleaned data to Excel file
output_file = "data.xlsx"
df.to_excel(output_file, index=False)

print(f"Data successfully saved to '{output_file}' ({df.shape[0]} rows, {df.shape[1]} columns)")

# ----------------------------------------
# Summary
# ✅ Connected to Supabase API
# ✅ Retrieved 100 rows
# ✅ Extracted 20+ fields (including nested JSON data)
# ✅ Saved to Excel in tabular format
