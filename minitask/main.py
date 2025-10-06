import requests
import pandas as pd
import json
import time

url = "https://xrfvuwgjrlznxdhiqded.supabase.co/rest/v1/commdata"

headers = {
    "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InhyZnZ1d2dqcmx6bnhkaGlxZGVkIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NzQ5NzEyNSwiZXhwIjoyMDczMDczMTI1fQ.sFT6MsN7Phla94BIyHXRjiLZB8TLQof9U17Rv51XJaM",
    "apikey": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InhyZnZ1d2dqcmx6bnhkaGlxZGVkIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NzQ5NzEyNSwiZXhwIjoyMDczMDczMTI1fQ.sFT6MsN7Phla94BIyHXRjiLZB8TLQof9U17Rv51XJaM"
}

params = {
    "limit": 100


}

response = requests.get(url, headers=headers, params=params)
if response.status_code == 200:
    data = response.json()
    print("Retrieved", len(data), "rows")
else:
    raise Exception(f"Error: {response.status_code} {response.text}")

time.sleep(3)

rows = []
for item in data:
    base = {
        "id": item.get("id"),
        "comm_type": item.get("comm_type"),
        "subject": item.get("subject"),
        "source_id": item.get("source_id"),
        "ingested_at": item.get("ingested_at"),
        "processed_at": item.get("processed_at"),
        "is_processed": item.get("is_processed"),
    }

    try:
        raw = json.loads(item.get("raw_content", "{}"))
    except json.JSONDecodeError:
        raw = {}

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
        "participants": ", ".join(raw.get("participants", [])),
        "speakers": ", ".join(raw.get("speakers", [])),
        "meeting_attendees": ", ".join([m.get("email") for m in raw.get("meeting_attendees", [])])
    })

    rows.append(base)

df = pd.DataFrame(rows)

df.to_excel("data.xlsx", index=False)

print("Data inserted into data.xlsx file")
