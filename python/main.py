import requests
import pandas as pd
import json
import itertools

# API Configuration

url = "https://xrfvuwgjrlznxdhiqded.supabase.co/rest/v1/commdata"

headers = {
    "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InhyZnZ1d2dqcmx6bnhkaGlxZGVkIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NzQ5NzEyNSwiZXhwIjoyMDczMDczMTI1fQ.sFT6MsN7Phla94BIyHXRjiLZB8TLQof9U17Rv51XJaM",
    "apikey": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InhyZnZ1d2dqcmx6bnhkaGlxZGVkIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NzQ5NzEyNSwiZXhwIjoyMDczMDczMTI1fQ.sFT6MsN7Phla94BIyHXRjiLZB8TLQof9U17Rv51XJaM"
}

params = {
    "limit": 100
}

# Send Request and Validate Response

response = requests.get(url, headers=headers, params=params)
if response.status_code != 200:
    raise Exception(f"Error {response.status_code}: {response.text}")
data = response.json()
print(f"Retrieved {len(data)} rows")

# Transform and Normalize JSON Data

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
        "participants": raw.get("participants", []),
        "speakers": raw.get("speakers", []),
        "meeting_attendees": [m.get("email", "") for m in raw.get("meeting_attendees", [])],
    })

    rows.append(base)

df = pd.DataFrame(rows)

# Identify a column that contain a list
list_columns = [
    col for col in df.columns
    if df[col].apply(lambda x: isinstance(x, list)).any()
]

print("List columns detected:", list_columns)

# If there are list columns, expand them
if list_columns:
    expanded_rows = []
    for _, row in df.iterrows():
        # Convert to dict so we can modify safely
        row_dict = row.to_dict()
        # Extract only list columns
        lists_to_expand = {col: row_dict[col] for col in list_columns if isinstance(row_dict[col], list)}

        # If none of them are lists → just append once
        if not lists_to_expand:
            expanded_rows.append(row_dict)
            continue

        # Otherwise, generate cartesian product of all list elements
        # (so if a row has 2 participants and 3 speakers → 2×3=6 rows)
        keys, values = zip(*lists_to_expand.items())
        for combination in itertools.product(*values):
            new_row = row_dict.copy()
            for i, key in enumerate(keys):
                new_row[key] = combination[i]
            expanded_rows.append(new_row)

for col in list_columns:
    df[col] = df[col].apply(lambda x: list(set(x)) if isinstance(x, list) else x)

    df = pd.DataFrame(expanded_rows)

output_file = "data.xlsx"
df.to_excel(output_file, index=False)
print(f"Dataset saved to '{output_file}' — {df.shape[0]} rows × {df.shape[1]} columns")
