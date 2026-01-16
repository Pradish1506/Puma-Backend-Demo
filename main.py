import os
import json
from typing import Optional, Dict, Any

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Puma Email Inbox API")

# ----------------------
# DB Connection Helper
# ----------------------
def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )

# ----------------------
# Request Model
# ----------------------
class EmailInboxIn(BaseModel):
    message_id: Optional[str]
    internet_message_id: Optional[str]

    from_name: Optional[str]
    from_email: EmailStr
    to_email: EmailStr

    subject: Optional[str]
    body_preview: Optional[str]
    body_html: Optional[str]

    received_at: Optional[str]   # ISO timestamp string

    channel: Optional[str] = "email"
    processing_status: Optional[str] = "new"

    linked_case_id: Optional[int]

    raw_payload: Optional[Dict[str, Any]]

# ----------------------
# Health Check
# ----------------------
@app.get("/health")
def health():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        cur.close()
        conn.close()
        return {"status": "ok", "db": "connected"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ----------------------
# INSERT Email API
# ----------------------
@app.post("/email-inbox")
def insert_email(email: EmailInboxIn):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        query = """
            INSERT INTO "Puma_L1_AI".email_inbox (
                message_id,
                internet_message_id,
                from_name,
                from_email,
                to_email,
                subject,
                body_preview,
                body_html,
                received_at,
                channel,
                processing_status,
                linked_case_id,
                raw_payload
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING *;
        """

        cur.execute(
            query,
            (
                email.message_id,
                email.internet_message_id,
                email.from_name,
                email.from_email,
                email.to_email,
                email.subject,
                email.body_preview,
                email.body_html,
                email.received_at,
                email.channel,
                email.processing_status,
                email.linked_case_id,
                json.dumps(email.raw_payload) if email.raw_payload else None,
            ),
        )

        row = cur.fetchone()
        conn.commit()

        cur.close()
        conn.close()

        return {"status": "inserted", "data": row}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insert failed: {e}")

# ======================================================
#                    FETCH APIs
# ======================================================

# ----------------------
# Get all emails
# ----------------------
@app.get("/email-inbox")
def get_emails(limit: int = 20, offset: int = 0):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute(
            """
            SELECT *
            FROM "Puma_L1_AI".email_inbox
            ORDER BY received_at DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )

        rows = cur.fetchall()
        cur.close()
        conn.close()

        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ----------------------
# Get one email by ID
# ----------------------
@app.get("/email-inbox/{email_id}")
def get_email(email_id: int):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute(
            """
            SELECT *
            FROM "Puma_L1_AI".email_inbox
            WHERE email_id = %s
            """,
            (email_id,),
        )

        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            raise HTTPException(status_code=404, detail="Email not found")

        return row
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ----------------------
# Get all cases
# ----------------------
@app.get("/cases")
def get_cases(limit: int = 20, offset: int = 0):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute(
            """
            SELECT *
            FROM "Puma_L1_AI".cases
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )

        rows = cur.fetchall()
        cur.close()
        conn.close()

        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ----------------------
# Get AI decisions
# ----------------------
@app.get("/ai-decisions")
def get_ai_decisions(limit: int = 20, offset: int = 0):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute(
            """
            SELECT *
            FROM "Puma_L1_AI".ai_decisions
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )

        rows = cur.fetchall()
        cur.close()
        conn.close()

        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ----------------------
# Get Risk events
# ----------------------
@app.get("/risk-events")
def get_risk_events(limit: int = 20, offset: int = 0):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute(
            """
            SELECT *
            FROM "Puma_L1_AI".risk_events
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )

        rows = cur.fetchall()
        cur.close()
        conn.close()

        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
