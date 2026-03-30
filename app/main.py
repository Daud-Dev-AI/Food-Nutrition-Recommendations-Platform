from contextlib import asynccontextmanager
from datetime import date
from typing import Optional
import json
import threading

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text
from kafka import KafkaProducer

from app.config import DATABASE_URL, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from app.logger import get_logger
from app.consumer import main as consumer_main

logger = get_logger("api")

# Controlled set of accepted gender values — keeps validation simple and
# interview-friendly without requiring a lookup table in the database.
VALID_GENDERS = {"male", "female", "other", "prefer_not_to_say"}


# ──────────────────────────────────────────────────────────────
# Startup / shutdown
# ──────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    thread = threading.Thread(target=consumer_main, daemon=True, name="kafka-consumer")
    thread.start()
    logger.info("Kafka consumer thread started")
    yield
    logger.info("Shutting down — Kafka consumer thread will exit with process")


app = FastAPI(title="Nutrition Recommendation API", lifespan=lifespan)

engine = create_engine(DATABASE_URL)

# json default=str handles date serialisation without extra deps
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
)


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def _next_user_id(conn) -> str:
    """Generate next sequential user_id (U001, U002, …) from existing rows."""
    result = conn.execute(text("""
        SELECT user_id FROM dim_user_profile  WHERE user_id ~ '^U[0-9]+$'
        UNION
        SELECT user_id FROM stg_user_profile_event WHERE user_id ~ '^U[0-9]+$'
    """))
    ids = [row[0] for row in result]
    next_num = max((int(uid[1:]) for uid in ids), default=0) + 1
    return f"U{next_num:03d}"


def _get_existing_user(conn, user_id: str) -> Optional[dict]:
    row = conn.execute(
        text("SELECT * FROM dim_user_profile WHERE user_id = :uid"),
        {"uid": user_id},
    ).mappings().first()
    return dict(row) if row else None


# ──────────────────────────────────────────────────────────────
# Pydantic models
# ──────────────────────────────────────────────────────────────

class UserProfileRequest(BaseModel):
    user_name: str = Field(..., min_length=1, max_length=100)
    gender: str = Field(..., description="One of: male, female, other, prefer_not_to_say")
    birth_date: date = Field(..., description="YYYY-MM-DD — age is derived automatically")
    height_cm: float = Field(..., gt=0, le=300)
    current_weight_lb: float = Field(..., gt=0, le=800)
    target_weight_lb: float = Field(..., gt=0, le=800)


class UserProfileUpdateRequest(BaseModel):
    """All fields optional — only supplied fields are updated."""
    user_name: Optional[str] = Field(None, min_length=1, max_length=100)
    gender: Optional[str] = None
    birth_date: Optional[date] = None
    height_cm: Optional[float] = Field(None, gt=0, le=300)
    current_weight_lb: Optional[float] = Field(None, gt=0, le=800)
    target_weight_lb: Optional[float] = Field(None, gt=0, le=800)


_FIELD_MESSAGES = {
    "height_cm":          "height_cm must be greater than 0 and at most 300 cm",
    "current_weight_lb":  "current_weight_lb must be greater than 0 and at most 800 lb",
    "target_weight_lb":   "target_weight_lb must be greater than 0 and at most 800 lb",
    "user_name":          "user_name is required and must be 1–100 characters",
    "gender":             "gender must be one of: male, female, other, prefer_not_to_say",
    "birth_date":         "birth_date must be a valid date in YYYY-MM-DD format",
}


@app.exception_handler(RequestValidationError)
async def validation_error_handler(request: Request, exc: RequestValidationError):
    messages = []
    for error in exc.errors():
        field = error["loc"][-1] if error["loc"] else "unknown"
        messages.append(_FIELD_MESSAGES.get(field, f"{field}: {error['msg']}"))
    logger.warning("Validation error on %s: %s", request.url.path, messages)
    return JSONResponse(
        status_code=422,
        content={"error": "Invalid input", "details": messages},
    )


# ──────────────────────────────────────────────────────────────
# Endpoints
# ──────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/recommendations/{user_id}")
def get_recommendations(user_id: str):
    logger.info("Fetching recommendations for user_id=%s", user_id)

    query = text("""
        SELECT user_id, food_name, goal_type,
               recommendation_score, recommendation_rank, recommendation_reason
        FROM fact_food_recommendation
        WHERE user_id = :user_id
        ORDER BY recommendation_rank
    """)

    with engine.connect() as conn:
        rows = conn.execute(query, {"user_id": user_id}).mappings().all()

    if not rows:
        logger.warning("No recommendations found for user_id=%s", user_id)
        raise HTTPException(status_code=404, detail="User not found")

    logger.info("Returned %d recommendations for user_id=%s", len(rows), user_id)
    return {"user_id": user_id, "recommendations": [dict(row) for row in rows]}


@app.post("/users", status_code=201)
def create_user_profile(payload: UserProfileRequest):
    """Create a new user. Publishes a 'create' event to Kafka."""
    if payload.gender not in VALID_GENDERS:
        raise HTTPException(
            status_code=422,
            detail=f"gender must be one of: {sorted(VALID_GENDERS)}",
        )

    insert_staging = text("""
        INSERT INTO stg_user_profile_event (
            user_id, event_type, user_name, gender, birth_date,
            height_cm, current_weight_lb, target_weight_lb
        ) VALUES (
            :user_id, 'create', :user_name, :gender, :birth_date,
            :height_cm, :current_weight_lb, :target_weight_lb
        )
    """)

    try:
        with engine.begin() as conn:
            user_id = _next_user_id(conn)
            params = {
                "user_id":            user_id,
                "user_name":          payload.user_name,
                "gender":             payload.gender,
                "birth_date":         str(payload.birth_date),
                "height_cm":          payload.height_cm,
                "current_weight_lb":  payload.current_weight_lb,
                "target_weight_lb":   payload.target_weight_lb,
            }
            conn.execute(insert_staging, params)

        event = {"event_type": "create", **params}
        producer.send(KAFKA_TOPIC, value=event)
        producer.flush()
        logger.info("Published create event for user_id=%s to topic=%s", user_id, KAFKA_TOPIC)

        return {
            "message":      "User profile created",
            "user_id":      user_id,
            "user_profile": params,
            "kafka_topic":  KAFKA_TOPIC,
        }

    except Exception as e:
        logger.error("Failed to create user: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.put("/users/{user_id}")
def update_user_profile(user_id: str, payload: UserProfileUpdateRequest):
    """
    Update an existing user. Only supplied fields are changed; the rest are
    merged from the current dim_user_profile row. Publishes an 'update' event.
    """
    if payload.gender is not None and payload.gender not in VALID_GENDERS:
        raise HTTPException(
            status_code=422,
            detail=f"gender must be one of: {sorted(VALID_GENDERS)}",
        )

    with engine.connect() as conn:
        existing = _get_existing_user(conn, user_id)

    if not existing:
        raise HTTPException(status_code=404, detail="User not found")

    # Merge: start from stored values, overwrite with supplied fields only
    updates = payload.model_dump(exclude_none=True)
    merged = {**existing, **updates}

    event = {
        "event_type":         "update",
        "user_id":            user_id,
        "user_name":          merged.get("user_name"),
        "gender":             merged.get("gender"),
        "birth_date":         str(merged["birth_date"]) if merged.get("birth_date") else None,
        "height_cm":          float(merged["height_cm"]) if merged.get("height_cm") else None,
        "current_weight_lb":  float(merged["current_weight_lb"]) if merged.get("current_weight_lb") else None,
        "target_weight_lb":   float(merged["target_weight_lb"]) if merged.get("target_weight_lb") else None,
    }

    insert_staging = text("""
        INSERT INTO stg_user_profile_event (
            user_id, event_type, user_name, gender, birth_date,
            height_cm, current_weight_lb, target_weight_lb
        ) VALUES (
            :user_id, 'update', :user_name, :gender, :birth_date,
            :height_cm, :current_weight_lb, :target_weight_lb
        )
    """)

    try:
        with engine.begin() as conn:
            conn.execute(insert_staging, event)

        producer.send(KAFKA_TOPIC, value=event)
        producer.flush()
        logger.info("Published update event for user_id=%s", user_id)

        return {
            "message":        "User update received",
            "user_id":        user_id,
            "updated_fields": updates,
        }

    except Exception as e:
        logger.error("Failed to update user %s: %s", user_id, e)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.delete("/users/{user_id}")
def delete_user_profile(user_id: str):
    """
    Delete a user. Publishes a 'delete' event; the Kafka consumer removes the
    user from dim_user_profile and all their rows in fact_food_recommendation.
    """
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT user_id FROM dim_user_profile WHERE user_id = :uid"),
            {"uid": user_id},
        ).first()

    if not row:
        raise HTTPException(status_code=404, detail="User not found")

    insert_staging = text("""
        INSERT INTO stg_user_profile_event (user_id, event_type)
        VALUES (:user_id, 'delete')
    """)

    try:
        with engine.begin() as conn:
            conn.execute(insert_staging, {"user_id": user_id})

        event = {"event_type": "delete", "user_id": user_id}
        producer.send(KAFKA_TOPIC, value=event)
        producer.flush()
        logger.info("Published delete event for user_id=%s", user_id)

        return {"message": "User deletion received", "user_id": user_id}

    except Exception as e:
        logger.error("Failed to delete user %s: %s", user_id, e)
        raise HTTPException(status_code=500, detail="Internal server error")
