from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text
from kafka import KafkaProducer
import json

from app.config import DATABASE_URL, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from app.logger import get_logger

logger = get_logger("api")

app = FastAPI(title="Nutrition Recommendation API")

engine = create_engine(DATABASE_URL)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def _next_user_id(conn) -> str:
    result = conn.execute(text("""
        SELECT user_id FROM dim_user_profile  WHERE user_id ~ '^U[0-9]+$'
        UNION
        SELECT user_id FROM stg_user_profile_event WHERE user_id ~ '^U[0-9]+$'
    """))
    ids = [row[0] for row in result]
    next_num = max((int(uid[1:]) for uid in ids), default=0) + 1
    return f"U{next_num:03d}"


class UserProfileRequest(BaseModel):
    height_cm: float = Field(..., gt=0, le=300)
    current_weight_lb: float = Field(..., gt=0, le=800)
    target_weight_lb: float = Field(..., gt=0, le=800)


_FIELD_MESSAGES = {
    "height_cm": "height_cm must be greater than 0 and at most 300 cm",
    "current_weight_lb": "current_weight_lb must be greater than 0 and at most 800 lb",
    "target_weight_lb": "target_weight_lb must be greater than 0 and at most 800 lb",
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


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/recommendations/{user_id}")
def get_recommendations(user_id: str):
    logger.info("Fetching recommendations for user_id=%s", user_id)

    query = text("""
        SELECT user_id, food_name, goal_type, recommendation_score, recommendation_rank, recommendation_reason
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
    return {
        "user_id": user_id,
        "recommendations": [dict(row) for row in rows],
    }


@app.post("/users")
def create_user_profile(payload: UserProfileRequest):
    insert_query = text("""
        INSERT INTO stg_user_profile_event (
            user_id,
            height_cm,
            current_weight_lb,
            target_weight_lb
        )
        VALUES (
            :user_id,
            :height_cm,
            :current_weight_lb,
            :target_weight_lb
        )
    """)

    try:
        with engine.begin() as conn:
            user_id = _next_user_id(conn)
            event = {
                "user_id": user_id,
                "height_cm": payload.height_cm,
                "current_weight_lb": payload.current_weight_lb,
                "target_weight_lb": payload.target_weight_lb,
            }
            conn.execute(insert_query, event)
        logger.info("Creating new user profile user_id=%s", user_id)
        logger.debug("Staged event for user_id=%s", user_id)

        producer.send(KAFKA_TOPIC, value=event)
        producer.flush()
        logger.info("Published Kafka event for user_id=%s to topic=%s", user_id, KAFKA_TOPIC)

        return {
            "message": "User profile received",
            "user_id": user_id,
            "user_profile": event,
            "kafka_topic": KAFKA_TOPIC,
        }

    except Exception as e:
        logger.error("Failed to process profile: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")
