from fastapi import FastAPI, HTTPException
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


class UserProfileRequest(BaseModel):
    user_id: str = Field(..., min_length=1, max_length=50)
    height_cm: float = Field(..., gt=0, le=300)
    current_weight_lb: float = Field(..., gt=0, le=800)
    target_weight_lb: float = Field(..., gt=0, le=800)


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
    logger.info("Received profile submission for user_id=%s", payload.user_id)

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

    event = {
        "user_id": payload.user_id,
        "height_cm": payload.height_cm,
        "current_weight_lb": payload.current_weight_lb,
        "target_weight_lb": payload.target_weight_lb,
    }

    try:
        with engine.begin() as conn:
            conn.execute(insert_query, event)
        logger.debug("Staged event for user_id=%s", payload.user_id)

        producer.send(KAFKA_TOPIC, value=event)
        producer.flush()
        logger.info("Published Kafka event for user_id=%s to topic=%s", payload.user_id, KAFKA_TOPIC)

        return {
            "message": "User profile received",
            "user_profile": event,
            "kafka_topic": KAFKA_TOPIC,
        }

    except Exception as e:
        logger.error("Failed to process profile for user_id=%s: %s", payload.user_id, e)
        raise HTTPException(status_code=500, detail="Internal server error")
