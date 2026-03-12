from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text
from kafka import KafkaProducer
import json

app = FastAPI(title="Nutrition Recommendation API")

DATABASE_URL = "postgresql+psycopg2://de_user:de_pass@localhost:5432/nutrition_dw"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "user_profiles"

engine = create_engine(DATABASE_URL)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


class UserProfileRequest(BaseModel):
    user_id: str = Field(..., min_length=1, max_length=50)
    height_cm: float = Field(..., gt=0)
    current_weight_lb: float = Field(..., gt=0)
    target_weight_lb: float = Field(..., gt=0)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/recommendations/{user_id}")
def get_recommendations(user_id: str):
    query = text("""
        SELECT user_id, food_name, goal_type, recommendation_score, recommendation_rank, recommendation_reason
        FROM fact_food_recommendation
        WHERE user_id = :user_id
        ORDER BY recommendation_rank
    """)

    with engine.connect() as conn:
        rows = conn.execute(query, {"user_id": user_id}).mappings().all()

    if not rows:
        raise HTTPException(status_code=404, detail="User not found")

    return {
        "user_id": user_id,
        "recommendations": [dict(row) for row in rows]
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

    event = {
        "user_id": payload.user_id,
        "height_cm": payload.height_cm,
        "current_weight_lb": payload.current_weight_lb,
        "target_weight_lb": payload.target_weight_lb
    }

    try:
        with engine.begin() as conn:
            conn.execute(insert_query, event)

        producer.send(KAFKA_TOPIC, value=event)
        producer.flush()

        return {
            "message": "User profile received",
            "user_profile": event,
            "kafka_topic": KAFKA_TOPIC
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))