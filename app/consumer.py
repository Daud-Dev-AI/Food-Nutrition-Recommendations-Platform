import json
import time
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
import pandas as pd

from app.config import DATABASE_URL, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_GROUP_ID
from app.logger import get_logger

logger = get_logger("consumer")

engine = create_engine(DATABASE_URL)

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=KAFKA_GROUP_ID,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)


def derive_goal_type(current_weight_lb: float, target_weight_lb: float) -> str:
    if target_weight_lb < current_weight_lb:
        return "weight_loss"
    if target_weight_lb > current_weight_lb:
        return "weight_gain"
    return "maintenance"


def score_foods(goal_type: str, foods_df: pd.DataFrame) -> pd.DataFrame:
    df = foods_df.copy()

    if goal_type == "weight_loss":
        df["recommendation_score"] = (
            df["protein_g"] * 2.5
            + df["fiber_g"] * 2.0
            - df["calories_kcal"] * 0.015
            - df["sugar_g"] * 0.8
            - df["fat_g"] * 0.2
        )
        reason = "Higher protein/fiber with relatively lower calories and sugar"

    elif goal_type == "weight_gain":
        df["recommendation_score"] = (
            df["calories_kcal"] * 0.02
            + df["protein_g"] * 2.0
            + df["carbohydrates_g"] * 0.8
            + df["fat_g"] * 0.5
        )
        reason = "Higher calories and protein for weight gain support"

    else:
        df["recommendation_score"] = (
            df["protein_g"] * 2.0
            + df["fiber_g"] * 1.5
            - df["sugar_g"] * 0.7
            - (df["calories_kcal"] - 400).abs() * 0.01
        )
        reason = "Balanced nutrition profile for maintenance"

    df["recommendation_score"] = df["recommendation_score"].round(2)
    df["recommendation_reason"] = reason

    df = df.sort_values(["food_name", "recommendation_score"], ascending=[True, False])
    df = df.drop_duplicates(subset=["food_name"], keep="first")

    df = df.sort_values("recommendation_score", ascending=False).head(10).copy()
    df["recommendation_rank"] = range(1, len(df) + 1)

    return df


def upsert_user_profile(user_event: dict) -> str:
    goal_type = derive_goal_type(
        user_event["current_weight_lb"],
        user_event["target_weight_lb"],
    )

    upsert_sql = text("""
        INSERT INTO dim_user_profile (
            user_id,
            height_cm,
            current_weight_lb,
            target_weight_lb,
            goal_type
        )
        VALUES (
            :user_id,
            :height_cm,
            :current_weight_lb,
            :target_weight_lb,
            :goal_type
        )
        ON CONFLICT (user_id)
        DO UPDATE SET
            height_cm = EXCLUDED.height_cm,
            current_weight_lb = EXCLUDED.current_weight_lb,
            target_weight_lb = EXCLUDED.target_weight_lb,
            goal_type = EXCLUDED.goal_type
    """)

    with engine.begin() as conn:
        conn.execute(upsert_sql, {
            "user_id": user_event["user_id"],
            "height_cm": user_event["height_cm"],
            "current_weight_lb": user_event["current_weight_lb"],
            "target_weight_lb": user_event["target_weight_lb"],
            "goal_type": goal_type,
        })

    logger.debug("Upserted dim_user_profile for user_id=%s goal_type=%s", user_event["user_id"], goal_type)
    return goal_type


def regenerate_recommendations(user_event: dict, goal_type: str) -> None:
    foods_df = pd.read_sql("SELECT * FROM dim_food", engine)
    logger.debug("Loaded %d foods from dim_food", len(foods_df))

    scored_df = score_foods(goal_type, foods_df)
    scored_df["user_id"] = user_event["user_id"]
    scored_df["goal_type"] = goal_type

    fact_df = scored_df[[
        "user_id",
        "food_name",
        "goal_type",
        "recommendation_score",
        "recommendation_rank",
        "recommendation_reason",
    ]].copy()

    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM fact_food_recommendation WHERE user_id = :user_id"),
            {"user_id": user_event["user_id"]},
        )

    fact_df.to_sql("fact_food_recommendation", engine, if_exists="append", index=False)
    logger.info(
        "Regenerated %d recommendations for user_id=%s goal_type=%s",
        len(fact_df),
        user_event["user_id"],
        goal_type,
    )


def main():
    logger.info("Kafka consumer started — topic=%s group=%s", KAFKA_TOPIC, KAFKA_GROUP_ID)

    for message in consumer:
        try:
            event = message.value
            logger.info(
                "Received event: user_id=%s partition=%d offset=%d",
                event.get("user_id"),
                message.partition,
                message.offset,
            )

            goal_type = upsert_user_profile(event)
            regenerate_recommendations(event, goal_type)

            logger.info("Processed user_id=%s goal_type=%s", event["user_id"], goal_type)

        except Exception as e:
            logger.error("Error processing message: %s", e, exc_info=True)
            time.sleep(1)


if __name__ == "__main__":
    main()
