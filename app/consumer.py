import json
import time
from datetime import date
from typing import Optional
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


# ──────────────────────────────────────────────────────────────
# Pure-function helpers (no DB side effects)
# ──────────────────────────────────────────────────────────────

def derive_goal_type(current_weight_lb: float, target_weight_lb: float) -> str:
    if target_weight_lb < current_weight_lb:
        return "weight_loss"
    if target_weight_lb > current_weight_lb:
        return "weight_gain"
    return "maintenance"


def calculate_age(birth_date_str) -> Optional[int]:
    """Return age in whole years from an ISO date string or date object."""
    if not birth_date_str:
        return None
    try:
        bd = date.fromisoformat(str(birth_date_str)) if not isinstance(birth_date_str, date) else birth_date_str
        today = date.today()
        return today.year - bd.year - ((today.month, today.day) < (bd.month, bd.day))
    except Exception:
        return None


def score_foods(goal_type: str, foods_df: pd.DataFrame) -> pd.DataFrame:
    """
    Score and rank all foods for a given goal_type.
    Returns a Pandas DataFrame with recommendation_score, recommendation_rank,
    recommendation_reason — top 10, deduplicated by food_name.
    """
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

    else:  # maintenance
        df["recommendation_score"] = (
            df["protein_g"] * 2.0
            + df["fiber_g"] * 1.5
            - df["sugar_g"] * 0.7
            - (df["calories_kcal"] - 400).abs() * 0.01
        )
        reason = "Balanced nutrition profile for maintenance"

    df["recommendation_score"] = df["recommendation_score"].round(2)
    df["recommendation_reason"] = reason

    # Deduplicate by food_name — keep the row with the highest score
    df = df.sort_values(["food_name", "recommendation_score"], ascending=[True, False])
    df = df.drop_duplicates(subset=["food_name"], keep="first")

    # Take top 10 and assign ordinal ranks
    df = df.sort_values("recommendation_score", ascending=False).head(10).copy()
    df["recommendation_rank"] = range(1, len(df) + 1)

    return df


# ──────────────────────────────────────────────────────────────
# Database write functions
# ──────────────────────────────────────────────────────────────

def upsert_user_profile(user_event: dict) -> str:
    """
    Insert or update dim_user_profile for this user.
    Returns the derived goal_type so the caller can regenerate recommendations.
    """
    goal_type = derive_goal_type(
        user_event["current_weight_lb"],
        user_event["target_weight_lb"],
    )
    age = calculate_age(user_event.get("birth_date"))

    upsert_sql = text("""
        INSERT INTO dim_user_profile (
            user_id, user_name, gender, birth_date, age,
            height_cm, current_weight_lb, target_weight_lb,
            goal_type, updated_at
        )
        VALUES (
            :user_id, :user_name, :gender, :birth_date, :age,
            :height_cm, :current_weight_lb, :target_weight_lb,
            :goal_type, CURRENT_TIMESTAMP
        )
        ON CONFLICT (user_id) DO UPDATE SET
            user_name         = EXCLUDED.user_name,
            gender            = EXCLUDED.gender,
            birth_date        = EXCLUDED.birth_date,
            age               = EXCLUDED.age,
            height_cm         = EXCLUDED.height_cm,
            current_weight_lb = EXCLUDED.current_weight_lb,
            target_weight_lb  = EXCLUDED.target_weight_lb,
            goal_type         = EXCLUDED.goal_type,
            updated_at        = CURRENT_TIMESTAMP
    """)

    with engine.begin() as conn:
        conn.execute(upsert_sql, {
            "user_id":           user_event["user_id"],
            "user_name":         user_event.get("user_name"),
            "gender":            user_event.get("gender"),
            "birth_date":        user_event.get("birth_date"),
            "age":               age,
            "height_cm":         user_event.get("height_cm"),
            "current_weight_lb": user_event.get("current_weight_lb"),
            "target_weight_lb":  user_event.get("target_weight_lb"),
            "goal_type":         goal_type,
        })

    logger.debug("Upserted dim_user_profile user_id=%s goal_type=%s", user_event["user_id"], goal_type)
    return goal_type


def delete_user(user_id: str) -> None:
    """
    Remove a user and all their recommendation rows from PostgreSQL.
    Fact rows are deleted first to maintain referential cleanliness.
    """
    with engine.begin() as conn:
        rec_count = conn.execute(
            text("DELETE FROM fact_food_recommendation WHERE user_id = :uid"),
            {"uid": user_id},
        ).rowcount
        conn.execute(
            text("DELETE FROM dim_user_profile WHERE user_id = :uid"),
            {"uid": user_id},
        )
    logger.info("Deleted user_id=%s — removed %d recommendation row(s)", user_id, rec_count)


def regenerate_recommendations(user_event: dict, goal_type: str) -> None:
    """
    Score all foods for the user's goal_type and replace their existing
    fact_food_recommendation rows with the freshly ranked top 10.
    """
    foods_df = pd.read_sql("SELECT * FROM dim_food", engine)
    logger.debug("Loaded %d foods from dim_food", len(foods_df))

    scored_df = score_foods(goal_type, foods_df)
    scored_df["user_id"]   = user_event["user_id"]
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


# ──────────────────────────────────────────────────────────────
# Main consumer loop
# ──────────────────────────────────────────────────────────────

def main():
    logger.info("Kafka consumer started — topic=%s group=%s", KAFKA_TOPIC, KAFKA_GROUP_ID)

    for message in consumer:
        try:
            event = message.value
            user_id    = event.get("user_id")
            event_type = event.get("event_type", "create")  # backward-compat default

            logger.info(
                "Received event: user_id=%s event_type=%s partition=%d offset=%d",
                user_id, event_type, message.partition, message.offset,
            )

            if event_type == "delete":
                delete_user(user_id)
                logger.info("Processed delete for user_id=%s", user_id)

            elif event_type in ("create", "update"):
                goal_type = upsert_user_profile(event)
                regenerate_recommendations(event, goal_type)
                logger.info(
                    "Processed %s for user_id=%s goal_type=%s",
                    event_type, user_id, goal_type,
                )

            else:
                logger.warning(
                    "Unknown event_type=%s for user_id=%s — skipping",
                    event_type, user_id,
                )

        except Exception as e:
            logger.error("Error processing message: %s", e, exc_info=True)
            time.sleep(1)


if __name__ == "__main__":
    main()
