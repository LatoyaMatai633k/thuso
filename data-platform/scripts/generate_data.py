import argparse
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# Add platform root to sys.path
SCRIPT_DIR = Path(__file__).resolve().parent
PLATFORM_DIR = SCRIPT_DIR.parent
if str(PLATFORM_DIR) not in sys.path:
    sys.path.insert(0, str(PLATFORM_DIR))

from ingestion.synthetic_generator import SyntheticDataGenerator
from ingestion.ingest import ingest_batch_events


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic, privacy-safe demonstration data for Thuso.")
    parser.add_argument("--verifications", type=int, default=500, help="Number of verification events to generate")
    parser.add_argument("--journeys", type=int, default=150, help="Number of Safe Journey events to generate")
    parser.add_argument("--days", type=int, default=30, help="Historical time window in days")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducible generation")
    args = parser.parse_args()

    print(f"Generating {args.verifications} verifications and {args.journeys} journeys (seed={args.seed}, days={args.days})...")
    generator = SyntheticDataGenerator(seed=args.seed)
    verifications, journeys = generator.generate_dataset(
        num_verification_records=args.verifications,
        num_journey_records=args.journeys,
        days_back=args.days,
    )

    v_counts = ingest_batch_events(verifications, domain="verification")
    j_counts = ingest_batch_events(journeys, domain="safe_journey")

    print(f"[OK] Generated and landed {sum(v_counts.values())} verification records across {len(v_counts)} date partitions.")
    print(f"[OK] Generated and landed {sum(j_counts.values())} safe journey records across {len(j_counts)} date partitions.")


if __name__ == "__main__":
    main()
