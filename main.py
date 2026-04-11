import argparse

from pipelines import data

def main() -> None:
    parser = argparse.ArgumentParser(description="Pitcher archetypes pipeline runner.")
    parser.add_argument(
        "pipeline",
        choices=["data"],
        help="Pipeline to run.",
    )
    args = parser.parse_args()

    if args.pipeline == "data":
        data.end_to_end()


if __name__ == "__main__":
    main()
