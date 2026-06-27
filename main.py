"""CLI entry point for the integrated-asset pipeline.

Usage:
    python main.py                    # run stock fetch -> decrypt -> analyze -> broker fetch
    python main.py --card             # run all card stages
    python main.py --fetch            # stock fetch only
    python main.py --since 10         # custom date range (days ago)
    python main.py --sync             # stock sync to Google Sheets only
    python main.py --ibkr             # fetch from IBKR Client Portal API
    python main.py --etrade           # fetch from E*TRADE REST API
"""

from __future__ import annotations

import argparse

from dotenv import load_dotenv


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stock & credit-card statement processor.",
        epilog=(
            "Examples:\n"
            "  python main.py                          # run stock fetch/decrypt/analyze and broker fetch\n"
            "  python main.py --since 10               # fetch since 10 days ago\n"
            "  python main.py --analyze                # only Gemini analysis\n"
            "  python main.py --card                   # credit card pipeline\n"
            "  python main.py --card --analyze         # card analyze only\n"
            "  python main.py --rebuild-card-all       # rebuild card aggregate CSV from monthly\n"
            "  python main.py --sync                   # sync CSV to Google Sheet\n"
            "  python main.py --ibkr                   # fetch IBKR transactions\n"
            "  python main.py --ibkr --since 30        # IBKR transactions from last 30 days\n"
            "  python main.py --etrade                 # fetch E*TRADE transactions\n"
            "  python main.py --etrade --since 90      # E*TRADE transactions from last 90 days\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--fetch", action="store_true", help="Stage 1: Fetch attachments from Gmail"
    )
    parser.add_argument(
        "--decrypt", action="store_true", help="Stage 2: Decrypt PDF attachments"
    )
    parser.add_argument(
        "--analyze", action="store_true", help="Stage 3: Analyze PDFs with Gemini"
    )
    parser.add_argument(
        "--card",
        action="store_true",
        help="Run credit-card pipeline instead of stock pipeline",
    )
    parser.add_argument(
        "--rebuild-card-all",
        action="store_true",
        help="Rebuild attachments/card/credit_card_all.csv from attachments/card/monthly",
    )
    parser.add_argument(
        "--since",
        type=int,
        metavar="DAYS",
        help="Only fetch emails/transactions from this many days ago (default: 7 for email/IBKR, 30 for E*TRADE)",
    )
    parser.add_argument(
        "--sync",
        action="store_true",
        help="Stage 4: Sync transactions.csv to Google Sheet",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print raw Gemini responses and parsed rows",
    )
    parser.add_argument(
        "--ibkr",
        action="store_true",
        help="Stage 5: Fetch transactions from IBKR Client Portal API",
    )
    parser.add_argument(
        "--etrade",
        action="store_true",
        help="Stage 6: Fetch transactions from E*TRADE REST API (OAuth 1.0a)",
    )
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = _parse_args()

    if args.card or args.rebuild_card_all:
        from pipelines.card import CardPipeline

        pipeline = CardPipeline()
        run_all = not (
            args.fetch or args.decrypt or args.analyze or args.rebuild_card_all
        )

        if run_all:
            pipeline.run_all(
                since=args.since,
                debug=args.debug,
            )
        else:
            if args.fetch:
                pipeline.run_stage(
                    "Card Stage 1: Fetching attachments",
                    pipeline.fetch,
                    since=args.since,
                )
            if args.decrypt:
                pipeline.run_stage(
                    "Card Stage 2: Decrypting PDFs",
                    pipeline.decrypt,
                )
            if args.analyze:
                pipeline.run_stage(
                    "Card Stage 3: Analyzing with Gemini",
                    pipeline.analyze,
                    debug=args.debug,
                )
            if args.rebuild_card_all:
                pipeline.run_stage(
                    "Card Utility: Rebuilding aggregate CSV",
                    pipeline.rebuild_all,
                )
    else:
        from pipelines.stock import StockPipeline

        pipeline = StockPipeline()
        run_all = not (
            args.fetch
            or args.decrypt
            or args.analyze
            or args.sync
            or args.ibkr
            or args.etrade
        )

        if run_all:
            pipeline.run_all(
                since=args.since,
                debug=args.debug,
            )
        else:
            if args.fetch:
                pipeline.run_stage(
                    "Stage 1: Fetching attachments",
                    pipeline.fetch,
                    since=args.since,
                )
            if args.decrypt:
                pipeline.run_stage(
                    "Stage 2: Decrypting PDFs",
                    pipeline.decrypt,
                )
            if args.analyze:
                pipeline.run_stage(
                    "Stage 3: Analyzing with Gemini",
                    pipeline.analyze,
                    debug=args.debug,
                )
            if args.ibkr:
                pipeline.run_stage(
                    "Stage 5: Fetching from IBKR",
                    pipeline.fetch_ibkr,
                    since=args.since,
                    debug=args.debug,
                )
            if args.etrade:
                pipeline.run_stage(
                    "Stage 6: Fetching from E*TRADE",
                    pipeline.fetch_etrade,
                    since=args.since,
                    debug=args.debug,
                )
            if args.sync:
                pipeline.run_stage(
                    "Final Stage: Syncing to Google Sheet",
                    pipeline.sync,
                )


if __name__ == "__main__":
    main()
