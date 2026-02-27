# asgp/nosql_cli.py
"""
Multi-NoSQL Database Agent CLI — Interactive Query Interface
Tests MongoDB, Redis, Cassandra, DynamoDB, Couchbase routing
"""
import asyncio
import os
import sys
from pathlib import Path

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from asgp.config.registry import ConfigRegistry
from asgp.agents.database_agent import DatabaseAgent


# ─── Display Helpers ───

DB_ICONS = {
    "mongodb":   "🍃",
    "redis":     "⚡",
    "cassandra": "📊",
    "dynamodb":  "☁️",
    "couchbase": "🪣",
}

HELP_TEXT = """
📖 Example Questions (the LLM will auto-route to the correct database):

  MongoDB:
   • Show me all electronics products
   • Find products over $1000
   • Calculate average price by category

  Redis:
   • Get session data for user 123
   • Show all active user sessions
   • Top 10 scores on the daily leaderboard

  Cassandra:
   • Show sensor readings from last hour
   • User activity for user XYZ

  DynamoDB:
   • Get user profile for user-456
   • Find orders for user-456

  Couchbase:
   • Find products in Electronics category
   • Search products with tag premium

Commands:
   help   — show this help
   stats  — show loaded database sources
   exit   — quit
"""


async def interactive_mode():
    """Interactive multi-database query mode."""
    print("\n" + "═" * 80)
    print("  ASGP — UNIFIED NOSQL DATABASE AGENT")
    print("  MongoDB │ Redis │ Cassandra │ DynamoDB │ Couchbase")
    print("═" * 80)

    # Environment check
    api_key = os.getenv('OPENAI_API_KEY')
    print(f"\n  OPENAI_API_KEY: {'✅ Set' if api_key else '❌ Missing'}")

    if not api_key or api_key == 'sk-proj-':
        print("  ⚠️  WARNING: OPENAI_API_KEY not configured!")
        print("     Add it to your .env file\n")
        response = input("  Continue anyway? (y/n): ").strip().lower()
        if response != 'y':
            return

    # Load configuration
    try:
        print("\n  Loading configuration...")
        registry = ConfigRegistry.load(['config/db_domain.yaml'])

        agent_cfg = registry.get_agent_config('database_agent')
        source_cfgs = {
            name: registry.get_source(name)
            for name in agent_cfg.source_bindings
        }
        source_details = {
            name: registry.get_source_details(name)
            for name in agent_cfg.source_bindings
        }

        agent = DatabaseAgent(agent_cfg, source_cfgs, source_details)

        print(f"\n  ✅ Agent ready!")
        print(f"     Model: {agent_cfg.model}")
        print(f"     Sources: {', '.join(agent_cfg.source_bindings)}")

    except Exception as e:
        print(f"\n  ❌ Setup failed: {e}")
        import traceback
        traceback.print_exc()
        return

    print(f"\n  Type 'help' for example queries, 'exit' to quit.")
    print("─" * 80)

    query_count = 0

    while True:
        try:
            prompt = input("\n💬 Your question: ").strip()

            if not prompt:
                continue

            if prompt.lower() in ['exit', 'quit', 'q']:
                print("\n👋 Goodbye!")
                break

            if prompt.lower() == 'help':
                print(HELP_TEXT)
                continue

            if prompt.lower() == 'stats':
                print("\n📊 Loaded Database Sources:")
                for name, cfg in source_cfgs.items():
                    icon = DB_ICONS.get(cfg.driver.value, "🔧") if cfg.driver else "🔧"
                    print(f"   {icon} {name} ({cfg.driver.value if cfg.driver else 'unknown'})")
                continue

            # Execute query
            query_count += 1
            print(f"\n⏳ Processing query #{query_count}...")

            result = await agent.execute(
                prompt=prompt,
                trace_id=f"cli-{query_count:04d}"
            )

            # Display results
            print(f"\n{'─' * 80}")

            if result.status.value == "success":
                db_type = result.metadata.get('db_type', 'unknown')
                icon = DB_ICONS.get(db_type, '🔧')

                print(f"  {icon} Routed to: {db_type.upper()} ({result.source_name})")
                print(f"  ✅ Found {result.row_count} results "
                      f"(confidence: {result.confidence:.0%}, "
                      f"latency: {result.latency_ms}ms)")

                if result.raw_query:
                    query_display = result.raw_query
                    if len(query_display) > 120:
                        query_display = query_display[:120] + "..."
                    print(f"  📝 Query: {query_display}")

                explanation = result.metadata.get('explanation', '')
                if explanation:
                    print(f"  💡 {explanation}")

                if result.row_count > 0:
                    print(f"\n  📊 Results (first 5):")
                    for i, doc in enumerate(result.data[:5], 1):
                        # Show key fields compactly
                        display = []
                        for key in ['name', 'sku', 'order_id', 'user_id', 'email',
                                    'sensor_id', 'category', 'price', 'total',
                                    'status', 'value', 'member', 'score',
                                    'field', 'key', 'temperature']:
                            if key in doc:
                                value = doc[key]
                                if key in ['price', 'total'] and isinstance(value, (int, float)):
                                    display.append(f"{key}=${value:.2f}")
                                else:
                                    val_str = str(value)
                                    if len(val_str) > 50:
                                        val_str = val_str[:50] + "..."
                                    display.append(f"{key}={val_str}")

                        if display:
                            print(f"     [{i}] {', '.join(display)}")
                        else:
                            # Fallback: show first 3 fields
                            fields = list(doc.items())[:3]
                            field_str = ', '.join(f"{k}={v}" for k, v in fields)
                            print(f"     [{i}] {field_str}")

                    if result.row_count > 5:
                        print(f"     ... and {result.row_count - 5} more")
                else:
                    print(f"\n  📭 No results found")
            else:
                print(f"  ❌ Query failed")
                if result.error_detail:
                    print(f"  Error: {result.error_detail[:300]}")

            print(f"{'─' * 80}")

        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}")


def main():
    """Entry point"""
    try:
        asyncio.run(interactive_mode())
    except KeyboardInterrupt:
        print("\n\n👋 Goodbye!")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
