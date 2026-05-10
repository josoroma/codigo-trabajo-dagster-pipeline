"""Register dynamic article partitions from existing chunk files."""

from __future__ import annotations

from dagster import DagsterInstance

from .partitions import discover_article_keys, sync_article_partitions


def main() -> None:
    keys = discover_article_keys()
    if not keys:
        print("No article chunk files found in data/chunks.")
        return

    instance = DagsterInstance.get()
    new_keys, stale_keys, total = sync_article_partitions(instance)

    print(
        f"Registered {len(new_keys)} new article partitions, "
        f"removed {len(stale_keys)} stale partitions "
        f"({len(keys)} discovered, {total} total)."
    )


if __name__ == "__main__":
    main()
