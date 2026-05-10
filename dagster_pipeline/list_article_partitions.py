"""Print registered article partition keys, one per line."""

from __future__ import annotations

from dagster import DagsterInstance

from .partitions import article_sort_key, is_article_partition_key


def main() -> None:
    instance = DagsterInstance.get()
    keys = sorted(
        (
            key
            for key in instance.get_dynamic_partitions("articles")
            if is_article_partition_key(key)
        ),
        key=article_sort_key,
    )
    for key in keys:
        print(key)


if __name__ == "__main__":
    main()
