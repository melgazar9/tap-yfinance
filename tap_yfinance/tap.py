"""YFinance tap class."""

from __future__ import annotations

import singer_sdk.typing as th  # JSON Schema typing helpers
from singer_sdk import Tap
from singer_sdk._singerlib.catalog import CatalogEntry, MetadataMapping, Schema
from tap_yfinance.streams import PriceStream
from tap_yfinance.schema import get_price_schema


class TapYFinance(Tap):
    """YFinance tap class."""

    name = "tap-yfinance"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "financial_group",
            th.ObjectType(
                additional_properties=th.ObjectType()
            ),
            required=True
        ),
    ).to_dict()

    @staticmethod
    def discover_catalog_entry(table_name: str, schema: dict) -> CatalogEntry:
        """Create `CatalogEntry` object for the given collection."""

        # TODO: Why is it if I set both tap_stream_id and stream to be financial_group + '|' + table_name it deselects ALL streams?
        return CatalogEntry(
                tap_stream_id=table_name,
                stream=table_name,
                table=table_name,
                key_properties=["timestamp", "ticker"],
                schema=Schema.from_dict(schema),
                replication_method=None,  # defined by user
                metadata=MetadataMapping.get_standard_metadata(
                    schema=schema,
                    replication_method=None,  # defined by user
                    key_properties=[
                        'timestamp', 'timestamp_tz_aware', 'timezone', 'ticker', 'open', 'high', 'low', 'close',
                        'volume', 'dividends', 'stock_splits', 'repaired'
                    ],
                    valid_replication_keys=None  # defined by user
                ),
                replication_key=None  # defined by user
            )

    @property
    def catalog_dict(self) -> dict:
        """Get catalog dictionary.

        Returns:
            The tap's catalog as a dict
        """

        if hasattr(self, "_catalog_dict") and self._catalog_dict:  # pylint: disable=access-member-before-definition
            return self._catalog_dict  # pylint: disable=access-member-before-definition

        if self.input_catalog:
            return self.input_catalog.to_dict()

        financial_groups: dict[str, dict[str, dict]] = self.config.get('financial_group', {})

        result: dict[str, list[dict]] = {"streams": []}

        for fg in financial_groups:
            schema = get_price_schema(fg)
            for table_name in financial_groups[fg]:
                catalog_entry: CatalogEntry = \
                    self.discover_catalog_entry(table_name=table_name, schema=schema)

                result["streams"].append(catalog_entry.to_dict())

        self._catalog_dict: dict = result  # pylint: disable=attribute-defined-outside-init
        return self._catalog_dict

    def discover_streams(self) -> list[PriceStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """

        return [
            PriceStream(self, catalog_entry=catalog_entry) for catalog_entry in self.catalog_dict["streams"]
        ]


if __name__ == "__main__":
    TapYFinance.cli()