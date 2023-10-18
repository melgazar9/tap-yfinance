"""YFinance tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk._singerlib.catalog import CatalogEntry, MetadataMapping, Schema
from tap_yfinance.streams import YFinancePriceStream
from tap_yfinance.schema import get_price_schema


class TapYFinance(Tap):
    """YFinance tap class."""
    name = "tap-yfinance"

    @staticmethod
    def discover_catalog_entry(table_name: str, schema: dict) -> CatalogEntry:
        """Create `CatalogEntry` object for the given collection."""

        catalog_entry = \
            CatalogEntry(
                tap_stream_id=table_name,
                stream=table_name,
                table=table_name,
                key_properties=["replication_key"],
                schema=Schema.from_dict(schema),
                replication_method=None,  # defined by user
                metadata=MetadataMapping.get_standard_metadata(
                    schema=schema,
                    replication_method=None,  # defined by user
                    key_properties=["replication_key"],
                    valid_replication_keys=None  # defined by user
                ),
                replication_key=None  # defined by user
            )
        return catalog_entry

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

        asset_classes = self.config['asset_class'].keys()

        result: dict[str, list[dict]] = {"streams": []}

        for asset_class in asset_classes:
            asset_schema = get_price_schema(asset_class)
            asset_table_names = self.config['asset_class'][asset_class].keys()
            for table_name in asset_table_names:
                # self.logger.info(f"Discovered table {table_name}")
                catalog_entry: CatalogEntry = \
                    self.discover_catalog_entry(table_name=table_name, schema=asset_schema)
                result["streams"].append(catalog_entry.to_dict())

        self._catalog_dict: dict = result  # pylint: disable=attribute-defined-outside-init
        return self._catalog_dict

    def discover_streams(self) -> list[YFinancePriceStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """

        output_streams = []
        for catalog_entry in self.catalog_dict["streams"]:
            stream = YFinancePriceStream(self, catalog_entry=catalog_entry)
            # stream.asset_class = catalog_entry['tap_stream_id'].split('|')[0]

            if catalog_entry['table_name'].startswith('stock'):
                stream.asset_class = 'stocks'
            elif catalog_entry['table_name'].startswith('forex'):
                stream.asset_class = 'forex'
            elif catalog_entry['table_name'].startswith('crypto'):
                stream.asset_class = 'crypto'
            else:
               raise ValueError('Could not parse asset class.')

            schema = get_price_schema(asset_class=stream.asset_class)
            stream._schema = schema
            output_streams.append(stream)
        return output_streams

if __name__ == "__main__":
    TapYFinance.cli()