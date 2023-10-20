from singer_sdk import typing as th

def get_price_schema(asset_class):
    if asset_class in ['stocks', 'forex', 'crypto']:
        schema = th.PropertiesList(
            th.Property("timestamp", th.DateTimeType, required=True),
            th.Property("timestamp_tz_aware", th.StringType),
            th.Property("timezone", th.StringType),
            th.Property("ticker", th.StringType, required=True),
            th.Property("open", th.NumberType),
            th.Property("high", th.NumberType),
            th.Property("low", th.NumberType),
            th.Property("close", th.NumberType),
            th.Property("volume", th.NumberType),
            th.Property("dividends", th.NumberType),
            th.Property("stock_splits", th.NumberType),
            th.Property("repaired", th.BooleanType),
            th.Property("replication_key", th.StringType)
        ).to_dict()
    else:
        raise NotImplementedError('Only price streams are currently supported.')
    return schema