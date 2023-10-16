from singer_sdk import typing as th

def get_price_schema(asset_class):
    if asset_class in ['stocks', 'forex', 'crypto']:
        schema = th.PropertiesList(  # Define the _schema attribute here
            th.Property("replication_key", th.StringType, required=True),
            th.Property("timestamp", th.DateTimeType, required=True),
            th.Property("timestamp_tz_aware", th.StringType, required=True),
            th.Property("timezone", th.StringType, required=True),
            th.Property("yahoo_ticker", th.StringType, required=True),
            th.Property("open", th.NumberType),
            th.Property("high", th.NumberType),
            th.Property("low", th.NumberType),
            th.Property("close", th.NumberType),
            th.Property("volume", th.NumberType),
            th.Property("dividends", th.NumberType),
            th.Property("stock_splits", th.NumberType),
            th.Property("repaired", th.StringType),
            th.Property("batch_timestamp", th.DateTimeType)
        ).to_dict()
    else:
        raise NotImplementedError('Only price streams are currently supported.')

    return schema