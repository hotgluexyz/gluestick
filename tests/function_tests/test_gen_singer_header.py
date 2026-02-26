import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd
from gluestick.singer import gen_singer_header


class TestBasicTypeMappings:
    def test_float_column(self):
        df = pd.DataFrame({"price": [1.5, 2.5, 3.5]})
        result_df, header = gen_singer_header(df, allow_objects=False)
        assert header["properties"]["price"] == {"type": ["number", "null"]}

    def test_int_column(self):
        df = pd.DataFrame({"count": [1, 2, 3]})
        result_df, header = gen_singer_header(df, allow_objects=False)
        assert header["properties"]["count"] == {"type": ["integer", "null"]}

    def test_bool_column(self):
        df = pd.DataFrame({"active": [True, False, True]})
        result_df, header = gen_singer_header(df, allow_objects=False)
        assert header["properties"]["active"] == {"type": ["boolean", "null"]}

    def test_string_column(self):
        df = pd.DataFrame({"name": ["alice", "bob", "carol"]})
        result_df, header = gen_singer_header(df, allow_objects=False)
        assert header["properties"]["name"] == {"type": ["string", "null"]}

    def test_datetime_column(self):
        df = pd.DataFrame({"created_at": pd.to_datetime(["2024-01-01", "2024-06-15"])})
        result_df, header = gen_singer_header(df, allow_objects=False)
        assert header["properties"]["created_at"] == {
            "format": "date-time",
            "type": ["string", "null"],
        }
        assert result_df["created_at"].iloc[0] == "2024-01-01T00:00:00.000000Z"

    def test_multiple_columns(self):
        df = pd.DataFrame({
            "customer_id": [1, 2],
            "customer_name": ["alice", "bob"],
            "balance": [9.5, 8.0],
            "is_active": [True, False],
        })
        _, header = gen_singer_header(df, allow_objects=False)
        assert header["properties"]["customer_id"] == {"type": ["integer", "null"]}
        assert header["properties"]["customer_name"] == {"type": ["string", "null"]}
        assert header["properties"]["balance"] == {"type": ["number", "null"]}
        assert header["properties"]["is_active"] == {"type": ["boolean", "null"]}

    def test_header_map_structure(self):
        df = pd.DataFrame({"product_id": [1]})
        _, header = gen_singer_header(df, allow_objects=False)
        assert header["type"] == ["object", "null"]
        assert "properties" in header


class TestSchemaOverride:
    def test_returns_provided_schema_when_not_catalog(self):
        df = pd.DataFrame({"order_id": [1]})
        custom_schema = {"type": "object", "properties": {"order_id": {"type": "custom"}}}
        result_df, header = gen_singer_header(df, allow_objects=False, schema=custom_schema, catalog_schema=False)
        assert header is custom_schema
        assert result_df is df

    def test_schema_override_does_not_modify_df(self):
        df = pd.DataFrame({"quantity": [10, 20]})
        schema = {"type": "object", "properties": {"quantity": {"type": "integer"}}}
        result_df, _ = gen_singer_header(df, allow_objects=False, schema=schema)
        assert result_df["quantity"].tolist() == [10, 20]


class TestCatalogSchema:
    def test_catalog_schema_merges_with_generated(self):
        df = pd.DataFrame({"sync_date": [1, 2], "account_name": ["acme", "globex"]})
        catalog = {
            "type": ["object", "null"],
            "properties": {
                "account_name": {"type": ["boolean", "null"]},
                "account_tier": {"type": ["number", "null"]},
            },
        }
        _, header = gen_singer_header(df, allow_objects=True, schema=catalog, catalog_schema=True)
        assert header["properties"]["account_name"] == {"type": ["boolean", "null"]}
        assert header["properties"]["account_tier"] == {"type": ["number", "null"]}
        assert "sync_date" in header["properties"]

    def test_catalog_schema_overrides_generated_types(self):
        df = pd.DataFrame({"status": ["pending", "completed"]})
        catalog = {
            "type": ["object", "null"],
            "properties": {
                "status": {"type": ["integer", "null"]},
            },
        }
        _, header = gen_singer_header(df, allow_objects=False, schema=catalog, catalog_schema=True)
        assert header["properties"]["status"] == {"type": ["integer", "null"]}


class TestAllowObjectsFalse:
    def test_object_column_defaults_to_string(self):
        df = pd.DataFrame({"shipping_info": [{"carrier": "ups"}, {"carrier": "fedex"}]})
        _, header = gen_singer_header(df, allow_objects=False)
        assert header["properties"]["shipping_info"] == {"type": ["string", "null"]}

    def test_object_column_values_serialized(self):
        df = pd.DataFrame({"extra_fields": [{"region": "US"}, [1, 2]]})
        result_df, _ = gen_singer_header(df, allow_objects=False)
        assert isinstance(result_df["extra_fields"].iloc[0], str)
        assert isinstance(result_df["extra_fields"].iloc[1], str)

    def test_null_values_pass_through_serialization(self):
        df = pd.DataFrame({"extra_fields": [None, {"region": "US"}]})
        result_df, _ = gen_singer_header(df, allow_objects=False)
        assert pd.isna(result_df["extra_fields"].iloc[0])


class TestAllowObjectsTrue:
    def test_all_nan_column_defaults_to_string(self):
        df = pd.DataFrame({"empty": [None, None, None]})
        _, header = gen_singer_header(df, allow_objects=True)
        assert header["properties"]["empty"] == {"type": ["string", "null"]}

    def test_dict_column_generates_object_schema(self):
        df = pd.DataFrame({"meta": [{"name": "alice", "age": 30}, {"name": "bob", "age": 25}]})
        _, header = gen_singer_header(df, allow_objects=True)
        prop = header["properties"]["meta"]
        assert prop["type"] == ["object", "null"]
        assert "name" in prop["properties"]
        assert "age" in prop["properties"]

    def test_dict_column_nested_types(self):
        df = pd.DataFrame({"meta": [{"name": "alice", "score": 9.5, "active": True}]})
        _, header = gen_singer_header(df, allow_objects=True)
        props = header["properties"]["meta"]["properties"]
        assert props["name"] == {"type": ["string", "null"]}
        assert props["score"] == {"type": ["number", "null"]}
        assert props["active"] == {"type": ["boolean", "null"]}

    def test_list_of_dicts_recursive_typing(self):
        df = pd.DataFrame({"line_items": [[{"item_id": 1, "label": "widget"}], [{"item_id": 2, "label": "gadget"}]]})
        _, header = gen_singer_header(df, allow_objects=True, recursive_typing=True)
        prop = header["properties"]["line_items"]
        assert prop["type"] == ["array", "null"]
        assert "properties" in prop["items"] or "type" in prop["items"]

    def test_list_of_dicts_no_recursive_typing(self):
        df = pd.DataFrame({"line_items": [[{"item_id": 1}], [{"item_id": 2}]]})
        _, header = gen_singer_header(df, allow_objects=True, recursive_typing=False)
        prop = header["properties"]["line_items"]
        assert prop == {"type": ["array", "null"], "items": {"type": ["object", "string", "null"]}}

    def test_list_of_scalars_recursive_typing(self):
        df = pd.DataFrame({"tags": [["electronics", "sale"], ["new"]]})
        _, header = gen_singer_header(df, allow_objects=True, recursive_typing=True)
        prop = header["properties"]["tags"]
        assert prop["type"] == ["array", "null"]

    def test_list_of_empty_lists(self):
        df = pd.DataFrame({"addresses": [[], []]})
        _, header = gen_singer_header(df, allow_objects=True, recursive_typing=True)
        prop = header["properties"]["addresses"]
        assert prop["type"] == ["array", "null"]
        assert prop["items"] == {"type": ["string", "null"]}

    def test_non_dict_non_list_object_defaults_to_string(self):
        df = pd.DataFrame({"coordinates": [(40.7, -74.0), (34.0, -118.2)]})
        _, header = gen_singer_header(df, allow_objects=True)
        assert header["properties"]["coordinates"] == {"type": ["string", "null"]}

    def test_list_with_nan_rows_uses_non_null(self):
        df = pd.DataFrame({"line_items": [None, [{"product_id": 1}]]})
        _, header = gen_singer_header(df, allow_objects=True, recursive_typing=True)
        prop = header["properties"]["line_items"]
        assert prop["type"] == ["array", "null"]

    def test_dict_with_nan_rows_uses_non_null(self):
        df = pd.DataFrame({"billing_address": [None, {"city": "New York"}]})
        _, header = gen_singer_header(df, allow_objects=True)
        prop = header["properties"]["billing_address"]
        assert prop["type"] == ["object", "null"]

    def test_list_of_dicts_prefers_float_values(self):
        """When recursive_typing merges dict keys, float values take priority."""
        df = pd.DataFrame({
            "payments": [
                [{"amount": 100}, {"amount": 99.5}],
            ]
        })
        _, header = gen_singer_header(df, allow_objects=True, recursive_typing=True)
        amount_type = header["properties"]["payments"]["items"]["properties"]["amount"]
        assert amount_type == {"type": ["number", "null"]}

    def test_list_of_dicts_merges_keys_across_rows(self):
        df = pd.DataFrame({
            "contacts": [
                [{"email": "alice@example.com"}],
                [{"phone": "555-1234"}],
            ]
        })
        _, header = gen_singer_header(df, allow_objects=True, recursive_typing=True)
        contact_props = header["properties"]["contacts"]["items"]["properties"]
        assert "email" in contact_props
        assert "phone" in contact_props


class TestDatetimeHandling:
    def test_datetime_formatted_as_singer_string(self):
        df = pd.DataFrame({
            "updated_at": pd.to_datetime(["2024-03-15 10:30:00", "2024-06-01 14:00:00"])
        })
        result_df, header = gen_singer_header(df, allow_objects=False)
        assert header["properties"]["updated_at"]["format"] == "date-time"
        assert "T" in result_df["updated_at"].iloc[0]
        assert result_df["updated_at"].iloc[0].endswith("Z")

    def test_datetime_with_timezone(self):
        df = pd.DataFrame({
            "updated_at": pd.to_datetime(["2024-01-01"]).tz_localize("UTC")
        })
        result_df, header = gen_singer_header(df, allow_objects=False)
        assert header["properties"]["updated_at"]["format"] == "date-time"


class TestEdgeCases:
    def test_empty_dataframe(self):
        df = pd.DataFrame()
        _, header = gen_singer_header(df, allow_objects=False)
        assert header["properties"] == {}

    def test_dataframe_with_no_rows(self):
        df = pd.DataFrame({"order_id": pd.Series([], dtype="int64"), "total": pd.Series([], dtype="float64")})
        _, header = gen_singer_header(df, allow_objects=False)
        assert header["properties"]["order_id"] == {"type": ["integer", "null"]}
        assert header["properties"]["total"] == {"type": ["number", "null"]}

    def test_mixed_types_in_object_column(self):
        df = pd.DataFrame({"metadata": [{"source": "api"}, "plain_string", None]})
        _, header = gen_singer_header(df, allow_objects=True)
        assert header["properties"]["metadata"]["type"] == ["object", "null"]

    def test_does_not_mutate_original_schema_arg(self):
        df = pd.DataFrame({"revenue": [1]})
        schema = {"type": ["object", "null"], "properties": {"currency": {"type": ["string", "null"]}}}
        gen_singer_header(df, allow_objects=False, schema=schema, catalog_schema=True)
        assert "currency" in schema["properties"]

    def test_deeply_nested_dict_in_list(self):
        df = pd.DataFrame({
            "invoices": [[{"billing_details": {"is_verified": True}}]]
        })
        _, header = gen_singer_header(df, allow_objects=True, recursive_typing=True)
        prop = header["properties"]["invoices"]
        assert prop["type"] == ["array", "null"]
        nested_props = prop["items"]["properties"]["billing_details"]
        assert nested_props["type"] == ["object", "null"]

    def test_int64_and_float64_detected(self):
        df = pd.DataFrame({
            "quantity": pd.array([1, 2], dtype="int64"),
            "unit_price": pd.array([1.0, 2.0], dtype="float64"),
        })
        _, header = gen_singer_header(df, allow_objects=False)
        assert header["properties"]["quantity"] == {"type": ["integer", "null"]}
        assert header["properties"]["unit_price"] == {"type": ["number", "null"]}

    def test_nullable_int_dtype(self):
        df = pd.DataFrame({"employee_count": pd.array([1, None], dtype="Int64")})
        _, header = gen_singer_header(df, allow_objects=False)
        assert header["properties"]["employee_count"] == {"type": ["integer", "null"]}

    def test_return_tuple_structure(self):
        df = pd.DataFrame({"invoice_id": [1]})
        result = gen_singer_header(df, allow_objects=False)
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], pd.DataFrame)
        assert isinstance(result[1], dict)
