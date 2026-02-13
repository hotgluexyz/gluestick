import gluestick as gs
import pandas as pd

# Tests for map_fields function
def test_map_fields_simple_flat_mapping():
    """Test simple flat field mapping."""
    print("=====")
    print("test_map_fields_simple_flat_mapping")

    row = {"first_name": "John", "last_name": "Doe", "age": 30}
    mapping = {
        "name": "first_name",
        "surname": "last_name",
        "years": "age"
    }

    result = gs.map_fields(row, mapping)
    expected = {"name": "John", "surname": "Doe", "years": 30}

    assert result == expected
    print("test_map_fields_simple_flat_mapping output is correct")

def test_map_fields_list_mapping():
    """Test list of dicts mapping."""
    print("=====")
    print("test_map_fields_list_mapping")

    row = {"phone1": "555-1234", "phone2": "555-5678", "email": "john@example.com"}
    
    mapping = {"contact_info": [{"phone_1": "phone1", "phone_2": "phone2", "email_address": "email"}]}


    result = gs.map_fields(row, mapping)
    # All items in the list should be mapped
    expected = {"contact_info": [{"phone_1": "555-1234", "phone_2": "555-5678", "email_address": "john@example.com"}]}

    assert result == expected
    print("test_map_fields_list_mapping output is correct")

def test_map_fields_none_value():
    """Test handling of missing values in row (NaN)."""
    print("=====")
    print("test_map_fields_missing_row_value")

    row = {"name": "John", "age": float('nan'), "city": None}
    mapping = {
        "person_name": "name",
        "person_age": "age",
        "location": "city"
    }

    result = gs.map_fields(row, mapping)
    # NaN and None values should be excluded
    expected = {"person_name": "John"}

    assert result == expected
    print("test_map_fields_missing_row_value output is correct")

def test_map_fields_nonexistent_field():
    """Test mapping to a field that doesn't exist in row."""
    print("=====")
    print("test_map_fields_nonexistent_field")

    row = {"name": "John"}
    mapping = {
        "person_name": "name",
        "person_age": "age"  # 'age' doesn't exist in row
    }

    result = gs.map_fields(row, mapping)
    expected = {"person_name": "John"}

    assert result == expected
    assert "person_age" not in result
    print("test_map_fields_nonexistent_field output is correct")

def test_map_fields_deeply_nested():
    """Test deeply nested mapping structures."""
    print("=====")
    print("test_map_fields_deeply_nested")

    row = {
        "fname": "John",
        "lname": "Doe",
        "street": "123 Main",
        "city": "NYC",
        "country": "USA"
    }
    mapping = {
        "person": {
            "name": {
                "first": "fname",
                "last": "lname"
            },
            "location": {
                "address": {
                    "street_name": "street",
                    "city_name": "city"
                },
                "country_name": "country"
            }
        }
    }

    result = gs.map_fields(row, mapping)
    expected = {
        "person": {
            "name": {
                "first": "John",
                "last": "Doe"
            },
            "location": {
                "address": {
                    "street_name": "123 Main",
                    "city_name": "NYC"
                },
                "country_name": "USA"
            }
        }
    }

    assert result == expected
    print("test_map_fields_deeply_nested output is correct")


# -------------------------------------------------------------------
# Tests for the "pick" branch in map_fields
# -------------------------------------------------------------------

def test_map_fields_pick_single_id_single_field():
    """Test pick branch: single filter_id (from rec.), single target field."""
    print("=====")
    print("test_map_fields_pick_single_id_single_field")

    row = {"vendor_id": 1, "amount": 500}
    other_data = {
        "vendors": pd.DataFrame([
            {"id": 1, "name": "Acme Corp", "city": "NYC"},
            {"id": 2, "name": "Globex", "city": "LA"},
        ])
    }
    mapping = {
        "vendor_name": {
            "pick": {
                "objects": "vendors",
                "id_field": "id",
                "filter_ids": "rec.vendor_id",
                "target_fields": "name",
            }
        }
    }

    result = gs.map_fields(row, mapping, other_data)
    expected = {"vendor_name": "Acme Corp"}

    assert result == expected
    print("test_map_fields_pick_single_id_single_field output is correct")


def test_map_fields_pick_single_id_multiple_fields():
    """Test pick branch: single filter_id, multiple target fields."""
    print("=====")
    print("test_map_fields_pick_single_id_multiple_fields")

    row = {"vendor_id": 2, "amount": 300}
    other_data = {
        "vendors": pd.DataFrame([
            {"id": 1, "name": "Acme Corp", "city": "NYC"},
            {"id": 2, "name": "Globex", "city": "LA"},
        ])
    }
    mapping = {
        "vendor_info": {
            "pick": {
                "objects": "vendors",
                "id_field": "id",
                "filter_ids": "rec.vendor_id",
                "target_fields": ["name", "city"],
            }
        }
    }

    result = gs.map_fields(row, mapping, other_data)
    expected = {"vendor_info": {"name": "Globex", "city": "LA"}}

    assert result == expected
    print("test_map_fields_pick_single_id_multiple_fields output is correct")


def test_map_fields_pick_missing_filter_id():
    """Test pick branch: filter_id field is missing/None in the row."""
    print("=====")
    print("test_map_fields_pick_missing_filter_id")

    row = {"vendor_id": None, "amount": 100}
    other_data = {
        "vendors": pd.DataFrame([
            {"id": 1, "name": "Acme Corp"},
        ])
    }
    mapping = {
        "vendor_name": {
            "pick": {
                "objects": "vendors",
                "id_field": "id",
                "filter_ids": "rec.vendor_id",
                "target_fields": "name",
            }
        }
    }

    result = gs.map_fields(row, mapping, other_data)
    expected = {"vendor_name": None}

    assert result == expected
    print("test_map_fields_pick_missing_filter_id output is correct")


def test_map_fields_pick_no_match():
    """Test pick branch: filter_id exists but no matching row in other_data."""
    print("=====")
    print("test_map_fields_pick_no_match")

    row = {"vendor_id": 999, "amount": 50}
    other_data = {
        "vendors": pd.DataFrame([
            {"id": 1, "name": "Acme Corp"},
            {"id": 2, "name": "Globex"},
        ])
    }
    mapping = {
        "vendor_name": {
            "pick": {
                "objects": "vendors",
                "id_field": "id",
                "filter_ids": "rec.vendor_id",
                "target_fields": "name",
            }
        }
    }

    result = gs.map_fields(row, mapping, other_data)
    # No match → pluck_fields returns None
    expected = {"vendor_name": None}

    assert result == expected
    print("test_map_fields_pick_no_match output is correct")


def test_map_fields_pick_with_partition_key():
    """Test pick branch: filtering with a partition_key from the row."""
    print("=====")
    print("test_map_fields_pick_with_partition_key")

    row = {"vendor_id": 1, "dept": "HR"}
    other_data = {
        "vendors": pd.DataFrame([
            {"id": 1, "name": "Alice", "dept": "HR"},
            {"id": 1, "name": "Alice-IT", "dept": "IT"},
            {"id": 2, "name": "Bob", "dept": "HR"},
        ])
    }
    mapping = {
        "vendor_name": {
            "pick": {
                "objects": "vendors",
                "id_field": "id",
                "filter_ids": "rec.vendor_id",
                "target_fields": "name",
                "partition_key": "dept",
            }
        }
    }

    result = gs.map_fields(row, mapping, other_data)
    expected = {"vendor_name": "Alice"}

    assert result == expected
    print("test_map_fields_pick_with_partition_key output is correct")


def test_map_fields_pick_empty_dataframe():
    """Test pick branch: other_data DataFrame is empty."""
    print("=====")
    print("test_map_fields_pick_empty_dataframe")

    row = {"vendor_id": 1}
    other_data = {
        "vendors": pd.DataFrame(columns=["id", "name"])
    }
    mapping = {
        "vendor_name": {
            "pick": {
                "objects": "vendors",
                "id_field": "id",
                "filter_ids": "rec.vendor_id",
                "target_fields": "name",
            }
        }
    }

    result = gs.map_fields(row, mapping, other_data)
    # Empty DataFrame → pick branch is skipped entirely
    assert "vendor_name" not in result
    print("test_map_fields_pick_empty_dataframe output is correct")


def test_map_fields_pick_mixed_with_flat():
    """Test pick branch alongside regular flat field mappings."""
    print("=====")
    print("test_map_fields_pick_mixed_with_flat")

    row = {"vendor_id": 2, "amount": 750}
    other_data = {
        "vendors": pd.DataFrame([
            {"id": 1, "name": "Acme Corp"},
            {"id": 2, "name": "Globex"},
        ])
    }
    mapping = {
        "total": "amount",
        "vendor_name": {
            "pick": {
                "objects": "vendors",
                "id_field": "id",
                "filter_ids": "rec.vendor_id",
                "target_fields": "name",
            }
        }
    }

    result = gs.map_fields(row, mapping, other_data)
    expected = {"total": 750, "vendor_name": "Globex"}

    assert result == expected
    print("test_map_fields_pick_mixed_with_flat output is correct")


# -------------------------------------------------------------------
# Tests for the "pickregex" branch in map_fields
# -------------------------------------------------------------------

def test_map_fields_pickregex_basic():
    """Test pickregex branch: match keys by regex pattern."""
    print("=====")
    print("test_map_fields_pickregex_basic")

    row = {"cf_color": "red", "cf_size": "large", "name": "Widget"}
    mapping = {
        "custom_fields": {
            "pickregex": {
                "regex_field": "^cf_",
            }
        }
    }

    result = gs.map_fields(row, mapping)
    expected = {"custom_fields": {"cf_color": "red", "cf_size": "large"}}

    assert result == expected
    print("test_map_fields_pickregex_basic output is correct")


def test_map_fields_pickregex_no_match():
    """Test pickregex branch: no keys match the regex."""
    print("=====")
    print("test_map_fields_pickregex_no_match")

    row = {"name": "Widget", "price": 9.99}
    mapping = {
        "custom_fields": {
            "pickregex": {
                "regex_field": "^cf_",
            }
        }
    }

    result = gs.map_fields(row, mapping)
    expected = {"custom_fields": {}}

    assert result == expected
    print("test_map_fields_pickregex_no_match output is correct")


def test_map_fields_pickregex_return_as_cf():
    """Test pickregex branch with return_as_cf=True (custom field structure)."""
    print("=====")
    print("test_map_fields_pickregex_return_as_cf")

    row = {"cf_color": "red", "cf_size": "large", "name": "Widget"}
    mapping = {
        "custom_fields": {
            "pickregex": {
                "regex_field": "^cf_",
                "return_as_cf": True,
            }
        }
    }

    result = gs.map_fields(row, mapping)
    # return_as_cf transforms dict into list of {"name": key, "value": value}
    cf_list = result["custom_fields"]

    assert isinstance(cf_list, list)
    assert len(cf_list) == 2
    assert {"name": "cf_color", "value": "red"} in cf_list
    assert {"name": "cf_size", "value": "large"} in cf_list
    print("test_map_fields_pickregex_return_as_cf output is correct")


def test_map_fields_pickregex_mixed_with_flat():
    """Test pickregex branch alongside regular flat field mappings."""
    print("=====")
    print("test_map_fields_pickregex_mixed_with_flat")

    row = {"cf_tier": "gold", "name": "Widget", "price": 9.99}
    mapping = {
        "product_name": "name",
        "product_price": "price",
        "extras": {
            "pickregex": {
                "regex_field": "^cf_",
            }
        }
    }

    result = gs.map_fields(row, mapping)
    expected = {
        "product_name": "Widget",
        "product_price": 9.99,
        "extras": {"cf_tier": "gold"},
    }

    assert result == expected
    print("test_map_fields_pickregex_mixed_with_flat output is correct")


def test_map_fields_pickregex_complex_pattern():
    """Test pickregex branch with a more specific regex pattern."""
    print("=====")
    print("test_map_fields_pickregex_complex_pattern")

    row = {
        "addr_line1": "123 Main",
        "addr_line2": "Suite 4",
        "addr_city": "NYC",
        "phone": "555-1234",
        "name": "John",
    }
    mapping = {
        "address": {
            "pickregex": {
                "regex_field": "^addr_line\\d+$",
            }
        }
    }

    result = gs.map_fields(row, mapping)
    expected = {"address": {"addr_line1": "123 Main", "addr_line2": "Suite 4"}}

    assert result == expected
    print("test_map_fields_pickregex_complex_pattern output is correct")
