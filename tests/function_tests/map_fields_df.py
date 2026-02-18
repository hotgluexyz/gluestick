import gluestick as gs
import pandas as pd


# -------------------------------------------------------------------
# Simple (scalar) mapping tests
# -------------------------------------------------------------------

def test_simple_rename_columns():
    """Simple string mappings rename columns correctly."""
    print("=====")
    print("test_simple_rename_columns")

    df = pd.DataFrame({
        "first_name": ["Alice", "Bob"],
        "last_name": ["Smith", "Jones"],
        "age": [30, 25],
    })
    mapping = {
        "name": "first_name",
        "surname": "last_name",
        "years": "age",
    }

    result = gs.map_fields_df(df.copy(), mapping, {})

    assert "name" in result.columns
    assert "surname" in result.columns
    assert "years" in result.columns
    assert list(result["name"]) == ["Alice", "Bob"]
    assert list(result["surname"]) == ["Smith", "Jones"]
    assert list(result["years"]) == [30, 25]
    print("test_simple_rename_columns output is correct")


def test_simple_mapping_none_values_become_none():
    """NaN values in the source column are mapped to None."""
    print("=====")
    print("test_simple_mapping_none_values_become_none")

    df = pd.DataFrame({
        "city": ["NYC", None, "LA"],
    })
    mapping = {"location": "city"}

    result = gs.map_fields_df(df.copy(), mapping, {})

    assert result["location"].iloc[0] == "NYC"
    assert result["location"].iloc[1] is None
    assert result["location"].iloc[2] == "LA"
    print("test_simple_mapping_none_values_become_none output is correct")


def test_simple_mapping_missing_source_column():
    """When source column doesn't exist in the DataFrame, the new column is not created."""
    print("=====")
    print("test_simple_mapping_missing_source_column")

    df = pd.DataFrame({"name": ["Alice"]})
    mapping = {"person_age": "age"}  # 'age' not in df

    result = gs.map_fields_df(df.copy(), mapping, {})

    assert "person_age" not in result.columns
    print("test_simple_mapping_missing_source_column output is correct")


def test_simple_mapping_preserves_original_columns():
    """Original columns are preserved alongside the new mapped columns."""
    print("=====")
    print("test_simple_mapping_preserves_original_columns")

    df = pd.DataFrame({"first_name": ["Alice"], "age": [30]})
    mapping = {"name": "first_name"}

    result = gs.map_fields_df(df.copy(), mapping, {})

    assert "first_name" in result.columns
    assert "age" in result.columns
    assert "name" in result.columns
    print("test_simple_mapping_preserves_original_columns output is correct")


def test_returns_dataframe():
    """map_fields_df always returns a DataFrame."""
    print("=====")
    print("test_returns_dataframe")

    df = pd.DataFrame({"x": [1]})
    result = gs.map_fields_df(df.copy(), {"y": "x"}, {})

    assert isinstance(result, pd.DataFrame)
    print("test_returns_dataframe output is correct")


# -------------------------------------------------------------------
# Pick mapping tests (merge-based lookup)
# -------------------------------------------------------------------

def test_pick_single_target_field():
    """Pick maps a single target field via merge from other_data."""
    print("=====")
    print("test_pick_single_target_field")

    df = pd.DataFrame({
        "vendor_id": [1, 2, 3],
        "amount": [100, 200, 300],
    })
    other_data = {
        "vendors": pd.DataFrame([
            {"id": 1, "name": "Acme Corp"},
            {"id": 2, "name": "Globex"},
            {"id": 3, "name": "Initech"},
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

    result = gs.map_fields_df(df.copy(), mapping, other_data)

    assert list(result["vendor_name"]) == ["Acme Corp", "Globex", "Initech"]
    print("test_pick_single_target_field output is correct")


def test_pick_no_match_yields_none():
    """Pick yields None for rows with no matching ID in other_data."""
    print("=====")
    print("test_pick_no_match_yields_none")

    df = pd.DataFrame({
        "vendor_id": [1, 999],
        "amount": [100, 200],
    })
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

    result = gs.map_fields_df(df.copy(), mapping, other_data)

    assert result["vendor_name"].iloc[0] == "Acme Corp"
    assert result["vendor_name"].iloc[1] is None
    print("test_pick_no_match_yields_none output is correct")


def test_pick_empty_other_data_skips():
    """Pick with an empty DataFrame in other_data skips the column entirely."""
    print("=====")
    print("test_pick_empty_other_data_skips")

    df = pd.DataFrame({"vendor_id": [1], "amount": [100]})
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

    result = gs.map_fields_df(df.copy(), mapping, other_data)

    assert "vendor_name" not in result.columns
    print("test_pick_empty_other_data_skips output is correct")


def test_pick_missing_other_data_key_skips():
    """Pick with a missing key in other_data skips the column entirely."""
    print("=====")
    print("test_pick_missing_other_data_key_skips")

    df = pd.DataFrame({"vendor_id": [1]})
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

    result = gs.map_fields_df(df.copy(), mapping, {})

    assert "vendor_name" not in result.columns
    print("test_pick_missing_other_data_key_skips output is correct")


def test_pick_with_partition_key():
    """Pick with a partition_key filters by both ID and partition value."""
    print("=====")
    print("test_pick_with_partition_key")

    df = pd.DataFrame({
        "vendor_id": [1, 1, 2],
        "dept": ["HR", "IT", "HR"],
        "amount": [100, 200, 300],
    })
    other_data = {
        "vendors": pd.DataFrame([
            {"id": 1, "name": "Alice-HR", "dept": "HR"},
            {"id": 1, "name": "Alice-IT", "dept": "IT"},
            {"id": 2, "name": "Bob-HR", "dept": "HR"},
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

    result = gs.map_fields_df(df.copy(), mapping, other_data)

    assert list(result["vendor_name"]) == ["Alice-HR", "Alice-IT", "Bob-HR"]
    print("test_pick_with_partition_key output is correct")


def test_pick_deduplicates_on_id():
    """Pick keeps only the last duplicate per id_field (drop_duplicates keep='last')."""
    print("=====")
    print("test_pick_deduplicates_on_id")

    df = pd.DataFrame({"vendor_id": [1]})
    other_data = {
        "vendors": pd.DataFrame([
            {"id": 1, "name": "Old Name"},
            {"id": 1, "name": "New Name"},
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

    result = gs.map_fields_df(df.copy(), mapping, other_data)

    assert result["vendor_name"].iloc[0] == "New Name"
    print("test_pick_deduplicates_on_id output is correct")


# -------------------------------------------------------------------
# Pick with is_list=True (apply-based lookup)
# -------------------------------------------------------------------

def test_pick_is_list_single_field():
    """Pick with is_list=True uses apply to return looked-up values per row."""
    print("=====")
    print("test_pick_is_list_single_field")

    df = pd.DataFrame({
        "tag_ids": [[1, 3], [2]],
    })
    other_data = {
        "tags": pd.DataFrame([
            {"id": 1, "label": "urgent"},
            {"id": 2, "label": "low"},
            {"id": 3, "label": "medium"},
        ])
    }
    mapping = {
        "tag_labels": {
            "pick": {
                "objects": "tags",
                "id_field": "id",
                "filter_ids": "rec.tag_ids",
                "target_fields": "label",
                "is_list": True,
            }
        }
    }

    result = gs.map_fields_df(df.copy(), mapping, other_data)

    assert result["tag_labels"].iloc[0] == ["urgent", "medium"]
    assert result["tag_labels"].iloc[1] == ["low"]
    print("test_pick_is_list_single_field output is correct")


def test_pick_is_list_with_partition_key():
    """Pick with is_list=True and partition_key filters correctly."""
    print("=====")
    print("test_pick_is_list_with_partition_key")

    df = pd.DataFrame({
        "tag_ids": [[1, 2]],
        "dept": ["HR"],
    })
    other_data = {
        "tags": pd.DataFrame([
            {"id": 1, "label": "urgent", "dept": "HR"},
            {"id": 1, "label": "urgent-IT", "dept": "IT"},
            {"id": 2, "label": "low", "dept": "HR"},
        ])
    }
    mapping = {
        "tag_labels": {
            "pick": {
                "objects": "tags",
                "id_field": "id",
                "filter_ids": "rec.tag_ids",
                "target_fields": "label",
                "is_list": True,
                "partition_key": "dept",
            }
        }
    }

    result = gs.map_fields_df(df.copy(), mapping, other_data)

    assert result["tag_labels"].iloc[0] == ["urgent", "low"]
    print("test_pick_is_list_with_partition_key output is correct")


# -------------------------------------------------------------------
# Mixed mapping tests
# -------------------------------------------------------------------

def test_mixed_simple_and_pick():
    """Simple and pick mappings can be combined in the same call."""
    print("=====")
    print("test_mixed_simple_and_pick")

    df = pd.DataFrame({
        "vendor_id": [1, 2],
        "amount": [500, 750],
    })
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

    result = gs.map_fields_df(df.copy(), mapping, other_data)

    assert list(result["total"]) == [500, 750]
    assert list(result["vendor_name"]) == ["Acme Corp", "Globex"]
    print("test_mixed_simple_and_pick output is correct")


def test_empty_dataframe():
    """Mapping an empty DataFrame returns an empty DataFrame without errors."""
    print("=====")
    print("test_empty_dataframe")

    df = pd.DataFrame(columns=["first_name", "age"])
    mapping = {"name": "first_name", "years": "age"}

    result = gs.map_fields_df(df.copy(), mapping, {})

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0
    assert "name" in result.columns
    assert "years" in result.columns
    print("test_empty_dataframe output is correct")


def test_no_complex_mappings():
    """When all mappings are simple, complex branch is never entered."""
    print("=====")
    print("test_no_complex_mappings")

    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    mapping = {"x": "a", "y": "b"}

    result = gs.map_fields_df(df.copy(), mapping, {})

    assert list(result["x"]) == [1, 2]
    assert list(result["y"]) == [3, 4]
    print("test_no_complex_mappings output is correct")
