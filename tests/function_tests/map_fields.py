import gluestick as gs

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