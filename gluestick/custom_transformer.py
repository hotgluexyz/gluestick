from singer.transform import Transformer, SchemaKey

class CustomTransformer(Transformer):

    def _transform(self, data, typ, schema, path):
        if self.pre_hook:
            data = self.pre_hook(data, typ, schema)

        if typ == "null":
            if data is None or data == "":
                return True, None
            else:
                return False, None

        elif schema.get("format") == "date-time":
            data = self._transform_datetime(data)
            if data is None:
                return False, None

            return True, data
        elif schema.get("format") == "singer.decimal":
            if data is None:
                return False, None

            if isinstance(data, (str, float, int)):
                try:
                    return True, str(decimal.Decimal(str(data)))
                except:
                    return False, None
            elif isinstance(data, decimal.Decimal):
                try:
                    if data.is_snan():
                        return True, 'NaN'
                    else:
                        return True, str(data)
                except:
                    return False, None

            return False, None
        elif typ == "object":
            # Objects do not necessarily specify properties
            return self._transform_object(data,
                                          schema.get("properties", {}),
                                          path,
                                          schema.get(SchemaKey.pattern_properties))

        elif typ == "array":
            if "items" in schema:
                return self._transform_array(data, schema["items"], path)
            return self._transform_array(data, schema.get("properties", {}), path)

        elif typ == "string":
            if data is not None:
                try:
                    return True, str(data)
                except:
                    return False, None
            else:
                return False, None

        elif typ == "integer":
            if isinstance(data, str):
                data = data.replace(",", "")

            try:
                return True, int(data)
            except:
                return False, None

        elif typ == "number":
            if isinstance(data, str):
                data = data.replace(",", "")

            try:
                return True, float(data)
            except:
                return False, None

        elif typ == "boolean":
            if isinstance(data, str) and data.lower() == "false":
                return True, False

            try:
                return True, bool(data)
            except:
                return False, None

        else:
            return False, None