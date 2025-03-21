import sqlalchemy

def cast_sql_to_typescript_types(sa_type):
        # String types
        if isinstance(sa_type, (sqlalchemy.String, sqlalchemy.Unicode, sqlalchemy.Text,
                                sqlalchemy.UnicodeText, sqlalchemy.CHAR, sqlalchemy.VARCHAR)):
            return "string"
        # Number types
        if isinstance(sa_type, (sqlalchemy.Integer, sqlalchemy.BigInteger,
                                sqlalchemy.SmallInteger, sqlalchemy.Float, sqlalchemy.Numeric)):
            return "number"
        # Boolean type
        if isinstance(sa_type, sqlalchemy.Boolean):
            return "boolean"
        # Date types
        if isinstance(sa_type, (sqlalchemy.Date, sqlalchemy.DateTime, sqlalchemy.TIMESTAMP)):
            return "Date"
        if isinstance(sa_type, sqlalchemy.Time):
            return "string" # convert to string for now
        # Array types
        if isinstance(sa_type, sqlalchemy.ARRAY):
            inner_type = cast_sql_to_typescript_types(sa_type.item_type)
            return f"{inner_type}[]"
        # Enum types
        if isinstance(sa_type, sqlalchemy.Enum):
            return "string"
        # Default fallback
        return "string"
    
def cast_informix_to_typescript_types(informix_type):
    """Maps Informix coltype to Typescript types."""
    
    informix_to_ts = {
        0: "string",      # CHAR
        1: "number",      # SMALLINT
        2: "number",      # INTEGER
        3: "number",      # FLOAT
        4: "number",      # SMALLFLOAT
        5: "number",      # DECIMAL
        6: "number",      # SERIAL (Auto-increment INT)
        7: "Date",        # DATE
        8: "number",      # MONEY
        9:  "null",       # NULL (unspecified type)
        10: "Date",       # DATETIME
        11: "binary",     # BYTE
        12: "string",     # TEXT
        13: "string",     # VARCHAR
        14: "string",     # INTERVAL (custom handling needed)
        15: "string",     # NCHAR
        16: "string",     # NVARCHAR
        17: "number",     # INT8
        18: "number",     # SERIAL8 (Auto-increment BIGINT)
        19: "unknown",    # SET (unsupported)
        20: "unknown",    # MULTISET (unsupported)
        21: "unknown",    # LIST (unsupported)
        22: "unknown",    # ROW (unnamed)
        23: "unknown",    # COLLECTION (unsupported)
        40: "string",     # LVARCHAR fixed-length opaque types
        41: "string",     # BLOB, BOOLEAN, CLOB (variable-length opaque types)
        43: "string",     # LVARCHAR (client-side only)
        45: "boolean",    # BOOLEAN
        52: "number",     # BIGINT
        53: "number",     # BIGSERIAL (Auto-increment BIGINT)
        2061: "string",   # IDSSECURITYLABEL (custom security label)
        262: "number",    # DISTINCT type
        4118: "unknown"   # ROW (named)
    }

    return informix_to_ts.get(informix_type, "unknown")  # Default to "unknown" if type is not listed