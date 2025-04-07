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
    
def cast_informix_to_typescript_types(informix_type: int) -> str:
    """Maps Informix coltype to Typescript types."""
    
    informix_to_ts = {
        # Basic numeric types
        1: "number",      # SMALLINT
        2: "number",      # INTEGER
        3: "number",      # FLOAT
        4: "number",      # SMALLFLOAT
        5: "number",      # DECIMAL
        6: "number",      # SERIAL (Auto-increment INT)
        8: "number",      # MONEY
        17: "number",     # INT8 (BIGINT)
        18: "number",     # SERIAL8 (Auto-increment BIGINT)
        52: "number",     # BIGINT
        53: "number",     # BIGSERIAL (Auto-increment BIGINT)
        25: "number",     # REFSERIAL
        26: "number",     # REFSERIAL8
        262: "number",    # DISTINCT type (numeric based)
        
        # String types
        0: "string",      # CHAR
        12: "string",     # TEXT (Large character object)
        13: "string",     # VARCHAR
        15: "string",     # NCHAR (Fixed-length Unicode)
        16: "string",     # NVARCHAR (Variable-length Unicode)
        40: "string",     # LVARCHAR (Large variable-length string)
        42: "string",     # CLOB (Character large object)
        43: "string",     # LVARCHAR (Client-side only)
        27: "string",     # LVARCHAR (alternate variant)
        35: "string",     # IDSXML
        37: "string",     # IDSCHARSET
        256: "string",    # IDSXML
        258: "string",    # IDSXML
        269: "string",    # VARCHAR with NOT NULL
        2061: "string",   # IDSSECURITYLABEL (security label string)
        
        # Date and time types
        7: "Date",        # DATE
        10: "Date",       # DATETIME
        14: "string",     # INTERVAL (Duration, might need parsing)
        263: "Date",      # DATE
        
        # Boolean types
        41: "boolean",    # BOOLEAN (newer Informix versions)
        45: "boolean",    # BOOLEAN
        28: "boolean",    # BOOLEAN (alias/variant)
        32: "boolean",    # BOOLEAN (older versions)
        
        # Binary types
        11: "binary",     # BYTE (Binary data)
        31: "binary",     # BLOB
        36: "binary",     # IDSBLOB
        
        # Collection types
        19: "string[]",   # SET (Unordered collection)
        20: "string[]",   # MULTISET (May contain duplicates)
        21: "string[]",   # LIST (Ordered collection)
        23: "any[]",      # COLLECTION (General collection type)
        
        # Record/Composite types
        22: "Record<string, any>",  # ROW (Unnamed composite type)
        24: "Record<string, any>",  # ROW (opaque UDT)
        4117: "Record<string, any>", # ROW (opaque composite)
        4118: "Record<string, any>", # ROW (Named composite type)
        
        # Special types
        9: "null",        # NULL (unspecified type)
    }

    return informix_to_ts.get(informix_type, "unknown")  # Default to "unknown" if type is not listed