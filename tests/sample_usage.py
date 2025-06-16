from src.dsl.parser import DSLParser
from src.vector_db.vector_store import VectorStore
from src.vector_db.vector_loader import _enhance_dsl_with_vector_db, _serialize_dsl_query
from src.db.schema_loader import SchemaLoader
from src.db.database import Database

# Create database connection
db = Database()

# This is how the flow would work with your intent
nl_query = "Show me sales by region for last quarter"

# 1. Parse NL to initial DSL
dsl_parser = DSLParser()
dsl_query = dsl_parser.parse_query(nl_query)

# 2. Enhance DSL via vector search
vector_store = VectorStore()
enhanced_dsl = _enhance_dsl_with_vector_db(dsl_query, vector_store)

# 3. Get schema metadata for relevant tables
schema_loader = SchemaLoader(db)
table_metadata = {}
for table in enhanced_dsl.from_:
    table_metadata[table.table_name] = schema_loader.get_table_columns(table.table_name)

# 4. Serialize DSL for passing to your LLM
serialized_dsl = _serialize_dsl_query(enhanced_dsl)

# 5. Pass to your existing framework (which you already have)
# your_sql_generator(nl_query, serialized_dsl, table_metadata)