import unittest
from src.dsl.sql_generator import SQLGenerator
from src.models.dsl_models import (
    DSLQuery, DSLColumn, DSLTable, DSLJoin, DSLFilter, 
    DSLAggregateFn, DSLGroupBy, DSLOrderBy, DSLLimit, 
    DSLOperator, DSLAggregate
)
from src.vector_db.vector_loader import _enhance_dsl_with_vector_db, _serialize_dsl_query
from src.db.database import Database
from src.db.schema_loader import SchemaLoader
from src.dsl.parser import DSLParser
from src.vector_db.vector_store import VectorStore

class TestSQLGenerator(unittest.TestCase):
    """Tests for the SQL generator"""
    
    def setUp(self):
        """Set up the test"""
        self.sql_generator = SQLGenerator()
    
    def test_generate_simple_query(self):
        """Test generating a simple SELECT query"""
        # Create a simple DSL query: SELECT col1, col2 FROM table1
        col1 = DSLColumn(column_name="col1", text="col1")
        col2 = DSLColumn(column_name="col2", text="col2")
        table1 = DSLTable(table_name="table1", text="table1")
        
        dsl_query = DSLQuery(
            select=[col1, col2],
            from_=[table1],
            original_query="Select col1 and col2 from table1",
            dsl_text="SELECT col1, col2 ; FROM table1"
        )
        
        # Generate SQL
        sql = self.sql_generator.generate_sql(dsl_query)
        
        # Check the SQL
        self.assertIn("SELECT", sql)
        self.assertIn("FROM", sql)
        self.assertIn("col1", sql)
        self.assertIn("col2", sql)
        self.assertIn("table1", sql)
    
    def test_generate_query_with_filter(self):
        """Test generating a query with a WHERE clause"""
        # Create DSL query: SELECT col1 FROM table1 WHERE col2 = 'value'
        col1 = DSLColumn(column_name="col1", text="col1")
        col2 = DSLColumn(column_name="col2", text="col2")
        table1 = DSLTable(table_name="table1", text="table1")
        
        filter1 = DSLFilter(
            column=col2,
            operator=DSLOperator.EQUALS,
            value="value",
            text="col2 equals value"
        )
        
        dsl_query = DSLQuery(
            select=[col1],
            from_=[table1],
            where=[filter1],
            original_query="Select col1 from table1 where col2 equals value",
            dsl_text="SELECT col1 ; FROM table1 ; WHERE col2 equals value"
        )
        
        # Generate SQL
        sql = self.sql_generator.generate_sql(dsl_query)
        
        # Check the SQL
        self.assertIn("WHERE", sql)
        self.assertIn("col2", sql)
        self.assertIn("=", sql)
        self.assertIn("'value'", sql)
    
    def test_generate_query_with_join(self):
        """Test generating a query with a JOIN clause"""
        # Create DSL query: SELECT t1.col1, t2.col2 FROM table1 t1 INNER JOIN table2 t2 ON t1.id = t2.table1_id
        col1 = DSLColumn(column_name="col1", table_name="table1", text="table1.col1")
        col2 = DSLColumn(column_name="col2", table_name="table2", text="table2.col2")
        table1 = DSLTable(table_name="table1", alias="t1", text="table1")
        table2 = DSLTable(table_name="table2", alias="t2", text="table2")
        
        join = DSLJoin(
            left_table=table1,
            right_table=table2,
            join_type="INNER",
            join_condition=[{
                "left_column": "id",
                "right_column": "table1_id"
            }],
            text="Join table1 with table2"
        )
        
        dsl_query = DSLQuery(
            select=[col1, col2],
            from_=[table1],
            joins=[join],
            original_query="Select col1 from table1 and col2 from table2",
            dsl_text="SELECT table1.col1, table2.col2 ; FROM table1 ; JOIN Join table1 with table2"
        )
        
        # Generate SQL
        sql = self.sql_generator.generate_sql(dsl_query)
        
        # Check the SQL
        self.assertIn("JOIN", sql)
        self.assertIn("ON", sql)
        self.assertIn("table1", sql)
        self.assertIn("table2", sql)
    
    def test_generate_query_with_aggregation(self):
        """Test generating a query with aggregation"""
        # Create DSL query: SELECT COUNT(col1) FROM table1
        col1 = DSLColumn(column_name="col1", text="col1")
        table1 = DSLTable(table_name="table1", text="table1")
        
        agg = DSLAggregateFn(
            function=DSLAggregate.COUNT,
            column=col1,
            text="count of col1"
        )
        
        dsl_query = DSLQuery(
            select=[agg],
            from_=[table1],
            original_query="Count col1 from table1",
            dsl_text="SELECT count of col1 ; FROM table1"
        )
        
        # Generate SQL
        sql = self.sql_generator.generate_sql(dsl_query)
        
        # Check the SQL
        self.assertIn("COUNT", sql)
        self.assertIn("col1", sql)
    
    def test_generate_query_with_group_by(self):
        """Test generating a query with a GROUP BY clause"""
        # Create DSL query: SELECT col1, COUNT(col2) FROM table1 GROUP BY col1
        col1 = DSLColumn(column_name="col1", text="col1")
        col2 = DSLColumn(column_name="col2", text="col2")
        table1 = DSLTable(table_name="table1", text="table1")
        
        agg = DSLAggregateFn(
            function=DSLAggregate.COUNT,
            column=col2,
            text="count of col2"
        )
        
        group_by = DSLGroupBy(
            columns=[col1],
            text="Group by col1"
        )
        
        dsl_query = DSLQuery(
            select=[col1, agg],
            from_=[table1],
            group_by=group_by,
            original_query="Count col2 from table1 grouped by col1",
            dsl_text="SELECT col1, count of col2 ; FROM table1 ; Group by col1"
        )
        
        # Generate SQL
        sql = self.sql_generator.generate_sql(dsl_query)
        
        # Check the SQL
        self.assertIn("GROUP BY", sql)
        self.assertIn("col1", sql)
        self.assertIn("COUNT", sql)
    
    def test_generate_query_with_order_by(self):
        """Test generating a query with an ORDER BY clause"""
        # Create DSL query: SELECT col1 FROM table1 ORDER BY col1 DESC
        col1 = DSLColumn(column_name="col1", text="col1")
        table1 = DSLTable(table_name="table1", text="table1")
        
        order_by = DSLOrderBy(
            columns=[col1],
            direction="DESC",
            text="Order by col1 DESC"
        )
        
        dsl_query = DSLQuery(
            select=[col1],
            from_=[table1],
            order_by=order_by,
            original_query="Select col1 from table1 ordered by col1 descending",
            dsl_text="SELECT col1 ; FROM table1 ; Order by col1 DESC"
        )
        
        # Generate SQL
        sql = self.sql_generator.generate_sql(dsl_query)
        
        # Check the SQL
        self.assertIn("ORDER BY", sql)
        self.assertIn("DESC", sql)
    
    def test_generate_query_with_limit(self):
        """Test generating a query with a LIMIT clause"""
        # Create DSL query: SELECT col1 FROM table1 LIMIT 10
        col1 = DSLColumn(column_name="col1", text="col1")
        table1 = DSLTable(table_name="table1", text="table1")
        
        limit = DSLLimit(
            limit=10,
            text="Limit to 10 results"
        )
        
        dsl_query = DSLQuery(
            select=[col1],
            from_=[table1],
            limit=limit,
            original_query="Select col1 from table1 limit 10",
            dsl_text="SELECT col1 ; FROM table1 ; Limit to 10 results"
        )
        
        # Generate SQL
        sql = self.sql_generator.generate_sql(dsl_query)
        
        # Check the SQL
        self.assertIn("LIMIT", sql)
        self.assertIn("10", sql)

    def test_nl2sql_pipeline():
        try:
            # Create database connection
            db = Database()
            
            # Test query
            nl_query = "Show me sales by region for last quarter"
            print(f"Testing query: {nl_query}")
            
            # 1. Parse NL to initial DSL
            dsl_parser = DSLParser()
            dsl_query = dsl_parser.parse_query(nl_query)
            print(f"Initial DSL: {dsl_query.dsl_text}")
            
            # 2. Enhance DSL via vector search
            vector_store = VectorStore()
            enhanced_dsl = _enhance_dsl_with_vector_db(dsl_query, vector_store)
            print(f"Enhanced DSL: {enhanced_dsl.dsl_text}")
            
            # 3. Get schema metadata for relevant tables
            schema_loader = SchemaLoader(db)
            table_metadata = {}
            for table in enhanced_dsl.from_:
                table_metadata[table.table_name] = schema_loader.get_table_columns(table.table_name)
            print(f"Retrieved metadata for tables: {list(table_metadata.keys())}")
            
            # 4. Serialize DSL for passing to your LLM
            serialized_dsl = _serialize_dsl_query(enhanced_dsl)
            print(f"Serialized DSL components: {list(serialized_dsl.keys())}")
            
            # 5. Results summary
            print("\nTest completed successfully!")
            return {
                "query": nl_query,
                "dsl": enhanced_dsl.dsl_text,
                "tables": list(table_metadata.keys()),
                "serialized_dsl": serialized_dsl
            }
        except Exception as e:
            print(f"Error during test: {str(e)}")
            return None


if __name__ == "__main__":
    unittest.main()
    test_result = TestSQLGenerator.test_nl2sql_pipeline()
    print("\nTest result:", "Success" if test_result else "Failed") 