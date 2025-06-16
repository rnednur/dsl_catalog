import os
import json
from typing import Dict, Any, List, Optional, Union
import logging

from src.dsl.parser import DSLParser
from src.dsl.sql_generator import SQLGenerator
from src.vector_db.vector_store import VectorStore
from src.db.database import Database
from src.db.schema_loader import SchemaLoader
from src.models.dsl_models import DSLType, DSLQuery

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class NL2SQL:
    """
    Natural Language to SQL converter using DSL as an intermediate representation
    with vector database for improved accuracy.
    """
    
    def __init__(self):
        self.db = Database()
        self.schema_loader = SchemaLoader(self.db)
        self.vector_store = VectorStore()
        self.dsl_parser = DSLParser()
        self.sql_generator = SQLGenerator(self.schema_loader)
        
        # Load schema information
        try:
            self.schema = self.schema_loader.load_schema_from_file()
            if not self.schema:
                self.schema = self.schema_loader.load_schema_from_db()
                self.schema_loader.save_schema_to_file()
        except Exception as e:
            logger.error(f"Error loading schema: {e}")
            self.schema = {}
    
    def process_query(self, query: str) -> Dict[str, Any]:
        """
        Process a natural language query and return the SQL query and results
        
        Args:
            query: Natural language query
            
        Returns:
            Dict containing DSL, SQL query, and results
        """
        logger.info(f"Processing query: {query}")
        
        try:
            # 1. Parse natural language to DSL
            dsl_query = self.dsl_parser.parse_query(query)
            logger.info(f"Generated DSL: {dsl_query.dsl_text}")
            
            # 2. Use DSL components to search vector DB for similar patterns
            enhanced_dsl_query = self._enhance_dsl_with_vector_db(dsl_query)
            
            # 3. Generate SQL from enhanced DSL
            sql_query = self.sql_generator.generate_sql(enhanced_dsl_query)
            logger.info(f"Generated SQL: {sql_query}")
            
            # 4. Execute SQL query
            try:
                results = self.db.execute_query(sql_query)
                logger.info(f"Query returned {len(results)} results")
            except Exception as e:
                logger.error(f"Error executing SQL query: {e}")
                results = []
            
            # 5. Return all information
            return {
                "natural_language_query": query,
                "dsl_query": enhanced_dsl_query.dsl_text,
                "sql_query": sql_query,
                "results": results,
                "dsl_components": self._serialize_dsl_query(enhanced_dsl_query)
            }
            
        except Exception as e:
            logger.error(f"Error processing query: {e}")
            return {
                "natural_language_query": query,
                "error": str(e)
            }
    
    def _enhance_dsl_with_vector_db(self, dsl_query: DSLQuery) -> DSLQuery:
        """
        Enhance DSL query with components from vector DB
        
        Args:
            dsl_query: Original DSL query
            
        Returns:
            Enhanced DSL query
        """
        # Create a dict to hold the enhanced components
        enhanced = {}
        
        # Use DSL text to search for each component type
        dsl_text = dsl_query.dsl_text
        
        # 1. Check for table components
        if not dsl_query.from_ or len(dsl_query.from_) == 0:
            table_components = self.vector_store.search(dsl_text, DSLType.TABLE, top_k=3)
            if table_components:
                enhanced["from_"] = table_components
        
        # 2. Check for column components
        if not dsl_query.select or len(dsl_query.select) == 0:
            column_components = self.vector_store.search(dsl_text, DSLType.COLUMN, top_k=5)
            if column_components:
                enhanced["select"] = column_components
        
        # 3. Check for join components if multiple tables
        if dsl_query.from_ and len(dsl_query.from_) >= 2 and not dsl_query.joins:
            join_components = self.vector_store.search(dsl_text, DSLType.JOIN, top_k=3)
            if join_components:
                enhanced["joins"] = join_components
        
        # 4. Check for filter components
        if not dsl_query.where or len(dsl_query.where) == 0:
            filter_components = self.vector_store.search(dsl_text, DSLType.FILTER, top_k=3)
            if filter_components:
                enhanced["where"] = filter_components
        
        # 5. Check for aggregate components
        if "aggregate" in dsl_text.lower() or "count" in dsl_text.lower() or "sum" in dsl_text.lower():
            if not any(hasattr(item, "function") for item in dsl_query.select):
                agg_components = self.vector_store.search(dsl_text, DSLType.AGGREGATE, top_k=3)
                if agg_components:
                    # Replace select items with aggregates
                    enhanced["select"] = agg_components
        
        # 6. Check for group by components
        if "group" in dsl_text.lower() and not dsl_query.group_by:
            group_components = self.vector_store.search(dsl_text, DSLType.GROUP_BY, top_k=2)
            if group_components:
                enhanced["group_by"] = group_components[0]
        
        # 7. Check for order by components
        if ("order" in dsl_text.lower() or "sort" in dsl_text.lower()) and not dsl_query.order_by:
            order_components = self.vector_store.search(dsl_text, DSLType.ORDER_BY, top_k=2)
            if order_components:
                enhanced["order_by"] = order_components[0]
        
        # Update original DSL query with enhanced components
        for key, value in enhanced.items():
            setattr(dsl_query, key, value)
        
        # Regenerate DSL text
        dsl_query.dsl_text = self._regenerate_dsl_text(dsl_query)
        
        return dsl_query
    
    def _regenerate_dsl_text(self, dsl_query: DSLQuery) -> str:
        """
        Regenerate DSL text from DSL query components
        
        Args:
            dsl_query: DSL query
            
        Returns:
            Updated DSL text
        """
        parts = []
        
        # SELECT
        select_text = "SELECT " + ", ".join([item.text for item in dsl_query.select])
        parts.append(select_text)
        
        # FROM
        from_text = "FROM " + ", ".join([table.text for table in dsl_query.from_])
        parts.append(from_text)
        
        # JOIN
        if dsl_query.joins:
            join_text = "JOIN " + " AND ".join([join.text for join in dsl_query.joins])
            parts.append(join_text)
        
        # WHERE
        if dsl_query.where:
            where_text = "WHERE " + " AND ".join([filter.text for filter in dsl_query.where])
            parts.append(where_text)
        
        # GROUP BY
        if dsl_query.group_by:
            parts.append(dsl_query.group_by.text)
        
        # ORDER BY
        if dsl_query.order_by:
            parts.append(dsl_query.order_by.text)
        
        # LIMIT
        if dsl_query.limit:
            parts.append(dsl_query.limit.text)
        
        return " ; ".join(parts)
    
    def _serialize_dsl_query(self, dsl_query: DSLQuery) -> Dict[str, Any]:
        """
        Serialize DSL query to dict
        
        Args:
            dsl_query: DSL query
            
        Returns:
            Dict representation of DSL query
        """
        return {
            "select": [component.dict() for component in dsl_query.select],
            "from": [table.dict() for table in dsl_query.from_],
            "joins": [join.dict() for join in dsl_query.joins] if dsl_query.joins else None,
            "where": [filter.dict() for filter in dsl_query.where] if dsl_query.where else None,
            "group_by": dsl_query.group_by.dict() if dsl_query.group_by else None,
            "order_by": dsl_query.order_by.dict() if dsl_query.order_by else None,
            "limit": dsl_query.limit.dict() if dsl_query.limit else None
        }
    
    def close(self):
        """Close database connection"""
        self.db.disconnect()


# Singleton instance for global use
_nl2sql_instance = None


def get_nl2sql() -> NL2SQL:
    """Get or create NL2SQL instance"""
    global _nl2sql_instance
    if _nl2sql_instance is None:
        _nl2sql_instance = NL2SQL()
    return _nl2sql_instance


def nl2sql(query: str) -> Dict[str, Any]:
    """
    Convert natural language to SQL
    
    Args:
        query: Natural language query
        
    Returns:
        Dict containing DSL, SQL query, and results
    """
    instance = get_nl2sql()
    return instance.process_query(query)


def main():
    """Main function for command-line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Convert natural language to SQL')
    parser.add_argument('query', nargs='?', help='Natural language query')
    parser.add_argument('--interactive', '-i', action='store_true', help='Interactive mode')
    
    args = parser.parse_args()
    
    if args.interactive:
        print("=== NL2SQL Interactive Mode ===")
        print("Type 'exit' or 'quit' to exit.")
        instance = get_nl2sql()
        
        while True:
            query = input("\nEnter query: ")
            if query.lower() in ['exit', 'quit']:
                break
            
            result = instance.process_query(query)
            
            if 'error' in result:
                print(f"Error: {result['error']}")
            else:
                print("\nDSL Query:")
                print(result['dsl_query'])
                print("\nSQL Query:")
                print(result['sql_query'])
                print("\nResults:")
                if result['results']:
                    # Pretty print first 5 results
                    for i, row in enumerate(result['results'][:5]):
                        print(f"{i+1}. {json.dumps(row, indent=2)}")
                    if len(result['results']) > 5:
                        print(f"...and {len(result['results']) - 5} more results.")
                else:
                    print("No results returned.")
        
        instance.close()
    elif args.query:
        result = nl2sql(args.query)
        print(json.dumps(result, indent=2))
        get_nl2sql().close()
    else:
        parser.print_help()


if __name__ == "__main__":
    main() 