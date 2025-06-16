import os
import json
from typing import List, Dict, Any, Optional
from pathlib import Path

from src.vector_db.vector_store import VectorStore
from src.models.dsl_models import (
    DSLType, DSLComponent, DSLColumn, DSLTable, DSLJoin, DSLFilter, 
    DSLAggregateFn, DSLGroupBy, DSLOrderBy, DSLOperator, DSLAggregate, DSLTimeframe
)
from src.db.schema_loader import SchemaLoader
from src.db.database import Database


class VectorLoader:
    """
    Utility to load DSL components into the vector database from various sources:
    1. Database schema (tables, columns)
    2. Predefined DSL patterns (filters, joins, aggregations)
    3. Custom DSL components
    """
    
    def __init__(self, vector_store: VectorStore, schema_loader: Optional[SchemaLoader] = None):
        self.vector_store = vector_store
        self.schema_loader = schema_loader
        
        if not self.schema_loader:
            db = Database()
            self.schema_loader = SchemaLoader(db)
    
    def load_schema_components(self) -> None:
        """Load database schema components into the vector database"""
        # Ensure schema is loaded
        schema = self.schema_loader.load_schema_from_file()
        if not schema:
            schema = self.schema_loader.load_schema_from_db()
        
        # Create table components
        table_components = []
        for table_name, table_info in schema.items():
            descriptions = self._generate_table_descriptions(table_name)
            
            for description in descriptions:
                table_components.append(
                    DSLTable(
                        table_name=table_name,
                        text=description
                    )
                )
        
        # Create column components
        column_components = []
        for table_name, table_info in schema.items():
            for column in table_info['columns']:
                column_name = column['column_name']
                descriptions = self._generate_column_descriptions(table_name, column_name, column)
                
                for description in descriptions:
                    column_components.append(
                        DSLColumn(
                            column_name=column_name,
                            table_name=table_name,
                            text=description
                        )
                    )
        
        # Add components to vector store
        print(f"Loading {len(table_components)} table components...")
        self.vector_store.add_components(table_components)
        
        print(f"Loading {len(column_components)} column components...")
        self.vector_store.add_components(column_components)
    
    def load_predefined_components(self, components_dir: str = "data/dsl_components") -> None:
        """Load predefined DSL components from JSON files"""
        Path(components_dir).mkdir(parents=True, exist_ok=True)
        
        # Check for component files
        for component_type in DSLType:
            file_path = os.path.join(components_dir, f"{component_type.value.lower()}_components.json")
            
            if os.path.exists(file_path):
                with open(file_path, 'r') as f:
                    components_data = json.load(f)
                
                # Create components
                components = []
                for data in components_data:
                    from src.dsl.parser import create_dsl_component
                    component = create_dsl_component(data)
                    components.append(component)
                
                # Add to vector store
                print(f"Loading {len(components)} {component_type.value} components...")
                self.vector_store.add_components(components)
    
    def generate_join_components(self) -> None:
        """Generate join components based on schema relationships"""
        # Get join paths from schema loader
        join_paths = self.schema_loader.generate_join_paths()
        
        # Create join components
        join_components = []
        
        for source_table, targets in join_paths.items():
            for target_table, path in targets.items():
                if path:
                    # Create join components
                    for i, relation in enumerate(path):
                        left_table = DSLTable(
                            table_name=relation['source_table'],
                            text=relation['source_table']
                        )
                        
                        right_table = DSLTable(
                            table_name=relation['target_table'],
                            text=relation['target_table']
                        )
                        
                        # Create descriptions
                        descriptions = [
                            f"Join {relation['source_table']} with {relation['target_table']}",
                            f"Connect {relation['source_table']} to {relation['target_table']}",
                            f"Link {relation['source_table']} and {relation['target_table']}"
                        ]
                        
                        for description in descriptions:
                            join_components.append(
                                DSLJoin(
                                    left_table=left_table,
                                    right_table=right_table,
                                    join_type="INNER",
                                    join_condition=[{
                                        "left_column": relation['source_column'],
                                        "right_column": relation['target_column']
                                    }],
                                    text=description
                                )
                            )
        
        # Add to vector store
        print(f"Loading {len(join_components)} join components...")
        self.vector_store.add_components(join_components)
    
    def generate_filter_components(self) -> None:
        """Generate filter components for common patterns"""
        # Ensure schema is loaded
        schema = self.schema_loader.schema
        if not schema:
            schema = self.schema_loader.load_schema_from_db()
        
        # Create filter components
        filter_components = []
        
        # Generate date-based filters
        date_columns = []
        for table_name, table_info in schema.items():
            for column in table_info['columns']:
                column_name = column['column_name']
                data_type = column['data_type']
                
                if 'date' in data_type.lower() or 'time' in data_type.lower():
                    date_columns.append((table_name, column_name))
        
        # Create date filter components
        for table_name, column_name in date_columns:
            # Create column object
            column = DSLColumn(
                column_name=column_name,
                table_name=table_name,
                text=f"{table_name}.{column_name}"
            )
            
            # Create filter descriptions for time periods
            time_periods = [
                ("today", DSLTimeframe.DAY.value),
                ("this week", DSLTimeframe.WEEK.value),
                ("this month", DSLTimeframe.MONTH.value),
                ("this quarter", DSLTimeframe.QUARTER.value),
                ("this year", DSLTimeframe.YEAR.value),
                ("yesterday", DSLTimeframe.LAST_DAY.value),
                ("last week", DSLTimeframe.LAST_WEEK.value),
                ("last month", DSLTimeframe.LAST_MONTH.value),
                ("last quarter", DSLTimeframe.LAST_QUARTER.value),
                ("last year", DSLTimeframe.LAST_YEAR.value)
            ]
            
            for period_text, period_value in time_periods:
                descriptions = [
                    f"Filter {table_name} for {period_text}",
                    f"Get {table_name} data from {period_text}",
                    f"Show {table_name} records for {period_text}"
                ]
                
                for description in descriptions:
                    filter_components.append(
                        DSLFilter(
                            column=column,
                            operator=DSLOperator.EQUALS,
                            value={"timeframe": period_value},
                            text=description
                        )
                    )
        
        # Add numeric filters
        numeric_columns = []
        for table_name, table_info in schema.items():
            for column in table_info['columns']:
                column_name = column['column_name']
                data_type = column['data_type']
                
                if any(t in data_type.lower() for t in ['int', 'float', 'numeric', 'decimal']):
                    numeric_columns.append((table_name, column_name))
        
        # Create numeric filter components
        for table_name, column_name in numeric_columns:
            # Create column object
            column = DSLColumn(
                column_name=column_name,
                table_name=table_name,
                text=f"{table_name}.{column_name}"
            )
            
            # Create filter descriptions for comparison operators
            comparisons = [
                ("greater than", DSLOperator.GREATER_THAN, "100"),
                ("less than", DSLOperator.LESS_THAN, "50"),
                ("equal to", DSLOperator.EQUALS, "75"),
                ("between", DSLOperator.BETWEEN, ["10", "20"]),
                ("at least", DSLOperator.GREATER_THAN_EQUALS, "30"),
                ("at most", DSLOperator.LESS_THAN_EQUALS, "40")
            ]
            
            for comp_text, comp_op, comp_value in comparisons:
                descriptions = [
                    f"Filter {table_name} where {column_name} is {comp_text} {comp_value}",
                    f"Get {table_name} data with {column_name} {comp_text} {comp_value}",
                    f"Show {table_name} records where {column_name} is {comp_text} {comp_value}"
                ]
                
                for description in descriptions:
                    filter_components.append(
                        DSLFilter(
                            column=column,
                            operator=comp_op,
                            value=comp_value,
                            text=description
                        )
                    )
        
        # Add to vector store
        print(f"Loading {len(filter_components)} filter components...")
        self.vector_store.add_components(filter_components)
    
    def generate_aggregate_components(self) -> None:
        """Generate aggregate function components"""
        # Ensure schema is loaded
        schema = self.schema_loader.schema
        if not schema:
            schema = self.schema_loader.load_schema_from_db()
        
        # Create aggregate components
        aggregate_components = []
        
        # Define aggregates to generate
        aggregates = [
            (DSLAggregate.COUNT, "count", "counting"),
            (DSLAggregate.SUM, "sum", "total"),
            (DSLAggregate.AVG, "average", "mean"),
            (DSLAggregate.MIN, "minimum", "lowest"),
            (DSLAggregate.MAX, "maximum", "highest")
        ]
        
        # Generate for all numeric columns
        for table_name, table_info in schema.items():
            for column in table_info['columns']:
                column_name = column['column_name']
                data_type = column['data_type']
                
                if any(t in data_type.lower() for t in ['int', 'float', 'numeric', 'decimal']):
                    # Create column object
                    dsl_column = DSLColumn(
                        column_name=column_name,
                        table_name=table_name,
                        text=f"{table_name}.{column_name}"
                    )
                    
                    # Create aggregate components
                    for agg_type, agg_name, agg_alt in aggregates:
                        descriptions = [
                            f"Calculate {agg_name} of {column_name} in {table_name}",
                            f"Find {agg_alt} {column_name} for {table_name}",
                            f"Get {agg_name} {column_name} from {table_name}"
                        ]
                        
                        for description in descriptions:
                            aggregate_components.append(
                                DSLAggregateFn(
                                    function=agg_type,
                                    column=dsl_column,
                                    text=description
                                )
                            )
        
        # Add to vector store
        print(f"Loading {len(aggregate_components)} aggregate components...")
        self.vector_store.add_components(aggregate_components)
    
    def generate_group_by_components(self) -> None:
        """Generate GROUP BY components"""
        # Ensure schema is loaded
        schema = self.schema_loader.schema
        if not schema:
            schema = self.schema_loader.load_schema_from_db()
        
        # Create group by components
        group_by_components = []
        
        # Generate for categorical columns
        for table_name, table_info in schema.items():
            for column in table_info['columns']:
                column_name = column['column_name']
                
                # Skip id or numeric columns for grouping
                if column_name.endswith('_id') or column_name == 'id':
                    continue
                
                # Create column object
                dsl_column = DSLColumn(
                    column_name=column_name,
                    table_name=table_name,
                    text=f"{table_name}.{column_name}"
                )
                
                # Create group by descriptions
                descriptions = [
                    f"Group by {column_name} in {table_name}",
                    f"Aggregate data by {column_name}",
                    f"Summarize {table_name} by {column_name}"
                ]
                
                for description in descriptions:
                    group_by_components.append(
                        DSLGroupBy(
                            columns=[dsl_column],
                            text=description
                        )
                    )
        
        # Add to vector store
        print(f"Loading {len(group_by_components)} group by components...")
        self.vector_store.add_components(group_by_components)
    
    def _generate_table_descriptions(self, table_name: str) -> List[str]:
        """Generate natural language descriptions for a table"""
        # Generate multiple variations for better matching
        descriptions = [
            f"table {table_name}",
            f"data from {table_name}",
            f"information in {table_name}",
            f"{table_name} records",
            f"{table_name} table"
        ]
        
        # Convert underscores to spaces for more natural language
        table_name_natural = table_name.replace('_', ' ')
        
        descriptions.extend([
            f"table {table_name_natural}",
            f"data from {table_name_natural}",
            f"information in {table_name_natural}",
            f"{table_name_natural} records",
            f"{table_name_natural} table"
        ])
        
        return descriptions
    
    def _generate_column_descriptions(self, table_name: str, column_name: str, column_info: Dict[str, Any]) -> List[str]:
        """Generate natural language descriptions for a column"""
        # Generate multiple variations for better matching
        descriptions = [
            f"{column_name} in {table_name}",
            f"{table_name}.{column_name}",
            f"{column_name} from {table_name}",
            f"{column_name} column in {table_name} table"
        ]
        
        # Convert underscores to spaces for more natural language
        column_name_natural = column_name.replace('_', ' ')
        table_name_natural = table_name.replace('_', ' ')
        
        descriptions.extend([
            f"{column_name_natural} in {table_name_natural}",
            f"{column_name_natural} from {table_name_natural}",
            f"{column_name_natural} column in {table_name_natural} table"
        ])
        
        return descriptions


def main():
    """Main function to load all components into the vector database"""
    db = Database()
    schema_loader = SchemaLoader(db)
    vector_store = VectorStore()
    
    loader = VectorLoader(vector_store, schema_loader)
    
    # Load schema components
    loader.load_schema_components()
    
    # Generate join components
    loader.generate_join_components()
    
    # Generate filter components
    loader.generate_filter_components()
    
    # Generate aggregate components
    loader.generate_aggregate_components()
    
    # Generate group by components
    loader.generate_group_by_components()
    
    # Load predefined components
    loader.load_predefined_components()
    
    print("All components loaded into vector database.")
    db.disconnect()


if __name__ == "__main__":
    main() 