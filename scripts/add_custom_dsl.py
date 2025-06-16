import os
import json
import argparse
from pathlib import Path

from src.models.dsl_models import (
    DSLType, DSLColumn, DSLTable, DSLJoin, DSLFilter, 
    DSLAggregateFn, DSLGroupBy, DSLOrderBy, DSLLimit, 
    DSLOperator, DSLAggregate, DSLTimeframe
)
from src.vector_db.vector_store import VectorStore
from src.dsl.parser import create_dsl_component


def load_components_from_json(file_path: str):
    """Load DSL components from a JSON file"""
    with open(file_path, 'r') as f:
        components_data = json.load(f)
    
    components = []
    for data in components_data:
        component = create_dsl_component(data)
        components.append(component)
    
    return components


def save_components_to_json(components, file_path: str):
    """Save components to a JSON file"""
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    # Convert components to dicts
    component_dicts = [component.dict() for component in components]
    
    # Save to file
    with open(file_path, 'w') as f:
        json.dump(component_dicts, f, indent=2)


def add_custom_filter(table_name: str, column_name: str, operator: str, 
                      value: str, text: str, save_to_file: bool = False):
    """Add a custom filter component to the vector database"""
    # Create column object
    column = DSLColumn(
        column_name=column_name,
        table_name=table_name,
        text=f"{table_name}.{column_name}"
    )
    
    # Map operator string to DSLOperator
    op_map = {
        "equals": DSLOperator.EQUALS,
        "not_equals": DSLOperator.NOT_EQUALS,
        "greater_than": DSLOperator.GREATER_THAN,
        "less_than": DSLOperator.LESS_THAN,
        "greater_than_equals": DSLOperator.GREATER_THAN_EQUALS,
        "less_than_equals": DSLOperator.LESS_THAN_EQUALS,
        "in": DSLOperator.IN,
        "not_in": DSLOperator.NOT_IN,
        "like": DSLOperator.LIKE,
        "not_like": DSLOperator.NOT_LIKE,
        "between": DSLOperator.BETWEEN,
        "not_between": DSLOperator.NOT_BETWEEN,
        "is_null": DSLOperator.IS_NULL,
        "is_not_null": DSLOperator.IS_NOT_NULL
    }
    
    dsl_operator = op_map.get(operator, DSLOperator.EQUALS)
    
    # Process value based on operator
    processed_value = value
    if dsl_operator == DSLOperator.BETWEEN and "," in value:
        processed_value = value.split(",")
    elif dsl_operator == DSLOperator.IN or dsl_operator == DSLOperator.NOT_IN:
        processed_value = value.split(",")
    
    # Create filter component
    filter_component = DSLFilter(
        column=column,
        operator=dsl_operator,
        value=processed_value,
        text=text
    )
    
    # Save to vector DB
    vector_store = VectorStore()
    vector_store.add_component(filter_component)
    print(f"Added filter component: {text}")
    
    # Save to file if requested
    if save_to_file:
        file_path = f"data/dsl_components/{DSLType.FILTER.value.lower()}_components.json"
        
        # Load existing components if file exists
        components = []
        if os.path.exists(file_path):
            try:
                components = load_components_from_json(file_path)
            except Exception as e:
                print(f"Error loading existing components: {e}")
        
        # Add new component and save
        components.append(filter_component)
        save_components_to_json(components, file_path)
        print(f"Saved filter component to {file_path}")


def add_custom_join(left_table: str, right_table: str, left_column: str, 
                    right_column: str, text: str, join_type: str = "INNER", 
                    save_to_file: bool = False):
    """Add a custom join component to the vector database"""
    # Create table objects
    left_table_obj = DSLTable(
        table_name=left_table,
        text=left_table
    )
    
    right_table_obj = DSLTable(
        table_name=right_table,
        text=right_table
    )
    
    # Create join component
    join_component = DSLJoin(
        left_table=left_table_obj,
        right_table=right_table_obj,
        join_type=join_type,
        join_condition=[{
            "left_column": left_column,
            "right_column": right_column
        }],
        text=text
    )
    
    # Save to vector DB
    vector_store = VectorStore()
    vector_store.add_component(join_component)
    print(f"Added join component: {text}")
    
    # Save to file if requested
    if save_to_file:
        file_path = f"data/dsl_components/{DSLType.JOIN.value.lower()}_components.json"
        
        # Load existing components if file exists
        components = []
        if os.path.exists(file_path):
            try:
                components = load_components_from_json(file_path)
            except Exception as e:
                print(f"Error loading existing components: {e}")
        
        # Add new component and save
        components.append(join_component)
        save_components_to_json(components, file_path)
        print(f"Saved join component to {file_path}")


def add_components_from_file(file_path: str):
    """Add components from a JSON file to the vector database"""
    try:
        components = load_components_from_json(file_path)
        
        vector_store = VectorStore()
        vector_store.add_components(components)
        
        print(f"Added {len(components)} components from {file_path}")
        
        # Group by type
        type_counts = {}
        for component in components:
            if component.type not in type_counts:
                type_counts[component.type] = 0
            type_counts[component.type] += 1
        
        for component_type, count in type_counts.items():
            print(f"  {component_type}: {count} components")
            
    except Exception as e:
        print(f"Error adding components from file: {e}")


def generate_dsl_components_json_template(output_file: str):
    """Generate a template JSON file for DSL components"""
    # Create example components
    components = [
        # Table
        DSLTable(
            table_name="sales",
            text="sales data"
        ),
        
        # Column
        DSLColumn(
            column_name="revenue",
            table_name="sales",
            text="revenue in sales"
        ),
        
        # Filter
        DSLFilter(
            column=DSLColumn(
                column_name="region",
                table_name="sales",
                text="region"
            ),
            operator=DSLOperator.EQUALS,
            value="North America",
            text="Filter sales for North America region"
        ),
        
        # Join
        DSLJoin(
            left_table=DSLTable(
                table_name="sales",
                text="sales"
            ),
            right_table=DSLTable(
                table_name="customers",
                text="customers"
            ),
            join_type="INNER",
            join_condition=[{
                "left_column": "customer_id",
                "right_column": "id"
            }],
            text="Join sales with customers"
        ),
        
        # Aggregate
        DSLAggregateFn(
            function=DSLAggregate.SUM,
            column=DSLColumn(
                column_name="revenue",
                table_name="sales",
                text="revenue"
            ),
            text="Calculate sum of revenue"
        ),
        
        # Group By
        DSLGroupBy(
            columns=[
                DSLColumn(
                    column_name="region",
                    table_name="sales",
                    text="region"
                )
            ],
            text="Group by region"
        ),
        
        # Order By
        DSLOrderBy(
            columns=[
                DSLColumn(
                    column_name="revenue",
                    table_name="sales",
                    text="revenue"
                )
            ],
            direction="DESC",
            text="Order by revenue descending"
        ),
    ]
    
    # Convert to dicts
    component_dicts = [component.dict() for component in components]
    
    # Add comments
    template = {
        "description": "Template for DSL components. Each component should follow this structure.",
        "usage": "You can add your own components following these examples.",
        "components": component_dicts
    }
    
    # Save to file
    with open(output_file, 'w') as f:
        json.dump(template, f, indent=2)
    
    print(f"Generated template file at {output_file}")


def main():
    """Main function for command-line usage"""
    parser = argparse.ArgumentParser(description="Add custom DSL components")
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Template generator
    template_parser = subparsers.add_parser("template", help="Generate a template JSON file")
    template_parser.add_argument("--output", "-o", default="data/dsl_components/template.json", 
                             help="Output file path")
    
    # File loader
    file_parser = subparsers.add_parser("from-file", help="Add components from a JSON file")
    file_parser.add_argument("file", help="Input JSON file path")
    
    # Filter component
    filter_parser = subparsers.add_parser("filter", help="Add a custom filter component")
    filter_parser.add_argument("--table", "-t", required=True, help="Table name")
    filter_parser.add_argument("--column", "-c", required=True, help="Column name")
    filter_parser.add_argument("--operator", "-o", required=True, 
                               choices=["equals", "not_equals", "greater_than", "less_than", 
                                        "greater_than_equals", "less_than_equals", "in", 
                                        "not_in", "like", "not_like", "between", "not_between", 
                                        "is_null", "is_not_null"],
                               help="Operator type")
    filter_parser.add_argument("--value", "-v", default="", help="Filter value (comma-separated for lists)")
    filter_parser.add_argument("--text", required=True, help="Natural language description")
    filter_parser.add_argument("--save", "-s", action="store_true", help="Save to file")
    
    # Join component
    join_parser = subparsers.add_parser("join", help="Add a custom join component")
    join_parser.add_argument("--left-table", required=True, help="Left table name")
    join_parser.add_argument("--right-table", required=True, help="Right table name")
    join_parser.add_argument("--left-column", required=True, help="Left column name")
    join_parser.add_argument("--right-column", required=True, help="Right column name")
    join_parser.add_argument("--join-type", default="INNER", 
                             choices=["INNER", "LEFT", "RIGHT", "FULL"],
                             help="Join type")
    join_parser.add_argument("--text", required=True, help="Natural language description")
    join_parser.add_argument("--save", "-s", action="store_true", help="Save to file")
    
    args = parser.parse_args()
    
    if args.command == "template":
        generate_dsl_components_json_template(args.output)
    elif args.command == "from-file":
        add_components_from_file(args.file)
    elif args.command == "filter":
        add_custom_filter(args.table, args.column, args.operator, args.value, args.text, args.save)
    elif args.command == "join":
        add_custom_join(args.left_table, args.right_table, args.left_column, 
                      args.right_column, args.text, args.join_type, args.save)
    else:
        parser.print_help()


if __name__ == "__main__":
    main() 