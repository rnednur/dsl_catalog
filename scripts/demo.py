import time
import json
from typing import Dict, Any, List

from src.main import nl2sql, get_nl2sql


def print_separator():
    """Print a separator line"""
    print("\n" + "=" * 80 + "\n")


def print_result(result: Dict[str, Any]):
    """Print the result of a query"""
    if "error" in result:
        print(f"Error: {result['error']}")
        return
    
    print("Natural Language Query:")
    print(f"  {result['natural_language_query']}")
    print("\nDSL Query:")
    print(f"  {result['dsl_query']}")
    print("\nSQL Query:")
    print(f"  {result['sql_query']}")
    print("\nResults:")
    
    if result['results']:
        # Get column names
        columns = list(result['results'][0].keys())
        
        # Print header
        header = " | ".join(columns)
        print("  " + header)
        print("  " + "-" * len(header))
        
        # Print results
        for row in result['results'][:5]:
            values = []
            for col in columns:
                values.append(str(row[col]))
            print("  " + " | ".join(values))
        
        if len(result['results']) > 5:
            print(f"  ... and {len(result['results']) - 5} more results")
    else:
        print("  No results returned")


def run_demo_queries():
    """Run a set of demo queries to showcase the system"""
    # Define a set of queries to run
    queries = [
        "Show me all sales from last month",
        "What is the average revenue by region?",
        "Which product had the highest sales last quarter?",
        "How many customers made purchases in each region?",
        "Show me orders over $1000 from the west region",
        "Count the number of transactions by payment type",
        "What was the total revenue for each product category?",
        "List the top 5 customers by purchase amount",
        "Compare sales performance between this year and last year"
    ]
    
    print("NL2SQL DEMO")
    print("===========")
    print("This demo will show how natural language queries are converted to SQL.")
    print(f"Running {len(queries)} example queries...\n")
    
    # Create NL2SQL instance
    nl2sql_instance = get_nl2sql()
    
    # Run each query
    for i, query in enumerate(queries, 1):
        print_separator()
        print(f"QUERY {i}: {query}")
        print_separator()
        
        start_time = time.time()
        result = nl2sql_instance.process_query(query)
        end_time = time.time()
        
        print_result(result)
        print(f"\nProcessing time: {end_time - start_time:.2f} seconds")
    
    # Clean up
    nl2sql_instance.close()


def interactive_demo():
    """Run an interactive demo"""
    print("NL2SQL INTERACTIVE DEMO")
    print("======================")
    print("Enter your natural language queries to convert them to SQL.")
    print("Type 'exit' or 'quit' to end the demo.")
    
    # Create NL2SQL instance
    nl2sql_instance = get_nl2sql()
    
    while True:
        print_separator()
        query = input("Enter query: ")
        
        if query.lower() in ['exit', 'quit']:
            break
        
        start_time = time.time()
        result = nl2sql_instance.process_query(query)
        end_time = time.time()
        
        print_separator()
        print_result(result)
        print(f"\nProcessing time: {end_time - start_time:.2f} seconds")
    
    # Clean up
    nl2sql_instance.close()


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="NL2SQL Demo")
    parser.add_argument("--interactive", "-i", action="store_true", help="Run in interactive mode")
    
    args = parser.parse_args()
    
    if args.interactive:
        interactive_demo()
    else:
        run_demo_queries()


if __name__ == "__main__":
    main() 