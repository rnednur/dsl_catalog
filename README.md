# NL2SQL with Domain-Specific Language (DSL)

This project implements a Natural Language to SQL (NL2SQL) framework using a Domain Specification Language (DSL) approach to improve query accuracy.

## Overview

The framework works as follows:
1. Natural Language Query → Parse to DSL representation
2. DSL components are matched against pre-stored DSL patterns in a vector database
3. Retrieved matches are used to construct accurate SQL queries for PostgreSQL
4. Results are returned to the user

## Features

- DSL-based query interpretation
- Vector database for semantic matching of query components
- Support for complex PostgreSQL queries
- Handles joins, filters, aggregations, and more
- Modular architecture for easy extension

## Architecture

The system consists of several components:

1. **DSL Parser** - Converts natural language to DSL components
2. **Vector Database** - Stores and retrieves DSL components semantically
3. **SQL Generator** - Converts DSL to SQL
4. **Database Connector** - Executes SQL queries against PostgreSQL
5. **Schema Loader** - Extracts database schema information
6. **API Server** - Exposes functionality via REST API


## Getting Started

### Prerequisites

- Python 3.8+
- PostgreSQL 13+ with pgvector extension installed
- Docker (optional, for containerized deployment)

### Installation

1. Clone this repository:
```bash
git clone https://github.com/yourusername/nl2sql-dsl.git
cd nl2sql-dsl
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your database credentials
```

5. Run the setup script:
```bash
python scripts/setup.py
```

### Usage

#### Command-line interface

```bash
# Interactive mode
python src/main.py -i

# Process a single query
python src/main.py "Show me sales by region for last quarter"
```

#### API Server

```bash
# Start the API server
python src/api.py
```

API endpoints:
- `POST /nl2sql` - Convert natural language to SQL
- `GET /schema` - Get database schema information
- `GET /health` - Health check endpoint

Example API request:
```bash
curl -X POST http://localhost:8000/nl2sql \
  -H "Content-Type: application/json" \
  -d '{"query": "Show me sales by region for last quarter"}'
```

#### Python Library

```python
from src.main import nl2sql

query = "Show me sales by region for last quarter"
result = nl2sql(query)

print(result["sql_query"])
print(result["results"])
```

## Customizing the DSL

You can customize the DSL components to improve query accuracy for your specific domain:

### Adding Custom DSL Components

```bash
# Generate a template
python scripts/add_custom_dsl.py template

# Add a custom filter component
python scripts/add_custom_dsl.py filter \
  --table sales \
  --column region \
  --operator equals \
  --value "North America" \
  --text "Filter sales for North America region" \
  --save

# Add a custom join component
python scripts/add_custom_dsl.py join \
  --left-table sales \
  --right-table customers \
  --left-column customer_id \
  --right-column id \
  --text "Join sales with customers" \
  --save

# Add components from a JSON file
python scripts/add_custom_dsl.py from-file data/dsl_components/custom_components.json
```

### DSL Component Structure

The DSL consists of the following component types:

- **TABLE** - Represents a database table
- **COLUMN** - Represents a column in a table
- **JOIN** - Represents a join between tables
- **FILTER** - Represents a filter condition
- **AGGREGATE** - Represents an aggregate function
- **GROUP_BY** - Represents a GROUP BY clause
- **ORDER_BY** - Represents an ORDER BY clause
- **LIMIT** - Represents a LIMIT clause

## Testing

Run unit tests:
```bash
pytest tests/
```

Run a demo with sample queries:
```bash
python scripts/demo.py
```

Run an interactive demo:
```bash
python scripts/demo.py -i
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- This project uses the vector embedding capabilities of Sentence Transformers
- Natural language processing is powered by SpaCy and Hugging Face Transformers

## Contact

For any questions or feedback, please open an issue on GitHub or contact the maintainers. 