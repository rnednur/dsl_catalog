import re
from typing import List, Dict, Any, Optional, Union
import spacy
from transformers import pipeline
import os

from src.models.dsl_models import (
    DSLType, DSLComponent, DSLColumn, DSLTable, DSLJoin, DSLFilter, 
    DSLAggregateFn, DSLGroupBy, DSLOrderBy, DSLLimit, DSLQuery,
    DSLOperator, DSLAggregate, DSLTimeframe
)


def create_dsl_component(data: Dict[str, Any]) -> DSLComponent:
    """Create a DSL component from a dictionary"""
    component_type = data.get('type')
    
    if component_type == DSLType.COLUMN.value:
        return DSLColumn(**data)
    elif component_type == DSLType.TABLE.value:
        return DSLTable(**data)
    elif component_type == DSLType.JOIN.value:
        return DSLJoin(**data)
    elif component_type == DSLType.FILTER.value:
        return DSLFilter(**data)
    elif component_type == DSLType.AGGREGATE.value:
        return DSLAggregateFn(**data)
    elif component_type == DSLType.GROUP_BY.value:
        return DSLGroupBy(**data)
    elif component_type == DSLType.ORDER_BY.value:
        return DSLOrderBy(**data)
    elif component_type == DSLType.LIMIT.value:
        return DSLLimit(**data)
    else:
        return DSLComponent(**data)


class DSLParser:
    """Parser to convert natural language queries to DSL components"""
    
    def __init__(self):
        try:
            self.nlp = spacy.load("en_core_web_md")
        except:
            print("Downloading spaCy model...")
            os.system("python -m spacy download en_core_web_md")
            self.nlp = spacy.load("en_core_web_md")
        
        self.zero_shot = pipeline("zero-shot-classification")
        
        # Define label sets for classification
        self.query_type_labels = [
            "select data", "aggregate data", "filter data", 
            "join data", "group data", "order data", "limit data"
        ]
        
        self.aggregate_labels = [
            "count", "sum", "average", "minimum", "maximum"
        ]
        
        self.operator_labels = [
            "equals", "not equals", "greater than", "less than",
            "greater than or equal", "less than or equal",
            "contains", "not contains", "is null", "is not null",
            "between", "not between", "in list", "not in list"
        ]
        
        self.time_period_labels = [
            "day", "week", "month", "quarter", "year",
            "last day", "last week", "last month", "last quarter", "last year",
            "current day", "current week", "current month", "current quarter", "current year"
        ]
    
    def parse_query(self, query: str) -> DSLQuery:
        """Parse a natural language query into a DSL query object"""
        # Identify the main components of the query
        doc = self.nlp(query)
        
        # Classify query type
        query_type_result = self.zero_shot(query, self.query_type_labels, multi_label=True)
        
        # Extract potential entities
        entities = self._extract_entities(doc)
        
        # Extract tables
        tables = self._extract_tables(doc, entities)
        
        # Extract columns
        columns = self._extract_columns(doc, entities)
        
        # Extract aggregations
        aggregates = self._extract_aggregates(doc, query_type_result, columns)
        
        # Extract filters
        filters = self._extract_filters(doc, columns)
        
        # Extract joins
        joins = self._extract_joins(doc, tables)
        
        # Extract group by
        group_by = self._extract_group_by(doc, columns, query_type_result)
        
        # Extract order by
        order_by = self._extract_order_by(doc, columns, query_type_result)
        
        # Extract limit
        limit = self._extract_limit(doc, query_type_result)
        
        # Create select items (columns and aggregates)
        select_items = []
        if aggregates:
            select_items.extend(aggregates)
        else:
            select_items.extend(columns)
        
        # Generate DSL text representation
        dsl_text = self._generate_dsl_text(
            select_items, tables, joins, filters, group_by, order_by, limit
        )
        
        # Create query object
        query_obj = DSLQuery(
            select=select_items,
            from_=tables,
            joins=joins,
            where=filters,
            group_by=group_by,
            order_by=order_by,
            limit=limit,
            original_query=query,
            dsl_text=dsl_text
        )
        
        return query_obj
    
    def _extract_entities(self, doc) -> Dict[str, Any]:
        """Extract entities from the parsed document"""
        entities = {
            "tables": [],
            "columns": [],
            "values": [],
            "numbers": []
        }
        
        for ent in doc.ents:
            if ent.label_ in ["ORG", "PRODUCT", "FAC", "GPE"]:
                entities["tables"].append(ent.text)
            elif ent.label_ in ["DATE", "TIME"]:
                entities["values"].append({"text": ent.text, "type": "date"})
            elif ent.label_ == "MONEY":
                entities["values"].append({"text": ent.text, "type": "money"})
            elif ent.label_ in ["CARDINAL", "QUANTITY", "PERCENT"]:
                entities["numbers"].append(ent.text)
        
        # Extract potential columns using part-of-speech patterns
        for token in doc:
            if token.pos_ in ["NOUN", "PROPN"] and token.text.lower() not in [t.lower() for t in entities["tables"]]:
                entities["columns"].append(token.text)
        
        return entities
    
    def _extract_tables(self, doc, entities: Dict[str, Any]) -> List[DSLTable]:
        """Extract tables from the document"""
        tables = []
        
        for table_name in entities["tables"]:
            tables.append(
                DSLTable(
                    table_name=table_name.lower().replace(" ", "_"),
                    text=table_name
                )
            )
        
        # If no tables were found, extract from noun chunks
        if not tables:
            for chunk in doc.noun_chunks:
                if chunk.root.pos_ in ["NOUN", "PROPN"]:
                    table_name = chunk.root.text
                    if all(table_name.lower() != t.table_name.lower() for t in tables):
                        tables.append(
                            DSLTable(
                                table_name=table_name.lower().replace(" ", "_"),
                                text=table_name
                            )
                        )
        
        return tables
    
    def _extract_columns(self, doc, entities: Dict[str, Any]) -> List[DSLColumn]:
        """Extract columns from the document"""
        columns = []
        
        for column_name in entities["columns"]:
            columns.append(
                DSLColumn(
                    column_name=column_name.lower().replace(" ", "_"),
                    text=column_name
                )
            )
        
        return columns
    
    def _extract_aggregates(self, doc, query_type_result, columns: List[DSLColumn]) -> List[DSLAggregateFn]:
        """Extract aggregate functions from the document"""
        aggregates = []
        
        # Check if the query contains aggregation intent
        has_aggregate = any("aggregate" in label.lower() for label in query_type_result["labels"] 
                            if query_type_result["scores"][query_type_result["labels"].index(label)] > 0.5)
        
        if has_aggregate:
            # Classify the specific aggregate function
            agg_result = self.zero_shot(doc.text, self.aggregate_labels)
            agg_type = agg_result["labels"][0]
            
            # Map to DSLAggregate enum
            agg_map = {
                "count": DSLAggregate.COUNT,
                "sum": DSLAggregate.SUM,
                "average": DSLAggregate.AVG,
                "minimum": DSLAggregate.MIN,
                "maximum": DSLAggregate.MAX
            }
            
            if agg_type in agg_map and columns:
                # Find the column to apply the aggregation to
                for column in columns:
                    aggregates.append(
                        DSLAggregateFn(
                            function=agg_map[agg_type],
                            column=column,
                            text=f"{agg_type} of {column.text}"
                        )
                    )
                    break
        
        return aggregates
    
    def _extract_filters(self, doc, columns: List[DSLColumn]) -> List[DSLFilter]:
        """Extract filter conditions from the document"""
        filters = []
        
        # Look for filter patterns
        filter_patterns = [
            r"where\s+(\w+)\s+(=|equals|is|equal to)\s+([\w\d]+)",
            r"with\s+(\w+)\s+(greater than|more than|higher than|>)\s+([\w\d]+)",
            r"(\w+)\s+(less than|lower than|<)\s+([\w\d]+)",
            r"(\w+)\s+(between)\s+([\w\d]+)\s+and\s+([\w\d]+)"
        ]
        
        for pattern in filter_patterns:
            matches = re.finditer(pattern, doc.text.lower())
            for match in matches:
                groups = match.groups()
                column_name = groups[0]
                operator_text = groups[1]
                value = groups[2]
                
                # Map operator text to DSLOperator
                operator_map = {
                    "=": DSLOperator.EQUALS,
                    "equals": DSLOperator.EQUALS,
                    "is": DSLOperator.EQUALS,
                    "equal to": DSLOperator.EQUALS,
                    "greater than": DSLOperator.GREATER_THAN,
                    "more than": DSLOperator.GREATER_THAN,
                    "higher than": DSLOperator.GREATER_THAN,
                    ">": DSLOperator.GREATER_THAN,
                    "less than": DSLOperator.LESS_THAN,
                    "lower than": DSLOperator.LESS_THAN,
                    "<": DSLOperator.LESS_THAN,
                    "between": DSLOperator.BETWEEN
                }
                
                operator = operator_map.get(operator_text, DSLOperator.EQUALS)
                
                # Find matching column
                matching_columns = [col for col in columns if col.column_name.lower() == column_name.lower()]
                if matching_columns:
                    column = matching_columns[0]
                    
                    # Process value based on operator
                    if operator == DSLOperator.BETWEEN and len(groups) >= 4:
                        value = [value, groups[3]]
                    
                    filters.append(
                        DSLFilter(
                            column=column,
                            operator=operator,
                            value=value,
                            text=f"{column.text} {operator_text} {value}"
                        )
                    )
        
        # Also look for time-related filters
        time_result = self.zero_shot(doc.text, self.time_period_labels)
        time_label = time_result["labels"][0]
        
        if time_result["scores"][0] > 0.7:
            # Map time period to DSLTimeframe
            time_map = {
                "day": DSLTimeframe.DAY,
                "week": DSLTimeframe.WEEK,
                "month": DSLTimeframe.MONTH,
                "quarter": DSLTimeframe.QUARTER,
                "year": DSLTimeframe.YEAR,
                "last day": DSLTimeframe.LAST_DAY,
                "last week": DSLTimeframe.LAST_WEEK,
                "last month": DSLTimeframe.LAST_MONTH,
                "last quarter": DSLTimeframe.LAST_QUARTER,
                "last year": DSLTimeframe.LAST_YEAR,
                "current day": DSLTimeframe.CURRENT_DAY,
                "current week": DSLTimeframe.CURRENT_WEEK,
                "current month": DSLTimeframe.CURRENT_MONTH,
                "current quarter": DSLTimeframe.CURRENT_QUARTER,
                "current year": DSLTimeframe.CURRENT_YEAR
            }
            
            timeframe = time_map.get(time_label)
            if timeframe and len(columns) > 0:
                # Find date-related columns
                date_columns = [col for col in columns if "date" in col.column_name.lower() 
                                or "time" in col.column_name.lower()
                                or "year" in col.column_name.lower()
                                or "month" in col.column_name.lower()]
                
                if date_columns:
                    date_column = date_columns[0]
                    filters.append(
                        DSLFilter(
                            column=date_column,
                            operator=DSLOperator.EQUALS,
                            value={"timeframe": timeframe.value},
                            text=f"{date_column.text} in {time_label}"
                        )
                    )
        
        return filters
    
    def _extract_joins(self, doc, tables: List[DSLTable]) -> Optional[List[DSLJoin]]:
        """Extract join operations from the document"""
        joins = None
        
        if len(tables) >= 2:
            # Start with simple joins between tables
            joins = []
            
            # Create a basic join between first two tables
            # In a real implementation, this would use schema information to determine join conditions
            joins.append(
                DSLJoin(
                    left_table=tables[0],
                    right_table=tables[1],
                    join_type="INNER",
                    join_condition=[{
                        "left_column": f"{tables[0].table_name}_id",
                        "right_column": f"{tables[0].table_name}_id"
                    }],
                    text=f"Join {tables[0].text} with {tables[1].text}"
                )
            )
        
        return joins
    
    def _extract_group_by(self, doc, columns: List[DSLColumn], query_type_result) -> Optional[DSLGroupBy]:
        """Extract GROUP BY clause from the document"""
        group_by = None
        
        # Check for group by intent
        has_group = any("group" in label.lower() for label in query_type_result["labels"]
                        if query_type_result["scores"][query_type_result["labels"].index(label)] > 0.5)
        
        if has_group and columns:
            # For simplicity, assume the first column not in aggregation is used for grouping
            group_by = DSLGroupBy(
                columns=[columns[0]],
                text=f"Group by {columns[0].text}"
            )
        
        return group_by
    
    def _extract_order_by(self, doc, columns: List[DSLColumn], query_type_result) -> Optional[DSLOrderBy]:
        """Extract ORDER BY clause from the document"""
        order_by = None
        
        # Check for order by intent
        has_order = any("order" in label.lower() for label in query_type_result["labels"]
                        if query_type_result["scores"][query_type_result["labels"].index(label)] > 0.5)
        
        if has_order and columns:
            # Look for direction indicators
            direction = "ASC"
            if any(token.text.lower() in ["descending", "desc", "highest", "largest", "most"] for token in doc):
                direction = "DESC"
            
            order_by = DSLOrderBy(
                columns=[columns[0]],
                direction=direction,
                text=f"Order by {columns[0].text} {direction}"
            )
        
        return order_by
    
    def _extract_limit(self, doc, query_type_result) -> Optional[DSLLimit]:
        """Extract LIMIT clause from the document"""
        limit = None
        
        # Check for limit intent
        has_limit = any("limit" in label.lower() for label in query_type_result["labels"]
                        if query_type_result["scores"][query_type_result["labels"].index(label)] > 0.5)
        
        if has_limit:
            # Look for number words
            number_words = ["top", "first", "last"]
            limit_value = 10  # Default
            
            for token in doc:
                if token.pos_ == "NUM":
                    try:
                        limit_value = int(token.text)
                        break
                    except ValueError:
                        pass
                elif token.text.lower() in number_words and token.i + 1 < len(doc):
                    next_token = doc[token.i + 1]
                    if next_token.pos_ == "NUM":
                        try:
                            limit_value = int(next_token.text)
                            break
                        except ValueError:
                            pass
            
            limit = DSLLimit(
                limit=limit_value,
                text=f"Limit to {limit_value} results"
            )
        
        return limit
    
    def _generate_dsl_text(self, select_items, tables, joins, filters, group_by, order_by, limit) -> str:
        """Generate a text representation of the DSL query"""
        parts = []
        
        # SELECT
        select_text = "SELECT " + ", ".join([item.text for item in select_items])
        parts.append(select_text)
        
        # FROM
        from_text = "FROM " + ", ".join([table.text for table in tables])
        parts.append(from_text)
        
        # JOIN
        if joins:
            join_text = "JOIN " + " AND ".join([join.text for join in joins])
            parts.append(join_text)
        
        # WHERE
        if filters:
            where_text = "WHERE " + " AND ".join([filter.text for filter in filters])
            parts.append(where_text)
        
        # GROUP BY
        if group_by:
            parts.append(group_by.text)
        
        # ORDER BY
        if order_by:
            parts.append(order_by.text)
        
        # LIMIT
        if limit:
            parts.append(limit.text)
        
        return " ; ".join(parts)


# Function for standalone testing
def parse_query(query: str) -> Dict[str, Any]:
    """Parse a natural language query to DSL for testing purposes"""
    parser = DSLParser()
    dsl_query = parser.parse_query(query)
    return {
        "dsl_text": dsl_query.dsl_text,
        "query_parts": {
            "select": [item.dict() for item in dsl_query.select],
            "from": [table.dict() for table in dsl_query.from_],
            "joins": [join.dict() for join in dsl_query.joins] if dsl_query.joins else None,
            "where": [filter.dict() for filter in dsl_query.where] if dsl_query.where else None,
            "group_by": dsl_query.group_by.dict() if dsl_query.group_by else None,
            "order_by": dsl_query.order_by.dict() if dsl_query.order_by else None,
            "limit": dsl_query.limit.dict() if dsl_query.limit else None
        }
    } 