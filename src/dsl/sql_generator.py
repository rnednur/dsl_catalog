from typing import Dict, Any, List, Optional
import datetime

from src.models.dsl_models import (
    DSLQuery, DSLComponent, DSLType, DSLColumn, DSLTable, 
    DSLJoin, DSLFilter, DSLAggregateFn, DSLAggregate, 
    DSLGroupBy, DSLOrderBy, DSLLimit, DSLOperator, DSLTimeframe
)
from src.db.schema_loader import SchemaLoader


class SQLGenerator:
    """Generate SQL queries from DSL components"""
    
    def __init__(self, schema_loader: Optional[SchemaLoader] = None):
        self.schema_loader = schema_loader
    
    def generate_sql(self, dsl_query: DSLQuery) -> str:
        """Generate a SQL query from a DSL query object"""
        parts = []
        
        # SELECT clause
        select_clause = self._generate_select(dsl_query.select)
        parts.append(select_clause)
        
        # FROM clause
        from_clause = self._generate_from(dsl_query.from_)
        parts.append(from_clause)
        
        # JOIN clause
        if dsl_query.joins:
            join_clause = self._generate_joins(dsl_query.joins)
            parts.append(join_clause)
        
        # WHERE clause
        if dsl_query.where:
            where_clause = self._generate_where(dsl_query.where)
            parts.append(where_clause)
        
        # GROUP BY clause
        if dsl_query.group_by:
            group_by_clause = self._generate_group_by(dsl_query.group_by)
            parts.append(group_by_clause)
        
        # HAVING clause
        if dsl_query.having:
            having_clause = self._generate_having(dsl_query.having)
            parts.append(having_clause)
        
        # ORDER BY clause
        if dsl_query.order_by:
            order_by_clause = self._generate_order_by(dsl_query.order_by)
            parts.append(order_by_clause)
        
        # LIMIT clause
        if dsl_query.limit:
            limit_clause = self._generate_limit(dsl_query.limit)
            parts.append(limit_clause)
        
        return " ".join(parts)
    
    def _generate_select(self, select_items: List[DSLComponent]) -> str:
        """Generate the SELECT clause"""
        select_parts = []
        
        for item in select_items:
            if isinstance(item, DSLAggregateFn):
                # Generate aggregate function
                column_ref = self._generate_column_reference(item.column)
                agg_fn = self._get_aggregate_function(item.function)
                alias = f" AS {item.alias}" if item.alias else ""
                select_parts.append(f"{agg_fn}({column_ref}){alias}")
            elif isinstance(item, DSLColumn):
                # Generate column reference
                column_ref = self._generate_column_reference(item)
                alias = f" AS {item.alias}" if item.alias else ""
                select_parts.append(f"{column_ref}{alias}")
        
        return "SELECT " + ", ".join(select_parts)
    
    def _generate_from(self, tables: List[DSLTable]) -> str:
        """Generate the FROM clause"""
        from_parts = []
        
        for table in tables:
            table_ref = self._generate_table_reference(table)
            alias = f" AS {table.alias}" if table.alias else ""
            from_parts.append(f"{table_ref}{alias}")
        
        return "FROM " + ", ".join(from_parts)
    
    def _generate_joins(self, joins: List[DSLJoin]) -> str:
        """Generate JOIN clauses"""
        join_parts = []
        
        for join in joins:
            left_table = self._generate_table_reference(join.left_table)
            right_table = self._generate_table_reference(join.right_table)
            
            # Generate the ON conditions
            on_conditions = []
            for condition in join.join_condition:
                left_col = condition["left_column"]
                right_col = condition["right_column"]
                
                if "." not in left_col:
                    left_col = f"{join.left_table.table_name}.{left_col}"
                if "." not in right_col:
                    right_col = f"{join.right_table.table_name}.{right_col}"
                
                on_conditions.append(f"{left_col} = {right_col}")
            
            join_type = join.join_type.upper()
            on_clause = " AND ".join(on_conditions)
            
            right_alias = f" AS {join.right_table.alias}" if join.right_table.alias else ""
            join_parts.append(f"{join_type} JOIN {right_table}{right_alias} ON {on_clause}")
        
        return " ".join(join_parts)
    
    def _generate_where(self, filters: List[DSLFilter]) -> str:
        """Generate the WHERE clause"""
        where_conditions = []
        
        for i, filter_item in enumerate(filters):
            # Add conjunction if not the first filter
            conjunction = ""
            if i > 0:
                conjunction = f"{filter_item.conjunction} "
            
            condition = self._generate_filter_condition(filter_item)
            where_conditions.append(f"{conjunction}{condition}")
        
        return "WHERE " + " ".join(where_conditions)
    
    def _generate_group_by(self, group_by: DSLGroupBy) -> str:
        """Generate the GROUP BY clause"""
        group_parts = []
        
        for column in group_by.columns:
            column_ref = self._generate_column_reference(column)
            group_parts.append(column_ref)
        
        return "GROUP BY " + ", ".join(group_parts)
    
    def _generate_having(self, filters: List[DSLFilter]) -> str:
        """Generate the HAVING clause"""
        having_conditions = []
        
        for i, filter_item in enumerate(filters):
            # Add conjunction if not the first filter
            conjunction = ""
            if i > 0:
                conjunction = f"{filter_item.conjunction} "
            
            condition = self._generate_filter_condition(filter_item)
            having_conditions.append(f"{conjunction}{condition}")
        
        return "HAVING " + " ".join(having_conditions)
    
    def _generate_order_by(self, order_by: DSLOrderBy) -> str:
        """Generate the ORDER BY clause"""
        order_parts = []
        
        for column in order_by.columns:
            column_ref = self._generate_column_reference(column)
            order_parts.append(f"{column_ref} {order_by.direction}")
        
        return "ORDER BY " + ", ".join(order_parts)
    
    def _generate_limit(self, limit: DSLLimit) -> str:
        """Generate the LIMIT clause"""
        if limit.offset and limit.offset > 0:
            return f"LIMIT {limit.limit} OFFSET {limit.offset}"
        else:
            return f"LIMIT {limit.limit}"
    
    def _generate_column_reference(self, column: DSLColumn) -> str:
        """Generate a reference to a column"""
        if column.table_name:
            table_ref = column.table_name
            if "." in table_ref:
                return f"{table_ref}"
            else:
                return f"{table_ref}.{column.column_name}"
        else:
            return column.column_name
    
    def _generate_table_reference(self, table: DSLTable) -> str:
        """Generate a reference to a table"""
        return table.table_name
    
    def _generate_filter_condition(self, filter_item: DSLFilter) -> str:
        """Generate a filter condition"""
        column_ref = self._generate_column_reference(filter_item.column)
        operator = self._get_sql_operator(filter_item.operator)
        value = self._format_value(filter_item.value, filter_item.operator)
        
        if filter_item.operator == DSLOperator.IS_NULL:
            return f"{column_ref} IS NULL"
        elif filter_item.operator == DSLOperator.IS_NOT_NULL:
            return f"{column_ref} IS NOT NULL"
        elif filter_item.operator == DSLOperator.BETWEEN:
            if isinstance(filter_item.value, list) and len(filter_item.value) >= 2:
                val1 = self._format_value(filter_item.value[0], DSLOperator.EQUALS)
                val2 = self._format_value(filter_item.value[1], DSLOperator.EQUALS)
                return f"{column_ref} BETWEEN {val1} AND {val2}"
            else:
                return f"{column_ref} = {value}"
        elif filter_item.operator == DSLOperator.IN:
            if isinstance(filter_item.value, list):
                values = [self._format_value(v, DSLOperator.EQUALS) for v in filter_item.value]
                return f"{column_ref} IN ({', '.join(values)})"
            else:
                return f"{column_ref} = {value}"
        elif filter_item.operator == DSLOperator.NOT_IN:
            if isinstance(filter_item.value, list):
                values = [self._format_value(v, DSLOperator.EQUALS) for v in filter_item.value]
                return f"{column_ref} NOT IN ({', '.join(values)})"
            else:
                return f"{column_ref} != {value}"
        elif isinstance(filter_item.value, dict) and "timeframe" in filter_item.value:
            # Handle timeframe filters
            timeframe = filter_item.value["timeframe"]
            time_filter = self._generate_timeframe_condition(column_ref, timeframe)
            return time_filter
        else:
            return f"{column_ref} {operator} {value}"
    
    def _get_sql_operator(self, operator: DSLOperator) -> str:
        """Map DSL operator to SQL operator"""
        operator_map = {
            DSLOperator.EQUALS: "=",
            DSLOperator.NOT_EQUALS: "!=",
            DSLOperator.GREATER_THAN: ">",
            DSLOperator.LESS_THAN: "<",
            DSLOperator.GREATER_THAN_EQUALS: ">=",
            DSLOperator.LESS_THAN_EQUALS: "<=",
            DSLOperator.LIKE: "LIKE",
            DSLOperator.NOT_LIKE: "NOT LIKE",
            DSLOperator.BETWEEN: "BETWEEN",
            DSLOperator.NOT_BETWEEN: "NOT BETWEEN",
            DSLOperator.IN: "IN",
            DSLOperator.NOT_IN: "NOT IN",
            DSLOperator.IS_NULL: "IS NULL",
            DSLOperator.IS_NOT_NULL: "IS NOT NULL",
        }
        
        return operator_map.get(operator, "=")
    
    def _get_aggregate_function(self, aggregate: DSLAggregate) -> str:
        """Map DSL aggregate to SQL function"""
        aggregate_map = {
            DSLAggregate.COUNT: "COUNT",
            DSLAggregate.SUM: "SUM",
            DSLAggregate.AVG: "AVG",
            DSLAggregate.MIN: "MIN",
            DSLAggregate.MAX: "MAX",
        }
        
        return aggregate_map.get(aggregate, "COUNT")
    
    def _format_value(self, value: Any, operator: DSLOperator) -> str:
        """Format a value for use in SQL"""
        if value is None:
            return "NULL"
        elif isinstance(value, str):
            # For LIKE operators, add wildcards if not present
            if operator in [DSLOperator.LIKE, DSLOperator.NOT_LIKE] and "%" not in value:
                return f"'%{value}%'"
            else:
                return f"'{value}'"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        elif isinstance(value, datetime.datetime):
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"
        elif isinstance(value, datetime.date):
            return f"'{value.strftime('%Y-%m-%d')}'"
        elif isinstance(value, list):
            return ", ".join([self._format_value(v, operator) for v in value])
        elif isinstance(value, dict):
            # Handle special cases like timeframes
            if "timeframe" in value:
                return f"'{value['timeframe']}'"
            else:
                return str(value)
        else:
            return str(value)
    
    def _generate_timeframe_condition(self, column_ref: str, timeframe: str) -> str:
        """Generate SQL condition for a timeframe"""
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        
        if timeframe == DSLTimeframe.DAY.value:
            return f"DATE({column_ref}) = CURRENT_DATE"
        elif timeframe == DSLTimeframe.WEEK.value:
            return f"EXTRACT(WEEK FROM {column_ref}) = EXTRACT(WEEK FROM CURRENT_DATE) AND EXTRACT(YEAR FROM {column_ref}) = EXTRACT(YEAR FROM CURRENT_DATE)"
        elif timeframe == DSLTimeframe.MONTH.value:
            return f"EXTRACT(MONTH FROM {column_ref}) = EXTRACT(MONTH FROM CURRENT_DATE) AND EXTRACT(YEAR FROM {column_ref}) = EXTRACT(YEAR FROM CURRENT_DATE)"
        elif timeframe == DSLTimeframe.QUARTER.value:
            return f"EXTRACT(QUARTER FROM {column_ref}) = EXTRACT(QUARTER FROM CURRENT_DATE) AND EXTRACT(YEAR FROM {column_ref}) = EXTRACT(YEAR FROM CURRENT_DATE)"
        elif timeframe == DSLTimeframe.YEAR.value:
            return f"EXTRACT(YEAR FROM {column_ref}) = EXTRACT(YEAR FROM CURRENT_DATE)"
        elif timeframe == DSLTimeframe.LAST_DAY.value:
            return f"DATE({column_ref}) = (CURRENT_DATE - INTERVAL '1 day')"
        elif timeframe == DSLTimeframe.LAST_WEEK.value:
            return f"EXTRACT(WEEK FROM {column_ref}) = EXTRACT(WEEK FROM CURRENT_DATE - INTERVAL '1 week') AND EXTRACT(YEAR FROM {column_ref}) = EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '1 week')"
        elif timeframe == DSLTimeframe.LAST_MONTH.value:
            return f"EXTRACT(MONTH FROM {column_ref}) = EXTRACT(MONTH FROM CURRENT_DATE - INTERVAL '1 month') AND EXTRACT(YEAR FROM {column_ref}) = EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '1 month')"
        elif timeframe == DSLTimeframe.LAST_QUARTER.value:
            return f"EXTRACT(QUARTER FROM {column_ref}) = EXTRACT(QUARTER FROM CURRENT_DATE - INTERVAL '3 months') AND EXTRACT(YEAR FROM {column_ref}) = EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '3 months')"
        elif timeframe == DSLTimeframe.LAST_YEAR.value:
            return f"EXTRACT(YEAR FROM {column_ref}) = EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '1 year')"
        elif timeframe == DSLTimeframe.CURRENT_DAY.value:
            return f"DATE({column_ref}) = CURRENT_DATE"
        elif timeframe == DSLTimeframe.CURRENT_WEEK.value:
            return f"EXTRACT(WEEK FROM {column_ref}) = EXTRACT(WEEK FROM CURRENT_DATE) AND EXTRACT(YEAR FROM {column_ref}) = EXTRACT(YEAR FROM CURRENT_DATE)"
        elif timeframe == DSLTimeframe.CURRENT_MONTH.value:
            return f"EXTRACT(MONTH FROM {column_ref}) = EXTRACT(MONTH FROM CURRENT_DATE) AND EXTRACT(YEAR FROM {column_ref}) = EXTRACT(YEAR FROM CURRENT_DATE)"
        elif timeframe == DSLTimeframe.CURRENT_QUARTER.value:
            return f"EXTRACT(QUARTER FROM {column_ref}) = EXTRACT(QUARTER FROM CURRENT_DATE) AND EXTRACT(YEAR FROM {column_ref}) = EXTRACT(YEAR FROM CURRENT_DATE)"
        elif timeframe == DSLTimeframe.CURRENT_YEAR.value:
            return f"EXTRACT(YEAR FROM {column_ref}) = EXTRACT(YEAR FROM CURRENT_DATE)"
        else:
            return f"{column_ref} = '{timeframe}'" 