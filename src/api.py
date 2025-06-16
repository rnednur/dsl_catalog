from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
import uvicorn
import json

from src.main import nl2sql, get_nl2sql, NL2SQL

app = FastAPI(
    title="NL2SQL API",
    description="Convert natural language queries to SQL using DSL and vector databases",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class NLQuery(BaseModel):
    """Natural language query request model"""
    query: str


class DSLResponse(BaseModel):
    """DSL component of the response"""
    select: List[Dict[str, Any]]
    from_: List[Dict[str, Any]]
    joins: Optional[List[Dict[str, Any]]] = None
    where: Optional[List[Dict[str, Any]]] = None
    group_by: Optional[Dict[str, Any]] = None
    order_by: Optional[Dict[str, Any]] = None
    limit: Optional[Dict[str, Any]] = None


class QueryResponse(BaseModel):
    """Response model for query results"""
    natural_language_query: str
    dsl_query: str
    sql_query: str
    results: List[Dict[str, Any]]
    dsl_components: Dict[str, Any]


@app.post("/nl2sql", response_model=QueryResponse)
def convert_nl_to_sql(query: NLQuery):
    """
    Convert natural language to SQL
    
    Args:
        query: Natural language query
        
    Returns:
        SQL query and results
    """
    try:
        result = nl2sql(query.query)
        
        if "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])
        
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/schema")
def get_schema():
    """
    Get database schema information
    
    Returns:
        Database schema
    """
    try:
        nl2sql_instance = get_nl2sql()
        return nl2sql_instance.schema
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
def health_check():
    """
    Health check endpoint
    
    Returns:
        Health status
    """
    return {"status": "healthy"}


@app.on_event("shutdown")
def shutdown_event():
    """Close connections on shutdown"""
    try:
        nl2sql_instance = get_nl2sql()
        nl2sql_instance.close()
    except:
        pass


def start():
    """Start the API server"""
    uvicorn.run("src.api:app", host="0.0.0.0", port=8000, reload=True)


if __name__ == "__main__":
    start() 