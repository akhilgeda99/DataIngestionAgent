"""FastAPI-based REST API for data ingestion and analysis.

This module provides endpoints for file upload, analysis, and data quality assessment.
Supports CSV, Excel, JSON file formats and SQL Server database tables with automated AI-powered analysis.
"""

from fastapi import FastAPI, File, UploadFile, HTTPException, Depends, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import json
import os
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from sqlalchemy.orm import Session

from src.database import get_db, get_table_metadata, read_table_data, list_tables

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Data Ingestion API",
    description="API for uploading and analyzing data files with AI-powered insights",
    version="1.0.0"
)

# Add CORS middleware to allow cross-origin requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Constants for file storage
UPLOAD_DIR = "uploads"

# Mount static directories for serving files
app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/uploads", StaticFiles(directory=UPLOAD_DIR), name="uploads")

@app.get("/")
async def read_root():
    """Serve the main application interface.
    
    Returns:
        FileResponse: The index.html file containing the web interface.
    """
    return FileResponse("static/index.html")

@app.get("/files")
async def list_files() -> List[Dict[str, Any]]:
    """List all uploaded files with their metadata.
    
    Returns:
        List[Dict[str, Any]]: List of file information including name, path, size, and modification time.
        
    Raises:
        HTTPException: If there's an error accessing the uploads directory.
    """
    try:
        files = []
        for filename in os.listdir(UPLOAD_DIR):
            file_path = os.path.join(UPLOAD_DIR, filename)
            if os.path.isfile(file_path):
                files.append({
                    "name": filename,
                    "path": f"/uploads/{filename}",
                    "size": os.path.getsize(file_path),
                    "modified": datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat()
                })
        return files
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tables")
async def list_database_tables(
    schema: Optional[str] = Query(None, description="Optional schema name to filter tables"),
    db: Session = Depends(get_db)
):
    """List all available database tables.
    
    Args:
        schema (str, optional): Schema name to filter tables
        
    Returns:
        List[Dict[str, Any]]: List of tables with their schemas
        
    Raises:
        HTTPException: If there's an error accessing the database.
    """
    try:
        return list_tables(schema=schema)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/tables/{schema}/{table_name}")
async def get_table_info(
    schema: str,
    table_name: str,
    db: Session = Depends(get_db)
):
    """Get metadata for a specific database table.
    
    Args:
        schema (str): Schema name
        table_name (str): Name of the table to analyze
        
    Returns:
        Dict: Table metadata including columns and their types.
        
    Raises:
        HTTPException: If table not found or database error occurs.
    """
    try:
        return get_table_metadata(f"{schema}.{table_name}")
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Table not found or error: {str(e)}")

@app.get("/analyze/table/{schema}/{table_name}")
async def analyze_table(
    schema: str,
    table_name: str,
    db: Session = Depends(get_db)
):
    """Analyze data from a database table.
    
    Performs comprehensive analysis including data quality assessment
    and AI-powered insights on the table data.
    
    Args:
        schema (str): Schema name
        table_name (str): Name of the table to analyze
        
    Returns:
        Dict: Analysis results including row count, columns, and AI-powered quality metrics.
        
    Raises:
        HTTPException: If table not found or analysis fails.
    """
    try:
        # Read table data into DataFrame
        df = read_table_data(table_name, schema=schema)
            
        # Get table metadata
        metadata = get_table_metadata(f"{schema}.{table_name}")
        
        if df.empty:
            return {
                "schema": schema,
                "table_name": table_name,
                "metadata": metadata,
                "rows": 0,
                "columns": [],
                "analysis": {
                    "status": "No data found in table",
                    "quality_metrics": {
                        "missing_values": {},
                        "data_types": {},
                        "unique_values": {},
                        "completeness_ratio": {},
                        "numeric_stats": {},
                        "categorical_stats": {}
                    },
                    "row_count": 0,
                    "column_count": 0
                }
            }
        
        # Analyze data quality
        analysis_results = analyze_data_quality(df)
        
        response = {
            "schema": schema,
            "table_name": table_name,
            "metadata": metadata,
            "rows": len(df),
            "columns": df.columns.tolist(),
            "analysis": {
                "status": "Data analyzed successfully",
                "quality_metrics": analysis_results,
                "row_count": len(df),
                "column_count": len(df.columns)
            }
        }
        
        return response
    except Exception as e:
        logger.error(f"Error analyzing table: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Analysis failed: {str(e)}"
        )

@app.get("/analyze/{filename}")
async def analyze_existing_file(filename: str):
    """Analyze an existing uploaded file.
    
    Args:
        filename (str): Name of the file to analyze
        
    Returns:
        Dict: Analysis results including row count, columns, and AI-powered quality metrics.
        
    Raises:
        HTTPException: If file not found or format not supported.
    """
    try:
        file_path = os.path.join(UPLOAD_DIR, filename)
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="File not found")

        # Read the file based on its extension
        ext = filename.lower().split('.')[-1]
        try:
            if ext == 'csv':
                df = pd.read_csv(file_path)
            elif ext in ['xlsx', 'xls']:
                df = pd.read_excel(file_path)
            elif ext == 'json':
                # Try regular JSON first
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                    df = pd.json_normalize(data)
                except:
                    # Try JSON Lines
                    df = pd.read_json(file_path, lines=True)
            else:
                raise HTTPException(status_code=400, detail="Unsupported file format")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Error reading file: {str(e)}")

        # Analyze data quality
        analysis_results = analyze_data_quality(df)
        
        return {
            "filename": filename,
            "rows": len(df),
            "columns": df.columns.tolist(),
            "ai_analysis": {
                "quality_metrics": analysis_results
            }
        }

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """Upload and analyze a new data file.
    
    Supports CSV, Excel (.xlsx, .xls), and JSON formats.
    Performs immediate data quality analysis using AI techniques.
    
    Args:
        file (UploadFile): The uploaded file object.
        
    Returns:
        Dict: Upload results including filename, path, and AI analysis results.
        
    Raises:
        HTTPException: If file format not supported or processing fails.
    """
    try:
        # Validate file extension
        filename = file.filename.lower()
        if not any(filename.endswith(ext) for ext in ['.csv', '.xlsx', '.xls', '.json']):
            raise HTTPException(status_code=400, detail="Unsupported file format")

        # Generate unique filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        name, ext = os.path.splitext(filename)
        unique_filename = f"{name}_{timestamp}{ext}"
        file_path = os.path.join(UPLOAD_DIR, unique_filename)

        # Save the file
        try:
            content = await file.read()
            with open(file_path, "wb") as f:
                f.write(content)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error saving file: {str(e)}")

        # Process the file
        try:
            if filename.endswith('.csv'):
                df = pd.read_csv(file_path)
            elif filename.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path)
            elif filename.endswith('.json'):
                # Try regular JSON first
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                    df = pd.json_normalize(data)
                except:
                    # Try JSON Lines
                    df = pd.read_json(file_path, lines=True)
        except Exception as e:
            os.remove(file_path)  # Clean up on error
            raise HTTPException(status_code=400, detail=f"Error reading file: {str(e)}")

        # Analyze data quality
        analysis_results = analyze_data_quality(df)
        
        return {
            "filename": unique_filename,
            "original_filename": filename,
            "path": f"/uploads/{unique_filename}",
            "rows": len(df),
            "columns": df.columns.tolist(),
            "ai_analysis": {
                "quality_metrics": analysis_results
            }
        }

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def analyze_data_quality(df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze data quality metrics for a DataFrame.
    
    Computes various quality metrics including:
    - Missing values per column
    - Data types
    - Unique value counts
    - Completeness ratio for each column
    - Basic statistics for numeric columns
    
    Args:
        df (pd.DataFrame): Input DataFrame to analyze.
        
    Returns:
        Dict[str, Any]: Dictionary containing quality metrics.
    """
    try:
        # Basic metrics for all columns
        metrics = {
            "missing_values": df.isnull().sum().to_dict(),
            "data_types": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "unique_values": df.nunique().to_dict(),
            "completeness_ratio": {
                col: float((len(df) - df[col].isnull().sum()) / len(df))
                for col in df.columns
            }
        }
        
        # Add numeric column statistics
        numeric_stats = {}
        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
        for col in numeric_cols:
            numeric_stats[col] = {
                "mean": float(df[col].mean()) if not df[col].isnull().all() else 0,
                "std": float(df[col].std()) if not df[col].isnull().all() else 0,
                "min": float(df[col].min()) if not df[col].isnull().all() else 0,
                "max": float(df[col].max()) if not df[col].isnull().all() else 0
            }
        metrics["numeric_stats"] = numeric_stats
        
        # Add categorical column statistics
        categorical_stats = {}
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns
        for col in categorical_cols:
            value_counts = df[col].value_counts()
            if not value_counts.empty:
                top_5_values = value_counts.head(5)
                categorical_stats[col] = {
                    "top_values": top_5_values.index.tolist(),
                    "frequencies": top_5_values.values.tolist()
                }
        metrics["categorical_stats"] = categorical_stats
        
        return metrics
    except Exception as e:
        logger.error(f"Error analyzing data quality: {str(e)}")
        return {
            "error": f"Failed to analyze data quality: {str(e)}",
            "missing_values": {},
            "data_types": {},
            "unique_values": {},
            "completeness_ratio": {}
        }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
