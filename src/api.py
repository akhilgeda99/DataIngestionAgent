"""FastAPI-based REST API for data ingestion and analysis."""
from fastapi import FastAPI, File, UploadFile, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import numpy as np
import json
import os
import logging
import io
from typing import Dict, Any, List, Optional
from datetime import datetime

from src.database import db
from src.ai_analysis import ai_analyzer
from src.analysis.core.data_analyzer import DataAnalyzer
from src.analysis.utils.type_converters import convert_polars_types, DateTimeEncoder
from src.validation.data_validator import validate_data
from src.validation.rule_storage import RuleStorage

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def convert_numpy_types(obj: Any) -> Any:
    """Convert numpy types to Python native types for JSON serialization."""
    if isinstance(obj, (np.integer, np.int64)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64)):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, (np.ndarray, pd.Series)):
        return [convert_numpy_types(x) for x in obj]
    elif isinstance(obj, dict):
        return {k: convert_numpy_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(x) for x in obj]
    return obj

# Initialize analyzers and rule storage
data_analyzer = DataAnalyzer()
rule_storage = RuleStorage()

app = FastAPI(
    title="Data Ingestion API",
    description="API for uploading and analyzing data files",
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

# Constants
UPLOAD_DIR = "uploads"

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/uploads", StaticFiles(directory=UPLOAD_DIR), name="uploads")

@app.get("/")
async def read_root():
    """Serve the main page."""
    return FileResponse("static/index.html")

@app.get("/databases")
async def list_databases():
    """List available database configurations."""
    try:
        configs = db.configs
        databases = [
            {
                "name": name,
                "type": config["type"],
                "host": config["host"],
                "database": config["database"]
            }
            for name, config in configs.items()
        ]
        return {
            "databases": databases,
            "default": db._get_default_database()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/files")
async def list_files() -> List[Dict[str, Any]]:
    """List uploaded files."""
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
    schema: Optional[str] = Query(None, description="Schema name filter"),
    database: Optional[str] = Query(None, description="Database to use")
):
    """List available tables in database."""
    try:
        return db.list_tables(database=database, schema=schema)
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tables/{schema}/{table_name}")
async def get_table_info(
    schema: str,
    table_name: str,
    database: Optional[str] = Query(None, description="Database to use")
):
    """Get table metadata."""
    try:
        return db.get_table_info(f"{schema}.{table_name}", database=database)
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/analyze/table/{schema}/{table_name}")
async def analyze_table(
    schema: str,
    table_name: str,
    database: Optional[str] = Query(None, description="Database to use"),
    use_ai: bool = Query(True, description="Use AI for advanced analysis"),
    output_file: Optional[str] = Query(None, description="Optional JSON output file path"),
    rules_file: Optional[UploadFile] = File(None, description="Optional JSON file containing validation rules")
):
    """Analyze table data with optional AI-powered insights."""
    try:
        # Read table data
        df = db.read_table(table_name, schema=schema, database=database)
        
        # Get table info
        info = db.get_table_info(f"{schema}.{table_name}", database=database)

        logger.info(f"Table info: {df.dtypes}")
        
        if df.empty:
            result = {
                "schema": schema,
                "table_name": table_name,
                "metadata": info,
                "rows": 0,
                "columns": []
            }
        else:
            # Get analysis results
            quality_metrics = data_analyzer.analyze_data_quality(df)

            # Get validation rules if provided
            validation_rules = []
            if rules_file:
                rules_contents = await rules_file.read()
                rules_json = json.loads(rules_contents)
                validation_rules = rules_json.get('rules', [])

            # Validate data using Great Expectations
            validation_result = validate_data(df, validation_rules)
            
            # Prepare the complete result
            result = {
                "schema": schema,
                "table_name": table_name,
                "metadata": info,
                "analysis_timestamp": datetime.now().isoformat(),
                "quality_metrics": quality_metrics,
                "validation_result": validation_result
            }

        # Convert all data types to JSON-serializable format
        try:
            json_ready_result = convert_polars_types(result)
        except Exception as e:
            logger.error(f"Error converting data types: {e}")
            raise HTTPException(status_code=500, detail=f"Error converting data types: {str(e)}")

        # Write to JSON file if output path is provided
        if output_file:
            try:
                # Create output directory if it doesn't exist
                os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
                
                # Write to JSON file with proper encoding and formatting
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(json_ready_result, f, indent=2, ensure_ascii=False, cls=DateTimeEncoder)
                
                logger.info(f"Analysis results written to {output_file}")
            except Exception as e:
                logger.error(f"Error writing to JSON file: {e}")
                raise HTTPException(status_code=500, detail=f"Error writing to JSON file: {str(e)}")

        return json_ready_result

    except Exception as e:
        logger.error(f"Error analyzing table: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analyze/{filename}")
async def analyze_existing_file(filename: str, use_ai: bool = Query(True, description="Use AI for advanced analysis")):
    """Get basic file information without analysis."""
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
                    # If that fails, try reading as JSON Lines
                    df = pd.read_json(file_path, lines=True)
            else:
                raise HTTPException(status_code=400, detail=f"Unsupported file type: {ext}")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Error reading file: {str(e)}")

        if df.empty:
            return {
                "filename": filename,
                "rows": 0,
                "columns": []
            }

        return {
            "filename": filename,
            "rows": len(df),
            "columns": df.columns.tolist()
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Analysis error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/analyze/quality/{filename}")
async def analyze_file_quality(
    filename: str,
    use_ai: bool = Query(True, description="Use AI for advanced analysis"),
    rules_file: Optional[UploadFile] = File(None, description="Optional JSON file containing validation rules")
):
    """Analyze data quality of an existing file."""
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
                    # If that fails, try reading as JSON Lines
                    df = pd.read_json(file_path, lines=True)
            else:
                raise HTTPException(status_code=400, detail=f"Unsupported file type: {ext}")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Error reading file: {str(e)}")

        if df.empty:
            return {
                "filename": filename,
                "status": "No data found",
                "quality_metrics": {},
                "ai_insights": None
            }

        # Use DataAnalyzer for comprehensive data quality analysis
        quality_metrics = data_analyzer.analyze_data_quality(df)

        # Add validation result if rules are provided
        validation_result = None
        saved_rules_info = None
        if rules_file:
            # Read rules file
            rules_contents = await rules_file.read()
            rules_json = json.loads(rules_contents)
            validation_rules = rules_json.get('rules', [])
            
            # Save the rules
            try:
                name = os.path.splitext(rules_file.filename)[0]
                filepath = rule_storage.save_rules_to_file(validation_rules, name)
                saved_rules_info = {
                    "filepath": filepath,
                    "rule_count": len(validation_rules),
                    "name": name
                }
            except Exception as e:
                logger.warning(f"Failed to save rules: {e}")
            
            # Perform validation
            validation_result = validate_data(df, validation_rules)
        
        # Add AI insights if requested
        ai_insights = None
        if use_ai:
            try:
                context = {
                    "source": "file",
                    "filename": filename,
                    "file_type": ext
                }
                ai_analysis = ai_analyzer.analyze_dataframe(df, context=context)
                ai_insights = convert_numpy_types(ai_analysis.get("ai_insights"))
            except Exception as e:
                logger.error(f"AI analysis error: {e}")
                ai_insights = {"error": str(e)}

        return {
            "filename": filename,
            "status": "Success",
            "quality_metrics": quality_metrics,
            "validation_result": validation_result,
            "saved_rules_info": saved_rules_info,
            "ai_insights": ai_insights
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Analysis error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    use_ai: bool = Query(True, description="Use AI for advanced analysis")
):
    """Upload and analyze a new data file with optional AI insights."""
    try:
        # Ensure upload directory exists
        os.makedirs(UPLOAD_DIR, exist_ok=True)
        
        # Validate file extension
        allowed_extensions = ['csv', 'xlsx', 'xls', 'json']
        file_extension = file.filename.split('.')[-1].lower()
        if file_extension not in allowed_extensions:
            raise HTTPException(status_code=400, detail=f"Unsupported file type: {file_extension}")
        
        # Create a unique filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_filename = f"{timestamp}_{file.filename}"
        file_path = os.path.join(UPLOAD_DIR, unique_filename)

        # Save the uploaded file
        with open(file_path, "wb") as f:
            content = await file.read()
            f.write(content)
        
        # Return basic file info without analysis
        return {
            "filename": unique_filename,
            "original_filename": file.filename,
            "size": len(content),
            "path": f"/uploads/{unique_filename}",
            "upload_time": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Upload error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/validate")
async def validate(file: UploadFile = File(...), rules_file: UploadFile = File(...)):
    """Validate data against business rules."""
    try:
        # Step 1: Read business rules from the uploaded file (assumed to be JSON)
        rules_contents = await rules_file.read()
        rules_json = json.loads(rules_contents)
        rules = rules_json.get('rules', [])

        # Step 2: Read the CSV file into a DataFrame
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents))

        # Step 3: Validate the DataFrame based on the rules
        validation_result = validate_data(df, rules)
        return validation_result

    except Exception as e:
        logger.error(f"Error during validation: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.post("/rules/save")
async def save_rules(rules_file: UploadFile = File(...), name: Optional[str] = None):
    """Save business rules from a JSON file to the rules storage."""
    try:
        # Read the rules file
        contents = await rules_file.read()
        rules_json = json.loads(contents)
        rules = rules_json.get('rules', [])
        
        # Use the original filename if no name provided
        if not name:
            name = os.path.splitext(rules_file.filename)[0]
            
        # Save rules to file
        filepath = rule_storage.save_rules_to_file(rules, name)
        
        return {
            "status": "success",
            "message": "Rules saved successfully",
            "filepath": filepath,
            "rule_count": len(rules)
        }
        
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON format in rules file")
    except Exception as e:
        logger.error(f"Error saving rules: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
