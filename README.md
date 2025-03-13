# Data Ingestion Agent

An intelligent data ingestion system that processes and analyzes various data file formats using AI techniques.

## Features

- File upload and storage with web interface
- Support for multiple file formats (CSV, Excel, JSON)
- AI-powered data analysis
- Data quality metrics
- Anomaly detection for numerical data
- Permanent file storage with unique timestamps

## Project Structure

```
DataIngestionAgent/
├── requirements.txt     # Project dependencies
├── src/                # Source code
│   ├── ai_agent.py     # AI analysis components
│   └── api.py          # FastAPI web server
├── static/             # Static web files
│   └── index.html      # Web interface
└── uploads/            # Uploaded files storage
```

## Setup and Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Start the server:
```bash
uvicorn src.api:app --reload
```

3. Access the web interface at http://127.0.0.1:8000

## Supported File Formats

- CSV (.csv)
- Excel (.xlsx, .xls)
- JSON (.json)

## Features

### Data Analysis
- Basic statistics for numerical columns
- Data type detection
- Missing value analysis
- Unique value counts
- Anomaly detection

### File Storage
- Automatic file organization
- Unique timestamped filenames
- Direct file access links
- Original filename preservation
