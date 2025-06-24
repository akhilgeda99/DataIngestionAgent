"""AI-powered data analyzer using LLMs."""

import os
import logging
from pathlib import Path
import yaml
import json
import base64
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, date
import time
import pandas as pd
import polars as pl
from huggingface_hub import HfApi, login, hf_hub_download
import requests
from src.analysis.profiling import generate_profile_report

from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
from ctransformers import AutoModelForCausalLM as CTAutoModelForCausalLM

# Configure logging
log_dir = Path(__file__).parent.parent / 'logs'
log_dir.mkdir(exist_ok=True)
log_file = log_dir / 'ai_analysis.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

from src.analysis import DataAnalyzer

class AIAnalyzer:
    """AI-powered data analyzer using local LLMs."""
    
    def __init__(self, model_path: Optional[str] = None, api_url: Optional[str] = None, 
                 username: Optional[str] = None, password: Optional[str] = None):
        """Initialize the AI analyzer.
        
        Args:
            model_path: Optional path to the model
            api_url: Optional API URL for authentication
            username: Optional username for authentication
            password: Optional password for authentication
        """
        try:
            self.model_path = model_path or "TheBloke/Mistral-7B-Instruct-v0.2-GGUF"
            self.api_url = api_url
            self.username = username
            self.password = password
            
            # Load HuggingFace credentials
            config_path = Path(__file__).parent.parent / 'config' / 'api_keys.yaml'
            if config_path.exists():
                with open(config_path) as f:
                    config = yaml.safe_load(f)
                    hf_config = config.get('huggingface', {})
                    
                    # Get credentials from config if not provided
                    self.username = username or hf_config.get('username')
                    self.password = password or hf_config.get('password')
                    self.api_url = api_url or hf_config.get('api_url')
                    
                    if self.username and self.password:
                        try:
                            # Login to HuggingFace or custom API
                            if self.api_url:
                                # Custom API authentication logic here
                                logger.info(f"Using custom API URL: {self.api_url}")
                                # You can implement custom API auth here
                                auth = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
                                headers = {
                                    "Authorization": f"Basic {auth}"
                                }
                                response = requests.get(self.api_url, headers=headers)
                                if response.status_code == 200:
                                    logger.info("Successfully authenticated with custom API")
                                else:
                                    logger.error("Failed to authenticate with custom API")
                            else:
                                # Default HuggingFace login
                                login(username=self.username, password=self.password)
                                logger.info(f"Successfully logged in to HuggingFace as {self.username}")
                            
                            # Get API token after login
                            api = HfApi()
                            self.hf_token = api.get_token()
                            if self.hf_token:
                                os.environ['HUGGING_FACE_HUB_TOKEN'] = self.hf_token
                                logger.info("Retrieved and set HuggingFace token")
                            else:
                                logger.warning("Could not retrieve HuggingFace token after login")
                        except Exception as e:
                            logger.error(f"Failed to login: {e}")
                            self.hf_token = None
                    else:
                        logger.warning("No valid credentials found in config")
                        self.hf_token = None
            
            # Initialize DataAnalyzer
            self.data_analyzer = DataAnalyzer()
            
            logger.info("Model initialized successfully")
            logger.info("Model initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing AIAnalyzer: {str(e)}")
            logger.exception("Full stack trace:")
            self.model = None
            self.tokenizer = None

    def analyze_dataframe(self, df: Union[pd.DataFrame, pl.DataFrame], context: Optional[Dict] = None) -> Dict:
        """Analyze DataFrame using GPT for insights.
        
        Args:
            df: Input DataFrame (either pandas or polars)
            context: Optional context dictionary with metadata
        """
        try:
            logger.info("Starting DataFrame analysis...")
            
            # Verify authentication if using custom API
            if self.api_url and not (self.username and self.password):
                raise ValueError("Username and password required for custom API URL")
            
            # Convert polars DataFrame to pandas if needed
            if isinstance(df, pl.DataFrame):
                logger.info("Converting polars DataFrame to pandas...")
                df = df.to_pandas()
            
            # Generate data profile report first
            try:
                output_dir = os.path.join(os.getcwd(), "reports")
                report_path = generate_profile_report(
                    df, 
                    output_dir=output_dir,
                    title=f"Data Profile Report - {len(df)} rows"
                )
                logger.info(f"Generated profile report at: {report_path}")
            except Exception as e:
                logger.warning(f"Could not generate profile report: {e}")
                report_path = None
            
            # Get statistics using DataAnalyzer
            logger.info("Computing data quality metrics using DataAnalyzer...")
            quality_analysis = self.data_analyzer.analyze_data_quality(df)
            
            # Add report path to context if available
            if report_path:
                if context is None:
                    context = {}
                context["profile_report_path"] = report_path
            
            # Add authentication context
            auth_context = {
                "api_url": self.api_url,
                "username": self.username,
                "authenticated": bool(self.username and self.password)
            }
            if context is None:
                context = {}
            context.update({"auth": auth_context})
            
            # Generate prompt for GPT
            logger.info("Building analysis prompt...")
            prompt = self._build_analysis_prompt(df, quality_analysis, context)
            logger.info(f"Generated prompt: {prompt[:200]}...")  # Log first 200 chars of prompt
            
            # Initialize variables for retry logic
            max_retries = 3
            insights = None
            
            for retry in range(max_retries):
                try:
                    # Get insights from GPT
                    logger.info(f"Requesting insights from GPT (attempt {retry + 1}/{max_retries})...")
                    
                    if self.api_url:
                        # Use custom API with Base64 authentication
                        auth = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
                        headers = {
                            "Authorization": f"Basic {auth}",
                            "Content-Type": "application/json"
                        }
                        
                        payload = {
                            "prompt": prompt,
                            "max_tokens": 1000
                        }
                        
                        response = requests.post(self.api_url, headers=headers, json=payload)
                        if response.status_code == 200:
                            response_content = response.json()
                        else:
                            raise Exception(f"API request failed with status {response.status_code}")
                    else:
                        # No API URL configured
                        raise Exception("No API URL configured for AI analysis")
                    
                    # Parse response
                    try:
                        if isinstance(response_content, str):
                            insights = json.loads(response_content)
                        else:
                            insights = response_content
                        
                        # Combine DataAnalyzer metrics with AI insights
                        insights.update({
                            "data_quality_metrics": quality_analysis.get("quality_metrics", {}),
                            "column_statistics": quality_analysis.get("column_statistics", {}),
                            "profile_report_path": report_path if report_path else None
                        })
                        break  # If successful, break retry loop
                    except json.JSONDecodeError:
                        logger.warning("Failed to parse response as JSON")
                        if retry == max_retries - 1:  # Last retry
                            insights = {
                                "error": "Invalid response format",
                                "message": "Failed to parse response as JSON"
                            }
                
                except Exception as e:
                    logger.error(f"Error during request (attempt {retry + 1}): {str(e)}")
                    if retry == max_retries - 1:  # Last retry
                        insights = {
                            "error": "Request failed",
                            "message": str(e)
                        }
                    time.sleep(2 ** retry)  # Exponential backoff
            
            return insights
            
        except Exception as e:
            logger.error(f"Error in analyze_dataframe: {str(e)}")
            logger.exception("Full stack trace:")
            return {
                "error": "Analysis failed",
                "message": str(e)
            }

    def _build_analysis_prompt(self, df: pd.DataFrame, quality_analysis: Dict, context: Optional[Dict[str, Any]] = None) -> str:
        """Build a prompt for GPT analysis."""
        # Add authentication header if using custom API
        auth_header = ""
        if context and context.get("auth", {}).get("api_url"):
            auth_header = f"""
Authentication Context:
- API URL: {context['auth']['api_url']}
- Username: {context['auth']['username']}
- Status: {'Authenticated' if context['auth']['authenticated'] else 'Not Authenticated'}

"""

        prompt = f"""<s>[INST] You are a data analysis expert. Please analyze this dataset:
{auth_header}
Dataset Summary:
- Total rows: {len(df)}
- Total columns: {len(df.columns)}

Data Quality Overview:
"""
        # Add quality metrics
        for metric, value in quality_analysis.get('quality_metrics', {}).items():
            prompt += f"- {metric}: {value}\n"

        prompt += "\nColumn Analysis:\n"
        for col, stats in quality_analysis.get('column_statistics', {}).items():
            prompt += f"\n{col}:\n"
            # Add basic stats
            null_count = stats.get('null_count', 0)
            unique_count = stats.get('unique_count', 0)
            prompt += f"- Unique values: {unique_count}\n"
            prompt += f"- Missing values: {null_count}\n"
            
            # Add numeric stats if available
            if 'mean' in stats:
                prompt += f"- Stats: mean={stats['mean']}, std={stats.get('std', 'N/A')}\n"
                prompt += f"- Range: [{stats.get('min', 'N/A')} to {stats.get('max', 'N/A')}]\n"
            
            # Add value distribution if available
            if 'value_counts' in stats and stats['value_counts']:
                top_values = stats['value_counts'][:3]
                prompt += f"- Top values: {', '.join(str(v) for v in top_values)}\n"
        
        # Add context if provided
        if context:
            prompt += "\nAdditional Context:\n"
            for key, value in context.items():
                prompt += f"{key}: {value}\n"
        
        # Add specific analysis requests
        prompt += """\nPlease provide a detailed analysis in JSON format with the following sections:

1. key_findings: Important patterns, trends, or anomalies
   - Look for correlations between variables
   - Identify any unusual patterns or outliers
   - Note any significant trends or seasonality

2. data_quality: Assessment of data quality and reliability
   - Evaluate completeness and accuracy
   - Identify potential data collection issues
   - Suggest data quality improvements

3. recommendations: Actionable suggestions
   - Business process improvements
   - Data collection enhancements
   - Potential areas for further investigation

4. potential_use_cases: Practical applications of this data
   - Business intelligence opportunities
   - Automation possibilities
   - Decision support scenarios

Keep your responses concise and focused on insights that can drive action or decisions.

Format your response as a valid JSON object with these exact keys: key_findings, data_quality, recommendations, potential_use_cases[/INST]</s>"""
        
        return prompt

# Create global instance
ai_analyzer = AIAnalyzer()