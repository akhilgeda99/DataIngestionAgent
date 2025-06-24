"""Data profiling module for generating detailed reports."""

import os
import pandas as pd
import json
from logging import getLogger

logger = getLogger(__name__)

def generate_profile_report(df: pd.DataFrame, output_dir: str, title: str = "Data Profile Report") -> str:
    """Generate a detailed profile report for the DataFrame.
    
    Args:
        df: The pandas DataFrame to analyze
        output_dir: Directory to save the report
        title: Title for the report
        
    Returns:
        str: Path to the generated HTML report
    """
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate report filename
        report_path = os.path.join(output_dir, "profile_report.html")
        
        # Generate profile data
        logger.info("Generating profile report...")
        profile_data = {
            'title': title,
            'summary': {
                'rows': len(df),
                'columns': len(df.columns),
                'missing_cells': df.isna().sum().sum(),
                'missing_cells_pct': round(df.isna().sum().sum() / (len(df) * len(df.columns)) * 100, 2),
                'duplicate_rows': len(df) - len(df.drop_duplicates()),
                'duplicate_rows_pct': round((len(df) - len(df.drop_duplicates())) / len(df) * 100, 2)
            },
            'columns': {}
        }
        
        # Analyze each column
        for col in df.columns:
            col_data = {
                'name': col,
                'type': str(df[col].dtype),
                'count': len(df[col]),
                'missing': int(df[col].isna().sum()),
                'missing_pct': round(df[col].isna().sum() / len(df[col]) * 100, 2),
                'unique': int(df[col].nunique()),
                'unique_pct': round(df[col].nunique() / len(df[col]) * 100, 2)
            }
            
            # Add descriptive statistics for numeric columns
            if pd.api.types.is_numeric_dtype(df[col]):
                col_data.update({
                    'mean': float(df[col].mean()) if not df[col].empty else None,
                    'std': float(df[col].std()) if not df[col].empty else None,
                    'min': float(df[col].min()) if not df[col].empty else None,
                    'max': float(df[col].max()) if not df[col].empty else None,
                    'quartiles': {
                        '25%': float(df[col].quantile(0.25)) if not df[col].empty else None,
                        '50%': float(df[col].quantile(0.50)) if not df[col].empty else None,
                        '75%': float(df[col].quantile(0.75)) if not df[col].empty else None
                    }
                })
            
            # Add value counts (top 10)
            value_counts = df[col].value_counts().head(10).to_dict()
            col_data['top_values'] = [
                {'value': str(k), 'count': int(v), 'percentage': round(v/len(df[col])*100, 2)}
                for k, v in value_counts.items()
            ]
            
            profile_data['columns'][col] = col_data
        
        # Generate HTML report
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>{title}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .summary {{ background: #f5f5f5; padding: 20px; border-radius: 5px; margin-bottom: 20px; }}
                .column {{ background: white; padding: 20px; border: 1px solid #ddd; margin-bottom: 10px; border-radius: 5px; }}
                table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
                th, td {{ text-align: left; padding: 8px; border-bottom: 1px solid #ddd; }}
                th {{ background-color: #f5f5f5; }}
                .chart {{ margin-top: 10px; }}
            </style>
        </head>
        <body>
            <h1>{title}</h1>
            
            <div class="summary">
                <h2>Dataset Summary</h2>
                <table>
                    <tr><th>Number of Rows</th><td>{profile_data['summary']['rows']}</td></tr>
                    <tr><th>Number of Columns</th><td>{profile_data['summary']['columns']}</td></tr>
                    <tr><th>Missing Cells</th><td>{profile_data['summary']['missing_cells']} ({profile_data['summary']['missing_cells_pct']}%)</td></tr>
                    <tr><th>Duplicate Rows</th><td>{profile_data['summary']['duplicate_rows']} ({profile_data['summary']['duplicate_rows_pct']}%)</td></tr>
                </table>
            </div>
            
            <h2>Column Analysis</h2>
            {''.join([f'''
            <div class="column">
                <h3>{col_info['name']} ({col_info['type']})</h3>
                <table>
                    <tr><th>Count</th><td>{col_info['count']}</td></tr>
                    <tr><th>Missing</th><td>{col_info['missing']} ({col_info['missing_pct']}%)</td></tr>
                    <tr><th>Unique</th><td>{col_info['unique']} ({col_info['unique_pct']}%)</td></tr>
                    {f'''
                    <tr><th>Mean</th><td>{col_info.get('mean', 'N/A')}</td></tr>
                    <tr><th>Std</th><td>{col_info.get('std', 'N/A')}</td></tr>
                    <tr><th>Min</th><td>{col_info.get('min', 'N/A')}</td></tr>
                    <tr><th>Max</th><td>{col_info.get('max', 'N/A')}</td></tr>
                    <tr><th>25%</th><td>{col_info.get('quartiles', {}).get('25%', 'N/A')}</td></tr>
                    <tr><th>50%</th><td>{col_info.get('quartiles', {}).get('50%', 'N/A')}</td></tr>
                    <tr><th>75%</th><td>{col_info.get('quartiles', {}).get('75%', 'N/A')}</td></tr>
                    ''' if 'mean' in col_info else ''}
                </table>
                
                <h4>Top Values</h4>
                <table>
                    <tr><th>Value</th><th>Count</th><th>Percentage</th></tr>
                    {''.join([f"<tr><td>{v['value']}</td><td>{v['count']}</td><td>{v['percentage']}%</td></tr>" for v in col_info['top_values']])}
                </table>
            </div>
            ''' for col, col_info in profile_data['columns'].items()])}
            
            <script>
                console.log('Profile data:', {json.dumps(profile_data)});
            </script>
        </body>
        </html>
        """
        
        # Save the report
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
            
        logger.info(f"Profile report saved to: {report_path}")
        
        return report_path
        
    except Exception as e:
        logger.error(f"Error generating profile report: {e}")
        raise
