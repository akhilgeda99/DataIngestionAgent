document.addEventListener('DOMContentLoaded', () => {
    // Initialize UI elements
    const dropZone = document.getElementById('dropZone');
    const fileInput = document.getElementById('fileInput');
    const uploadProgress = document.getElementById('uploadProgress');
    const progressBar = uploadProgress.querySelector('.progress-bar');
    const fileList = document.getElementById('fileList');
    const fileAnalysis = document.getElementById('fileAnalysis');
    const tableList = document.getElementById('tableList');
    const tableAnalysis = document.getElementById('tableAnalysis');
    
    // Event listeners for file upload
    dropZone.addEventListener('click', () => fileInput.click());
    dropZone.addEventListener('dragover', (e) => {
        e.preventDefault();
        dropZone.style.borderColor = '#0d6efd';
        dropZone.style.background = '#f1f7ff';
    });
    dropZone.addEventListener('dragleave', () => {
        dropZone.style.borderColor = '#ccc';
        dropZone.style.background = '#f8f9fa';
    });
    dropZone.addEventListener('drop', handleFileDrop);
    fileInput.addEventListener('change', () => handleFileUpload(fileInput.files[0]));

    // Refresh buttons
    document.getElementById('refreshFiles').addEventListener('click', loadFiles);
    document.getElementById('refreshTables').addEventListener('click', loadTables);

    // Load initial data
    loadFiles();
    loadTables();

    // File handling functions
    async function handleFileDrop(e) {
        e.preventDefault();
        dropZone.style.borderColor = '#ccc';
        dropZone.style.background = '#f8f9fa';
        
        const file = e.dataTransfer.files[0];
        if (file) {
            await handleFileUpload(file);
        }
    }

    async function handleFileUpload(file) {
        const formData = new FormData();
        formData.append('file', file);

        uploadProgress.classList.remove('d-none');
        progressBar.style.width = '0%';
        progressBar.textContent = '0%';

        try {
            // Simulate upload progress
            const interval = setInterval(() => {
                const currentWidth = parseInt(progressBar.style.width);
                if (currentWidth < 90) {
                    const newWidth = currentWidth + 10;
                    progressBar.style.width = `${newWidth}%`;
                    progressBar.textContent = `${newWidth}%`;
                }
            }, 200);

            const response = await fetch('/upload', {
                method: 'POST',
                body: formData
            });

            clearInterval(interval);
            progressBar.style.width = '100%';
            progressBar.textContent = '100%';

            if (!response.ok) {
                throw new Error(`Upload failed: ${response.statusText}`);
            }

            const result = await response.json();
            showAlert('success', `File ${file.name} uploaded and analyzed successfully!`);
            
            // Transform the upload result to match the analysis structure
            const analysis = {
                analysis: {
                    row_count: result.rows,
                    column_count: result.columns.length,
                    quality_metrics: result.ai_analysis.quality_metrics
                }
            };

            // Update both the file list and analysis panel
            loadFiles();
            displayAnalysis(analysis, fileAnalysis, file.name);
        } catch (error) {
            showAlert('danger', `Upload failed: ${error.message}`);
            fileAnalysis.innerHTML = `
                <div class="alert alert-danger">
                    <i class="fas fa-exclamation-circle me-2"></i>
                    Upload failed: ${error.message}
                </div>
            `;
        } finally {
            setTimeout(() => {
                uploadProgress.classList.add('d-none');
            }, 1000);
        }
    }

    async function loadFiles() {
        try {
            const response = await fetch('/files');
            const files = await response.json();

            fileList.innerHTML = files.map(file => `
                <div class="card mb-2">
                    <div class="card-body p-2">
                        <div class="d-flex justify-content-between align-items-center">
                            <div>
                                <i class="fas fa-file me-2"></i>
                                <span>${file.name}</span>
                            </div>
                            <button class="btn btn-sm btn-primary analyze-file" data-filename="${file.name}">
                                Analyze
                            </button>
                        </div>
                    </div>
                </div>
            `).join('');

            // Add click handlers for analyze buttons
            document.querySelectorAll('.analyze-file').forEach(button => {
                button.addEventListener('click', () => analyzeFile(button.dataset.filename));
            });
        } catch (error) {
            showAlert('danger', `Failed to load files: ${error.message}`);
        }
    }

    async function analyzeFile(filename) {
        try {
            fileAnalysis.innerHTML = '<div class="text-center"><div class="spinner-border" role="status"></div></div>';
            
            const response = await fetch(`/analyze/quality/${filename}`);
            const data = await response.json();
            console.log('API Response:', data);

            // Transform the data to match the expected structure
            const analysis = {
                analysis: {
                    row_count: data.quality_metrics?.total_rows || 0,
                    column_count: data.quality_metrics?.total_columns || 0,
                    quality_metrics: data.quality_metrics || {},
                    ai_insights: data.ai_insights
                }
            };
            console.log('Transformed Analysis:', analysis);

            displayAnalysis(analysis, fileAnalysis, filename);
        } catch (error) {
            console.error('Analysis Error:', error);
            showAlert('danger', `Analysis failed: ${error.message}`);
            fileAnalysis.innerHTML = `
                <div class="alert alert-danger">
                    <i class="fas fa-exclamation-circle me-2"></i>
                    Analysis failed: ${error.message}
                </div>
            `;
        }
    }

    // Database functions
    async function loadTables() {
        try {
            const response = await fetch('/tables');
            const tables = await response.json();

            tableList.innerHTML = tables.map(table => `
                <div class="card mb-2">
                    <div class="card-body p-2">
                        <div class="d-flex justify-content-between align-items-center">
                            <div>
                                <i class="fas fa-table me-2"></i>
                                <span>${table.schema}.${table.table}</span>
                            </div>
                            <button class="btn btn-sm btn-primary analyze-table" 
                                    data-schema="${table.schema}" 
                                    data-table="${table.table}">
                                Analyze
                            </button>
                        </div>
                    </div>
                </div>
            `).join('');

            // Add click handlers for analyze buttons
            document.querySelectorAll('.analyze-table').forEach(button => {
                button.addEventListener('click', () => {
                    analyzeTable(button.dataset.schema, button.dataset.table);
                });
            });
        } catch (error) {
            showAlert('danger', `Failed to load tables: ${error.message}`);
        }
    }

    async function analyzeTable(schema, tableName) {
        try {
            tableAnalysis.innerHTML = '<div class="text-center"><div class="spinner-border" role="status"></div></div>';
            
            const response = await fetch(`/analyze/table/${schema}/${tableName}`);
            const analysis = await response.json();

            displayAnalysis(analysis, tableAnalysis, `${schema}.${tableName}`);
        } catch (error) {
            showAlert('danger', `Analysis failed: ${error.message}`);
        }
    }

    // Utility functions
    function displayAnalysis(analysis, container, itemName = '') {
        console.log('Display Analysis Input:', { analysis, itemName });
        
        // Handle case when analysis is empty or doesn't have expected structure
        if (!analysis || !analysis.analysis || !analysis.analysis.quality_metrics) {
            console.warn('Invalid analysis structure:', analysis);
            container.innerHTML = `
                <div class="alert alert-info">
                    <i class="fas fa-info-circle me-2"></i>
                    No analysis data available for this item.
                </div>
            `;
            return;
        }

        const metrics = analysis.analysis;
        const quality = metrics.quality_metrics;
        console.log('Quality Metrics:', quality);
        
        container.innerHTML = `
            <div class="analysis-header mb-4 text-center">
                <h4 class="analysis-title">
                    <i class="fas ${itemName.includes('.') ? 'fa-file-alt' : 'fa-table'} me-2"></i>
                    ${itemName}
                </h4>
                <div class="badge bg-primary">
                    <i class="fas fa-table me-1"></i> ${quality.total_rows} rows
                </div>
                <div class="badge bg-secondary">
                    <i class="fas fa-columns me-1"></i> ${quality.total_columns} columns
                </div>
            </div>

            <div class="row">
                <div class="col-12">
                    ${renderQualityMetrics(quality)}
                </div>
            </div>

            <div class="row mt-4">
                <div class="col-md-6">
                    ${renderNumericStats(quality)}
                </div>
                <div class="col-md-6">
                    ${renderCategoricalStats(quality)}
                </div>
            </div>

            ${metrics.ai_insights ? `
                <div class="row mt-4">
                    <div class="col-12">
                        <div class="card">
                            <div class="card-header">
                                <i class="fas fa-robot me-2"></i>
                                AI Insights
                            </div>
                            <div class="card-body">
                                <pre class="mb-0">${JSON.stringify(metrics.ai_insights, null, 2)}</pre>
                            </div>
                        </div>
                    </div>
                </div>
            ` : ''}
        `;

        // Add custom styling to tables
        container.querySelectorAll('.table').forEach(table => {
            table.classList.add('table-hover', 'table-striped', 'border');
        });
    }

    function renderQualityMetrics(metrics) {
        console.log('Render Quality Metrics Input:', metrics);
        
        if (!metrics || !metrics.column_stats) {
            console.warn('Invalid metrics structure:', metrics);
            return `
                <div class="alert alert-warning">
                    <i class="fas fa-exclamation-triangle me-2"></i>
                    No column statistics available.
                </div>
            `;
        }

        // Check if we have any columns to display
        const columns = Object.keys(metrics.column_stats);
        if (columns.length === 0) {
            return `
                <div class="alert alert-info">
                    <i class="fas fa-info-circle me-2"></i>
                    No columns to analyze.
                </div>
            `;
        }

        return `
            <div class="card shadow-sm">
                <div class="card-header bg-light">
                    <h6 class="card-title mb-0">
                        <i class="fas fa-chart-bar me-2"></i>
                        Data Quality Overview
                    </h6>
                </div>
                <div class="card-body p-0">
                    <div class="table-responsive">
                        <table class="table table-sm mb-0">
                            <thead class="table-light">
                                <tr>
                                    <th>Column</th>
                                    <th>Type</th>
                                    <th>Missing</th>
                                    <th>Unique</th>
                                    <th>Completeness</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${Object.entries(metrics.column_stats).map(([column, stats]) => {
                                    console.log('Processing column:', column, 'stats:', stats);
                                    const total = metrics.total_rows || 0;
                                    const missing = stats.null_count || 0;
                                    const completeness = total > 0 ? (total - missing) / total : 0;
                                    const completenessClass = completeness > 0.9 ? 'success' : 
                                                         completeness > 0.7 ? 'warning' : 'danger';
                                    return `
                                        <tr>
                                            <td><strong>${column}</strong></td>
                                            <td><span class="badge bg-secondary">${stats.dtype || 'N/A'}</span></td>
                                            <td>${missing}</td>
                                            <td>${stats.unique_count || 'N/A'}</td>
                                            <td>
                                                <div class="d-flex align-items-center">
                                                    <div class="progress flex-grow-1" style="height: 6px;">
                                                        <div class="progress-bar bg-${completenessClass}" 
                                                             role="progressbar" 
                                                             style="width: ${(completeness * 100).toFixed(1)}%"
                                                             aria-valuenow="${(completeness * 100).toFixed(1)}"
                                                             aria-valuemin="0"
                                                             aria-valuemax="100"></div>
                                                    </div>
                                                    <small class="text-muted ms-2">
                                                        ${(completeness * 100).toFixed(1)}%
                                                    </small>
                                                </div>
                                            </td>
                                        </tr>
                                    `;
                                }).join('')}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        `;
    }

    function renderNumericStats(metrics) {
        console.log('Render Numeric Stats Input:', metrics);
        
        // Find numeric columns from column_stats
        const numericColumns = Object.entries(metrics.column_stats || {})
            .filter(([_, stats]) => {
                const dtype = (stats.dtype || '').toLowerCase();
                return dtype.includes('float') || dtype.includes('int');
            });

        console.log('Numeric Columns:', numericColumns);

        if (numericColumns.length === 0) {
            return `
                <div class="alert alert-info">
                    <i class="fas fa-info-circle me-2"></i>
                    No numeric columns found.
                </div>
            `;
        }

        return `
            <div class="card shadow-sm h-100">
                <div class="card-header bg-light">
                    <h6 class="card-title mb-0">
                        <i class="fas fa-calculator me-2"></i>
                        Numeric Statistics
                    </h6>
                </div>
                <div class="card-body p-0">
                    <div class="table-responsive">
                        <table class="table table-sm mb-0">
                            <thead class="table-light">
                                <tr>
                                    <th>Column</th>
                                    <th>Mean</th>
                                    <th>Std</th>
                                    <th>Range</th>
                                    <th>Quartiles</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${numericColumns.map(([column, stats]) => {
                                    console.log('Processing numeric column:', column, 'stats:', stats);
                                    const formatNumber = (num) => {
                                        if (num === undefined || num === null) return 'N/A';
                                        const n = parseFloat(num);
                                        return isNaN(n) ? 'N/A' : n.toFixed(2);
                                    };

                                    const mean = formatNumber(stats.mean);
                                    const std = formatNumber(stats.std);
                                    const range = stats.min !== undefined && stats.max !== undefined
                                        ? `${formatNumber(stats.min)} - ${formatNumber(stats.max)}`
                                        : 'N/A';
                                    
                                    let quartiles = 'N/A';
                                    if (stats.quartiles) {
                                        const q1 = formatNumber(stats.quartiles['25']);
                                        const q2 = formatNumber(stats.quartiles['50']);
                                        const q3 = formatNumber(stats.quartiles['75']);
                                        if (q1 !== 'N/A' && q2 !== 'N/A' && q3 !== 'N/A') {
                                            quartiles = `25%: ${q1}<br>50%: ${q2}<br>75%: ${q3}`;
                                        }
                                    }

                                    return `
                                        <tr>
                                            <td><strong>${column}</strong></td>
                                            <td>${mean}</td>
                                            <td>${std}</td>
                                            <td>${range}</td>
                                            <td>${quartiles}</td>
                                        </tr>
                                    `;
                                }).join('')}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        `;
    }

    function renderCategoricalStats(metrics) {
        console.log('Render Categorical Stats Input:', metrics);
        
        // Find categorical columns from column_stats
        const categoricalColumns = Object.entries(metrics.column_stats || {})
            .filter(([_, stats]) => {
                const dtype = (stats.dtype || '').toLowerCase();
                return dtype === 'str' || dtype === 'category' || dtype.includes('object');
            });

        console.log('Categorical Columns:', categoricalColumns);

        if (categoricalColumns.length === 0) {
            return `
                <div class="alert alert-info">
                    <i class="fas fa-info-circle me-2"></i>
                    No categorical columns found.
                </div>
            `;
        }

        return `
            <div class="card shadow-sm h-100">
                <div class="card-header bg-light">
                    <h6 class="card-title mb-0">
                        <i class="fas fa-chart-pie me-2"></i>
                        Categorical Statistics
                    </h6>
                </div>
                <div class="card-body p-0">
                    <div class="table-responsive">
                        <table class="table table-sm mb-0">
                            <thead class="table-light">
                                <tr>
                                    <th>Column</th>
                                    <th>Unique Values</th>
                                    <th>Sample Values</th>
                                    <th>Missing</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${categoricalColumns.map(([column, stats]) => {
                                    console.log('Processing categorical column:', column, 'stats:', stats);
                                    const uniqueCount = stats.unique_count || 'N/A';
                                    const missing = stats.null_count || 0;
                                    const sampleValues = Array.isArray(stats.sample_values) && stats.sample_values.length > 0
                                        ? stats.sample_values.slice(0, 3).map(v => 
                                            v === null || v === undefined ? '(empty)' : String(v).slice(0, 30)
                                          ).join(', ')
                                        : 'N/A';
                                    
                                    return `
                                        <tr>
                                            <td><strong>${column}</strong></td>
                                            <td>${uniqueCount}</td>
                                            <td>
                                                <span class="text-muted small">
                                                    ${sampleValues}
                                                </span>
                                            </td>
                                            <td>${missing}</td>
                                        </tr>
                                    `;
                                }).join('')}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        `;
    }

    function showAlert(type, message) {
        const alert = document.createElement('div');
        alert.className = `alert alert-${type} alert-dismissible fade show position-fixed top-0 end-0 m-3`;
        alert.innerHTML = `
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        `;
        document.body.appendChild(alert);
        setTimeout(() => alert.remove(), 5000);
    }
});
