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
            
            const response = await fetch(`/analyze/${filename}`);
            const data = await response.json();

            // Transform the data to match the expected structure
            const analysis = {
                analysis: {
                    row_count: data.rows,
                    column_count: data.columns.length,
                    quality_metrics: data.ai_analysis.quality_metrics
                }
            };

            displayAnalysis(analysis, fileAnalysis, filename);
        } catch (error) {
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
        // Handle case when analysis is empty or doesn't have expected structure
        if (!analysis || !analysis.analysis) {
            container.innerHTML = `
                <div class="alert alert-info">
                    <i class="fas fa-info-circle me-2"></i>
                    No analysis data available for this item.
                </div>
            `;
            return;
        }

        const metrics = analysis.analysis;
        
        container.innerHTML = `
            <div class="analysis-header mb-4 text-center">
                <h4 class="analysis-title">
                    <i class="fas ${itemName.includes('.') ? 'fa-file-alt' : 'fa-table'} me-2"></i>
                    ${itemName}
                </h4>
                <div class="badge bg-primary">
                    <i class="fas fa-table me-1"></i> ${metrics.row_count} rows
                </div>
                <div class="badge bg-secondary">
                    <i class="fas fa-columns me-1"></i> ${metrics.column_count} columns
                </div>
            </div>

            <div class="row">
                <div class="col-12">
                    ${metrics.quality_metrics ? renderQualityMetrics(metrics.quality_metrics) : ''}
                </div>
            </div>

            ${metrics.quality_metrics ? `
                <div class="row mt-4">
                    <div class="col-md-6">
                        ${renderNumericStats(metrics.quality_metrics)}
                    </div>
                    <div class="col-md-6">
                        ${renderCategoricalStats(metrics.quality_metrics)}
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
        if (!metrics || !metrics.data_types) {
            return '';
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
                                ${Object.keys(metrics.data_types).map(column => {
                                    const completeness = metrics.completeness_ratio?.[column] || 0;
                                    const completenessClass = completeness > 0.9 ? 'success' : 
                                                            completeness > 0.7 ? 'warning' : 'danger';
                                    return `
                                        <tr>
                                            <td><strong>${column}</strong></td>
                                            <td><span class="badge bg-secondary">${metrics.data_types[column] || 'N/A'}</span></td>
                                            <td>${metrics.missing_values?.[column] || 0}</td>
                                            <td>${metrics.unique_values?.[column] || 'N/A'}</td>
                                            <td>
                                                <div class="d-flex align-items-center">
                                                    <div class="progress flex-grow-1" style="height: 6px;">
                                                        <div class="progress-bar bg-${completenessClass}" 
                                                             role="progressbar" 
                                                             style="width: ${(completeness * 100).toFixed(1)}%">
                                                        </div>
                                                    </div>
                                                    <span class="ms-2 small">${(completeness * 100).toFixed(1)}%</span>
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
        if (!metrics.numeric_stats || Object.keys(metrics.numeric_stats).length === 0) {
            return '';
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
                                </tr>
                            </thead>
                            <tbody>
                                ${Object.entries(metrics.numeric_stats).map(([column, stats]) => `
                                    <tr>
                                        <td><strong>${column}</strong></td>
                                        <td>${stats?.mean?.toFixed(2) || 'N/A'}</td>
                                        <td>${stats?.std?.toFixed(2) || 'N/A'}</td>
                                        <td>
                                            <span class="text-muted">${stats?.min ?? 'N/A'}</span>
                                            <i class="fas fa-arrow-right mx-1 small"></i>
                                            <span class="text-muted">${stats?.max ?? 'N/A'}</span>
                                        </td>
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        `;
    }

    function renderCategoricalStats(metrics) {
        if (!metrics.categorical_stats || Object.keys(metrics.categorical_stats).length === 0) {
            return '';
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
                                    <th>Top Values</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${Object.entries(metrics.categorical_stats).map(([column, stats]) => `
                                    <tr>
                                        <td><strong>${column}</strong></td>
                                        <td>
                                            ${stats?.top_values?.map((value, index) => `
                                                <div class="mb-1">
                                                    <div class="d-flex justify-content-between align-items-center">
                                                        <span class="text-truncate me-2">${value}</span>
                                                        <span class="badge bg-info">${stats.frequencies[index]}</span>
                                                    </div>
                                                    <div class="progress" style="height: 4px;">
                                                        <div class="progress-bar bg-info" 
                                                             role="progressbar" 
                                                             style="width: ${(stats.frequencies[index] / Math.max(...stats.frequencies) * 100)}%">
                                                        </div>
                                                    </div>
                                                </div>
                                            `).join('') || 'N/A'}
                                        </td>
                                    </tr>
                                `).join('')}
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
