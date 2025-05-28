document.addEventListener('DOMContentLoaded', function() {
    const fileSelectionArea = document.getElementById('file-selection-area');
    const fileSearchInput = document.getElementById('file-search-input');
    const fileCheckboxContainer = document.getElementById('file-checkbox-container');
    const showAllFilesBtn = document.getElementById('show-all-files-btn');

    const loadColumnsBtn = document.getElementById('load-columns-btn');
    const columnsDisplay = document.getElementById('columns-display');
    const selectTargetColumnsInput = document.getElementById('select-target-columns');
    const getDataBtn = document.getElementById('get-data-btn');
    const resultsTable = document.getElementById('results-table');
    const resultsArea = document.getElementById('results-area'); // 用于放置标题和下载按钮
    const resultsTableContainer = document.createElement('div'); // 新增：用于包裹表格，方便清空
    resultsTableContainer.id = 'results-table-container';
    resultsArea.appendChild(resultsTableContainer); // 将表格容器添加到resultsArea

    let availableFilesCache = [];
    let allAvailableColumnsCache = [];
    let showingAllFiles = false;
    const FILES_TO_SHOW_INITIALLY = 15;
    const ROWS_TO_DISPLAY_IN_TABLE = 10; // 新增：表格中显示的最大行数

    let currentDataForDownload = null; // 用于存储当前可供下载的数据
    let currentHeadersForDownload = null; // 用于存储当前可供下载的表头

    function renderFiles(filesToDisplay) {
        fileCheckboxContainer.innerHTML = '';
        const limit = showingAllFiles ? filesToDisplay.length : Math.min(filesToDisplay.length, FILES_TO_SHOW_INITIALLY);

        if (filesToDisplay.length === 0 && fileSearchInput.value.trim() !== '') {
            fileCheckboxContainer.innerHTML = "<p>未找到匹配搜索条件的文件。</p>";
        } else if (filesToDisplay.length === 0) {
            fileCheckboxContainer.innerHTML = "<p>未找到可用的Parquet文件。</p>";
        }

        for (let i = 0; i < limit; i++) {
            const file = filesToDisplay[i];
            const div = document.createElement('div');
            div.classList.add('file-item');
            const label = document.createElement('label');
            label.title = file;
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.name = 'selected_files';
            checkbox.value = file;
            label.appendChild(checkbox);
            label.appendChild(document.createTextNode(' ' + file));
            div.appendChild(label);
            fileCheckboxContainer.appendChild(div);
        }

        if (!showingAllFiles && filesToDisplay.length > FILES_TO_SHOW_INITIALLY) {
            showAllFilesBtn.style.display = 'block';
        } else {
            showAllFilesBtn.style.display = 'none';
        }
    }

    fetch('/api/files')
        .then(response => {
            if (!response.ok) {
                fileCheckboxContainer.innerHTML = `<p style="color:red;">获取文件列表时发生网络错误: ${response.status}</p>`;
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return response.json();
        })
        .then(files => {
            if (files.error) {
                fileCheckboxContainer.innerHTML = `<p style="color:red;">错误: ${files.error}</p>`;
                return;
            }
            availableFilesCache = files || [];
            if (availableFilesCache.length === 0) {
                fileCheckboxContainer.innerHTML = "<p>未找到可用的Parquet文件，或目录为空。</p>";
            }
            showingAllFiles = false;
            renderFiles(availableFilesCache);
        })
        .catch(error => {
            console.error('获取文件列表失败:', error);
            if (!fileCheckboxContainer.innerHTML.includes('网络错误')) {
                fileCheckboxContainer.innerHTML = `<p style="color:red;">获取文件列表失败: ${error.message}</p>`;
            }
        });

    showAllFilesBtn.addEventListener('click', function() {
        showingAllFiles = true;
        const searchTerm = fileSearchInput.value.toLowerCase().trim();
        const filteredFiles = searchTerm
            ? availableFilesCache.filter(file => file.toLowerCase().includes(searchTerm))
            : availableFilesCache;
        renderFiles(filteredFiles);
    });

    fileSearchInput.addEventListener('input', function() {
        const searchTerm = fileSearchInput.value.toLowerCase().trim();
        showingAllFiles = false;
        const filteredFiles = availableFilesCache.filter(file => file.toLowerCase().includes(searchTerm));
        renderFiles(filteredFiles);
    });

    loadColumnsBtn.addEventListener('click', function() {
        const selectedCheckboxes = document.querySelectorAll('#file-checkbox-container input[name="selected_files"]:checked');
        const selectedFiles = Array.from(selectedCheckboxes).map(cb => cb.value);

        if (selectedFiles.length === 0) {
            alert('请至少选择一个文件！');
            return;
        }

        columnsDisplay.value = '正在加载列名...';
        resultsArea.innerHTML = '<p>正在加载样本数据和列名...</p>';
        resultsTableContainer.innerHTML = ''; // 清空旧表格
        removeDownloadButton(); // 清除旧的下载按钮

        fetch('/api/get_data', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                selected_files: selectedFiles,
                target_columns: 'GET_COLUMN_NAMES_ONLY'
            })
        })
        .then(response => response.json())
        .then(data => {
            if (data.error) {
                alert(`错误: ${data.error}`);
                columnsDisplay.value = `错误: ${data.error}`;
                resultsArea.innerHTML = `<p style="color:red;">错误: ${data.error}</p>`;
                return;
            }
            allAvailableColumnsCache = data.all_columns || [];
            columnsDisplay.value = allAvailableColumnsCache.join('\n');
            // 修改点1: 默认填充所有合并后的列名到输入框
            selectTargetColumnsInput.value = allAvailableColumnsCache.join(',');

            const sampleHeaders = data.sample_data_columns || (data.sample_data && data.sample_data.length > 0 ? Object.keys(data.sample_data[0]) : []);
            if (data.sample_data && data.sample_data.length > 0) {
                displayDataInTable(`样本数据 (${data.sample_data_row_count} 行):`, data.sample_data, sampleHeaders, true, data.sample_data_row_count);
            } else {
                resultsArea.innerHTML = '<p>列名已加载。请输入您想查询的具体列。（未获取到样本数据）</p>';
            }
        })
        .catch(error => {
            console.error('加载列名失败:', error);
            alert('加载列名失败!');
            columnsDisplay.value = '加载列名失败。';
            resultsArea.innerHTML = `<p style="color:red;">加载列名失败: ${error.message}</p>`;
        });
    });

    getDataBtn.addEventListener('click', function() {
        const selectedCheckboxes = document.querySelectorAll('#file-checkbox-container input[name="selected_files"]:checked');
        const selectedFiles = Array.from(selectedCheckboxes).map(cb => cb.value);
        const targetColumns = selectTargetColumnsInput.value.trim();

        if (selectedFiles.length === 0) {
            alert('请至少选择一个文件！');
            return;
        }
        if (!targetColumns) {
            alert('请输入要查询的列名，或用 * 表示所有列。');
            return;
        }

        resultsArea.innerHTML = '<p>正在获取数据...</p>';
        resultsTableContainer.innerHTML = ''; // 清空旧表格
        removeDownloadButton(); // 清除旧的下载按钮

        fetch('/api/get_data', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                selected_files: selectedFiles,
                target_columns: targetColumns
            })
        })
        .then(response => response.json())
        .then(data => {
            if (data.error) {
                alert(`错误: ${data.error}`);
                resultsArea.innerHTML = `<p style="color:red;">获取数据失败: ${data.error}</p>`;
                currentDataForDownload = null; // 清空下载数据
                currentHeadersForDownload = null;
                return;
            }
            // 存储数据以供下载
            currentDataForDownload = data.data;
            currentHeadersForDownload = data.columns;

            displayDataInTable(`查询结果 (共 ${data.row_count} 行):`, data.data, data.columns, false, data.row_count);
            
            // 修改点2: 获取数据成功后，添加下载按钮
            if (data.data && data.data.length > 0) {
                addDownloadButton(selectedFiles, targetColumns === '*' ? 'all_columns' : targetColumns.split(',').length + '_columns');
            }
        })
        .catch(error => {
            console.error('获取数据失败:', error);
            alert('获取数据失败!');
            resultsArea.innerHTML = `<p style="color:red;">获取数据失败: ${error.message}</p>`;
            currentDataForDownload = null;
            currentHeadersForDownload = null;
        });
    });

    function displayDataInTable(title, dataRows, headers, isSample, totalRowCount) {
        resultsTableContainer.innerHTML = ''; // 清空旧表格和其容器内的其他内容（如旧标题）
        
        const titleElement = document.createElement('h3');
        titleElement.textContent = title;
        resultsTableContainer.appendChild(titleElement);

        if (!headers || headers.length === 0 || !dataRows ) {
             if (isSample && headers && headers.length > 0 && (!dataRows || dataRows.length === 0)) {
                // 对于样本，即使没数据行也显示表头
             } else {
                const noDataP = document.createElement('p');
                noDataP.textContent = "(无数据返回或未选择有效列/表头)";
                resultsTableContainer.appendChild(noDataP);
                return;
             }
        }
        
        // 克隆或重新创建表格元素，以确保它是干净的
        const newTable = document.createElement('table');
        newTable.id = 'results-table'; // 保持ID以便CSS生效
        const thead = newTable.createTHead();
        const tbody = newTable.createTBody();

        let headerHTML = '<tr>';
        headers.forEach(header => headerHTML += `<th>${header}</th>`);
        headerHTML += '</tr>';
        thead.innerHTML = headerHTML;

        let bodyHTML = '';
        // 修改点3: 只显示前N行
        const rowsToDisplay = isSample ? dataRows.length : Math.min(dataRows.length, ROWS_TO_DISPLAY_IN_TABLE);

        if (dataRows && dataRows.length > 0) {
            for (let i = 0; i < rowsToDisplay; i++) {
                const row = dataRows[i];
                bodyHTML += '<tr>';
                headers.forEach(header => {
                    const cellValue = row[header] !== undefined && row[header] !== null ? row[header] : '';
                    bodyHTML += `<td>${cellValue}</td>`;
                });
                bodyHTML += '</tr>';
            }
        } else {
            bodyHTML = `<tr><td colspan="${headers.length}">(无数据行)</td></tr>`;
        }
        tbody.innerHTML = bodyHTML;
        
        resultsTableContainer.appendChild(newTable);

        // 显示总行数和当前显示行数的提示 (仅对非样本数据)
        if (!isSample && totalRowCount > rowsToDisplay) {
            const infoP = document.createElement('p');
            infoP.textContent = `（表格中仅显示前 ${rowsToDisplay} 行，总共 ${totalRowCount} 行。请使用下载功能获取全部数据。）`;
            infoP.style.fontSize = '0.9em';
            infoP.style.marginTop = '5px';
            resultsTableContainer.appendChild(infoP);
        } else if (!isSample && totalRowCount === 0 && headers.length > 0) {
             // 如果有表头但确实没有数据行
        }
    }

    // 修改点2: 添加下载按钮的函数
    function addDownloadButton(selectedFileBasenames, selectedColumnInfo) {
        removeDownloadButton(); // 先移除已存在的按钮

        const downloadBtn = document.createElement('button');
        downloadBtn.id = 'download-data-btn';
        downloadBtn.textContent = '下载查询结果 (CSV)';
        downloadBtn.style.marginLeft = '10px'; // 与“获取数据”按钮有点间距
        downloadBtn.addEventListener('click', function() {
            if (currentDataForDownload && currentHeadersForDownload) {
                downloadCSV(currentDataForDownload, currentHeadersForDownload, selectedFileBasenames, selectedColumnInfo);
            } else {
                alert('没有可供下载的数据。');
            }
        });
        // 将下载按钮添加到 "获取选中列的数据" 按钮旁边，或者一个固定的下载区域
        getDataBtn.parentNode.insertBefore(downloadBtn, getDataBtn.nextSibling);
    }

    function removeDownloadButton() {
        const existingBtn = document.getElementById('download-data-btn');
        if (existingBtn) {
            existingBtn.parentNode.removeChild(existingBtn);
        }
    }

    // 修改点2: CSV下载核心逻辑
    function escapeCsvCell(cell) {
        if (cell === null || typeof cell === 'undefined') {
            return '';
        }
        cell = String(cell);
        // 如果单元格包含逗号、双引号或换行符，则用双引号包围，并将内部双引号转义为两个双引号
        if (cell.includes(',') || cell.includes('"') || cell.includes('\n') || cell.includes('\r')) {
            return `"${cell.replace(/"/g, '""')}"`;
        }
        return cell;
    }

    function downloadCSV(dataRows, headers, selectedFileBasenames, selectedColumnInfo) {
        let csvContent = "";
        // 添加表头
        csvContent += headers.map(header => escapeCsvCell(header)).join(',') + '\r\n';
        // 添加数据行
        dataRows.forEach(row => {
            const rowArray = headers.map(header => escapeCsvCell(row[header]));
            csvContent += rowArray.join(',') + '\r\n';
        });

        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
        const link = document.createElement('a');
        const url = URL.createObjectURL(blob);
        link.setAttribute('href', url);
        
        // 构建文件名
        let filename = 'downloaded_data';
        if (selectedFileBasenames && selectedFileBasenames.length > 0) {
            if (selectedFileBasenames.length <= 2) {
                filename = selectedFileBasenames.join('_');
            } else {
                filename = `${selectedFileBasenames[0]}_and_${selectedFileBasenames.length - 1}_more_files`;
            }
        }
        filename += `_${selectedColumnInfo}.csv`;
        filename = filename.replace(/[^a-zA-Z0-9_.-]/g, '_'); // 清理文件名

        link.setAttribute('download', filename);
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }
});