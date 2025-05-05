// static/js/dashboard_controls.js (Refatorado)

document.addEventListener('DOMContentLoaded', function() {

    // --- 1. Estado e Instâncias ---
    let chartInstances = {
        monthlyLine: null,
        fornecedoraPie: null,
        concessionariaBar: null,
        overduePaymentsBar: null
    };

    let sortState = {}; // Estado da ordenação das tabelas

    // --- 2. Seletores DOM (Centralizado) ---
    function getDOMElements() {
        return {
            monthSelect: document.getElementById('filter-month'),
            yearSelectChart: document.getElementById('chart-year'),
            overdueDaysFilter: document.getElementById('overdue-days-filter'),
            kpi: {
                totalKwh: document.getElementById('kpi-total-kwh'),
                clientesAtivos: document.getElementById('kpi-clientes-ativos-count'),
                clientesRegistrados: document.getElementById('kpi-clientes-registrados-count')
            },
            summaryTables: {
                fornecedora: {
                    table: document.getElementById('fornecedora-summary-table'),
                    body: document.getElementById('fornecedora-summary-tbody'),
                    status: document.getElementById('fornecedora-summary-status'),
                    id: 'fornecedora-summary-table'
                },
                concessionaria: {
                    table: document.getElementById('concessionaria-summary-table'),
                    body: document.getElementById('concessionaria-summary-tbody'),
                    status: document.getElementById('concessionaria-summary-status'),
                    id: 'concessionaria-summary-table'
                },
                fornecedoraNoRcb: {
                    table: document.getElementById('fornecedora-no-rcb-table'),
                    body: document.getElementById('fornecedora-no-rcb-tbody'),
                    status: document.getElementById('fornecedora-no-rcb-status'),
                    id: 'fornecedora-no-rcb-table'
                }
            },
            charts: {
                monthlyLine: { canvas: document.getElementById('remunerationChart'), status: null }, // Status não aplicável aqui
                fornecedoraPie: { canvas: document.getElementById('fornecedoraPieChart'), status: document.getElementById('fornecedora-pie-chart-status') },
                concessionariaBar: { canvas: document.getElementById('concessionariaBarChart'), status: document.getElementById('concessionaria-bar-chart-status') },
                overduePaymentsBar: { canvas: document.getElementById('overduePaymentsChart'), status: document.getElementById('overdue-payments-chart-status') }
            },
            gridContainer: document.querySelector('.dashboard-grid')
        };
    }

    // --- 3. Funções Utilitárias ---
    function formatNumber(num, decimalPlaces = 0) {
        if (typeof num !== 'number' || isNaN(num)) { return '0'; }
        return num.toLocaleString('pt-BR', {
            minimumFractionDigits: decimalPlaces,
            maximumFractionDigits: decimalPlaces
        });
    }

    async function fetchData(url, description) {
        console.debug(`Workspaceing ${description} from ${url}`);
        try {
            const response = await fetch(url);
            if (!response.ok) {
                let errorMsg = `Erro HTTP ${response.status}`;
                try {
                    const data = await response.json();
                    errorMsg = data.error || `Erro ${response.status} ao buscar ${description}`;
                } catch (e) {
                    errorMsg = `Erro ${response.status} (sem detalhes JSON) ao buscar ${description}`;
                }
                console.error(`Falha ao buscar ${description}: ${errorMsg} (URL: ${url})`);
                throw new Error(errorMsg); // Lança o erro para ser pego pelo chamador
            }
            return await response.json();
        } catch (error) {
            console.error(`Erro de rede/JSON ao buscar ${description}:`, error);
            throw error; // Relança o erro
        }
    }

    // --- 4. Funções de Atualização de Componentes ---

    /** Atualiza um KPI específico */
    function updateSingleKPI(element, value, formatDecimalPlaces = 0) {
        if (element) {
            element.textContent = formatNumber(value, formatDecimalPlaces);
        } else {
            console.warn("Elemento KPI não encontrado para atualização.");
        }
    }

    /** Busca e atualiza todos os KPIs de uma vez */
    async function updateKPIs(elements, month) {
        // Mostra loading nos KPIs
        Object.values(elements.kpi).forEach(el => { if(el) el.innerHTML = '<i class="fas fa-spinner fa-spin fa-xs"></i>'; });

        const endpoints = {
            kpiKwh: `/api/kpi/total-kwh?month=${month}`,
            kpiAtivos: `/api/kpi/clientes-ativos?month=${month}`,
            kpiRegistrados: `/api/kpi/clientes-registrados?month=${month}`
        };

        try {
            const [dataKpiKwh, dataKpiAtivos, dataKpiRegistrados] = await Promise.all([
                fetchData(endpoints.kpiKwh, "KPI Total kWh"),
                fetchData(endpoints.kpiAtivos, "KPI Clientes Ativos"),
                fetchData(endpoints.kpiRegistrados, "KPI Clientes Registrados")
            ]);

            updateSingleKPI(elements.kpi.totalKwh, dataKpiKwh?.total_kwh, 0);
            updateSingleKPI(elements.kpi.clientesAtivos, dataKpiAtivos?.clientes_ativos_count, 0);
            updateSingleKPI(elements.kpi.clientesRegistrados, dataKpiRegistrados?.clientes_registrados_count, 0);
            console.log("KPIs atualizados com sucesso.");

        } catch (error) {
            console.error('Erro ao buscar dados dos KPIs (Promise.all falhou):', error);
            Object.values(elements.kpi).forEach(el => { if(el) el.innerHTML = `<span style="font-size: 0.7em; color: red;">Erro!</span>`; });
        }
    }

    /** Atualiza uma tabela de resumo genérica (Fornecedora, Concessionária) */
    async function updateSummaryTable(tableConfig, apiUrl, description, rowGenerator) {
        if (!tableConfig || !tableConfig.body || !tableConfig.status) {
            console.error(`Configuração inválida para tabela ${description}`);
            return;
        }
        tableConfig.status.innerHTML = '<i class="fas fa-spinner fa-spin fa-sm"></i> Carregando...';
        tableConfig.status.style.display = 'block';
        tableConfig.body.innerHTML = `<tr><td colspan="3" style="text-align: center; color: #999; font-style: italic;">Carregando ${description}...</td></tr>`;

        try {
            const data = await fetchData(apiUrl, description);
            tableConfig.status.style.display = 'none';
            tableConfig.body.innerHTML = ''; // Limpa antes de adicionar

            if (data && Array.isArray(data) && data.length > 0) {
                data.forEach(rowData => {
                    const tr = rowGenerator(rowData); // Usa a função específica para gerar a linha
                    if (tr) tableConfig.body.appendChild(tr);
                });
                reapplySort(tableConfig.id); // Reaplicar ordenação
            } else if (data && Array.isArray(data) && data.length === 0) {
                tableConfig.body.innerHTML = `<tr><td colspan="3" style="text-align: center;">Nenhum dado de ${description}.</td></tr>`;
            } else {
                 throw new Error(`Formato de resposta inválido para ${description}.`);
            }
             console.log(`Tabela ${description} atualizada.`);

        } catch (error) {
            console.error(`Erro ao atualizar tabela ${description}:`, error);
            tableConfig.status.innerHTML = `<span style="color: red; font-size: 0.9em;"><i class="fas fa-exclamation-triangle"></i> Erro</span>`;
            tableConfig.status.style.display = 'block';
            tableConfig.body.innerHTML = `<tr><td colspan="3" style="text-align: center; color: red;">Falha ao carregar ${description}.</td></tr>`;
        }
    }

     /** Função geradora de linha para tabelas de resumo padrão (Fornecedora/Concessionaria) */
     function createSummaryTableRow(rowData, labelField) {
        const tr = document.createElement('tr');
        // Ajusta para pegar o nome da coluna correto ('fornecedora' ou 'concessionaria')
        const labelValue = rowData[labelField] || 'N/A';
        tr.innerHTML = `
            <td>${labelValue}</td>
            <td style="text-align: right;">${formatNumber(rowData.qtd_clientes, 0)}</td>
            <td style="text-align: right;">${formatNumber(rowData.soma_consumo || rowData.soma_consumomedio, 2)} kWh</td>
        `;
        return tr;
    }

    /** Atualiza especificamente o card Fornecedora s/ RCB */
    async function updateFornecedoraNoRcbCard(tableConfig) {
        if (!tableConfig || !tableConfig.body || !tableConfig.status) {
            console.error("Configuração inválida para tabela Fornecedora s/ RCB"); return;
        }
        tableConfig.status.innerHTML = '<i class="fas fa-spinner fa-spin fa-sm"></i> Carregando...';
        tableConfig.status.style.display = 'block';
        tableConfig.body.innerHTML = '<tr><td colspan="3" style="text-align: center; color: #999; font-style: italic;">Carregando...</td></tr>';

        const apiUrl = "/api/summary/fornecedora-no-rcb";
        try {
            const apiResponse = await fetchData(apiUrl, "Fornecedora s/ RCB");
            tableConfig.status.style.display = 'none';
            tableConfig.body.innerHTML = '';

            if (apiResponse && Array.isArray(apiResponse.data) && apiResponse.data.length > 0) {
                 apiResponse.data.forEach(row => {
                    const tr = document.createElement('tr');
                    // Usa as chaves retornadas pela API ('fornecedora', 'numero_clientes', 'soma_consumomedio')
                    tr.innerHTML = `
                        <td>${row.fornecedora || 'N/A'}</td>
                        <td style="text-align: right;">${formatNumber(row.numero_clientes, 0)}</td>
                        <td style="text-align: right;">${formatNumber(row.soma_consumomedio, 0)} kWh</td>
                    `;
                    tableConfig.body.appendChild(tr);
                 });
                reapplySort(tableConfig.id);
            } else if (apiResponse && Array.isArray(apiResponse.data) && apiResponse.data.length === 0) {
                tableConfig.body.innerHTML = '<tr><td colspan="3" style="text-align: center;">Nenhum dado encontrado.</td></tr>';
            } else {
                 throw new Error(apiResponse.error || 'Formato de resposta inválido da API.');
            }
            console.log("Card Fornecedora s/ RCB atualizado.");
        } catch (error) {
            console.error(`Erro ao buscar ou renderizar card Fornecedora sem RCB:`, error);
            tableConfig.status.innerHTML = `<span style="color: red;"><i class="fas fa-exclamation-triangle"></i> Erro ao carregar</span>`;
            tableConfig.status.style.display = 'block';
            tableConfig.body.innerHTML = '<tr><td colspan="3" style="text-align: center; color: red;">Falha ao carregar dados.</td></tr>';
        }
    }

    /** Atualiza o gráfico de linha mensal */
    async function updateMonthlyChart(chartInstance, canvasElement, year) {
        if (!chartInstance) { console.error("Instância do gráfico de linha não inicializada."); return; }
        const apiUrl = `/api/chart/monthly-active-clients?year=${year}`;
        try {
            const chartData = await fetchData(apiUrl, `Gráfico Mensal ${year}`);
            if (chartData && Array.isArray(chartData.monthly_counts) && chartData.monthly_counts.length === 12) {
                chartInstance.data.datasets[0].data = chartData.monthly_counts;
                chartInstance.data.datasets[0].label = `Ativações ${year}`;
                chartInstance.update();
                console.log(`Gráfico mensal atualizado para ${year}.`);
            } else {
                console.warn(`Dados inválidos ou incompletos recebidos para o gráfico do ano ${year}:`, chartData);
                chartInstance.data.datasets[0].data = Array(12).fill(0);
                chartInstance.data.datasets[0].label = `Sem dados ${year}`;
                chartInstance.update();
            }
        } catch (error) {
            console.error(`Erro ao buscar dados do gráfico para ${year}:`, error);
             chartInstance.data.datasets[0].data = Array(12).fill(0);
             chartInstance.data.datasets[0].label = `Erro ${year}`;
             chartInstance.update();
        }
    }

    /** Atualiza o gráfico de pizza de fornecedoras */
    async function updateFornecedoraPieChart(chartConfig, month) {
        const { canvas, status } = chartConfig;
        const instance = chartInstances.fornecedoraPie; // Usar a instância global

        if (!canvas || !status) { console.error("Elementos do gráfico de pizza Fornecedora não encontrados."); return; }
        status.innerHTML = '<i class="fas fa-spinner fa-spin fa-lg"></i><br>Carregando...';
        status.style.display = 'block';
        canvas.style.display = 'none';

        const apiUrl = `/api/pie/clientes-fornecedora?month=${month}`;
        try {
            const apiData = await fetchData(apiUrl, `Gráfico Pizza Fornecedora ${month}`);
            status.style.display = 'none';

            if (apiData && Array.isArray(apiData.labels) && Array.isArray(apiData.data) && apiData.labels.length > 0) {
                const backgroundColors = ['#00B034', '#FFC107', '#2196F3', '#E91E63', '#9C27B0', '#4CAF50', '#FF9800', '#03A9F4', '#F44336', '#673AB7', '#8BC34A', '#FF5722', '#00BCD4', '#CDDC39', '#795548'];
                const pieColors = Array.from({ length: apiData.data.length }, (_, i) => backgroundColors[i % backgroundColors.length]);

                if (instance) {
                    instance.data.labels = apiData.labels;
                    instance.data.datasets[0].data = apiData.data;
                    instance.data.datasets[0].backgroundColor = pieColors;
                    instance.update();
                } else {
                    const config = { type: 'pie', data: { labels: apiData.labels, datasets: [{ label: 'Clientes Ativos', data: apiData.data, backgroundColor: pieColors }] }, options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom', labels: { boxWidth: 15, padding: 15 } }, tooltip: { callbacks: { label: function(context) { let label = context.label || ''; let value = context.parsed || 0; let total = context.chart.data.datasets[0].data.reduce((a, b) => a + b, 0); let percentage = total > 0 ? ((value / total) * 100).toFixed(1) : 0; return ` ${label}: ${formatNumber(value)} (${percentage}%)`; } } }, title: { display: false } } } };
                    chartInstances.fornecedoraPie = new Chart(canvas, config); // Atualiza instância global
                }
                 canvas.style.display = 'block';
                 console.log(`Gráfico Pizza Fornecedora atualizado para ${month}.`);
            } else {
                status.innerHTML = '<i class="fas fa-info-circle"></i><br>Nenhum dado para exibir.';
                status.style.display = 'block';
                 if (instance) { instance.destroy(); chartInstances.fornecedoraPie = null; } // Limpa instância global
            }
        } catch (error) {
            console.error(`Erro ao buscar/renderizar gráfico pizza fornecedora para ${month}:`, error);
            status.innerHTML = `<span style="color: red;"><i class="fas fa-exclamation-triangle"></i> Erro</span>`;
            status.style.display = 'block';
             if (instance) { instance.destroy(); chartInstances.fornecedoraPie = null; } // Limpa instância global
        }
    }

    /** Atualiza o gráfico de barras de concessionárias */
    async function updateConcessionariaBarChart(chartConfig, month) {
        const { canvas, status } = chartConfig;
        const instance = chartInstances.concessionariaBar;

        if (!canvas || !status) { console.error("Elementos do gráfico de barras Concessionária não encontrados."); return; }
        status.innerHTML = '<i class="fas fa-spinner fa-spin fa-lg"></i><br>Carregando...';
        status.style.display = 'block';
        canvas.style.display = 'none';

        const apiUrl = `/api/bar/clientes-concessionaria?month=${month}&limit=8`;
        try {
            const apiData = await fetchData(apiUrl, `Gráfico Barras Concessionária ${month}`);
            status.style.display = 'none';

            if (apiData && Array.isArray(apiData.labels) && Array.isArray(apiData.data) && apiData.labels.length > 0) {
                const barBackgroundColor = 'rgba(33, 150, 243, 0.6)';
                const barBorderColor = 'rgba(33, 150, 243, 1)';

                if (instance) {
                    instance.data.labels = apiData.labels;
                    instance.data.datasets[0].data = apiData.data;
                    instance.update();
                } else {
                    const config = { type: 'bar', data: { labels: apiData.labels, datasets: [{ label: 'Clientes Ativos', data: apiData.data, backgroundColor: barBackgroundColor, borderColor: barBorderColor, borderWidth: 1 }] }, options: { indexAxis: 'y', responsive: true, maintainAspectRatio: false, scales: { x: { beginAtZero: true, grid: { color: 'rgba(0,0,0,0.05)' }, ticks: { precision: 0 } }, y: { grid: { display: false } } }, plugins: { legend: { display: false }, tooltip: { callbacks: { label: function(context) { let label = context.dataset.label || ''; let value = context.parsed.x || 0; return ` ${label}: ${formatNumber(value)}`; } } }, title: { display: false } } } };
                    chartInstances.concessionariaBar = new Chart(canvas, config);
                }
                canvas.style.display = 'block';
                console.log(`Gráfico Barras Concessionária atualizado para ${month}.`);
            } else {
                status.innerHTML = '<i class="fas fa-info-circle"></i><br>Nenhum dado para exibir.';
                status.style.display = 'block';
                if (instance) { instance.destroy(); chartInstances.concessionariaBar = null; }
            }
        } catch (error) {
             console.error(`Erro ao buscar/renderizar gráfico barras concessionária para ${month}:`, error);
             status.innerHTML = `<span style="color: red;"><i class="fas fa-exclamation-triangle"></i> Erro</span>`;
             status.style.display = 'block';
             if (instance) { instance.destroy(); chartInstances.concessionariaBar = null; }
        }
    }

    /** Atualiza o gráfico de barras de pagamentos vencidos */
    async function updateOverduePaymentsChart(chartConfig, days) {
        const { canvas, status } = chartConfig;
        const instance = chartInstances.overduePaymentsBar;

        if (!canvas || !status) { console.error("Elementos do gráfico de Pagamentos Vencidos não encontrados."); return; }
        status.innerHTML = '<i class="fas fa-spinner fa-spin fa-lg"></i><br>Carregando...';
        status.style.display = 'block';
        canvas.style.display = 'none';

        const apiUrl = `/api/chart/overdue-payments?days=${days}`;
        try {
            const apiData = await fetchData(apiUrl, `Gráfico Vencidos ${days} dias`);
            status.style.display = 'none';

            if (apiData && Array.isArray(apiData.labels) && Array.isArray(apiData.data) && apiData.labels.length > 0) {
                const barBackgroundColor = 'rgba(239, 68, 68, 0.6)';
                const barBorderColor = 'rgba(220, 38, 38, 1)';

                if (instance) {
                    instance.data.labels = apiData.labels;
                    instance.data.datasets[0].data = apiData.data;
                    instance.options.plugins.title.text = `Vencidos s/ Baixa > ${days} dias`;
                    instance.update();
                } else {
                    const config = { type: 'bar', data: { labels: apiData.labels, datasets: [{ label: 'Qtd. Vencidos', data: apiData.data, backgroundColor: barBackgroundColor, borderColor: barBorderColor, borderWidth: 1 }] }, options: { indexAxis: 'y', responsive: true, maintainAspectRatio: false, scales: { x: { beginAtZero: true, grid: { color: 'rgba(0,0,0,0.05)' }, ticks: { precision: 0 } }, y: { grid: { display: false } } }, plugins: { legend: { display: false }, tooltip: { callbacks: { label: function(context) { let label = context.dataset.label || ''; let value = context.parsed.x || 0; return ` ${label}: ${formatNumber(value)}`; } } }, title: { display: true, text: `Vencidos s/ Baixa > ${days} dias`, padding: { top: 5, bottom: 10 }, font: { size: 14, weight: 'normal' }, color: '#666' } } } };
                    chartInstances.overduePaymentsBar = new Chart(canvas, config);
                }
                canvas.style.display = 'block';
                console.log(`Gráfico Vencidos (${days} dias) atualizado.`);
            } else {
                status.innerHTML = '<i class="fas fa-info-circle"></i><br>Nenhum dado para exibir.';
                status.style.display = 'block';
                if (instance) { instance.destroy(); chartInstances.overduePaymentsBar = null; }
            }
        } catch (error) {
             console.error(`Erro ao buscar/renderizar gráfico vencidos (${days} dias):`, error);
             status.innerHTML = `<span style="color: red;"><i class="fas fa-exclamation-triangle"></i> Erro</span>`;
             status.style.display = 'block';
             if (instance) { instance.destroy(); chartInstances.overduePaymentsBar = null; }
        }
    }


    // --- 5. Lógica de Ordenação de Tabelas (Reutilizada) ---
    function getCellValue(row, columnIndex, sortType) { const cell = row.cells[columnIndex]; if (!cell) return sortType === 'number' ? 0 : ''; let value = cell.textContent || cell.innerText || ''; if (sortType === 'number') { value = value.replace(/\./g, '').replace(/ kWh/gi, '').replace(/,/g, '.').trim(); const num = parseFloat(value); return isNaN(num) ? -Infinity : num; } return value.trim().toLowerCase(); }
    function updateSortIcons(tableId, clickedHeader) { const table = document.getElementById(tableId); if (!table) return; const headers = table.querySelectorAll('thead th[data-column-index]'); const state = sortState[tableId] || { colIndex: -1, direction: 'none' }; const iconSelector = '.sort-icon'; headers.forEach(header => { const iconSpan = header.querySelector(iconSelector); if (iconSpan) { iconSpan.textContent = (header === clickedHeader) ? (state.direction === 'asc' ? ' ▲' : (state.direction === 'desc' ? ' ▼' : '')) : ''; } }); }
    function sortTable(tableId, columnIndex, sortType) { const table = document.getElementById(tableId); const tbody = table ? table.querySelector('tbody') : null; if (!tbody) { console.error(`TBody não encontrado para ${tableId}`); return; } if (!sortState[tableId]) sortState[tableId] = { colIndex: -1, direction: 'none' }; const state = sortState[tableId]; state.direction = (columnIndex === state.colIndex && state.direction === 'asc') ? 'desc' : 'asc'; state.colIndex = columnIndex; const rows = Array.from(tbody.querySelectorAll('tr')); rows.sort((rowA, rowB) => { const valueA = getCellValue(rowA, columnIndex, sortType); const valueB = getCellValue(rowB, columnIndex, sortType); let comparison = (sortType === 'number') ? valueA - valueB : valueA.localeCompare(valueB, 'pt-BR', { sensitivity: 'base' }); return state.direction === 'asc' ? comparison : comparison * -1; }); tbody.innerHTML = ''; rows.forEach(row => tbody.appendChild(row)); const clickedHeader = table.querySelector(`thead th[data-column-index="${columnIndex}"]`); updateSortIcons(tableId, clickedHeader); }
    function reapplySort(tableId) { const state = sortState[tableId]; if (state && state.colIndex !== -1 && state.direction !== 'none') { const table = document.getElementById(tableId); const header = table ? table.querySelector(`thead th[data-column-index="${state.colIndex}"]`) : null; if (header) { const sortType = header.getAttribute('data-sort-type'); const originalDirection = state.direction; // Store original direction
        // Force reset before sorting again to ensure correct toggle
        const tempDirection = state.direction === 'asc' ? 'desc' : 'asc'; // Temporarily store the *next* direction
        state.direction = tempDirection; // Set to the opposite to ensure the next call toggles correctly
        sortTable(tableId, parseInt(header.getAttribute('data-column-index')), sortType); // Sort, toggling direction
        } } else { updateSortIcons(tableId, null); } } // Clear icons if no sort state
    function addSortListeners(tableId, headerSelectorClass) { const table = document.getElementById(tableId); const headers = table ? table.querySelectorAll(`thead th.${headerSelectorClass}`) : []; if (!headers.length) { /* console.warn(`Cabeçalhos .${headerSelectorClass} não achados em #${tableId}.`); */ return; } headers.forEach(header => { if (header.hasAttribute('data-column-index') && header.hasAttribute('data-sort-type')) { header.style.cursor = 'pointer'; header.addEventListener('click', function() { const columnIndex = parseInt(this.getAttribute('data-column-index'), 10); const sortType = this.getAttribute('data-sort-type'); sortTable(tableId, columnIndex, sortType); }); } }); }

    // --- 6. Orquestração e Event Handlers ---

    /** Atualiza todos os dados dependentes do mês selecionado */
    function updateDashboardForMonth(month, elements) {
        console.log(`Orquestrando atualização para o mês: ${month}`);
        // Atualiza KPIs
        updateKPIs(elements, month);

        // Atualiza Tabelas de Resumo
        updateSummaryTable(
            elements.summaryTables.fornecedora,
            `/api/summary/fornecedora?month=${month}`,
            'Fornecedora',
            (row) => createSummaryTableRow(row, 'fornecedora') // Passa a chave correta
        );
        updateSummaryTable(
            elements.summaryTables.concessionaria,
            `/api/summary/concessionaria?month=${month}`,
            'Concessionária',
            (row) => createSummaryTableRow(row, 'concessionaria') // Passa a chave correta
        );

        // Atualiza Gráficos dependentes do mês
        updateFornecedoraPieChart(elements.charts.fornecedoraPie, month);
        updateConcessionariaBarChart(elements.charts.concessionariaBar, month);

        // O Card Fornecedora s/ RCB e Vencidos são atualizados independentemente
    }

    /** Handler para mudança no seletor de mês */
    function handleMonthChange(event, elements) {
        const selectedMonth = event.target.value;
        const currentUrl = new URL(window.location);
        currentUrl.searchParams.set('month', selectedMonth);
        window.history.replaceState({}, '', currentUrl); // Atualiza URL sem recarregar
        updateDashboardForMonth(selectedMonth, elements);
    }

    /** Handler para mudança no seletor de ano do gráfico mensal */
    function handleYearChange(event, elements) {
        updateMonthlyChart(chartInstances.monthlyLine, elements.charts.monthlyLine.canvas, event.target.value);
    }

     /** Handler para mudança no seletor de dias do gráfico de vencidos */
     function handleDaysOverdueChange(event, elements) {
        updateOverduePaymentsChart(elements.charts.overduePaymentsBar, event.target.value);
    }

    // --- 7. Inicialização ---

    /** Inicializa os gráficos que precisam de configuração base */
    function initializeCharts(elements) {
        // Gráfico de Linha Mensal
        if (elements.charts.monthlyLine.canvas) {
            const initialYear = elements.yearSelectChart?.value || new Date().getFullYear();
            const labels = ['JAN', 'FEV', 'MAR', 'ABR', 'MAI', 'JUN', 'JUL', 'AGO', 'SET', 'OUT', 'NOV', 'DEZ'];
            const initialData = { labels: labels, datasets: [{ label: 'Carregando...', data: Array(12).fill(0), fill: true, backgroundColor: 'rgba(0, 201, 59, 0.1)', borderColor: 'rgb(0, 201, 59)', borderWidth: 2, tension: 0.3, pointBackgroundColor: 'rgb(0, 201, 59)', pointRadius: 3, pointHoverRadius: 5 }] };
            const config = { type: 'line', data: initialData, options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false }, tooltip: { backgroundColor: 'rgba(0, 0, 0, 0.7)', titleFont: { weight: 'bold'}, callbacks: { label: function(context) { return (context.dataset.label || '') + ': ' + formatNumber(context.parsed.y); } } } }, scales: { y: { beginAtZero: true, grid: { color: 'rgba(0,0,0,0.05)' } }, x: { grid: { display: false } } }, interaction: { intersect: false, mode: 'index', } } };
            chartInstances.monthlyLine = new Chart(elements.charts.monthlyLine.canvas, config);
            updateMonthlyChart(chartInstances.monthlyLine, elements.charts.monthlyLine.canvas, initialYear); // Busca dados iniciais
        } else {
             console.error("Canvas do gráfico de linha mensal não encontrado.");
        }
         // Outros gráficos são inicializados quando os dados chegam pela primeira vez
    }

    /** Configura os event listeners */
    function setupEventListeners(elements) {
        if (elements.monthSelect) {
            elements.monthSelect.addEventListener('change', (event) => handleMonthChange(event, elements));
        } else { console.error("Elemento #filter-month não encontrado para listener."); }

        if (elements.yearSelectChart) {
            elements.yearSelectChart.addEventListener('change', (event) => handleYearChange(event, elements));
        } else { console.error("Elemento #chart-year não encontrado para listener."); }

         if (elements.overdueDaysFilter) {
             elements.overdueDaysFilter.addEventListener('change', (event) => handleDaysOverdueChange(event, elements));
         } else { console.error("Elemento #overdue-days-filter não encontrado para listener."); }
    }

    /** Configura a ordenação das tabelas */
    function initializeTableSorting(tableConfigs) {
        Object.values(tableConfigs).forEach(config => {
            if (config && config.id) {
                addSortListeners(config.id, 'sortable-header');
            }
        });
    }

    /** Configura o SortableJS */
    function initializeSortable(containerElement) {
        if (containerElement && typeof Sortable !== 'undefined') {
            new Sortable(containerElement, {
                animation: 150, ghostClass: 'sortable-ghost', chosenClass: 'sortable-chosen', dragClass: 'sortable-drag',
                // handle: '.card-header', // Descomente para arrastar só pelo header
                store: {
                    get: function (sortable) { const order = localStorage.getItem(sortable.options.group.name || 'dashboardGridOrder'); return order ? order.split('|') : []; },
                    set: function (sortable) { const order = sortable.toArray(); localStorage.setItem(sortable.options.group.name || 'dashboardGridOrder', order.join('|')); console.log("Nova ordem salva:", order); }
                },
                onEnd: function (evt) { console.log('Item movido:', evt.item.dataset.id); }
            });
            console.log("SortableJS inicializado para .dashboard-grid");
        } else {
            if (!containerElement) console.error("Container .dashboard-grid não encontrado para SortableJS.");
            if (typeof Sortable === 'undefined') console.error("SortableJS não está carregado.");
        }
    }

    /** Função Principal de Inicialização */
    function main() {
        const elements = getDOMElements();

        if (!elements.monthSelect) {
            console.error("Seletor de mês não encontrado. Dashboard não pode ser inicializado corretamente.");
            return; // Aborta se o elemento principal de controle não existe
        }

        const initialMonth = elements.monthSelect.value;
        const initialOverdueDays = elements.overdueDaysFilter?.value || '30'; // Pega valor inicial dos vencidos

        initializeCharts(elements); // Inicializa gráficos base (linha)
        setupEventListeners(elements); // Configura listeners de filtros
        initializeTableSorting(elements.summaryTables); // Configura ordenação das tabelas
        initializeSortable(elements.gridContainer); // Configura arrastar/soltar

        // Carrega dados iniciais
        updateDashboardForMonth(initialMonth, elements); // Carrega dados dependentes do MÊS
        updateFornecedoraNoRcbCard(elements.summaryTables.fornecedoraNoRcb); // Carrega card Fornecedora s/ RCB
        updateOverduePaymentsChart(elements.charts.overduePaymentsBar, initialOverdueDays); // Carrega gráfico de Vencidos inicial

        console.log("Dashboard inicializado.");
    }

    // --- Ponto de Entrada ---
    main();

}); // Fim DOMContentLoaded