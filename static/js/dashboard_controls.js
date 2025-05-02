// static/js/dashboard_controls.js

document.addEventListener('DOMContentLoaded', () => {

    // --- Elementos do DOM (Controles de Visibilidade) ---
    const modal = document.getElementById('card-selection-modal');
    const openModalBtn = document.getElementById('select-cards-btn');
    const closeModalBtn = document.getElementById('close-modal-btn');
    const saveBtn = document.getElementById('save-card-selection-btn'); // Botão de fechar
    const checkboxes = document.querySelectorAll('#card-selection-modal input[name="card-visibility"]');
    const dashboardGrid = document.querySelector('.dashboard-grid');

    // --- Elementos do DOM (Dados e Gráficos) ---
    const monthSelect = document.getElementById('filter-month');
    const fornecedoraTableBody = document.getElementById('fornecedora-summary-tbody');
    const fornecedoraStatus = document.getElementById('fornecedora-summary-status');
    const concessionariaTableBody = document.getElementById('concessionaria-summary-tbody');
    const concessionariaStatus = document.getElementById('concessionaria-summary-status');
    const kpiTotalKwhElement = document.getElementById('kpi-total-kwh');
    const kpiClientesAtivosElement = document.getElementById('kpi-clientes-ativos-count');
    const kpiClientesRegistradosElement = document.getElementById('kpi-clientes-registrados-count');
    const chartCanvas = document.getElementById('remunerationChart'); // Canvas do gráfico de linha
    const yearSelectChart = document.getElementById('chart-year'); // Select de ano do gráfico de linha
    const fornecedoraPieCanvas = document.getElementById('fornecedoraPieChart');
    const fornecedoraPieStatus = document.getElementById('fornecedora-pie-chart-status');
    const concessionariaBarCanvas = document.getElementById('concessionariaBarChart');
    const concessionariaBarStatus = document.getElementById('concessionaria-bar-chart-status');
    const fornecedoraNoRcbTableBody = document.getElementById('fornecedora-no-rcb-tbody');
    const fornecedoraNoRcbStatus = document.getElementById('fornecedora-no-rcb-status');
    const overdueDaysFilter = document.getElementById('overdue-days-filter');
    const overduePaymentsCanvas = document.getElementById('overduePaymentsChart');
    const overduePaymentsStatus = document.getElementById('overdue-payments-chart-status');

    // --- Variáveis de Estado ---
    const STORAGE_KEY = 'dashboardCardVisibility';
    let monthlyChartInstance = null; // Instância do gráfico de linha
    let fornecedoraPieChartInstance = null;
    let concessionariaBarChartInstance = null;
    let overduePaymentsChartInstance = null;
    const sortState = {}; // Para ordenação das tabelas

    // --- Funções de Persistência (localStorage) ---
    const loadCardVisibility = () => {
        const storedSettings = localStorage.getItem(STORAGE_KEY);
        return storedSettings ? JSON.parse(storedSettings) : {};
    };

    const saveCardVisibility = (settings) => {
        localStorage.setItem(STORAGE_KEY, JSON.stringify(settings));
    };

    // --- Funções do Modal ---
    const openModal = () => {
        if (modal) modal.style.display = 'block';
        updateCheckboxStates();
    };

    const closeModal = () => {
        if (modal) modal.style.display = 'none';
    };

    // --- Funções de Aplicação da Visibilidade ---
    const applyVisibility = (settings) => {
        if (!dashboardGrid) return;
        console.log("Aplicando visibilidade:", settings);
        const allCards = dashboardGrid.querySelectorAll('.card[id]'); // Pega só cards com ID
        allCards.forEach(card => {
            const cardId = card.id;
            // Se uma configuração não existe, considera visível por padrão
            const isVisible = settings[cardId] !== false;
            // Aplica uma classe para ocultar via CSS
            // Garanta que existe uma classe .card-hidden { display: none !important; } no seu CSS
            card.classList.toggle('card-hidden', !isVisible);
        });
    };

    // --- Função para Atualizar Estado dos Checkboxes no Modal ---
    const updateCheckboxStates = () => {
        const currentSettings = loadCardVisibility();
        checkboxes.forEach(checkbox => {
            const cardId = checkbox.value;
            checkbox.checked = currentSettings[cardId] !== false; // Marcado se não for explicitamente false
        });
    };

    // --- Função Inicializadora de Visibilidade ---
    const initializeCardVisibility = () => {
        const settings = loadCardVisibility();
        // Define padrões apenas se não houver NADA salvo
        if (Object.keys(settings).length === 0) {
            console.log("Nenhuma configuração salva. Definindo visibilidade padrão (todos visíveis).");
            const initialSettings = {};
            checkboxes.forEach(checkbox => {
                 initialSettings[checkbox.value] = true;
                 checkbox.checked = true;
            });
            saveCardVisibility(initialSettings);
            applyVisibility(initialSettings);
        } else {
            applyVisibility(settings);
            updateCheckboxStates();
        }
    };


    // --- Função para formatar número ---
    function formatNumber(num, decimalPlaces = 0) {
        if (typeof num !== 'number' || isNaN(num)) { return '0'; }
        return num.toLocaleString('pt-BR', {
            minimumFractionDigits: decimalPlaces,
            maximumFractionDigits: decimalPlaces
        });
    }

    // --- Funções de Atualização de Dados e Gráficos (com verificação de visibilidade) ---
    // (Funções updateChartData, updateFornecedoraPieChart, etc., permanecem as mesmas da versão anterior)
    // ... (Cole aqui as funções de update da versão anterior, elas já checam visibilidade) ...
     // Atualiza o gráfico de linha "Evolução Ativações"
    async function updateChartData(year) {
        const cardId = 'card-chart-evolucao';
        const cardElement = document.getElementById(cardId);
        const currentVisibility = loadCardVisibility(); // Pega configurações atuais

        // PULA a atualização se o card estiver oculto
        if (!cardElement || currentVisibility[cardId] === false) {
            if (monthlyChartInstance) { monthlyChartInstance.destroy(); monthlyChartInstance = null; }
            return; // Sai da função
        }

        if (!monthlyChartInstance && chartCanvas) {
           console.log(`Recriando instância do gráfico de evolução para o ano ${year}`);
           const labels = ['JAN', 'FEV', 'MAR', 'ABR', 'MAI', 'JUN', 'JUL', 'AGO', 'SET', 'OUT', 'NOV', 'DEZ'];
           const initialData = { labels: labels, datasets: [{ label: `Carregando ${year}...`, data: Array(12).fill(0), fill: true, backgroundColor: 'rgba(0, 201, 59, 0.1)', borderColor: 'rgb(0, 201, 59)', borderWidth: 2, tension: 0.3, pointBackgroundColor: 'rgb(0, 201, 59)', pointRadius: 3, pointHoverRadius: 5 }] };
           const config = { type: 'line', data: initialData, options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false }, tooltip: { backgroundColor: 'rgba(0, 0, 0, 0.7)', titleFont: { weight: 'bold'}, callbacks: { label: function(context) { return (context.dataset.label || '') + ': ' + formatNumber(context.parsed.y); } } } }, scales: { y: { beginAtZero: true, grid: { color: 'rgba(0,0,0,0.05)' } }, x: { grid: { display: false } } }, interaction: { intersect: false, mode: 'index', } } };
           monthlyChartInstance = new Chart(chartCanvas, config);
        } else if (!chartCanvas) {
            console.error("Elemento canvas do gráfico de linha (#remunerationChart) não encontrado.");
            return;
        }

        if(monthlyChartInstance) {
            monthlyChartInstance.data.datasets[0].label = `Carregando ${year}...`;
            monthlyChartInstance.data.datasets[0].data = Array(12).fill(0);
            monthlyChartInstance.update();
        }

        try {
            const apiUrl = `/api/chart/monthly-active-clients?year=${year}`;
            const response = await fetch(apiUrl);
            if (!response.ok) throw new Error(`HTTP error ${response.status}`);
            const chartData = await response.json();

            if (monthlyChartInstance && chartData && Array.isArray(chartData.monthly_counts) && chartData.monthly_counts.length === 12) {
                monthlyChartInstance.data.datasets[0].data = chartData.monthly_counts;
                monthlyChartInstance.data.datasets[0].label = `Ativações ${year}`;
                monthlyChartInstance.update();
            } else if (monthlyChartInstance) {
                monthlyChartInstance.data.datasets[0].data = Array(12).fill(0);
                monthlyChartInstance.data.datasets[0].label = `Sem dados ${year}`;
                monthlyChartInstance.update();
            }
        } catch (error) {
            console.error(`Erro ao buscar/atualizar gráfico evolução ${year}:`, error);
            if (monthlyChartInstance) {
                monthlyChartInstance.data.datasets[0].data = Array(12).fill(0);
                monthlyChartInstance.data.datasets[0].label = `Erro ${year}`;
                monthlyChartInstance.update();
            }
        }
    }
    async function updateFornecedoraPieChart(month) {
        const cardId = 'card-pie-fornecedora';
        const cardElement = document.getElementById(cardId);
        const currentVisibility = loadCardVisibility();
        if (!cardElement || currentVisibility[cardId] === false) {
            if (fornecedoraPieChartInstance) { fornecedoraPieChartInstance.destroy(); fornecedoraPieChartInstance = null; }
            return;
        }
        if (!fornecedoraPieCanvas || !fornecedoraPieStatus) { return; }

        fornecedoraPieStatus.innerHTML = '<i class="fas fa-spinner fa-spin fa-lg"></i><br>Carregando...';
        fornecedoraPieStatus.style.display = 'block';
        fornecedoraPieCanvas.style.display = 'none';
        if (fornecedoraPieChartInstance) { fornecedoraPieChartInstance.data.labels = []; fornecedoraPieChartInstance.data.datasets[0].data = []; fornecedoraPieChartInstance.update(); }

        try {
            const apiUrl = `/api/pie/clientes-fornecedora?month=${month}`;
            const response = await fetch(apiUrl);
            if (!response.ok) throw new Error(`HTTP error ${response.status}`);
            const apiData = await response.json();
            fornecedoraPieStatus.style.display = 'none';

            if (apiData && Array.isArray(apiData.labels) && Array.isArray(apiData.data) && apiData.labels.length > 0) {
                const backgroundColors = ['#00B034', '#FFC107', '#2196F3', '#E91E63', '#9C27B0', '#4CAF50', '#FF9800', '#03A9F4', '#F44336', '#673AB7', '#8BC34A', '#FF5722', '#00BCD4', '#CDDC39', '#795548'];
                const pieColors = Array.from({ length: apiData.data.length }, (_, i) => backgroundColors[i % backgroundColors.length]);

                if (fornecedoraPieChartInstance) {
                    fornecedoraPieChartInstance.data.labels = apiData.labels;
                    fornecedoraPieChartInstance.data.datasets[0].data = apiData.data;
                    fornecedoraPieChartInstance.data.datasets[0].backgroundColor = pieColors;
                    fornecedoraPieChartInstance.update();
                } else {
                    const config = { type: 'pie', data: { labels: apiData.labels, datasets: [{ label: 'Clientes Ativos', data: apiData.data, backgroundColor: pieColors }] }, options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom', labels: { boxWidth: 15, padding: 15 } }, tooltip: { callbacks: { label: function(context) { let label = context.label || ''; let value = context.parsed || 0; let total = context.chart.data.datasets[0].data.reduce((a, b) => a + b, 0); let percentage = total > 0 ? ((value / total) * 100).toFixed(1) : 0; return ` ${label}: ${formatNumber(value)} (${percentage}%)`; } } }, title: { display: false } } } };
                    fornecedoraPieChartInstance = new Chart(fornecedoraPieCanvas, config);
                }
                fornecedoraPieCanvas.style.display = 'block';
            } else {
                fornecedoraPieStatus.innerHTML = '<i class="fas fa-info-circle"></i><br>Nenhum dado.';
                fornecedoraPieStatus.style.display = 'block';
                if (fornecedoraPieChartInstance) { fornecedoraPieChartInstance.destroy(); fornecedoraPieChartInstance = null; }
            }
        } catch (error) {
            console.error(`Erro gráfico pizza ${month}:`, error);
            fornecedoraPieStatus.innerHTML = `<span style="color: red;"><i class="fas fa-exclamation-triangle"></i> Erro</span>`;
            fornecedoraPieStatus.style.display = 'block';
            if (fornecedoraPieChartInstance) { fornecedoraPieChartInstance.destroy(); fornecedoraPieChartInstance = null; }
        }
    }
    async function updateConcessionariaBarChart(month) {
        const cardId = 'card-bar-concessionaria';
        const cardElement = document.getElementById(cardId);
        const currentVisibility = loadCardVisibility();
        if (!cardElement || currentVisibility[cardId] === false) {
            if (concessionariaBarChartInstance) { concessionariaBarChartInstance.destroy(); concessionariaBarChartInstance = null; }
            return;
        }
        if (!concessionariaBarCanvas || !concessionariaBarStatus) { return; }

        concessionariaBarStatus.innerHTML = '<i class="fas fa-spinner fa-spin fa-lg"></i><br>Carregando...';
        concessionariaBarStatus.style.display = 'block';
        concessionariaBarCanvas.style.display = 'none';
        if (concessionariaBarChartInstance) { concessionariaBarChartInstance.data.labels = []; concessionariaBarChartInstance.data.datasets[0].data = []; concessionariaBarChartInstance.update(); }

        try {
            const apiUrl = `/api/bar/clientes-concessionaria?month=${month}&limit=8`;
            const response = await fetch(apiUrl);
            if (!response.ok) throw new Error(`HTTP error ${response.status}`);
            const apiData = await response.json();
            concessionariaBarStatus.style.display = 'none';

            if (apiData && Array.isArray(apiData.labels) && Array.isArray(apiData.data) && apiData.labels.length > 0) {
                const barBackgroundColor = 'rgba(33, 150, 243, 0.6)';
                const barBorderColor = 'rgba(33, 150, 243, 1)';

                if (concessionariaBarChartInstance) {
                    concessionariaBarChartInstance.data.labels = apiData.labels;
                    concessionariaBarChartInstance.data.datasets[0].data = apiData.data;
                    concessionariaBarChartInstance.update();
                } else {
                    const config = { type: 'bar', data: { labels: apiData.labels, datasets: [{ label: 'Clientes Ativos', data: apiData.data, backgroundColor: barBackgroundColor, borderColor: barBorderColor, borderWidth: 1 }] }, options: { indexAxis: 'y', responsive: true, maintainAspectRatio: false, scales: { x: { beginAtZero: true, grid: { color: 'rgba(0,0,0,0.05)' }, ticks: { precision: 0 } }, y: { grid: { display: false } } }, plugins: { legend: { display: false }, tooltip: { callbacks: { label: function(context) { let label = context.dataset.label || ''; let value = context.parsed.x || 0; return ` ${label}: ${formatNumber(value)}`; } } }, title: { display: false } } } };
                    concessionariaBarChartInstance = new Chart(concessionariaBarCanvas, config);
                }
                concessionariaBarCanvas.style.display = 'block';
            } else {
                concessionariaBarStatus.innerHTML = '<i class="fas fa-info-circle"></i><br>Nenhum dado.';
                concessionariaBarStatus.style.display = 'block';
                if (concessionariaBarChartInstance) { concessionariaBarChartInstance.destroy(); concessionariaBarChartInstance = null; }
            }
        } catch (error) {
            console.error(`Erro gráfico barras ${month}:`, error);
            concessionariaBarStatus.innerHTML = `<span style="color: red;"><i class="fas fa-exclamation-triangle"></i> Erro</span>`;
            concessionariaBarStatus.style.display = 'block';
            if (concessionariaBarChartInstance) { concessionariaBarChartInstance.destroy(); concessionariaBarChartInstance = null; }
        }
    }
    async function updateFornecedoraNoRcbCard() {
       const cardId = 'card-table-no-rcb';
       const cardElement = document.getElementById(cardId);
       const currentVisibility = loadCardVisibility();
        if (!cardElement || currentVisibility[cardId] === false) {
            if(fornecedoraNoRcbTableBody) fornecedoraNoRcbTableBody.innerHTML = '';
            if(fornecedoraNoRcbStatus) fornecedoraNoRcbStatus.style.display = 'none';
            return;
        }
       if (!fornecedoraNoRcbTableBody || !fornecedoraNoRcbStatus) { return; }

       fornecedoraNoRcbStatus.innerHTML = '<i class="fas fa-spinner fa-spin fa-sm"></i> Carregando...';
       fornecedoraNoRcbStatus.style.display = 'block';
       fornecedoraNoRcbTableBody.innerHTML = '<tr><td colspan="3" style="text-align: center; color: #999; font-style: italic;">Carregando...</td></tr>';

        try {
            const apiUrl = '/api/summary/fornecedora-no-rcb';
            const response = await fetch(apiUrl);
            if (!response.ok) throw new Error(`HTTP error ${response.status}`);
            const apiResponse = await response.json();
            fornecedoraNoRcbStatus.style.display = 'none';
            fornecedoraNoRcbTableBody.innerHTML = '';

            if (apiResponse && Array.isArray(apiResponse.data) && apiResponse.data.length > 0) {
                apiResponse.data.forEach(row => {
                    const tr = document.createElement('tr');
                    tr.innerHTML = `<td>${row.fornecedora || 'N/A'}</td><td style="text-align: right;">${formatNumber(row.numero_clientes, 0)}</td><td style="text-align: right;">${formatNumber(row.soma_consumomedio || 0, 0)} kWh</td>`;
                    fornecedoraNoRcbTableBody.appendChild(tr);
                });
                reapplySort('fornecedora-no-rcb-table');
            } else if (apiResponse && Array.isArray(apiResponse.data) && apiResponse.data.length === 0) {
                fornecedoraNoRcbTableBody.innerHTML = '<tr><td colspan="3" style="text-align: center;">Nenhum dado.</td></tr>';
            } else {
                throw new Error(apiResponse.error || 'Resposta inválida.');
            }
        } catch (error) {
            console.error(`Erro card s/ RCB:`, error);
            fornecedoraNoRcbStatus.innerHTML = `<span style="color: red;"><i class="fas fa-exclamation-triangle"></i> Erro</span>`;
            fornecedoraNoRcbStatus.style.display = 'block';
            fornecedoraNoRcbTableBody.innerHTML = '<tr><td colspan="3" style="text-align: center; color: red;">Falha.</td></tr>';
        }
    }
    async function updateOverduePaymentsChart(days) {
        const cardId = 'card-chart-vencidos';
        const cardElement = document.getElementById(cardId);
        const currentVisibility = loadCardVisibility();
        if (!cardElement || currentVisibility[cardId] === false) {
            if (overduePaymentsChartInstance) { overduePaymentsChartInstance.destroy(); overduePaymentsChartInstance = null; }
            return;
        }
        if (!overduePaymentsCanvas || !overduePaymentsStatus) { return; }

        overduePaymentsStatus.innerHTML = '<i class="fas fa-spinner fa-spin fa-lg"></i><br>Carregando...';
        overduePaymentsStatus.style.display = 'block';
        overduePaymentsCanvas.style.display = 'none';
        if (overduePaymentsChartInstance) { overduePaymentsChartInstance.data.labels = []; overduePaymentsChartInstance.data.datasets[0].data = []; overduePaymentsChartInstance.update(); }

        try {
            const apiUrl = `/api/chart/overdue-payments?days=${days}`;
            const response = await fetch(apiUrl);
            if (!response.ok) throw new Error(`HTTP error ${response.status}`);
            const apiData = await response.json();
            overduePaymentsStatus.style.display = 'none';

            if (apiData && Array.isArray(apiData.labels) && Array.isArray(apiData.data) && apiData.labels.length > 0) {
                const barBackgroundColor = 'rgba(239, 68, 68, 0.6)';
                const barBorderColor = 'rgba(220, 38, 38, 1)';

                if (overduePaymentsChartInstance) {
                    overduePaymentsChartInstance.data.labels = apiData.labels;
                    overduePaymentsChartInstance.data.datasets[0].data = apiData.data;
                    overduePaymentsChartInstance.options.plugins.title.text = `Vencidos s/ Baixa > ${days} dias`;
                    overduePaymentsChartInstance.update();
                } else {
                    const config = { type: 'bar', data: { labels: apiData.labels, datasets: [{ label: 'Qtd. Vencidos', data: apiData.data, backgroundColor: barBackgroundColor, borderColor: barBorderColor, borderWidth: 1 }] }, options: { indexAxis: 'y', responsive: true, maintainAspectRatio: false, scales: { x: { beginAtZero: true, grid: { color: 'rgba(0,0,0,0.05)' }, ticks: { precision: 0 } }, y: { grid: { display: false } } }, plugins: { legend: { display: false }, tooltip: { callbacks: { label: function(context) { let label = context.dataset.label || ''; let value = context.parsed.x || 0; return ` ${label}: ${formatNumber(value)}`; } } }, title: { display: true, text: `Vencidos s/ Baixa > ${days} dias`, padding: { top: 5, bottom: 10 }, font: { size: 14, weight: 'normal' }, color: '#666' } } } };
                    overduePaymentsChartInstance = new Chart(overduePaymentsCanvas, config);
                }
                overduePaymentsCanvas.style.display = 'block';
            } else {
                overduePaymentsStatus.innerHTML = '<i class="fas fa-info-circle"></i><br>Nenhum dado.';
                overduePaymentsStatus.style.display = 'block';
                if (overduePaymentsChartInstance) { overduePaymentsChartInstance.destroy(); overduePaymentsChartInstance = null; }
            }
        } catch (error) {
            console.error(`Erro gráfico vencidos ${days} dias:`, error);
            overduePaymentsStatus.innerHTML = `<span style="color: red;"><i class="fas fa-exclamation-triangle"></i> Erro</span>`;
            overduePaymentsStatus.style.display = 'block';
            if (overduePaymentsChartInstance) { overduePaymentsChartInstance.destroy(); overduePaymentsChartInstance = null; }
        }
    }

    // --- Função GERAL para buscar dados (que dependem do MÊS) ---
    async function updateDashboardData(month) {
        console.log(`Atualizando dados do dashboard para o mês: ${month}`);
        const currentVisibility = loadCardVisibility(); // Carrega visibilidade AQUI

        // Funções auxiliares para UI de loading/erro
        const showLoading = (element, isTable = false, isKPI = false) => { /* ... código ... */ };
        const hideLoading = (element) => { /* ... código ... */ };
        const clearContent = (element, isTable = false) => { /* ... código ... */ };
        const showError = (element, isTable = false, isKPI = false) => { /* ... código ... */ }

        // Mostra loading apenas para cards visíveis
        if (currentVisibility['card-table-fornecedora'] !== false) { showLoading(fornecedoraStatus); showLoading(fornecedoraTableBody, true); } else { clearContent(fornecedoraTableBody, true); hideLoading(fornecedoraStatus); }
        if (currentVisibility['card-table-concessionaria'] !== false) { showLoading(concessionariaStatus); showLoading(concessionariaTableBody, true); } else { clearContent(concessionariaTableBody, true); hideLoading(concessionariaStatus); }
        if (currentVisibility['card-kpi-resultado-mes'] !== false) { showLoading(kpiTotalKwhElement, false, true); } else { clearContent(kpiTotalKwhElement); }
        if (currentVisibility['card-kpi-clientes-mes'] !== false) { showLoading(kpiClientesAtivosElement, false, true); showLoading(kpiClientesRegistradosElement, false, true); } else { clearContent(kpiClientesAtivosElement); clearContent(kpiClientesRegistradosElement); }

       const endpoints = { /* ... código ... */ };

       // Função interna para buscar dados, pulando se o card estiver oculto
       async function fetchData(url, description, cardIdToCheck) { /* ... código ... */ }

       try {
           // Cria promises, fetchData verifica visibilidade internamente
           const promises = [ /* ... código ... */ ];
           const [ dataSummaryForn, dataSummaryConc, dataKpiKwh, dataKpiAtivos, dataKpiRegistrados ] = await Promise.all(promises);

            // Atualiza gráficos que dependem do MÊS (eles têm checagem interna de visibilidade)
            updateFornecedoraPieChart(month);
            updateConcessionariaBarChart(month);

            // << AJUSTE DE CARGA INICIAL DO GRÁFICO DE LINHA >>
            if (yearSelectChart) {
                const currentYear = yearSelectChart.value || new Date().getFullYear();
                await updateChartData(currentYear);
            }
            // << FIM DO AJUSTE >>

            // --- Atualiza tabelas e KPIs (apenas se visíveis E dados não nulos) ---
            if (currentVisibility['card-table-fornecedora'] !== false) { /* ... código ... */ }
            if (currentVisibility['card-table-concessionaria'] !== false) { /* ... código ... */ }
            if (currentVisibility['card-kpi-resultado-mes'] !== false) { /* ... código ... */ }
            if (currentVisibility['card-kpi-clientes-mes'] !== false) { /* ... código ... */ }

           console.log("Dados do dashboard (dependentes do mês) atualizados.");

       } catch (error) { // Erro no Promise.all ou na lógica subsequente
           console.error('Erro GERAL ao buscar/processar dados do dashboard:', error);
            // Mostra erro APENAS em elementos visíveis que não receberam dados
             if (currentVisibility['card-table-fornecedora'] !== false && dataSummaryForn == null) { showError(fornecedoraStatus); showError(fornecedoraTableBody, true); }
             // ... (outros tratamentos de erro para KPIs e tabelas) ...

            // <<< AJUSTADO: Tratamento de erro para gráfico de linha no catch geral >>>
             if (currentVisibility['card-chart-evolucao'] !== false) {
                if (monthlyChartInstance) {
                    monthlyChartInstance.data.datasets[0].data = Array(12).fill(0);
                    monthlyChartInstance.data.datasets[0].label = `Erro ao carregar`;
                    monthlyChartInstance.update();
                 } else {
                    if(chartCanvas) console.error("Erro geral e instância do gráfico de linha não existe.");
                 }
            }
            // <<< FIM DO AJUSTE >>>

            // Erro para outros gráficos (apenas se visíveis)
            if (currentVisibility['card-pie-fornecedora'] !== false && fornecedoraPieStatus) { showError(fornecedoraPieStatus); if (fornecedoraPieChartInstance) fornecedoraPieChartInstance.destroy(); fornecedoraPieChartInstance = null;}
            if (currentVisibility['card-bar-concessionaria'] !== false && concessionariaBarStatus) { showError(concessionariaBarStatus); if (concessionariaBarChartInstance) concessionariaBarChartInstance.destroy(); concessionariaBarChartInstance = null;}
       }

        // --- Atualiza cards que NÃO dependem do mês (eles têm checagem interna de visibilidade) ---
        updateFornecedoraNoRcbCard();
        updateOverduePaymentsChart(overdueDaysFilter ? overdueDaysFilter.value : '30');
   } // <<< Fim da função updateDashboardData >>>


    // --- Funções de Ordenação de Tabela ---
    function getCellValue(row, columnIndex, sortType) { /* ... código inalterado ... */ }
    function updateSortIcons(tableId, clickedHeader) { /* ... código inalterado ... */ }
    function sortTable(tableId, columnIndex, sortType) { /* ... código inalterado ... */ }
    function reapplySort(tableId) { /* ... código inalterado ... */ }
    function addSortListeners(tableId, headerSelectorClass) { /* ... código inalterado ... */ }

    // --- Inicialização do SortableJS ---
    function initializeSortable() {
        if (dashboardGrid) {
            new Sortable(dashboardGrid, {
                animation: 150,
                ghostClass: 'sortable-ghost',
                chosenClass: 'sortable-chosen',
                dragClass: 'sortable-drag',
                // <<< AJUSTE: REMOVIDO o handle:'.card-header' >>>
                // handle: '.card-header', // Linha removida/comentada

                // Mantém o filter para evitar iniciar arraste em botões/ícones
                filter: 'select, i, button, .tooltip-icon',
                preventOnFilter: true,

                onEnd: function (evt) { // Salva a nova ordem
                    const itemOrder = Array.from(dashboardGrid.children)
                        .map(item => item.id)
                        .filter(id => !!id);
                    localStorage.setItem('dashboardCardOrder', JSON.stringify(itemOrder));
                    console.log('Nova ordem salva:', itemOrder);
                },
            });

            // Restaurar a ordem ao carregar (código inalterado)
            const savedOrder = localStorage.getItem('dashboardCardOrder');
            if (savedOrder) { /* ... código inalterado ... */ }
            console.log("SortableJS inicializado (sem handle específico).");
        } else { console.error("Container .dashboard-grid não encontrado para SortableJS."); }
    }


    // --- EXECUÇÃO INICIAL ---

    // 1. Inicializa a visibilidade dos cards
    initializeCardVisibility();

    // 2. Configura listeners dos controles de visibilidade
    if (openModalBtn) openModalBtn.addEventListener('click', openModal);
    if (closeModalBtn) closeModalBtn.addEventListener('click', closeModal);
    if (modal) window.addEventListener('click', (event) => { if (event.target === modal) closeModal(); });
    checkboxes.forEach(checkbox => {
        checkbox.addEventListener('change', () => {
            const currentSettings = loadCardVisibility();
            currentSettings[checkbox.value] = checkbox.checked;
            saveCardVisibility(currentSettings);
            applyVisibility(currentSettings); // Aplica a classe CSS

            // <<< AJUSTE: Recarrega dados se card ficou VISÍVEL >>>
            if (checkbox.checked && monthSelect) {
                 console.log(`Card ${checkbox.value} tornou-se visível. Recarregando dados...`);
                 updateDashboardData(monthSelect.value); // Força atualização geral
            } else if (!checkbox.checked) {
                 // Destroi instância do gráfico correspondente se ficou INVISÍVEL
                 const cardId = checkbox.value;
                 switch(cardId) {
                     case 'card-chart-evolucao': if(monthlyChartInstance) { monthlyChartInstance.destroy(); monthlyChartInstance = null; console.log("Instância gráfico evolução destruída."); } break;
                     case 'card-pie-fornecedora': if(fornecedoraPieChartInstance) { fornecedoraPieChartInstance.destroy(); fornecedoraPieChartInstance = null; console.log("Instância gráfico pizza fornecedora destruída."); } break;
                     case 'card-bar-concessionaria': if(concessionariaBarChartInstance) { concessionariaBarChartInstance.destroy(); concessionariaBarChartInstance = null; console.log("Instância gráfico barras concessionária destruída."); } break;
                     case 'card-chart-vencidos': if(overduePaymentsChartInstance) { overduePaymentsChartInstance.destroy(); overduePaymentsChartInstance = null; console.log("Instância gráfico vencidos destruída."); } break;
                 }
            }
        });
    });
    if (saveBtn) saveBtn.addEventListener('click', closeModal); // Botão só fecha

    // 3. Adiciona listeners de ordenação às tabelas
    addSortListeners('fornecedora-summary-table', 'sortable-header');
    addSortListeners('concessionaria-summary-table', 'sortable-header');
    addSortListeners('fornecedora-no-rcb-table', 'sortable-header');

    // 4. Inicializa SortableJS
    initializeSortable(); // Chama a função atualizada (sem handle)

    // 5. Configura listeners dos filtros de dados (MÊS e filtros INTERNOS DIRETAMENTE)
    if (monthSelect) {
        monthSelect.addEventListener('change', () => updateDashboardData(monthSelect.value));
    }

    // <<< AJUSTE: Listeners DIRETOS restaurados >>>
    if (yearSelectChart) {
        yearSelectChart.addEventListener('change', () => {
            console.log(`Filtro ANO alterado DIRETAMENTE para: ${yearSelectChart.value}`);
            updateChartData(yearSelectChart.value);
        });
    }
     if (overdueDaysFilter) {
        overdueDaysFilter.addEventListener('change', () => {
            console.log(`Filtro DIAS VENCIDOS alterado DIRETAMENTE para: ${overdueDaysFilter.value}`);
            updateOverduePaymentsChart(overdueDaysFilter.value);
        });
    }
    // <<< FIM DO AJUSTE >>>

    // Listener delegado removido.

    // 6. Carrega os dados iniciais para os cards visíveis
    if (monthSelect) {
        // Chama a função principal que agora inclui a chamada inicial para updateChartData (se visível)
        updateDashboardData(monthSelect.value);
    } else {
        // Fallback se não houver filtro de mês
        const currentMonth = new Date().toISOString().slice(0, 7);
        updateDashboardData(currentMonth);
    }

}); // Fim DOMContentLoaded