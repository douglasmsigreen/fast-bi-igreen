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
    const chartCanvas = document.getElementById('remunerationChart');
    const yearSelectChart = document.getElementById('chart-year');
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
    let monthlyChartInstance = null;
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
            const isVisible = settings[cardId] !== false;
            card.classList.toggle('card-hidden', !isVisible);
            console.log(`Card ID: ${cardId}, Visível: ${isVisible}`);
        });
        // Força re-layout (pode não ser estritamente necessário, mas ajuda)
        dashboardGrid.style.display = 'none';
        dashboardGrid.offsetHeight; // Trigger reflow
        dashboardGrid.style.display = 'grid';
    };

    // --- Função para Atualizar Estado dos Checkboxes no Modal ---
    const updateCheckboxStates = () => {
        const currentSettings = loadCardVisibility();
        checkboxes.forEach(checkbox => {
            const cardId = checkbox.value;
            checkbox.checked = currentSettings[cardId] !== false;
        });
    };

    // --- Função Inicializadora de Visibilidade ---
    const initializeCardVisibility = () => {
        const settings = loadCardVisibility();
        if (Object.keys(settings).length === 0) {
            console.log("Nenhuma configuração salva. Definindo visibilidade padrão.");
            const initialSettings = {};
            checkboxes.forEach(checkbox => {
                // Verifica se o card correspondente existe no DOM antes de habilitar por padrão
                const cardElement = document.getElementById(checkbox.value);
                initialSettings[checkbox.value] = !!cardElement; // true se o card existe, false se não
            });
            saveCardVisibility(initialSettings);
            applyVisibility(initialSettings);
        } else {
            applyVisibility(settings);
        }
        updateCheckboxStates();
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

    async function updateChartData(year) {
        console.log(`[updateChartData] Iniciando para o ano: ${year}`); // Log inicial
        const cardId = 'card-chart-evolucao';
        const cardElement = document.getElementById(cardId);
        const currentVisibility = loadCardVisibility(); // loadCardVisibility deve estar definida no mesmo arquivo

        // --- 1. Verificação de Visibilidade e Elemento Canvas ---
        if (!cardElement) {
            console.error(`[updateChartData] Elemento do card #${cardId} não encontrado.`);
            return;
        }
        if (currentVisibility[cardId] === false) {
            console.log(`[updateChartData] Card ${cardId} oculto, pulando atualização.`);
            if (monthlyChartInstance) {
                monthlyChartInstance.destroy();
                monthlyChartInstance = null;
                console.log(`[updateChartData] Instância do gráfico destruída pois o card está oculto.`);
            }
            return;
        }
        if (!chartCanvas) { // chartCanvas é a variável global definida no início do DOMContentLoaded
             console.error("[updateChartData] Elemento canvas #remunerationChart não encontrado globalmente.");
             return;
        }

        // --- 2. Garantir Instância do Gráfico (Criar se necessário) ---
        if (!monthlyChartInstance) {
            console.log(`[updateChartData] Instância do gráfico não existe. Tentando criar...`);
            const labels = ['JAN', 'FEV', 'MAR', 'ABR', 'MAI', 'JUN', 'JUL', 'AGO', 'SET', 'OUT', 'NOV', 'DEZ'];
            const initialData = {
                labels: labels,
                datasets: [{
                    label: `Carregando ${year}...`,
                    data: Array(12).fill(0),
                    fill: true,
                    backgroundColor: 'rgba(0, 176, 52, 0.1)', // Verde primário com transparência
                    borderColor: 'rgb(0, 176, 52)', // Verde primário sólido
                    borderWidth: 2,
                    tension: 0.3,
                    pointBackgroundColor: 'rgb(0, 176, 52)',
                    pointRadius: 3,
                    pointHoverRadius: 5
                }]
            };
            const config = {
                type: 'line',
                data: initialData,
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            backgroundColor: 'rgba(0, 0, 0, 0.7)',
                            titleFont: { weight: 'bold' },
                            callbacks: {
                                label: function(context) {
                                    return (context.dataset.label || '').replace(/ \d{4}/, '') + ': ' + formatNumber(context.parsed.y); // Usa formatNumber
                                }
                            }
                        }
                    },
                    scales: {
                        y: { beginAtZero: true, grid: { color: 'rgba(0,0,0,0.05)' } },
                        x: { grid: { display: false } }
                    },
                    interaction: { intersect: false, mode: 'index' }
                }
            };
            try {
                monthlyChartInstance = new Chart(chartCanvas, config);
                console.log(`[updateChartData] Instância do Chart.js criada com sucesso para ${cardId}.`);
            } catch (error) {
                console.error(`[updateChartData] Erro CRÍTICO ao criar Chart.js instance para ${cardId}:`, error);
                // Opcional: Mostrar erro no card
                const cardBody = cardElement.querySelector('.card-body');
                if (cardBody) cardBody.innerHTML = `<p style="color:red; text-align:center; padding:20px;">Erro ao inicializar o gráfico.</p>`;
                return; // Para a execução se não puder criar o gráfico
            }
        }

        // --- 3. Buscar Dados da API ---
        const apiUrl = `/api/chart/monthly-active-clients?year=${year}`;
        console.log(`[updateChartData] Buscando dados de: ${apiUrl}`);
        // Adiciona um estado de "carregando" visualmente se desejar
        if(monthlyChartInstance) {
            monthlyChartInstance.data.datasets[0].label = `Carregando ${year}...`;
            monthlyChartInstance.data.datasets[0].data = Array(12).fill(0); // Limpa dados antigos
            monthlyChartInstance.update();
        }


        try {
            const response = await fetch(apiUrl);
            const responseBodyText = await response.text(); // Ler como texto primeiro para depuração
            console.log(`[updateChartData] Resposta recebida para ${year}. Status: ${response.status}. Corpo:`, responseBodyText);

            if (!response.ok) {
                console.error(`[updateChartData] Erro HTTP ${response.status} ao buscar dados para ${year}. Resposta: ${responseBodyText}`);
                throw new Error(`HTTP error ${response.status}`);
            }

            const chartData = JSON.parse(responseBodyText); // Tenta parsear o JSON

            // --- 4. Atualizar o Gráfico com os Dados ---
            if (monthlyChartInstance && chartData && Array.isArray(chartData.monthly_counts) && chartData.monthly_counts.length === 12) {
                console.log(`[updateChartData] Atualizando gráfico com dados para ${year}:`, chartData.monthly_counts);
                monthlyChartInstance.data.datasets[0].data = chartData.monthly_counts;
                monthlyChartInstance.data.datasets[0].label = `Ativações ${year}`;
                monthlyChartInstance.update();
                console.log(`[updateChartData] Gráfico atualizado com sucesso para ${year}.`);
            } else if (monthlyChartInstance) {
                console.warn(`[updateChartData] Dados inválidos ou vazios recebidos para ${year}. Resetando gráfico.`);
                monthlyChartInstance.data.datasets[0].data = Array(12).fill(0);
                monthlyChartInstance.data.datasets[0].label = `Sem dados ${year}`;
                monthlyChartInstance.update();
            } else {
                 console.warn(`[updateChartData] Instância do gráfico não encontrada no momento de atualizar dados para ${year}.`);
            }

        } catch (error) {
            console.error(`[updateChartData] Erro no fetch ou atualização do gráfico para ${year}:`, error);
            if (monthlyChartInstance) {
                monthlyChartInstance.data.datasets[0].data = Array(12).fill(0);
                monthlyChartInstance.data.datasets[0].label = `Erro ${year}`;
                monthlyChartInstance.update();
            }
            // Opcional: Mostrar erro no card
            const cardBody = cardElement.querySelector('.card-body');
            if (cardBody) {
                 cardBody.innerHTML = `<p style="color:red; text-align:center; padding-top:20px;">Erro ao carregar dados do gráfico.</p>`;
                 // Remove o canvas para evitar tentativas futuras se der erro crítico
                 if (chartCanvas) chartCanvas.remove();
            }
        }
    }
    // Fim da função updateChartData substituída

    async function updateFornecedoraPieChart(month) {
        const cardId = 'card-pie-fornecedora';
        const cardElement = document.getElementById(cardId);
        const currentVisibility = loadCardVisibility();
        if (!cardElement || currentVisibility[cardId] === false) {
            console.log(`Card ${cardId} oculto, pulando atualização.`);
            if (fornecedoraPieChartInstance) { fornecedoraPieChartInstance.destroy(); fornecedoraPieChartInstance = null; }
            return;
        }
        if (!fornecedoraPieCanvas || !fornecedoraPieStatus) return;

        fornecedoraPieStatus.innerHTML = '<i class="fas fa-spinner fa-spin fa-lg"></i><br>Carregando...';
        fornecedoraPieStatus.style.display = 'block';
        fornecedoraPieCanvas.style.display = 'none';

        try {
            const apiUrl = `/api/pie/clientes-fornecedora?month=${month}`; // URL relativa
            const response = await fetch(apiUrl);
            if (!response.ok) throw new Error(`HTTP error ${response.status}`);
            const apiData = await response.json();
            fornecedoraPieStatus.style.display = 'none';

            if (apiData && Array.isArray(apiData.labels) && Array.isArray(apiData.data) && apiData.labels.length > 0) {
                const backgroundColors = ['#00B034', '#FFC107', '#2196F3', '#E91E63', '#9C27B0', '#4CAF50', '#FF9800', '#03A9F4', '#F44336', '#673AB7', '#8BC34A', '#FF5722', '#00BCD4', '#CDDC39', '#795548'];
                const pieColors = Array.from({ length: apiData.data.length }, (_, i) => backgroundColors[i % backgroundColors.length]);

                if (fornecedoraPieChartInstance) { // Atualiza existente
                    fornecedoraPieChartInstance.data.labels = apiData.labels;
                    fornecedoraPieChartInstance.data.datasets[0].data = apiData.data;
                    fornecedoraPieChartInstance.data.datasets[0].backgroundColor = pieColors;
                    fornecedoraPieChartInstance.update();
                } else { // Cria novo
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
            console.log(`Card ${cardId} oculto, pulando atualização.`);
            if (concessionariaBarChartInstance) { concessionariaBarChartInstance.destroy(); concessionariaBarChartInstance = null; }
            return;
        }
        if (!concessionariaBarCanvas || !concessionariaBarStatus) return;

        concessionariaBarStatus.innerHTML = '<i class="fas fa-spinner fa-spin fa-lg"></i><br>Carregando...';
        concessionariaBarStatus.style.display = 'block';
        concessionariaBarCanvas.style.display = 'none';

        try {
            const apiUrl = `/api/bar/clientes-concessionaria?month=${month}&limit=8`; // URL relativa
            const response = await fetch(apiUrl);
            if (!response.ok) throw new Error(`HTTP error ${response.status}`);
            const apiData = await response.json();
            concessionariaBarStatus.style.display = 'none';

            if (apiData && Array.isArray(apiData.labels) && Array.isArray(apiData.data) && apiData.labels.length > 0) {
                const barBackgroundColor = 'rgba(33, 150, 243, 0.6)';
                const barBorderColor = 'rgba(33, 150, 243, 1)';

                if (concessionariaBarChartInstance) { // Atualiza
                    concessionariaBarChartInstance.data.labels = apiData.labels;
                    concessionariaBarChartInstance.data.datasets[0].data = apiData.data;
                    concessionariaBarChartInstance.update();
                } else { // Cria
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
            console.log(`Card ${cardId} oculto, pulando atualização.`);
            if(fornecedoraNoRcbTableBody) fornecedoraNoRcbTableBody.innerHTML = '';
            if(fornecedoraNoRcbStatus) fornecedoraNoRcbStatus.style.display = 'none';
            return;
        }
       if (!fornecedoraNoRcbTableBody || !fornecedoraNoRcbStatus) return;

       fornecedoraNoRcbStatus.innerHTML = '<i class="fas fa-spinner fa-spin fa-sm"></i> Carregando...';
       fornecedoraNoRcbStatus.style.display = 'block';
       fornecedoraNoRcbTableBody.innerHTML = '<tr><td colspan="3" style="text-align: center; color: #999; font-style: italic;">Carregando...</td></tr>';

        try {
            const apiUrl = '/api/summary/fornecedora-no-rcb'; // URL relativa
            const response = await fetch(apiUrl);
            if (!response.ok) throw new Error(`HTTP error ${response.status}`);
            const apiResponse = await response.json();
            fornecedoraNoRcbStatus.style.display = 'none';
            fornecedoraNoRcbTableBody.innerHTML = '';

            if (apiResponse && Array.isArray(apiResponse.data) && apiResponse.data.length > 0) {
                apiResponse.data.forEach(row => {
                    const tr = document.createElement('tr');

                    // <<< CORREÇÃO APLICADA AQUI >>>
                    const tdForn = document.createElement('td');
                    tdForn.textContent = row.fornecedora || 'N/A';
                    tr.appendChild(tdForn);

                    const tdClientes = document.createElement('td');
                    tdClientes.style.textAlign = 'right';
                    tdClientes.textContent = formatNumber(row.numero_clientes, 0); // Usa numero_clientes
                    tr.appendChild(tdClientes);

                    const tdKwh = document.createElement('td');
                    tdKwh.style.textAlign = 'right';
                    tdKwh.textContent = `${formatNumber(row.soma_consumomedio, 0)} kWh`; // Usa soma_consumomedio
                    tr.appendChild(tdKwh);
                    // <<< FIM DA CORREÇÃO >>>

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
            console.log(`Card ${cardId} oculto, pulando atualização.`);
            if (overduePaymentsChartInstance) { overduePaymentsChartInstance.destroy(); overduePaymentsChartInstance = null; }
            return;
        }
        if (!overduePaymentsCanvas || !overduePaymentsStatus) return;

        overduePaymentsStatus.innerHTML = '<i class="fas fa-spinner fa-spin fa-lg"></i><br>Carregando...';
        overduePaymentsStatus.style.display = 'block';
        overduePaymentsCanvas.style.display = 'none';

        try {
            const apiUrl = `/api/chart/overdue-payments?days=${days}`; // URL relativa
            const response = await fetch(apiUrl);
            if (!response.ok) throw new Error(`HTTP error ${response.status}`);
            const apiData = await response.json();
            overduePaymentsStatus.style.display = 'none';

            if (apiData && Array.isArray(apiData.labels) && Array.isArray(apiData.data) && apiData.labels.length > 0) {
                const barBackgroundColor = 'rgba(239, 68, 68, 0.6)';
                const barBorderColor = 'rgba(220, 38, 38, 1)';

                if (overduePaymentsChartInstance) { // Atualiza
                    overduePaymentsChartInstance.data.labels = apiData.labels;
                    overduePaymentsChartInstance.data.datasets[0].data = apiData.data;
                    overduePaymentsChartInstance.options.plugins.title.text = `Vencidos s/ Baixa > ${days} dias`;
                    overduePaymentsChartInstance.update();
                } else { // Cria
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

        // Funções auxiliares para mostrar/esconder loading
        const showLoading = (element, isTable = false) => {
            if (element) {
                element.innerHTML = isTable
                    ? '<tr><td colspan="3" style="text-align: center; color: #999; font-style: italic;">Carregando...</td></tr>'
                    : '<i class="fas fa-spinner fa-spin fa-sm"></i> Carregando...';
                if (!isTable && element.tagName === 'DIV') element.style.display = 'block';
                else if (isTable && element.tagName === 'TBODY') {} // Já está visível
                else if (element.tagName === 'SPAN' || element.tagName === 'DIV') element.innerHTML = '<i class="fas fa-spinner fa-spin fa-xs"></i>'; // Para KPIs
            }
        };
        const hideLoading = (element) => { if (element && element.tagName === 'DIV') element.style.display = 'none'; };
        const clearContent = (element, isTable = false) => { if(element) element.innerHTML = isTable ? '' : ''; };
        const showError = (element, isTable = false) => {
            if (element) {
                element.innerHTML = isTable
                    ? '<tr><td colspan="3" style="text-align: center; color: red;">Falha.</td></tr>'
                    : `<span style="color: red; font-size: 0.9em;"><i class="fas fa-exclamation-triangle"></i> Erro</span>`;
                if (!isTable && element.tagName === 'DIV') element.style.display = 'block';
                else if (element.tagName === 'SPAN' || element.tagName === 'DIV') element.innerHTML = `<span style="font-size: 0.7em; color: red;">Erro!</span>`; // Para KPIs
            }
        }

        // Mostra loading apenas para cards visíveis
        if (currentVisibility['card-table-fornecedora']) showLoading(fornecedoraStatus); showLoading(fornecedoraTableBody, true);
        if (currentVisibility['card-table-concessionaria']) showLoading(concessionariaStatus); showLoading(concessionariaTableBody, true);
        if (currentVisibility['card-kpi-resultado-mes']) showLoading(kpiTotalKwhElement);
        if (currentVisibility['card-kpi-clientes-mes']) showLoading(kpiClientesAtivosElement); showLoading(kpiClientesRegistradosElement);

       const endpoints = {
           summaryForn: `/api/summary/fornecedora?month=${month}`,
           summaryConc: `/api/summary/concessionaria?month=${month}`,
           kpiKwh: `/api/kpi/total-kwh?month=${month}`,
           kpiAtivos: `/api/kpi/clientes-ativos?month=${month}`,
           kpiRegistrados: `/api/kpi/clientes-registrados?month=${month}`
       };

       async function fetchData(url, description) {
            // Não busca se o card correspondente estiver oculto (otimização)
            let cardIdToCheck = null;
            if (description.includes("Fornecedora")) cardIdToCheck = 'card-table-fornecedora';
            else if (description.includes("Concessionária")) cardIdToCheck = 'card-table-concessionaria';
            else if (description.includes("Total kWh")) cardIdToCheck = 'card-kpi-resultado-mes';
            else if (description.includes("Ativos") || description.includes("Registrados")) cardIdToCheck = 'card-kpi-clientes-mes';

            if (cardIdToCheck && currentVisibility[cardIdToCheck] === false) {
                console.log(`Workspace pulado para ${description} (card ${cardIdToCheck} oculto)`);
                return null; // Retorna nulo para indicar que não buscou
            }

            // Busca se visível ou sem card associado
           try {
               const response = await fetch(url);
               if (!response.ok) { let errorMsg = `Erro ${response.status}`; try { const data = await response.json(); errorMsg = data.error || errorMsg; } catch (e) {} console.error(`Falha ao buscar ${description}: ${errorMsg} (URL: ${url})`); throw new Error(`Falha: ${description}`); } return await response.json();
           } catch (error) { console.error(`Erro de rede/JSON ${description}:`, error); throw error; }
       }

       try {
           // Promises são criadas incondicionalmente, mas fetchData retorna null se card oculto
           const promises = [
               fetchData(endpoints.summaryForn, "Resumo Fornecedora"),
               fetchData(endpoints.summaryConc, "Resumo Concessionária"),
               fetchData(endpoints.kpiKwh, "KPI Total kWh"),
               fetchData(endpoints.kpiAtivos, "KPI Clientes Ativos"),
               fetchData(endpoints.kpiRegistrados, "KPI Clientes Registrados")
           ];

           const [ dataSummaryForn, dataSummaryConc, dataKpiKwh, dataKpiAtivos, dataKpiRegistrados ] = await Promise.all(promises);

            // Atualiza gráficos (eles têm checagem interna de visibilidade)
            updateFornecedoraPieChart(month);
            updateConcessionariaBarChart(month);

            // Atualiza tabelas e KPIs apenas se visíveis E se os dados foram buscados (não são nulos)
            if (currentVisibility['card-table-fornecedora']) {
                hideLoading(fornecedoraStatus);
                clearContent(fornecedoraTableBody, true);
                if (dataSummaryForn && dataSummaryForn.length > 0) {
                    dataSummaryForn.forEach(row => {
                        const tr = document.createElement('tr');

                        // <<< CORREÇÃO APLICADA AQUI >>>
                        const tdForn = document.createElement('td');
                        tdForn.textContent = row.fornecedora || 'N/A';
                        tr.appendChild(tdForn);

                        const tdClientes = document.createElement('td');
                        tdClientes.style.textAlign = 'right';
                        tdClientes.textContent = formatNumber(row.qtd_clientes, 0);
                        tr.appendChild(tdClientes);

                        const tdKwh = document.createElement('td');
                        tdKwh.style.textAlign = 'right';
                        tdKwh.textContent = `${formatNumber(row.soma_consumo, 2)} kWh`;
                        tr.appendChild(tdKwh);
                        // <<< FIM DA CORREÇÃO >>>

                        fornecedoraTableBody.appendChild(tr);
                    });
                    reapplySort('fornecedora-summary-table');
                } else if (dataSummaryForn) { // Chegou resposta, mas vazia
                    fornecedoraTableBody.innerHTML = '<tr><td colspan="3" style="text-align: center;">Nenhum dado.</td></tr>';
                } else if (dataSummaryForn === null) { /* Já oculto */ }
                else { showError(fornecedoraTableBody, true); } // Erro no fetch
            }

            if (currentVisibility['card-table-concessionaria']) {
                hideLoading(concessionariaStatus);
                clearContent(concessionariaTableBody, true);
                if (dataSummaryConc && dataSummaryConc.length > 0) {
                    dataSummaryConc.forEach(row => {
                        const tr = document.createElement('tr');

                        // <<< CORREÇÃO APLICADA AQUI >>>
                        const tdConc = document.createElement('td');
                        tdConc.textContent = row.concessionaria || 'N/A';
                        tr.appendChild(tdConc);

                        const tdClientes = document.createElement('td');
                        tdClientes.style.textAlign = 'right';
                        tdClientes.textContent = formatNumber(row.qtd_clientes, 0);
                        tr.appendChild(tdClientes);

                        const tdKwh = document.createElement('td');
                        tdKwh.style.textAlign = 'right';
                        tdKwh.textContent = `${formatNumber(row.soma_consumo, 2)} kWh`;
                        tr.appendChild(tdKwh);
                        // <<< FIM DA CORREÇÃO >>>

                        concessionariaTableBody.appendChild(tr);
                    });
                    reapplySort('concessionaria-summary-table');
                } else if (dataSummaryConc) {
                    concessionariaTableBody.innerHTML = '<tr><td colspan="3" style="text-align: center;">Nenhum dado.</td></tr>';
                } else if (dataSummaryConc === null) { /* Já oculto */ }
                 else { showError(concessionariaTableBody, true); }
            }

            if (currentVisibility['card-kpi-resultado-mes']) {
                 if (dataKpiKwh) kpiTotalKwhElement.textContent = formatNumber(dataKpiKwh.total_kwh, 0);
                 else if (dataKpiKwh === null) { /* Já oculto */ }
                 else { showError(kpiTotalKwhElement); }
            }

            if (currentVisibility['card-kpi-clientes-mes']) {
                if (dataKpiAtivos) kpiClientesAtivosElement.textContent = formatNumber(dataKpiAtivos.clientes_ativos_count, 0);
                else if (dataKpiAtivos === null) { /* Já oculto */ }
                else { showError(kpiClientesAtivosElement); }

                if (dataKpiRegistrados) kpiClientesRegistradosElement.textContent = formatNumber(dataKpiRegistrados.clientes_registrados_count, 0);
                 else if (dataKpiRegistrados === null) { /* Já oculto */ }
                else { showError(kpiClientesRegistradosElement); }
            }

           console.log("Dados do dashboard (mês) atualizados.");

       } catch (error) {
           console.error('Erro ao buscar dados do dashboard:', error);
            // Mostra erro apenas em elementos visíveis
            if (currentVisibility['card-table-fornecedora']) showError(fornecedoraStatus); showError(fornecedoraTableBody, true);
            if (currentVisibility['card-table-concessionaria']) showError(concessionariaStatus); showError(concessionariaTableBody, true);
            if (currentVisibility['card-kpi-resultado-mes']) showError(kpiTotalKwhElement);
            if (currentVisibility['card-kpi-clientes-mes']) showError(kpiClientesAtivosElement); showError(kpiClientesRegistradosElement);
            // Também pode indicar erro nos status dos gráficos
            if (currentVisibility['card-pie-fornecedora'] && fornecedoraPieStatus) { fornecedoraPieStatus.innerHTML = `<span style="color: red;"><i class="fas fa-exclamation-triangle"></i> Erro</span>`; fornecedoraPieStatus.style.display = 'block'; if (fornecedoraPieChartInstance) { fornecedoraPieChartInstance.destroy(); fornecedoraPieChartInstance = null;} }
            if (currentVisibility['card-bar-concessionaria'] && concessionariaBarStatus) { concessionariaBarStatus.innerHTML = `<span style="color: red;"><i class="fas fa-exclamation-triangle"></i> Erro</span>`; concessionariaBarStatus.style.display = 'block'; if (concessionariaBarChartInstance) { concessionariaBarChartInstance.destroy(); concessionariaBarChartInstance = null;} }
       }

        // Atualiza cards que não dependem do mês (eles têm checagem interna)
        updateFornecedoraNoRcbCard();
        updateOverduePaymentsChart(overdueDaysFilter ? overdueDaysFilter.value : '30');
   }

    // --- Funções de Ordenação de Tabela ---
    function getCellValue(row, columnIndex, sortType) { const cell = row.cells[columnIndex]; if (!cell) return sortType === 'number' ? -Infinity : ''; let value = cell.textContent || cell.innerText || ''; if (sortType === 'number') { value = value.replace(/\./g, '').replace(/ kWh/gi, '').replace(/,/g, '.').trim(); const num = parseFloat(value); return isNaN(num) ? -Infinity : num; } return value.trim().toLowerCase(); }
    function updateSortIcons(tableId, clickedHeader) { const table = document.getElementById(tableId); if (!table) return; const headers = table.querySelectorAll('thead th[data-column-index]'); const state = sortState[tableId] || { colIndex: -1, direction: 'none' }; const iconSelector = '.sort-icon'; headers.forEach(header => { const iconSpan = header.querySelector(iconSelector); if (iconSpan) { iconSpan.textContent = (header === clickedHeader) ? (state.direction === 'asc' ? ' ▲' : (state.direction === 'desc' ? ' ▼' : '')) : ''; } }); }
    function sortTable(tableId, columnIndex, sortType) { const table = document.getElementById(tableId); const tbody = table ? table.querySelector('tbody') : null; if (!tbody) { console.error(`TBody não encontrado para ${tableId}`); return; } if (!sortState[tableId]) sortState[tableId] = { colIndex: -1, direction: 'none' }; const state = sortState[tableId]; state.direction = (columnIndex === state.colIndex && state.direction === 'asc') ? 'desc' : 'asc'; state.colIndex = columnIndex; const rows = Array.from(tbody.querySelectorAll('tr:has(td)')); // Seleciona apenas linhas com <td>
       if (rows.length === 0) return; // Não ordena se não houver dados
       rows.sort((rowA, rowB) => { const valueA = getCellValue(rowA, columnIndex, sortType); const valueB = getCellValue(rowB, columnIndex, sortType); let comparison = (sortType === 'number') ? valueA - valueB : valueA.localeCompare(valueB, 'pt-BR', { sensitivity: 'base' }); return state.direction === 'asc' ? comparison : comparison * -1; });
       // Preserva a linha de "Carregando" ou "Nenhum dado" se existir
       const placeholderRow = tbody.querySelector('tr:not(:has(td))');
       tbody.innerHTML = ''; // Limpa
       if(placeholderRow) tbody.appendChild(placeholderRow); // Readiciona se existia
        rows.forEach(row => tbody.appendChild(row)); // Adiciona linhas ordenadas
       const clickedHeader = table.querySelector(`thead th[data-column-index="${columnIndex}"]`);
       updateSortIcons(tableId, clickedHeader); }
    function reapplySort(tableId) { const state = sortState[tableId]; if (state && state.colIndex !== -1 && state.direction !== 'none') { const table = document.getElementById(tableId); const header = table ? table.querySelector(`thead th[data-column-index="${state.colIndex}"]`) : null; if (header) { const sortType = header.getAttribute('data-sort-type'); const originalDirection = state.direction; state.colIndex = -1; state.direction = 'none'; sortTable(tableId, parseInt(header.getAttribute('data-column-index')), sortType); if (sortState[tableId].direction !== originalDirection) { sortTable(tableId, parseInt(header.getAttribute('data-column-index')), sortType); } } } else { updateSortIcons(tableId, null); } }
    function addSortListeners(tableId, headerSelectorClass) { const table = document.getElementById(tableId); const headers = table ? table.querySelectorAll(`thead th.${headerSelectorClass}`) : []; if (!headers.length) { return; } headers.forEach(header => { if (header.hasAttribute('data-column-index') && header.hasAttribute('data-sort-type')) { header.style.cursor = 'pointer'; header.addEventListener('click', function() { const columnIndex = parseInt(this.getAttribute('data-column-index'), 10); const sortType = this.getAttribute('data-sort-type'); sortTable(tableId, columnIndex, sortType); }); } }); }

    // --- Inicialização do SortableJS ---
    function initializeSortable() {
        if (dashboardGrid) {
            new Sortable(dashboardGrid, {
                animation: 150, ghostClass: 'sortable-ghost', chosenClass: 'sortable-chosen', dragClass: 'sortable-drag', handle: '.card-header', // Permite arrastar pelo header
                onEnd: function (evt) {
                    const itemOrder = Array.from(dashboardGrid.children).map(item => item.id).filter(id => !!id); // Pega IDs válidos
                    localStorage.setItem('dashboardCardOrder', JSON.stringify(itemOrder));
                    console.log('Nova ordem salva:', itemOrder);
                },
            });
            // Restaurar a ordem ao carregar
            const savedOrder = localStorage.getItem('dashboardCardOrder');
            if (savedOrder) {
                try {
                    const order = JSON.parse(savedOrder);
                    // Valida se os IDs salvos ainda existem no DOM atual
                    const currentIds = new Set(Array.from(dashboardGrid.querySelectorAll('.card[id]')).map(el => el.id));
                    const validOrder = order.filter(id => currentIds.has(id));

                    // Adiciona IDs que estão no DOM mas não na ordem salva (novos cards?)
                    currentIds.forEach(id => { if(!validOrder.includes(id)) validOrder.push(id); });

                   validOrder.forEach(itemId => {
                       const element = document.getElementById(itemId);
                       if (element) dashboardGrid.appendChild(element); // Move para o final na ordem correta
                   });
                   console.log("Ordem dos cards restaurada/atualizada.");
                    if(order.length !== validOrder.length) localStorage.setItem('dashboardCardOrder', JSON.stringify(validOrder)); // Salva ordem corrigida

                } catch (e) {
                    console.error("Erro ao restaurar ordem dos cards:", e);
                    localStorage.removeItem('dashboardCardOrder');
                }
            }
            console.log("SortableJS inicializado.");
        } else { console.error("Container .dashboard-grid não encontrado."); }
    }


    // --- Execução Inicial ---

    // 1. Inicializa a visibilidade dos cards (lê localStorage, aplica classe .card-hidden)
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
            applyVisibility(currentSettings); // Aplica imediatamente
            // Reavalia quais dados buscar após mudança de visibilidade
            if (monthSelect) updateDashboardData(monthSelect.value);
        });
    });
    if (saveBtn) saveBtn.addEventListener('click', closeModal); // Botão só fecha

    // 3. Adiciona listeners de ordenação às tabelas
    addSortListeners('fornecedora-summary-table', 'sortable-header');
    addSortListeners('concessionaria-summary-table', 'sortable-header');
    addSortListeners('fornecedora-no-rcb-table', 'sortable-header');

    // 4. Inicializa SortableJS para arrastar cards
    initializeSortable();

    // 5. Configura listeners dos filtros de dados (mês, ano, dias vencidos)
    if (monthSelect) monthSelect.addEventListener('change', () => updateDashboardData(monthSelect.value));
    if (yearSelectChart) yearSelectChart.addEventListener('change', () => updateChartData(yearSelectChart.value));
    if (overdueDaysFilter) overdueDaysFilter.addEventListener('change', () => updateOverduePaymentsChart(overdueDaysFilter.value));

    // 6. Carrega os dados iniciais para os cards visíveis
    if (monthSelect) {
        updateDashboardData(monthSelect.value); // Chama a função principal que agora verifica visibilidade
    } else {
        // Se não houver filtro de mês, talvez carregar dados gerais ou do mês atual?
        const currentMonth = new Date().toISOString().slice(0, 7);
        updateDashboardData(currentMonth);
    }

}); // Fim DOMContentLoaded