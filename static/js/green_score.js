// static/js/green_score.js - ATUALIZADO PARA INCLUIR GRÁFICO DE LINHA E CORRIGIR ERROS DE NULL

document.addEventListener('DOMContentLoaded', function() {
    // Definir fetchData aqui, pois dashboard_controls.js não é carregado na página green_score.html
    async function fetchData(url, description) {
        console.debug(`Fetching ${description} from ${url}`);
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

    const mainContentWrapper = document.getElementById('green-score-content-wrapper');
    const gaugeAndKpisRow = document.getElementById('gauge-and-kpis-row');
    const gaugeContainer = document.getElementById('gauge-container');
    const fornecedoraFilter = document.getElementById('fornecedora-filter');
    const placeholderMessage = document.getElementById('placeholder-message');
    
    const kpiSummaryContainer = document.getElementById('kpi-summary-container'); // KPIs do Mês
    const consolidatedKpisWrapper = document.getElementById('consolidated-kpis-wrapper'); // KPIs Consolidados
    
    // Elementos dos KPIs
    const kpiTotalKwh = document.getElementById('kpi-total-kwh-green-score');
    const kpiClientesRegistrados = document.getElementById('kpi-clientes-registrados-green-score');
    const kpiClientesAtivos = document.getElementById('kpi-clientes-ativos-green-score');

    // NOVOS Elementos dos KPIs CONSOLIDADOS
    const kpiTotalKwhConsolidado = document.getElementById('kpi-total-kwh-consolidado');
    const kpiClientesRegistradosConsolidado = document.getElementById('kpi-clientes-registrados-consolidado');
    const kpiClientesAtivosConsolidado = document.getElementById('kpi-clientes-ativos-consolidado');

    // NOVO: Elemento para o KPI de Clientes com Atraso na Injeção
    const kpiClientesAtrasoInjecao = document.getElementById('kpi-clientes-atraso-injecao');

    // Elementos para o gráfico de linha anual
    const greenScoreChartYearSelect = document.getElementById('green-score-chart-year');
    const greenScoreMonthlyChartCanvas = document.getElementById('green-score-monthly-chart-canvas');
    const greenScoreMonthlyChartStatus = document.getElementById('green-score-monthly-chart-status');
    let greenScoreMonthlyLineChartInstance = null; // Instância do Chart.js
    const greenScoreMonthlyChartCard = document.getElementById('green-score-monthly-chart'); // Referência ao card (agora por ID)

    const supplierLogos = {
        'SOLATIO': '/static/img/fornecedoras/solatio.png',
        'COMERC': '/static/img/fornecedoras/comerc.png',
        'RZK': '/static/img/fornecedoras/rzk.png',
        'BC ENERGIA': '/static/img/fornecedoras/bc_energia.png',
        'BOM FUTURO': '/static/img/fornecedoras/bom_futuro.png',
        'ULTRA': '/static/img/fornecedoras/ultra.png',
        'FIT': '/static/img/fornecedoras/fit.png',
        'COTESA': '/static/img/fornecedoras/cotesa.png',
        'SINERGI': '/static/img/fornecedoras/sinergi.png',
        'ATUA': '/static/img/fornecedoras/atua.png',
        'MATRIX': '/static/img/fornecedoras/matrix.png',
        'REENERGISA': '/static/img/fornecedoras/reenergisa.png',
        'VANTAGE': '/static/img/fornecedoras/vantage.png',
        'EDP': '/static/img/fornecedoras/edp.png',
        'GV': '/static/img/fornecedoras/gv.png',
        'FARO': '/static/img/fornecedoras/faro.png',
    };

    const supplierLogoDisplay = document.getElementById('supplier-logo-display');
    const supplierLogoImg = document.getElementById('supplier-logo-img');

    function getScoreColors(value) {
        if (value >= 80) { return ['#86efac', '#16a34a']; }
        else if (value >= 60) { return ['#fde047', '#f97316']; }
        else if (value >= 40) { return ['#fdba74', '#dc2626']; }
        else { return ['#fca5a5', '#b91c1c']; }
    }

    function createGauge(containerId, title, value) {
        const colors = getScoreColors(value);

        const options = {
            series: [Math.round(value)],
            chart: {
                height: 300,
                type: 'radialBar',
                offsetY: -20,
                sparkline: { enabled: false }
            },
            plotOptions: {
                radialBar: {
                    startAngle: -135,
                    endAngle: 135,
                    hollow: {
                        margin: 0,
                        size: '70%',
                        background: '#fff',
                        image: undefined,
                        position: 'front',
                        dropShadow: {
                            enabled: true,
                            top: 3,
                            left: 0,
                            blur: 4,
                            opacity: 0.24
                        }
                    },
                    track: {
                        background: '#f2f2f2',
                        strokeWidth: '67%',
                        margin: 0,
                        dropShadow: {
                            enabled: true,
                            top: -3,
                            left: 0,
                            blur: 4,
                            opacity: 0.35
                        }
                    },
                    dataLabels: {
                        show: true,
                        name: {
                            offsetY: -30,
                            show: true,
                            color: '#888',
                            fontSize: '17px',
                            fontFamily: 'Inter, sans-serif',
                            fontWeight: 600,
                        },
                        value: {
                            formatter: function(val) {
                                return value.toFixed(1) + "%";
                            },
                            offsetY: 7,
                            color: '#111',
                            fontSize: '44px',
                            fontFamily: 'Inter, sans-serif',
                            fontWeight: 700,
                            show: true,
                        }
                    }
                }
            },
            fill: {
                type: 'gradient',
                gradient: {
                    shade: 'dark',
                    type: 'horizontal',
                    shadeIntensity: 0.5,
                    gradientToColors: [colors[1]],
                    inverseColors: true,
                    opacityFrom: 1,
                    opacityTo: 1,
                    stops: [0, 100]
                }
            },
            colors: [colors[0]],
            labels: [title],
            stroke: { lineCap: 'round' },
            markers: {
                size: 0,
                discrete: [{
                    seriesIndex: 0,
                    dataPointIndex: 0,
                    fillColor: '#242424',
                    strokeColor: '#fff',
                    size: 5,
                    shape: 'circle'
                }],
            },
            tooltip: {
                enabled: true,
                y: {
                    formatter: function (val) {
                        return val.toFixed(1) + " %";
                    }
                }
            }
        };

        const chart = new ApexCharts(document.getElementById(containerId), options);
        chart.render();
    }
    
    function formatNumber(num, decimalPlaces = 0) {
        if (typeof num !== 'number' || isNaN(num)) { return '0'; }
        return num.toLocaleString('pt-BR', {
            minimumFractionDigits: decimalPlaces,
            maximumFractionDigits: decimalPlaces
        });
    }

    async function updateKPIs(fornecedora) {
        const today = new Date();
        const currentMonth = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, '0')}`;
        
        if (kpiTotalKwh) kpiTotalKwh.innerHTML = '<i class="fas fa-spinner fa-spin fa-xs"></i>';
        if (kpiClientesRegistrados) kpiClientesRegistrados.innerHTML = '<i class="fas fa-spinner fa-spin fa-xs"></i>';
        if (kpiClientesAtivos) kpiClientesAtivos.innerHTML = '<i class="fas fa-spinner fa-spin fa-xs"></i>';

        try {
            const kwhResponse = await fetchData(`/api/kpi/total-kwh?month=${currentMonth}&fornecedora=${encodeURIComponent(fornecedora)}`, "KPI Total kWh (Green Score)");
            if (kwhResponse && kwhResponse.total_kwh !== undefined) {
                if (kpiTotalKwh) kpiTotalKwh.textContent = formatNumber(kwhResponse.total_kwh, 0);
            } else {
                if (kpiTotalKwh) kpiTotalKwh.innerHTML = '<span style="color: red; font-size: 0.7em;">Erro!</span>';
                console.error('Erro ao buscar kWh:', kwhResponse?.error || 'Resposta inválida');
            }

            const registradosResponse = await fetchData(`/api/kpi/clientes-registrados?month=${currentMonth}&fornecedora=${encodeURIComponent(fornecedora)}`, "KPI Clientes Registrados (Green Score)");
            if (registradosResponse && registradosResponse.clientes_registrados_count !== undefined) {
                if (kpiClientesRegistrados) kpiClientesRegistrados.textContent = formatNumber(registradosResponse.clientes_registrados_count, 0);
            } else {
                if (kpiClientesRegistrados) kpiClientesRegistrados.innerHTML = '<span style="color: red; font-size: 0.7em;">Erro!</span>';
                console.error('Erro ao buscar clientes registrados:', registradosResponse?.error || 'Resposta inválida');
            }

            const ativosResponse = await fetchData(`/api/kpi/clientes-ativos?month=${currentMonth}&fornecedora=${encodeURIComponent(fornecedora)}`, "KPI Clientes Ativos (Green Score)");
            if (ativosResponse && ativosResponse.clientes_ativos_count !== undefined) {
                if (kpiClientesAtivos) kpiClientesAtivos.textContent = formatNumber(ativosResponse.clientes_ativos_count, 0);
            } else {
                if (kpiClientesAtivos) kpiClientesAtivos.innerHTML = '<span style="color: red; font-size: 0.7em;">Erro!</span>';
                console.error('Erro ao buscar clientes ativados:', ativosResponse?.error || 'Resposta inválida');
            }
            
        } catch (error) {
            console.error("Erro geral ao buscar KPIs da Green Score:", error);
            if (kpiTotalKwh) kpiTotalKwh.innerHTML = '<span style="color: red; font-size: 0.7em;">Erro!</span>';
            if (kpiClientesRegistrados) kpiClientesRegistrados.innerHTML = '<span style="color: red; font-size: 0.7em;">Erro!</span>';
            if (kpiClientesAtivos) kpiClientesAtivos.innerHTML = '<span style="color: red; font-size: 0.7em;">Erro!</span>';
            if (kpiSummaryContainer) kpiSummaryContainer.style.display = 'none'; 
        }
    }

    // NOVA FUNÇÃO: Atualiza os KPIs CONSOLIDADOS (sem filtro de mês, com filtro de fornecedora)
    async function updateConsolidatedKPIs(fornecedora) {
        
        // Exibir spinners nos KPIs consolidados
        if (kpiTotalKwhConsolidado) kpiTotalKwhConsolidado.innerHTML = '<i class="fas fa-spinner fa-spin fa-xs"></i>';
        if (kpiClientesRegistradosConsolidado) kpiClientesRegistradosConsolidado.innerHTML = '<i class="fas fa-spinner fa-spin fa-xs"></i>';
        if (kpiClientesAtivosConsolidado) kpiClientesAtivosConsolidado.innerHTML = '<i class="fas fa-spinner fa-spin fa-xs"></i>';
        if (kpiClientesAtrasoInjecao) kpiClientesAtrasoInjecao.innerHTML = '<i class="fas fa-spinner fa-spin fa-xs"></i>'; // NOVO: Spinner para atraso na injeção

        try {
            // Se a fornecedora for 'Consolidado', não passamos o parâmetro 'fornecedora' na URL.
            // Caso contrário, passamos a fornecedora selecionada.
            const fornecedoraParam = (fornecedora && fornecedora.toLowerCase() !== 'consolidado') ? `&fornecedora=${encodeURIComponent(fornecedora)}` : '';

            // Busca kWh Total Consolidado
            const kwhResponse = await fetchData(`/api/kpi/total-kwh-consolidated?${fornecedoraParam}`, "KPI Total kWh (Consolidado)");
            if (kwhResponse && kwhResponse.total_kwh !== undefined) {
                if (kpiTotalKwhConsolidado) kpiTotalKwhConsolidado.textContent = formatNumber(kwhResponse.total_kwh, 0);
            } else {
                if (kpiTotalKwhConsolidado) kpiTotalKwhConsolidado.innerHTML = '<span style="color: red; font-size: 0.7em;">Erro!</span>';
                console.error('Erro ao buscar kWh consolidado:', kwhResponse?.error || 'Resposta inválida');
            }

            // Busca Clientes Registrados Consolidado
            const registradosResponse = await fetchData(`/api/kpi/clientes-registrados-consolidated?${fornecedoraParam}`, "KPI Clientes Registrados (Consolidado)");
            if (registradosResponse && registradosResponse.clientes_registrados_count !== undefined) {
                if (kpiClientesRegistradosConsolidado) kpiClientesRegistradosConsolidado.textContent = formatNumber(registradosResponse.clientes_registrados_count, 0);
            } else {
                if (kpiClientesRegistradosConsolidado) kpiClientesRegistradosConsolidado.innerHTML = '<span style="color: red; font-size: 0.7em;">Erro!</span>';
                console.error('Erro ao buscar clientes registrados consolidados:', registradosResponse?.error || 'Resposta inválida');
            }

            // Busca Clientes Ativos Consolidado
            const ativosResponse = await fetchData(`/api/kpi/clientes-ativos-consolidated?${fornecedoraParam}`, "KPI Clientes Ativos (Consolidado)");
            if (ativosResponse && ativosResponse.clientes_ativos_count !== undefined) {
                if (kpiClientesAtivosConsolidado) kpiClientesAtivosConsolidado.textContent = formatNumber(ativosResponse.clientes_ativos_count, 0);
            } else {
                if (kpiClientesAtivosConsolidado) kpiClientesAtivosConsolidado.innerHTML = '<span style="color: red; font-size: 0.7em;">Erro!</span>';
                console.error('Erro ao buscar clientes ativados consolidados:', ativosResponse?.error || 'Resposta inválida');
            }

            // NOVO: Busca Clientes com Atraso na Injeção
            const atrasoInjecaoResponse = await fetchData(`/api/kpi/overdue-injection-clients?${fornecedoraParam}`, "KPI Clientes com Atraso na Injeção");
            if (atrasoInjecaoResponse && atrasoInjecaoResponse.overdue_injection_clients_count !== undefined) {
                if (kpiClientesAtrasoInjecao) kpiClientesAtrasoInjecao.textContent = formatNumber(atrasoInjecaoResponse.overdue_injection_clients_count, 0);
            } else {
                if (kpiClientesAtrasoInjecao) kpiClientesAtrasoInjecao.innerHTML = '<span style="color: red; font-size: 0.7em;">Erro!</span>';
                console.error('Erro ao buscar clientes com atraso na injeção:', atrasoInjecaoResponse?.error || 'Resposta inválida');
            }

            console.log("KPIs consolidados e de atraso na injeção atualizados com sucesso.");

        } catch (error) {
            console.error("Erro geral ao buscar KPIs consolidados da Green Score:", error);
            if (kpiTotalKwhConsolidado) kpiTotalKwhConsolidado.innerHTML = '<span style="color: red; font-size: 0.7em;">Erro!</span>';
            if (kpiClientesRegistradosConsolidado) kpiClientesRegistradosConsolidado.innerHTML = '<span style="color: red; font-size: 0.7em;">Erro!</span>';
            if (kpiClientesAtivosConsolidado) kpiClientesAtivosConsolidado.innerHTML = '<span style="color: red; font-size: 0.7em;">Erro!</span>';
            if (kpiClientesAtrasoInjecao) kpiClientesAtrasoInjecao.innerHTML = '<span style="color: red; font-size: 0.7em;">Erro!</span>'; // NOVO: Mensagem de erro para atraso na injeção
            // Se houver um contêiner para os KPIs consolidados, pode-se esconder em caso de erro
            if (consolidatedKpisWrapper) consolidatedKpisWrapper.style.display = 'none'; 
        }
    }

    async function updateGreenScoreMonthlyChart(fornecedora, year) {
        if (!greenScoreMonthlyChartCanvas) {
            console.error("Canvas do gráfico de linha Green Score não encontrado.");
            return;
        }

        if (greenScoreMonthlyChartStatus) {
            greenScoreMonthlyChartStatus.innerHTML = '<i class="fas fa-spinner fa-spin fa-lg"></i><br>Carregando...';
            greenScoreMonthlyChartStatus.style.display = 'block';
        }
        greenScoreMonthlyChartCanvas.style.display = 'none'; // Esconde o canvas enquanto carrega

        const apiUrl = `/api/chart/monthly-active-clients?year=${year}&fornecedora=${encodeURIComponent(fornecedora)}`;
        console.log(`Buscando dados para o gráfico de linha (Green Score): ${apiUrl}`);

        try {
            const chartData = await fetchData(apiUrl, `Gráfico Mensal Ativações (Green Score) - ${fornecedora} ${year}`);

            if (greenScoreMonthlyChartStatus) greenScoreMonthlyChartStatus.style.display = 'none'; // Esconde o status
            
            if (chartData && Array.isArray(chartData.monthly_counts) && chartData.monthly_counts.length === 12) {
                const labels = ['JAN', 'FEV', 'MAR', 'ABR', 'MAI', 'JUN', 'JUL', 'AGO', 'SET', 'OUT', 'NOV', 'DEZ'];
                const dataValues = chartData.monthly_counts;

                if (greenScoreMonthlyLineChartInstance) {
                    greenScoreMonthlyLineChartInstance.data.labels = labels;
                    greenScoreMonthlyLineChartInstance.data.datasets[0].data = dataValues;
                    greenScoreMonthlyLineChartInstance.data.datasets[0].label = `Ativações ${fornecedora} ${year}`;
                    greenScoreMonthlyLineChartInstance.update();
                } else {
                    const config = {
                        type: 'line',
                        data: {
                            labels: labels,
                            datasets: [{
                                label: `Ativações ${fornecedora} ${year}`,
                                data: dataValues,
                                fill: true,
                                backgroundColor: 'rgba(0, 201, 59, 0.1)',
                                borderColor: 'rgb(0, 201, 59)',
                                borderWidth: 2,
                                tension: 0.3,
                                pointBackgroundColor: 'rgb(0, 201, 59)',
                                pointRadius: 3,
                                pointHoverRadius: 5
                            }]
                        },
                        options: {
                            responsive: true,
                            maintainAspectRatio: false,
                            plugins: {
                                legend: { display: false },
                                tooltip: {
                                    backgroundColor: 'rgba(0, 0, 0, 0.7)',
                                    titleFont: { weight: 'bold'},
                                    callbacks: {
                                        label: function(context) {
                                            return (context.dataset.label || '') + ': ' + formatNumber(context.parsed.y);
                                        }
                                    }
                                }
                            },
                            scales: {
                                y: { beginAtZero: true, grid: { color: 'rgba(0,0,0,0.05)' }, ticks: { precision: 0 } },
                                x: { grid: { display: false } }
                            },
                            interaction: { intersect: false, mode: 'index', }
                        }
                    };
                    greenScoreMonthlyLineChartInstance = new Chart(greenScoreMonthlyChartCanvas, config);
                }
                greenScoreMonthlyChartCanvas.style.display = 'block'; // Mostra o canvas
                console.log(`Gráfico de linha Green Score atualizado para ${fornecedora} - ${year}.`);
            } else {
                if (greenScoreMonthlyChartStatus) {
                    greenScoreMonthlyChartStatus.innerHTML = '<i class="fas fa-info-circle"></i><br>Nenhum dado para exibir.';
                    greenScoreMonthlyChartStatus.style.display = 'block';
                }
                if (greenScoreMonthlyLineChartInstance) {
                    greenScoreMonthlyLineChartInstance.destroy();
                    greenScoreMonthlyLineChartInstance = null;
                }
            }
        } catch (error) {
            console.error(`Erro ao buscar/renderizar gráfico de linha Green Score para ${fornecedora} - ${year}:`, error);
            if (greenScoreMonthlyChartStatus) {
                greenScoreMonthlyChartStatus.innerHTML = `<span style="color: red;"><i class="fas fa-exclamation-triangle"></i> Erro</span>`;
                greenScoreMonthlyChartStatus.style.display = 'block';
            }
            if (greenScoreMonthlyLineChartInstance) {
                greenScoreMonthlyLineChartInstance.destroy();
                greenScoreMonthlyLineChartInstance = null;
            }
        }
    }


    async function loadScoreFor(fornecedora) {
        // Esconder tudo e exibir placeholder se a opção for vazia
        if (fornecedora === '' || fornecedora === null) {
            if (mainContentWrapper) mainContentWrapper.style.display = 'none';
            if (placeholderMessage) placeholderMessage.style.display = 'block';
            if (supplierLogoDisplay) supplierLogoDisplay.style.display = 'none';
            if (kpiSummaryContainer) kpiSummaryContainer.style.display = 'none';
            if (greenScoreMonthlyChartCard) greenScoreMonthlyChartCard.style.display = 'none';
            if (gaugeContainer) gaugeContainer.style.display = 'none';
            if (consolidatedKpisWrapper) consolidatedKpisWrapper.style.display = 'none';
            return;
        }

        if (placeholderMessage) placeholderMessage.style.display = 'none';
        if (mainContentWrapper) mainContentWrapper.style.display = 'flex';

        if (gaugeContainer) {
            gaugeContainer.innerHTML = `<div class="message-center"><i class="fas fa-spinner fa-spin fa-2x text-primary"></i><p class="mt-2 text-muted">Carregando score${fornecedora === 'Consolidado' ? 's' : ''} para **${fornecedora}**...</p></div>`;
            gaugeContainer.classList.remove('gauge-single-container', 'gauge-grid-consolidated');
            gaugeContainer.style.display = 'flex';
        }

        if (fornecedora === 'Consolidado') {
            if (supplierLogoDisplay) supplierLogoDisplay.style.display = 'none';
            if (kpiSummaryContainer) kpiSummaryContainer.style.display = 'none'; // Esconder KPIs do mês
            if (greenScoreMonthlyChartCard) greenScoreMonthlyChartCard.style.display = 'none';
            if (consolidatedKpisWrapper) consolidatedKpisWrapper.style.display = 'grid'; // Mostrar KPIs consolidados
            if (gaugeContainer) {
                gaugeContainer.classList.add('gauge-grid-consolidated');
                gaugeContainer.style.display = 'grid';
            }
            if (gaugeAndKpisRow) gaugeAndKpisRow.style.display = 'block'; // Manter flex ou block para que os gauges apareçam
            
            try {
                const apiUrl = `/api/scores/green-score?fornecedora=Consolidado`;
                const response = await fetch(apiUrl);
                if (!response.ok) {
                    const errorData = await response.json();
                    throw new Error(errorData.error || `Erro HTTP ${response.status}`);
                }
                const data = await response.json();
                if (gaugeContainer) gaugeContainer.innerHTML = '';
                if (data.length > 0) {
                    data.forEach((scoreInfo, index) => {
                        const cardDiv = document.createElement('div');
                        cardDiv.className = 'gauge-card';
                        const chartDiv = document.createElement('div');
                        chartDiv.id = `gauge-chart-${index}`;
                        chartDiv.className = 'gauge-chart-container';
                        cardDiv.appendChild(chartDiv);
                        if (gaugeContainer) gaugeContainer.appendChild(cardDiv);
                        createGauge(chartDiv.id, scoreInfo.fornecedora, scoreInfo.score);
                    });
                } else {
                    if (gaugeContainer) gaugeContainer.innerHTML = `<div class="message-center alert alert-warning">Nenhum score encontrado para as fornecedoras.</div>`;
                }
            } catch (error) {
                console.error("Erro ao carregar scores consolidados:", error);
                if (gaugeContainer) gaugeContainer.innerHTML = `<div class="message-center alert alert-danger">Falha ao carregar scores. Tente novamente mais tarde.<br><small>${error.message}</small></div>`;
            }
            updateConsolidatedKPIs('Consolidado'); // Chamar para atualizar os KPIs consolidados para "Consolidado"
        } else {
            if (supplierLogoDisplay) supplierLogoDisplay.style.display = 'block';
            if (kpiSummaryContainer) kpiSummaryContainer.style.display = 'grid'; // Mostrar KPIs do mês
            if (greenScoreMonthlyChartCard) greenScoreMonthlyChartCard.style.display = 'block';
            if (consolidatedKpisWrapper) consolidatedKpisWrapper.style.display = 'grid'; // Mostrar KPIs consolidados
            if (gaugeContainer) {
                gaugeContainer.classList.add('gauge-single-container');
                gaugeContainer.style.display = 'flex';
            }
            if (gaugeAndKpisRow) gaugeAndKpisRow.style.display = 'flex';

            const logoPath = supplierLogos[fornecedora.toUpperCase()];
            if (logoPath) {
                if (supplierLogoImg) supplierLogoImg.src = logoPath;
                if (supplierLogoDisplay) supplierLogoDisplay.style.display = 'block';
            } else {
                if (supplierLogoImg) supplierLogoImg.src = '';
                if (supplierLogoDisplay) supplierLogoDisplay.style.display = 'none';
            }
            updateKPIs(fornecedora);
            const selectedYear = greenScoreChartYearSelect ? greenScoreChartYearSelect.value : new Date().getFullYear();
            updateGreenScoreMonthlyChart(fornecedora, selectedYear);
            try {
                const apiUrl = `/api/scores/green-score?fornecedora=${encodeURIComponent(fornecedora)}`;
                const response = await fetch(apiUrl);
                if (!response.ok) {
                    const errorData = await response.json();
                    throw new Error(errorData.error || `Erro HTTP ${response.status}`);
                }
                const data = await response.json();
                if (gaugeContainer) gaugeContainer.innerHTML = '';
                if (data.length > 0) {
                    const scoreInfo = data[0];
                    const chartDiv = document.createElement('div');
                    chartDiv.id = 'gauge-chart-single';
                    chartDiv.className = 'gauge-chart-container';
                    if (gaugeContainer) gaugeContainer.appendChild(chartDiv);
                    createGauge('gauge-chart-single', scoreInfo.fornecedora, scoreInfo.score);
                } else {
                    if (gaugeContainer) gaugeContainer.innerHTML = `<div class="message-center alert alert-warning">Nenhum score encontrado para ${fornecedora}.</div>`;
                }
            } catch (error) {
                console.error("Erro ao carregar score:", error);
                if (gaugeContainer) gaugeContainer.innerHTML = `<div class="message-center alert alert-danger">Falha ao carregar o score. Tente novamente mais tarde.<br><small>${error.message}</small></div>`;
            }
            updateConsolidatedKPIs(fornecedora);
        }
    }

    fornecedoraFilter.addEventListener('change', function() {
        const selectedSupplier = this.value;
        loadScoreFor(selectedSupplier);
    });

    if (greenScoreChartYearSelect) {
        greenScoreChartYearSelect.addEventListener('change', function() {
            const selectedSupplier = fornecedoraFilter.value;
            const selectedYear = this.value;
            if (selectedSupplier && selectedSupplier !== 'Consolidado') {
                updateGreenScoreMonthlyChart(selectedSupplier, selectedYear);
            }
        });
    }

    // Estado inicial (ao carregar a página)
    if (mainContentWrapper) mainContentWrapper.style.display = 'none';
    if (placeholderMessage) placeholderMessage.style.display = 'block';
    if (kpiSummaryContainer) kpiSummaryContainer.style.display = 'none';
    if (supplierLogoDisplay) supplierLogoDisplay.style.display = 'none';
    if (greenScoreMonthlyChartCard) greenScoreMonthlyChartCard.style.display = 'none';
    if (gaugeContainer) gaugeContainer.style.display = 'none';
    if (consolidatedKpisWrapper) consolidatedKpisWrapper.style.display = 'none';

    loadScoreFor(fornecedoraFilter.value);
});