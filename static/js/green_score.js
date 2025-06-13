document.addEventListener('DOMContentLoaded', function() {
    const gaugeContainer = document.getElementById('gauge-container');
    const fornecedoraFilter = document.getElementById('fornecedora-filter');
    const placeholderMessage = document.getElementById('placeholder-message');
    const additionalInfoItems = document.querySelectorAll('.info-card');
    
    // NOVO: Elementos do logo
    const supplierLogoDisplay = document.getElementById('supplier-logo-display');
    const supplierLogoImg = document.getElementById('supplier-logo-img');

    // Elementos para os novos KPIs
    const infoKwhVendidos = document.getElementById('info-kwh-vendidos');
    const infoClientesRegistrados = document.getElementById('info-clientes-registrados');
    const infoClientesAtivados = document.getElementById('info-clientes-ativados');

    // Elementos para o novo gráfico de linha
    const monthlyLineChartCanvas = document.getElementById('monthlyActivationChart');
    const monthlyLineChartYearSelect = document.getElementById('monthly-chart-year'); // NOVO ID
    const monthlyLineChartStatus = document.getElementById('monthly-chart-status'); // NOVO ID
    let monthlyLineChartInstance = null; // Para armazenar a instância do Chart.js

    // NOVO: Mapeamento de fornecedoras para caminhos de logo
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
        // Adicione mais fornecedoras aqui conforme necessário
    };

    /**
     * Determina as cores do gradiente baseado no valor do score.
     * @param {number} value - O score (0 a 100).
     * @returns {string[]} Um array com as cores de início e fim do gradiente.
     */
    function getScoreColors(value) {
        if (value >= 80) { return ['#86efac', '#16a34a']; }
        else if (value >= 60) { return ['#fde047', '#f97316']; }
        else if (value >= 40) { return ['#fdba74', '#dc2626']; }
        else { return ['#fca5a5', '#b91c1c']; }
    }

    /**
     * Cria um único gráfico de velocímetro (gauge) usando ApexCharts.
     * @param {string} containerId - O ID do div onde o gráfico será renderizado.
     * @param {string} title - O nome da fornecedora (rótulo principal).
     * @param {number} value - O score (0 a 100).
     */
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

    /**
     * Busca o score para uma fornecedora específica ou para todas e renderiza os gráficos.
     * @param {string} fornecedora - O nome da fornecedora selecionada (ou "Consolidado").
     */
    async function loadScoreFor(fornecedora) {
        gaugeContainer.innerHTML = `<div class="message-center"><i class="fas fa-spinner fa-spin fa-2x text-primary"></i><p class="mt-2 text-muted">Carregando score${fornecedora === 'Consolidado' ? 's' : ''} para **${fornecedora}**...</p></div>`;
        
        // Esconde todos os cards de informação e o logo ao carregar novos dados
        document.getElementById('additional-info-container').style.display = 'none';
        document.getElementById('monthly-chart-card').style.display = 'none'; // Esconde o card do gráfico
        supplierLogoDisplay.style.display = 'none';
        supplierLogoImg.src = '';
        
        // Limpa a instância do gráfico de linha, se existir
        if (monthlyLineChartInstance) {
            monthlyLineChartInstance.destroy();
            monthlyLineChartInstance = null;
        }

        try {
            let apiUrl = '';
            if (fornecedora === 'Consolidado') {
                apiUrl = `/api/scores/green-score?fornecedora=Consolidado`;
            } else {
                apiUrl = `/api/scores/green-score?fornecedora=${encodeURIComponent(fornecedora)}`;
            }
            
            const response = await fetch(apiUrl);
            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || `Erro HTTP ${response.status}`);
            }
            const data = await response.json();

            gaugeContainer.innerHTML = '';

            if (data.length > 0) {
                if (fornecedora === 'Consolidado') {
                    data.forEach((scoreInfo, index) => {
                        const cardDiv = document.createElement('div');
                        cardDiv.className = 'gauge-card';
                        
                        const chartDiv = document.createElement('div');
                        chartDiv.id = `gauge-chart-${index}`;
                        chartDiv.className = 'gauge-chart-container';
                        cardDiv.appendChild(chartDiv);
                        gaugeContainer.appendChild(cardDiv);

                        createGauge(chartDiv.id, scoreInfo.fornecedora, scoreInfo.score);
                    });
                    // Esconde os cards de informação adicionais quando em modo Consolidado
                    document.getElementById('additional-info-container').style.display = 'none';
                    document.getElementById('monthly-chart-card').style.display = 'none'; // Esconde o card do gráfico
                } else {
                    const scoreInfo = data[0];
                    const cardDiv = document.createElement('div');
                    cardDiv.className = 'gauge-card';
                    
                    const chartDiv = document.createElement('div');
                    chartDiv.id = 'gauge-chart-single';
                    chartDiv.className = 'gauge-chart-container';
                    cardDiv.appendChild(chartDiv);
                    gaugeContainer.appendChild(cardDiv);

                    createGauge('gauge-chart-single', scoreInfo.fornecedora, scoreInfo.score);

                    const logoPath = supplierLogos[fornecedora.toUpperCase()];
                    if (logoPath) {
                        supplierLogoImg.src = logoPath;
                        supplierLogoDisplay.style.display = 'block';
                    } else {
                        supplierLogoDisplay.style.display = 'none';
                    }

                    // Mostra os cards de informação adicionais
                    document.getElementById('additional-info-container').style.display = 'grid';
                    document.getElementById('monthly-chart-card').style.display = 'block'; // Mostra o card do gráfico

                    // Atualiza os KPIs e o gráfico de linha para a fornecedora específica
                    await updateAdditionalInfoCards(fornecedora);
                    await updateMonthlyChart(fornecedora);
                }
            } else {
                gaugeContainer.innerHTML = `<div class="message-center alert alert-warning">Nenhum score encontrado para ${fornecedora}.</div>`;
                document.getElementById('additional-info-container').style.display = 'none';
                document.getElementById('monthly-chart-card').style.display = 'none';
                supplierLogoDisplay.style.display = 'none';
            }

        } catch (error) {
            console.error("Erro ao carregar score:", error);
            gaugeContainer.innerHTML = `<div class="message-center alert alert-danger">Falha ao carregar os dados. Tente novamente mais tarde.<br><small>${error.message}</small></div>`;
            document.getElementById('additional-info-container').style.display = 'none';
            document.getElementById('monthly-chart-card').style.display = 'none';
            supplierLogoDisplay.style.display = 'none';
        }
    }

    /**
     * Busca e atualiza os cards de informação adicionais (kWh, clientes) para uma fornecedora.
     * @param {string} fornecedora - A fornecedora selecionada.
     */
    async function updateAdditionalInfoCards(fornecedora) {
        const currentYear = new Date().getFullYear();
        const currentMonth = currentYear + '-' + String(new Date().getMonth() + 1).padStart(2, '0');

        // Mostra spinners nos KPIs
        infoKwhVendidos.innerHTML = '<i class="fas fa-spinner fa-spin fa-sm"></i>';
        infoClientesRegistrados.innerHTML = '<i class="fas fa-spinner fa-spin fa-sm"></i>';
        infoClientesAtivados.innerHTML = '<i class="fas fa-spinner fa-spin fa-sm"></i>';

        try {
            const [kwhData, registeredClientsData, activeClientsData] = await Promise.all([
                fetch(`/api/kpi/total-kwh?month=${currentMonth}&fornecedora=${encodeURIComponent(fornecedora)}`).then(res => res.json()),
                fetch(`/api/kpi/clientes-registrados?month=${currentMonth}&fornecedora=${encodeURIComponent(fornecedora)}`).then(res => res.json()),
                fetch(`/api/kpi/clientes-ativos?month=${currentMonth}&fornecedora=${encodeURIComponent(fornecedora)}`).then(res => res.json())
            ]);

            infoKwhVendidos.textContent = formatNumber(kwhData.total_kwh || 0, 0);
            infoClientesRegistrados.textContent = formatNumber(registeredClientsData.clientes_registrados_count || 0, 0);
            infoClientesAtivados.textContent = formatNumber(activeClientsData.clientes_ativos_count || 0, 0);

        } catch (error) {
            console.error("Erro ao carregar informações adicionais:", error);
            infoKwhVendidos.innerHTML = '<span style="color: red;">Erro</span>';
            infoClientesRegistrados.innerHTML = '<span style="color: red;">Erro</span>';
            infoClientesAtivados.innerHTML = '<span style="color: red;">Erro</span>';
        }
    }

    /**
     * Atualiza o gráfico de linha mensal de ativações para uma fornecedora e ano específicos.
     * @param {string} fornecedora - A fornecedora selecionada.
     */
    async function updateMonthlyChart(fornecedora) {
        const year = monthlyLineChartYearSelect.value;
        monthlyLineChartStatus.innerHTML = '<i class="fas fa-spinner fa-spin fa-lg"></i><br>Carregando...';
        monthlyLineChartStatus.style.display = 'block';
        monthlyLineChartCanvas.style.display = 'none';

        const apiUrl = `/api/chart/monthly-active-clients?year=${year}&fornecedora=${encodeURIComponent(fornecedora)}`;
        try {
            const chartData = await fetch(apiUrl).then(res => {
                if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
                return res.json();
            });
            
            monthlyLineChartStatus.style.display = 'none';

            if (chartData && Array.isArray(chartData.monthly_counts) && chartData.monthly_counts.length === 12) {
                const labels = ['JAN', 'FEV', 'MAR', 'ABR', 'MAI', 'JUN', 'JUL', 'AGO', 'SET', 'OUT', 'NOV', 'DEZ'];
                
                if (monthlyLineChartInstance) {
                    monthlyLineChartInstance.data.labels = labels;
                    monthlyLineChartInstance.data.datasets[0].data = chartData.monthly_counts;
                    monthlyLineChartInstance.data.datasets[0].label = `Ativações ${year} (${fornecedora})`;
                    monthlyLineChartInstance.update();
                } else {
                    const config = {
                        type: 'line',
                        data: {
                            labels: labels,
                            datasets: [{
                                label: `Ativações ${year} (${fornecedora})`,
                                data: chartData.monthly_counts,
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
                                            return (context.dataset.label || '') + ': ' + formatNumber(context.parsed.y, 0);
                                        }
                                    }
                                }
                            },
                            scales: {
                                x: {
                                    grid: {
                                        display: false
                                    }
                                },
                                y: {
                                    beginAtZero: true,
                                    grid: {
                                        color: '#e0e0e0',
                                        lineWidth: 1
                                    },
                                    ticks: {
                                        color: '#333',
                                        font: {
                                            size: 12
                                        }
                                    }
                                }
                            }
                        }
                    };

                    monthlyLineChartInstance = new Chart(monthlyLineChartCanvas, config);
                }
            } else {
                monthlyLineChartStatus.innerHTML = 'Nenhum dado disponível para o gráfico.';
                monthlyLineChartStatus.style.display = 'block';
                monthlyLineChartCanvas.style.display = 'none';
            }

        } catch (error) {
            console.error("Erro ao carregar dados do gráfico mensal:", error);
            monthlyLineChartStatus.innerHTML = 'Erro ao carregar dados.';
            monthlyLineChartStatus.style.display = 'block';
            monthlyLineChartCanvas.style.display = 'none';
        }
    }

    // Adiciona o listener para o evento de mudança no filtro
    fornecedoraFilter.addEventListener('change', function() {
        const selectedSupplier = this.value;

        if (selectedSupplier) {
            loadScoreFor(selectedSupplier);
        } else {
            gaugeContainer.innerHTML = '';
            gaugeContainer.appendChild(placeholderMessage);
            placeholderMessage.style.display = 'block';
            additionalInfoItems.forEach(item => item.style.display = 'none');
            supplierLogoDisplay.style.display = 'none'; // Esconde o logo
        }
    });

    // Estado inicial: garante que a mensagem de placeholder seja exibida e info cards estejam escondidos
    if (fornecedoraFilter.value === "") {
        gaugeContainer.innerHTML = '';
        gaugeContainer.appendChild(placeholderMessage);
        placeholderMessage.style.display = 'block';
        additionalInfoItems.forEach(item => item.style.display = 'none');
        supplierLogoDisplay.style.display = 'none'; // Esconde o logo no início
    }

    function debounce(func, wait) {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func(...args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    }

    const debouncedUpdateChart = debounce(updateMonthlyChart, 300);

    function formatNumber(value, decimals = 0) {
        const formatter = new Intl.NumberFormat('pt-BR', {
            minimumFractionDigits: decimals,
            maximumFractionDigits: decimals,
            notation: value > 1000000 ? 'compact' : 'standard',
            compactDisplay: 'short'
        });
        return formatter.format(value);
    }

    const helpers = {
        formatNumber,
        debounce,
        handleApiError,
        loadChartLibrary
    };

    async function loadChartLibrary() {
        if (window.Chart) return window.Chart;
        
        return new Promise((resolve, reject) => {
            const script = document.createElement('script');
            script.src = 'https://cdn.jsdelivr.net/npm/chart.js';
            script.onload = () => resolve(window.Chart);
            script.onerror = reject;
            document.head.appendChild(script);
        });
    }

    monthlyLineChartYearSelect.addEventListener('change', 
        debounce(function() {
            const selectedSupplier = fornecedoraFilter.value;
            if (selectedSupplier && selectedSupplier !== 'Consolidado') {
                updateMonthlyChart(selectedSupplier);
            }
        }, 300)
    );
});