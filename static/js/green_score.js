// static/js/green_score.js - VERSÃO SIMPLIFICADA PARA EXIBIR APENAS O SCORE

document.addEventListener('DOMContentLoaded', function() {
    const mainContentWrapper = document.getElementById('green-score-content-wrapper'); // NOVO: Wrapper principal
    const gaugeAndKpisRow = document.getElementById('gauge-and-kpis-row'); // NOVO: Container do gauge e kpis
    const gaugeContainer = document.getElementById('gauge-container');
    const fornecedoraFilter = document.getElementById('fornecedora-filter');
    const placeholderMessage = document.getElementById('placeholder-message');
    const kpiSummaryContainer = document.getElementById('kpi-summary-container');
    
    // Elementos dos KPIs
    const kpiTotalKwh = document.getElementById('kpi-total-kwh-green-score');
    const kpiClientesRegistrados = document.getElementById('kpi-clientes-registrados-green-score');
    const kpiClientesAtivos = document.getElementById('kpi-clientes-ativos-green-score');

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
        
        kpiTotalKwh.innerHTML = '<i class="fas fa-spinner fa-spin fa-sm"></i>';
        kpiClientesRegistrados.innerHTML = '<i class="fas fa-spinner fa-spin fa-sm"></i>';
        kpiClientesAtivos.innerHTML = '<i class="fas fa-spinner fa-spin fa-sm"></i>';

        try {
            const kwhResponse = await fetch(`/api/kpi/total-kwh?month=${currentMonth}&fornecedora=${encodeURIComponent(fornecedora)}`);
            const kwhData = await kwhResponse.json();
            if (kwhResponse.ok) {
                kpiTotalKwh.textContent = formatNumber(kwhData.total_kwh, 0);
            } else {
                kpiTotalKwh.innerHTML = '<span style="color: red; font-size: 0.7em;">Erro!</span>';
                console.error('Erro ao buscar kWh:', kwhData.error);
            }

            const registradosResponse = await fetch(`/api/kpi/clientes-registrados?month=${currentMonth}&fornecedora=${encodeURIComponent(fornecedora)}`);
            const registradosData = await registradosResponse.json();
            if (registradosResponse.ok) {
                kpiClientesRegistrados.textContent = formatNumber(registradosData.clientes_registrados_count, 0);
            } else {
                kpiClientesRegistrados.innerHTML = '<span style="color: red; font-size: 0.7em;">Erro!</span>';
                console.error('Erro ao buscar clientes registrados:', registradosData.error);
            }

            const ativosResponse = await fetch(`/api/kpi/clientes-ativos?month=${currentMonth}&fornecedora=${encodeURIComponent(fornecedora)}`);
            const ativosData = await ativosResponse.json();
            if (ativosResponse.ok) {
                kpiClientesAtivos.textContent = formatNumber(ativosData.clientes_ativos_count, 0);
            } else {
                kpiClientesAtivos.innerHTML = '<span style="color: red; font-size: 0.7em;">Erro!</span>';
                console.error('Erro ao buscar clientes ativados:', ativosData.error);
            }
            
            // O kpiSummaryContainer.style.display será controlado por loadScoreFor
            
        } catch (error) {
            console.error("Erro geral ao buscar KPIs da Green Score:", error);
            kpiTotalKwh.innerHTML = '<span style="color: red; font-size: 0.7em;">Erro!</span>';
            kpiClientesRegistrados.innerHTML = '<span style="color: red; font-size: 0.7em;">Erro!</span>';
            kpiClientesAtivos.innerHTML = '<span style="color: red; font-size: 0.7em;">Erro!</span>';
            // Oculta os KPIs se houver erro grave, ou se já não estiver visível.
            kpiSummaryContainer.style.display = 'none'; 
        }
    }


    async function loadScoreFor(fornecedora) {
        // Oculta o placeholder e mostra o wrapper principal de conteúdo
        placeholderMessage.style.display = 'none';
        mainContentWrapper.style.display = 'flex'; // Exibe o wrapper principal

        // Oculta o logo e os KPIs no início do carregamento
        if (supplierLogoDisplay) supplierLogoDisplay.style.display = 'none';
        if (supplierLogoImg) supplierLogoImg.src = '';
        if (kpiSummaryContainer) kpiSummaryContainer.style.display = 'none'; // Sempre esconde os KPIs inicialmente
        
        // Exibe spinner no contêiner do gauge
        gaugeContainer.innerHTML = `<div class="message-center"><i class="fas fa-spinner fa-spin fa-2x text-primary"></i><p class="mt-2 text-muted">Carregando score${fornecedora === 'Consolidado' ? 's' : ''} para **${fornecedora}**...</p></div>`;
        
        // Remove classes de layout antigas do gaugeContainer
        gaugeContainer.classList.remove('gauge-single-container', 'gauge-grid-consolidated');

        try {
            let apiUrl = '';
            if (fornecedora === 'Consolidado') {
                apiUrl = `/api/scores/green-score?fornecedora=Consolidado`;
                // Para consolidado, o gaugeContainer se torna um grid de múltiplos gauges
                gaugeContainer.classList.add('gauge-grid-consolidated');
                gaugeContainer.style.display = 'grid'; // Garante display grid
                gaugeAndKpisRow.style.display = 'block'; // Ou 'flex-column' se preferir, mas 'block' é mais simples aqui

            } else {
                apiUrl = `/api/scores/green-score?fornecedora=${encodeURIComponent(fornecedora)}`;
                // Para fornecedora específica, o gaugeContainer é um único gauge
                gaugeContainer.classList.add('gauge-single-container');
                gaugeContainer.style.display = 'flex'; // Garante display flex para centralizar
                gaugeAndKpisRow.style.display = 'flex'; // Exibe a linha de gauge + kpis
                updateKPIs(fornecedora); // Chama para carregar KPIs
            }
            
            const response = await fetch(apiUrl);
            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || `Erro HTTP ${response.status}`);
            }
            const data = await response.json();

            gaugeContainer.innerHTML = ''; // Limpa o spinner de carregamento

            if (data.length > 0) {
                if (fornecedora === 'Consolidado') {
                    // Exibe logo e KPIs apenas para fornecedora específica
                    if (supplierLogoDisplay) supplierLogoDisplay.style.display = 'none';
                    if (kpiSummaryContainer) kpiSummaryContainer.style.display = 'none';

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
                } else {
                    // Exibe logo e KPIs para fornecedora específica
                    const logoPath = supplierLogos[fornecedora.toUpperCase()];
                    if (logoPath) {
                        supplierLogoImg.src = logoPath;
                        supplierLogoDisplay.style.display = 'block';
                    } else {
                        supplierLogoDisplay.style.display = 'none';
                    }
                    if (kpiSummaryContainer) kpiSummaryContainer.style.display = 'grid'; // Exibe KPIs

                    const scoreInfo = data[0];
                    const chartDiv = document.createElement('div'); // Cria o div do chart
                    chartDiv.id = 'gauge-chart-single';
                    chartDiv.className = 'gauge-chart-container';
                    gaugeContainer.appendChild(chartDiv); // Adiciona ao gaugeContainer
                    createGauge('gauge-chart-single', scoreInfo.fornecedora, scoreInfo.score);
                }
            } else {
                gaugeContainer.innerHTML = `<div class="message-center alert alert-warning">Nenhum score encontrado para ${fornecedora}.</div>`;
                supplierLogoDisplay.style.display = 'none';
                kpiSummaryContainer.style.display = 'none'; // Esconde KPIs
            }

        } catch (error) {
            console.error("Erro ao carregar score:", error);
            gaugeContainer.innerHTML = `<div class="message-center alert alert-danger">Falha ao carregar os dados. Tente novamente mais tarde.<br><small>${error.message}</small></div>`;
            supplierLogoDisplay.style.display = 'none';
            kpiSummaryContainer.style.display = 'none'; // Esconde KPIs
        }
    }

    fornecedoraFilter.addEventListener('change', function() {
        const selectedSupplier = this.value;

        if (selectedSupplier) {
            loadScoreFor(selectedSupplier);
        } else {
            // Se o valor for vazio, exibe o placeholder e esconde os outros elementos
            mainContentWrapper.style.display = 'none'; // Oculta todo o conteúdo
            placeholderMessage.style.display = 'block'; // Mostra apenas o placeholder
            // Garante que os KPIs também sejam escondidos explicitamente
            if (kpiSummaryContainer) kpiSummaryContainer.style.display = 'none';
            // E o logo
            if (supplierLogoDisplay) supplierLogoDisplay.style.display = 'none';
        }
    });

    // Estado inicial (ao carregar a página)
    if (fornecedoraFilter.value) {
        loadScoreFor(fornecedoraFilter.value);
    } else {
        mainContentWrapper.style.display = 'none';
        placeholderMessage.style.display = 'block';
        if (kpiSummaryContainer) kpiSummaryContainer.style.display = 'none';
        if (supplierLogoDisplay) supplierLogoDisplay.style.display = 'none';
    }
});