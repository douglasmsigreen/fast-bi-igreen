// static/js/green_score.js - VERSÃO SIMPLIFICADA PARA EXIBIR APENAS O SCORE

document.addEventListener('DOMContentLoaded', function() {
    const gaugeContainer = document.getElementById('gauge-container');
    const fornecedoraFilter = document.getElementById('fornecedora-filter');
    const placeholderMessage = document.getElementById('placeholder-message');
    
    // Mapeamento de fornecedoras para caminhos de logo (mantido, mas opcional se não for exibir o logo)
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

    // Elementos do logo - Certifique-se de que existem no HTML se for usá-los
    const supplierLogoDisplay = document.getElementById('supplier-logo-display');
    const supplierLogoImg = document.getElementById('supplier-logo-img');

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
        // Esconde o placeholderMessage e mostra um spinner no gaugeContainer
        placeholderMessage.style.display = 'none'; // Esconde a mensagem de placeholder
        gaugeContainer.style.display = 'grid'; // Garante que o grid do gaugeContainer esteja visível
        gaugeContainer.innerHTML = `<div class="message-center"><i class="fas fa-spinner fa-spin fa-2x text-primary"></i><p class="mt-2 text-muted">Carregando score${fornecedora === 'Consolidado' ? 's' : ''} para **${fornecedora}**...</p></div>`;
        
        // Esconde o logo ao carregar novos dados, se os elementos existirem
        if (supplierLogoDisplay) supplierLogoDisplay.style.display = 'none';
        if (supplierLogoImg) supplierLogoImg.src = '';
        
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

            gaugeContainer.innerHTML = ''; // Limpa o spinner de carregamento após a requisição

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
                }
            } else {
                gaugeContainer.innerHTML = `<div class="message-center alert alert-warning">Nenhum score encontrado para ${fornecedora}.</div>`;
                supplierLogoDisplay.style.display = 'none';
            }

        } catch (error) {
            console.error("Erro ao carregar score:", error);
            gaugeContainer.innerHTML = `<div class="message-center alert alert-danger">Falha ao carregar os dados. Tente novamente mais tarde.<br><small>${error.message}</small></div>`;
            supplierLogoDisplay.style.display = 'none';
        }
    }

    // Adiciona o listener para o evento de mudança no filtro
    fornecedoraFilter.addEventListener('change', function() {
        const selectedSupplier = this.value;

        if (selectedSupplier) {
            loadScoreFor(selectedSupplier);
        } else {
            // Se o valor for vazio, exibe o placeholder e esconde os outros elementos
            gaugeContainer.innerHTML = ''; // Limpa qualquer conteúdo de velocímetro ou spinner
            gaugeContainer.style.display = 'none'; // Esconde o grid de velocímetros
            placeholderMessage.style.display = 'block'; // Mostra o placeholder
            // Destrói a instância do gráfico de linha se houver
            if (monthlyLineChartInstance) {
                monthlyLineChartInstance.destroy();
                monthlyLineChartInstance = null;
            }
        }
    });

    // Estado inicial (ao carregar a página)
    if (fornecedoraFilter.value === "") {
        gaugeContainer.style.display = 'none'; // Esconde o grid de velocímetros
        placeholderMessage.style.display = 'block'; // Mostra o placeholder
    } else {
        // Se já houver uma fornecedora selecionada na URL (ex: recarga de página), carrega os dados
        loadScoreFor(fornecedoraFilter.value);
    }
});