document.addEventListener('DOMContentLoaded', function() {
    const gaugeContainer = document.getElementById('gauge-container');
    const fornecedoraFilter = document.getElementById('fornecedora-filter');
    const placeholderMessage = document.getElementById('placeholder-message');
    const additionalInfoContainer = document.getElementById('additional-info-container'); // Novo!
    const infoKwhVendidos = document.getElementById('info-kwh-vendidos'); // Novo!
    const infoClientesRegistrados = document.getElementById('info-clientes-registrados'); // Novo!
    const infoClientesAtivados = document.getElementById('info-clientes-ativados'); // Novo!

    /**
     * Determina as cores do gradiente baseado no valor do score.
     * @param {number} value - O score (0 a 100).
     * @returns {string[]} Um array com as cores de início e fim do gradiente.
     */
    function getScoreColors(value) {
        // Cores para o gradiente baseado no Green Score
        if (value >= 80) {
            // Excelente: Verde claro para Verde escuro
            return ['#86efac', '#16a34a']; // tailwind green-300 to green-700
        } else if (value >= 60) {
            // Bom: Amarelo claro para Laranja
            return ['#fde047', '#f97316']; // tailwind yellow-300 to orange-500
        } else if (value >= 40) {
            // Regular: Laranja claro para Vermelho
            return ['#fdba74', '#dc2626']; // tailwind orange-300 to red-600
        } else {
            // Crítico: Vermelho claro para Vermelho escuro
            return ['#fca5a5', '#b91c1c']; // tailwind red-300 to red-800
        }
    }

    /**
     * Cria um único gráfico de velocímetro (gauge) usando ApexCharts.
     * @param {string} containerId - O ID do div onde o gráfico será renderizado.
     * @param {string} title - O nome da fornecedora (rótulo principal).
     * @param {number} value - O score (0 a 100).
     */
    function createGauge(containerId, title, value) {
        const colors = getScoreColors(value); // Obtém as cores baseadas no valor

        const options = {
            series: [Math.round(value)], // O valor do score para a série
            chart: {
                height: 300, // Altura ajustada para o grid
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
        // Limpa o container e mostra uma mensagem de carregamento
        gaugeContainer.innerHTML = `
            <div class="message-center">
                <i class="fas fa-spinner fa-spin fa-2x text-primary"></i>
                <p class="mt-2 text-muted">Carregando score${fornecedora === 'Consolidado' ? 's' : ''} para **${fornecedora}**...</p>
            </div>`;
        
        // Esconde informações adicionais por padrão
        additionalInfoContainer.style.display = 'none';

        try {
            let apiUrl = '';
            if (fornecedora === 'Consolidado') {
                apiUrl = `/api/scores/green-score?fornecedora=Consolidado`; // Chama a API sem filtro específico
            } else {
                apiUrl = `/api/scores/green-score?fornecedora=${encodeURIComponent(fornecedora)}`;
            }
            
            const response = await fetch(apiUrl);
            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || `Erro HTTP ${response.status}`);
            }
            const data = await response.json(); // Data será uma lista de objetos: [{'fornecedora': '...', 'score': ...}, ...]

            gaugeContainer.innerHTML = ''; // Limpa antes de desenhar os gráficos

            if (data.length > 0) {
                if (fornecedora === 'Consolidado') {
                    // Cria múltiplos velocímetros em um grid
                    data.forEach((scoreInfo, index) => {
                        const cardDiv = document.createElement('div');
                        cardDiv.className = 'gauge-card'; // Adiciona classe para estilização individual
                        
                        const chartDiv = document.createElement('div');
                        chartDiv.id = `gauge-chart-${index}`; // ID único para cada gráfico
                        chartDiv.className = 'gauge-chart-container'; // Adiciona classe para o contêiner do gráfico
                        cardDiv.appendChild(chartDiv);
                        gaugeContainer.appendChild(cardDiv);

                        // Cria o gráfico
                        createGauge(chartDiv.id, scoreInfo.fornecedora, scoreInfo.score);
                    });
                } else {
                    // Comportamento para fornecedora específica (um único gauge e informações adicionais)
                    const scoreInfo = data[0]; // Pega o primeiro (e único) score retornado
                    const cardDiv = document.createElement('div');
                    cardDiv.className = 'gauge-card message-center'; // Centraliza o card único
                    
                    const chartDiv = document.createElement('div');
                    chartDiv.id = 'gauge-chart-single'; // ID fixo para o único
                    chartDiv.className = 'gauge-chart-container';
                    cardDiv.appendChild(chartDiv);
                    gaugeContainer.appendChild(cardDiv);

                    createGauge('gauge-chart-single', scoreInfo.fornecedora, scoreInfo.score);

                    // ***** NOVO: ATUALIZA OS CARDS DE INFORMAÇÃO ADICIONAIS *****
                    additionalInfoContainer.style.display = 'grid'; // Mostra o contêiner
                    await updateAdditionalInfoCards(fornecedora); // Chama a função para buscar e atualizar os dados
                    // ************************************************************
                }
            } else {
                gaugeContainer.innerHTML = `<div class="message-center alert alert-warning">Nenhum score encontrado para ${fornecedora}.</div>`;
            }

        } catch (error) {
            console.error("Erro ao carregar score:", error);
            gaugeContainer.innerHTML = `<div class="message-center alert alert-danger">Falha ao carregar os dados. Tente novamente mais tarde.<br><small>${error.message}</small></div>`;
            additionalInfoContainer.style.display = 'none'; // Esconde em caso de erro
        }
    }

    /**
     * Busca e atualiza os cards de informação adicionais (kWh, clientes).
     * @param {string} fornecedora - A fornecedora selecionada.
     */
    async function updateAdditionalInfoCards(fornecedora) {
        // Mês atual para as APIs de KPI, como no dashboard
        const currentDate = new Date();
        const currentMonth = currentDate.getFullYear() + '-' + String(currentDate.getMonth() + 1).padStart(2, '0');

        // Mostra spinners
        infoKwhVendidos.innerHTML = '<i class="fas fa-spinner fa-spin fa-sm"></i>';
        infoClientesRegistrados.innerHTML = '<i class="fas fa-spinner fa-spin fa-sm"></i>';
        infoClientesAtivados.innerHTML = '<i class="fas fa-spinner fa-spin fa-sm"></i>';

        try {
            // Requisições para os KPIs (modificadas para incluir filtro de fornecedora)
            const [kwhData, registeredClientsData, activeClientsData] = await Promise.all([
                fetch(`/api/kpi/total-kwh?month=${currentMonth}&fornecedora=${encodeURIComponent(fornecedora)}`).then(res => res.json()),
                fetch(`/api/kpi/clientes-registrados?month=${currentMonth}&fornecedora=${encodeURIComponent(fornecedora)}`).then(res => res.json()),
                fetch(`/api/kpi/clientes-ativos?month=${currentMonth}&fornecedora=${encodeURIComponent(fornecedora)}`).then(res => res.json())
            ]);

            // Atualiza os elementos HTML
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


    // Adiciona o listener para o evento de mudança no filtro
    fornecedoraFilter.addEventListener('change', function() {
        const selectedSupplier = this.value;

        if (selectedSupplier) {
            // Se uma fornecedora (ou Consolidado) for selecionada, busca os dados
            loadScoreFor(selectedSupplier);
        } else {
            // Se a opção "-- Selecione..." for escolhida, volta ao estado inicial
            gaugeContainer.innerHTML = '';
            gaugeContainer.appendChild(placeholderMessage);
            placeholderMessage.style.display = 'block';
            additionalInfoContainer.style.display = 'none'; // Esconde os cards de informação
        }
    });

    // Estado inicial: garante que a mensagem de placeholder seja exibida e info cards estejam escondidos
    if (fornecedoraFilter.value === "") {
        gaugeContainer.innerHTML = '';
        gaugeContainer.appendChild(placeholderMessage);
        placeholderMessage.style.display = 'block';
        additionalInfoContainer.style.display = 'none';
    }
});