document.addEventListener('DOMContentLoaded', function() {
    const gaugeContainer = document.getElementById('gauge-container');
    const fornecedoraFilter = document.getElementById('fornecedora-filter');
    const placeholderMessage = document.getElementById('placeholder-message');

    /**
     * Cria um único gráfico de velocímetro (gauge) usando ApexCharts.
     * @param {string} containerId - O ID do div onde o gráfico será renderizado.
     * @param {string} title - O nome da fornecedora.
     * @param {number} value - O score (0 a 100).
     */
    function createGauge(containerId, title, value) {
        const options = {
            // A série de dados, nosso score arredondado para inteiro
            series: [Math.round(value)], 
            chart: {
                height: 350,
                type: 'radialBar', // Tipo de gráfico radial (velocímetro)
                offsetY: -10
            },
            plotOptions: {
                radialBar: {
                    startAngle: -135, // Início do arco
                    endAngle: 135,    // Fim do arco (criando um semi-círculo)
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
                            offsetY: -10,
                            show: true,
                            color: '#888',
                            fontSize: '17px'
                        },
                        value: {
                            formatter: function(val) {
                                // Exibe o valor original com uma casa decimal
                                return value.toFixed(1) + "%";
                            },
                            color: '#111',
                            fontSize: '44px',
                            show: true,
                        }
                    }
                }
            },
            // Preenchimento com gradiente
            fill: {
                type: 'gradient',
                gradient: {
                    shade: 'dark',
                    type: 'horizontal',
                    shadeIntensity: 0.5,
                    gradientToColors: ['#22c55e'], // Cor final do gradiente
                    inverseColors: true,
                    opacityFrom: 1,
                    opacityTo: 1,
                    stops: [0, 100]
                }
            },
            // As cores que o gradiente vai usar
            colors: ["#86efac"], // Cor inicial do gradiente
            // O nome da fornecedora
            labels: [title],
            stroke: {
                lineCap: 'round'
            },
        };

        // Renderiza o gráfico
        const chart = new ApexCharts(document.getElementById(containerId), options);
        chart.render();
    }

    /**
     * Busca o score para uma fornecedora específica e renderiza o gráfico.
     * @param {string} fornecedora - O nome da fornecedora selecionada.
     */
    async function loadScoreFor(fornecedora) {
        // Limpa o container e mostra uma mensagem de carregamento
        gaugeContainer.innerHTML = `
            <div class="text-center">
                <i class="fas fa-spinner fa-spin fa-2x"></i>
                <p class="mt-2">Carregando score para <strong>${fornecedora}</strong>...</p>
            </div>`;

        try {
            // Constrói a URL com o parâmetro de busca
            const response = await fetch(`/api/scores/green-score?fornecedora=${encodeURIComponent(fornecedora)}`);
            if (!response.ok) {
                throw new Error(`Erro HTTP ${response.status}`);
            }
            const data = await response.json();

            // Limpa o container novamente antes de desenhar o gráfico
            gaugeContainer.innerHTML = ''; 

            if (data.length > 0) {
                const scoreInfo = data[0];
                const chartDiv = document.createElement('div');
                chartDiv.id = 'gauge-chart'; // ID fixo, pois só haverá um
                gaugeContainer.appendChild(chartDiv);
                createGauge('gauge-chart', scoreInfo.fornecedora, scoreInfo.score);
            } else {
                gaugeContainer.innerHTML = `<div class="alert alert-warning">Nenhum score encontrado para ${fornecedora}.</div>`;
            }

        } catch (error) {
            console.error("Erro ao carregar score:", error);
            gaugeContainer.innerHTML = `<div class="alert alert-danger">Falha ao carregar os dados. Tente novamente mais tarde.</div>`;
        }
    }

    // Adiciona o listener para o evento de mudança no filtro
    fornecedoraFilter.addEventListener('change', function() {
        const selectedSupplier = this.value;

        if (selectedSupplier) {
            // Se uma fornecedora for selecionada, busca os dados dela
            loadScoreFor(selectedSupplier);
        } else {
            // Se a opção "Selecione" for escolhida, volta ao estado inicial
            gaugeContainer.innerHTML = '';
            gaugeContainer.appendChild(placeholderMessage);
        }
    });
});