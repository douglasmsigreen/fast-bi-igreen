document.addEventListener('DOMContentLoaded', function() {
    const gaugeContainer = document.getElementById('gauge-container');
    const loadingPlaceholder = document.getElementById('loading-placeholder');
    const fornecedoraFilter = document.getElementById('fornecedora-filter');

    /**
     * Cria um único gráfico de velocímetro (gauge) para uma fornecedora.
     * @param {string} containerId - O ID do div onde o gráfico será renderizado.
     * @param {string} title - O nome da fornecedora.
     * @param {number} value - O score (0 a 100).
     */
    function createGauge(containerId, title, value) {
        const data = [{
            domain: { x: [0, 1], y: [0, 1] },
            value: value,
            title: { text: `<b>${title}</b>`, font: { size: 16 } },
            type: "indicator",
            mode: "gauge+number",
            gauge: {
                axis: { range: [0, 100], tickwidth: 1, tickcolor: "darkblue" },
                bar: { color: "#00B034" }, // Cor da barra do score
                bgcolor: "white",
                borderwidth: 2,
                bordercolor: "#ccc",
                steps: [
                    { range: [0, 50], color: "#fee2e2" },  // Vermelho
                    { range: [50, 80], color: "#fef3c7" }, // Amarelo
                    { range: [80, 100], color: "#dcfce7" }  // Verde
                ],
                threshold: {
                    line: { color: "#b91c1c", width: 4 },
                    thickness: 0.75,
                    value: 99.9 // Para visualização de meta alta
                }
            }
        }];

        const layout = {
            width: 320,
            height: 250,
            margin: { t: 25, r: 25, l: 25, b: 25 },
            paper_bgcolor: 'rgba(0,0,0,0)',
            plot_bgcolor: 'rgba(0,0,0,0)'
        };

        Plotly.newPlot(containerId, data, layout, {responsive: true, displaylogo: false});
    }

    /**
     * Busca os dados da API e renderiza todos os gráficos.
     */
    async function loadAllScores() {
        try {
            const response = await fetch("/api/scores/green-score");
            if (!response.ok) {
                throw new Error(`Erro HTTP ${response.status}`);
            }
            const scores = await response.json();

            // Limpa o container e remove o placeholder
            gaugeContainer.innerHTML = '';

            if (scores.length === 0) {
                gaugeContainer.innerHTML = `<p class="text-center w-100">Nenhum score encontrado.</p>`;
                return;
            }

            scores.forEach((item, index) => {
                // Cria um card para cada gráfico para manter o estilo do dashboard
                const cardDiv = document.createElement('div');
                cardDiv.className = 'card';
                cardDiv.dataset.fornecedora = item.fornecedora; // Para o filtro

                const chartId = `gauge-${index}`;
                const chartDiv = document.createElement('div');
                chartDiv.id = chartId;
                
                cardDiv.appendChild(chartDiv);
                gaugeContainer.appendChild(cardDiv);

                createGauge(chartId, item.fornecedora, item.score);
            });

        } catch (error) {
            console.error("Erro ao carregar scores:", error);
            gaugeContainer.innerHTML = `<div class="alert alert-danger w-100">Falha ao carregar os dados do Green Score. Tente novamente mais tarde.</div>`;
        }
    }

    /**
     * Filtra os cards de velocímetro visíveis com base na seleção do dropdown.
     */
    function filterGauges() {
        const selectedValue = fornecedoraFilter.value;
        const allGauges = gaugeContainer.querySelectorAll('.card[data-fornecedora]');

        allGauges.forEach(gauge => {
            if (selectedValue === 'all' || gauge.dataset.fornecedora === selectedValue) {
                gauge.style.display = 'block';
            } else {
                gauge.style.display = 'none';
            }
        });
    }

    // Adiciona o listener para o filtro
    fornecedoraFilter.addEventListener('change', filterGauges);

    // Carrega os dados iniciais
    loadAllScores();
});