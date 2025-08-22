// static/js/tv_dashboard.js
document.addEventListener('DOMContentLoaded', function() {
    const API_URL = '/api/tv-data';
    let chartInstance = null; // Para guardar a instância do Chart.js

    // Formata um número para ter separadores de milhar
    function formatNumber(num) {
        if (num === null || num === undefined) return 'N/A';
        return num.toLocaleString('pt-BR');
    }

    // Atualiza o relógio no topo da página
    function updateClock() {
        const now = new Date();
        const formattedTime = now.toLocaleTimeString('pt-BR', {
            hour: '2-digit',
            minute: '2-digit'
        });
        document.getElementById('relogio').textContent = formattedTime;
    }

    // Calcula o percentual de variação entre o mês atual e o anterior
    function calculatePercentage(current, previous) {
        if (previous === 0 || previous === null) {
            return current > 0 ? { text: '+100%', className: 'positive' } : { text: '0%', className: 'neutral' };
        }
        const diff = current - previous;
        const percentage = (diff / previous) * 100;
        const sign = percentage > 0 ? '+' : '';
        const className = percentage >= 0 ? 'positive' : 'negative';
        return { text: `${sign}${percentage.toFixed(2)}%`, className: className };
    }

    // Renderiza o gráfico de ativações mensais (por dia, múltiplos meses)
    function renderChart(data) {
        const meses = [
            'Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun',
            'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'
        ];
        
        const labels = Array.from({ length: 31 }, (_, i) => i + 1);

        // Ordena por ano e mês, do mais antigo para o mais recente
        const sortedData = [...data].sort((a, b) => {
            if (a.ano !== b.ano) return a.ano - b.ano;
            return a.mes - b.mes;
        });

        // Pega apenas os últimos 6 meses
        const last6 = sortedData.slice(-6);

        const datasets = last6.map(row => {
            const mesIndex = row.mes - 1;
            const color = `hsl(${mesIndex * 30}, 70%, 50%)`;
            const dataValues = [];
            for (let i = 1; i <= 31; i++) {
                const diaKey = `dia_${i}`;
                const value = row[diaKey];
                dataValues.push(value > 0 ? value : NaN); // Substitui 0 por NaN
            }
            return {
                label: meses[mesIndex],
                data: dataValues,
                borderColor: color,
                backgroundColor: 'transparent',
                tension: 0.1,
                pointRadius: 0,
                pointHoverRadius: 0,
                pointHitRadius: 0
            };
        });
        
        const ctx = document.getElementById('ativacoesMensalChart').getContext('2d');
        
        if (chartInstance) {
            chartInstance.destroy();
        }

        Chart.register(ChartDataLabels);

        chartInstance = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: datasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                aspectRatio: 2,
                layout: {
                    padding: {
                        right: 40
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: { color: '#e0e0e0' }
                    },
                    x: {
                        title: {
                            display: true,
                            text: 'Dias do Mês',
                            color: '#e0e0e0'
                        },
                        ticks: { color: '#e0e0e0' }
                    }
                },
                plugins: {
                    legend: {
                        display: false
                    },
                    datalabels: {
                        display: true,
                        color: (context) => context.dataset.borderColor,
                        align: 'right',
                        anchor: 'end',
                        font: {
                            size: 16,
                            weight: 'bold'
                        },
                        formatter: (value, context) => {
                            const dataArr = context.dataset.data;
                            let lastValidIndex = dataArr.length - 1;
                            while (lastValidIndex >= 0 && (isNaN(dataArr[lastValidIndex]) || dataArr[lastValidIndex] === null)) {
                                lastValidIndex--;
                            }
                            if (context.dataIndex === lastValidIndex) {
                                return context.dataset.label;
                            } else {
                                return '';
                            }
                        }
                    }
                }
            }
        });
    }

    // Preenche a tabela com os dados fornecidos
    function populateTable(elementId, data) {
        const tbody = document.getElementById(elementId);
        tbody.innerHTML = '';
        if (data && data.length > 0) {
            data.forEach(item => {
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td>${item.fornecedora || item.região}</td>
                    <td>${formatNumber(item.quantidade_registros)}</td>
                    <td>${formatNumber(item.soma_consumo)}</td>
                `;
                tbody.appendChild(row);
            });
        } else {
            tbody.innerHTML = `<tr><td colspan="3" class="loading-text">Nenhum dado encontrado.</td></tr>`;
        }
    }

    // NOVO: Função para preencher a tabela de licenciados (com UF)
    function populateLicenciadoTable(elementId, data) {
        const tbody = document.getElementById(elementId);
        tbody.innerHTML = ''; // Limpa o conteúdo
        if (data && data.length > 0) {
            data.forEach(item => {
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td>${item.licenciado}</td>
                    <td>${item.uf}</td>
                    <td>${formatNumber(item.quantidade_registros)}</td>
                    <td>${formatNumber(item.soma_consumo)}</td>
                `;
                tbody.appendChild(row);
            });
        } else {
            tbody.innerHTML = `<tr><td colspan="4" class="loading-text">Nenhum dado encontrado.</td></tr>`;
        }
    }

    // Função principal para buscar e atualizar todos os dados
    async function fetchDataAndRender() {
        try {
            const response = await fetch(API_URL);
            const result = await response.json();
            
            if (result.status === 'success') {
                const data = result.data;
                
                // 1. Ativações
                const ativacoes = data.ativacoes;
                if (ativacoes) {
                    const contagemAtual = ativacoes.contagem_mes_atual;
                    const contagemAnterior = ativacoes.contagem_mes_anterior;
                    document.getElementById('ativacoes-mes').textContent = formatNumber(contagemAtual);
                    const percentualAtivacoes = calculatePercentage(contagemAtual, contagemAnterior);
                    const percentElement = document.getElementById('ativacoes-percentual');
                    percentElement.textContent = percentualAtivacoes.text;
                    percentElement.className = `percent-text ${percentualAtivacoes.className}`;
                }

                // 2. kWh
                const kwh = data.kwh;
                if (kwh) {
                    const somaAtual = kwh.soma_consumo_mes_atual;
                    const somaAnterior = kwh.soma_consumo_mes_anterior;
                    document.getElementById('kwh-mes').textContent = `${formatNumber(somaAtual)} kWh`;
                    const percentualKwh = calculatePercentage(somaAtual, somaAnterior);
                    const percentElement = document.getElementById('kwh-percentual');
                    percentElement.textContent = percentualKwh.text;
                    percentElement.className = `percent-text ${percentualKwh.className}`;
                }

                // 3. Top 5 Regiões
                populateTable('top-regioes-table', data.top_regioes);
                
                // 4. Top 5 Fornecedoras
                populateTable('top-fornecedoras-table', data.top_fornecedoras);
                
                // 5. Gráfico
                renderChart(data.grafico_ativacoes_mes);
                
                // --- NOVO: Top 5 Licenciados ---
                populateLicenciadoTable('top-licenciados-table', data.top_licenciados);
                // --- FIM NOVO ---

            } else {
                console.error('Erro na API:', result.message);
            }
        } catch (error) {
            console.error('Erro ao buscar dados:', error);
            // Mostrar mensagem de erro na interface
            document.getElementById('ativacoes-mes').textContent = 'Erro';
            document.getElementById('kwh-mes').textContent = 'Erro';
        }
    }
    
    // Inicia o relógio
    updateClock();
    setInterval(updateClock, 1000);

    // Inicia a busca de dados e configura a atualização a cada 60 segundos
    fetchDataAndRender();
    setInterval(fetchDataAndRender, 60000); // 60 segundos
});