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

    // Renderiza o gráfico de ativações mensais
    function renderChart(data) {
        const meses = [
            'Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun',
            'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'
        ];
        
        // CORRIGIDO: Mapeia dados de lista de listas para rótulos e valores
        const labels = data.map(item => meses[item.mes - 1] || `${item.mes}/${item.ano}` ); // Fallback para ano
        const values = data.map(item => item.quantidade);
        
        const ctx = document.getElementById('ativacoesMensalChart').getContext('2d');
        
        if (chartInstance) {
            chartInstance.destroy(); // Destrói a instância anterior para evitar duplicidade
        }

        chartInstance = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    label: 'Ativações',
                    data: values,
                    borderColor: 'rgb(75, 192, 192)',
                    backgroundColor: 'rgba(75, 192, 192, 0.2)',
                    fill: true,
                    tension: 0.1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: true, // Alterado para true
                aspectRatio: 2, // Define a proporção largura/altura (2 significa que a largura será 2x a altura)
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: { color: '#e0e0e0' }
                    },
                    x: {
                        ticks: { color: '#e0e0e0' }
                    }
                },
                plugins: {
                    legend: {
                        display: false
                    }
                }
            }
        });
    }

    // Preenche a tabela com os dados fornecidos
    function populateTable(elementId, data) {
        const tbody = document.getElementById(elementId);
        tbody.innerHTML = ''; // Limpa o conteúdo
        if (data && data.length > 0) {
            data.forEach(item => {
                const row = document.createElement('tr');
                // Acessa os dados por chave (nome do campo)
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