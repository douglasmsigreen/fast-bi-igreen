// static/js/tv_dashboard.js - Versão Aprimorada
document.addEventListener('DOMContentLoaded', function() {
    const API_URL = '/api/tv-data';
    let chartInstance = null;

    // Formata um número para ter separadores de milhar
    function formatNumber(num) {
        if (num === null || num === undefined) return 'N/A';
        return num.toLocaleString('pt-BR');
    }

    // Formata números grandes (milhões, milhares)
    function formatLargeNumber(num) {
        if (num === null || num === undefined) return 'N/A';
        if (num >= 1000000) {
            return (num / 1000000).toFixed(1) + 'M';
        } else if (num >= 1000) {
            return (num / 1000).toFixed(1) + 'K';
        }
        return num.toString();
    }

    // Animação de contador para números
    function animateValue(element, start, end, duration, suffix = '', formatter = formatNumber) {
        if (!element || isNaN(end)) return;
        
        let startTimestamp = null;
        const step = (timestamp) => {
            if (!startTimestamp) startTimestamp = timestamp;
            const progress = Math.min((timestamp - startTimestamp) / duration, 1);
            const current = Math.floor(progress * (end - start) + start);
            element.textContent = formatter(current) + suffix;
            
            if (progress < 1) {
                window.requestAnimationFrame(step);
            }
        };
        window.requestAnimationFrame(step);
    }

    // Atualiza o relógio no topo da página
    function updateClock() {
        const now = new Date();
        const formattedTime = now.toLocaleTimeString('pt-BR', {
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit'
        });
        const clockElement = document.getElementById('relogio');
        if (clockElement) {
            clockElement.textContent = formattedTime;
        }
    }

    // Calcula o percentual de variação entre o mês atual e o anterior
    function calculatePercentage(current, previous) {
        if (previous === 0 || previous === null || previous === undefined) {
            return current > 0 ? { text: '+100%', className: 'positive' } : { text: '0%', className: 'neutral' };
        }
        const diff = current - previous;
        const percentage = (diff / previous) * 100;
        const sign = percentage > 0 ? '+' : '';
        const className = percentage >= 0 ? 'positive' : 'negative';
        return { text: `${sign}${percentage.toFixed(1)}%`, className: className };
    }

    // Remove classes de loading com animação
    function removeLoadingState(elementId) {
        const element = document.getElementById(elementId);
        if (element) {
            element.classList.remove('loading-pulse');
            element.style.transition = 'all 0.5s ease';
        }
    }

    // Renderiza o gráfico de ativações mensais aprimorado
    function renderChart(data) {
        if (!data || data.length === 0) return;

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

        // Cores mais vibrantes para o gráfico
        const colors = [
            '#EF4444', // Vermelho
            '#F59E0B', // Âmbar
            '#10B981', // Verde esmeralda
            '#3B82F6', // Azul
            '#8B5CF6', // Violeta
            '#22C55E'  // Verde principal
        ];

        const datasets = last6.map((row, index) => {
            const mesIndex = row.mes - 1;
            const color = colors[index % colors.length];
            const dataValues = [];
            
            for (let i = 1; i <= 31; i++) {
                const diaKey = `dia_${i}`;
                const value = row[diaKey];
                dataValues.push(value > 0 ? value : null); // null em vez de NaN
            }
            
            return {
                label: `${meses[mesIndex]} ${row.ano}`,
                data: dataValues,
                borderColor: color,
                backgroundColor: color + '20', // Adiciona transparência
                tension: 0.4,
                borderWidth: 3,
                pointRadius: 0,
                pointHoverRadius: 6,
                pointHoverBackgroundColor: color,
                pointHoverBorderColor: '#ffffff',
                pointHoverBorderWidth: 2,
                fill: false
            };
        });
        
        const ctx = document.getElementById('ativacoesMensalChart');
        if (!ctx) return;
        
        const context = ctx.getContext('2d');
        
        if (chartInstance) {
            chartInstance.destroy();
        }

        Chart.register(ChartDataLabels);

        chartInstance = new Chart(context, {
            type: 'line',
            data: {
                labels: labels,
                datasets: datasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                layout: {
                    padding: {
                        right: 50,
                        top: 20
                    }
                },
                interaction: {
                    intersect: false,
                    mode: 'index'
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        grid: {
                            color: 'rgba(255, 255, 255, 0.1)',
                            borderColor: 'rgba(255, 255, 255, 0.2)'
                        },
                        ticks: { 
                            color: '#cbd5e1',
                            font: {
                                size: 11,
                                weight: 500
                            }
                        }
                    },
                    x: {
                        title: {
                            display: true,
                            text: 'Dias do Mês',
                            color: '#cbd5e1',
                            font: {
                                size: 12,
                                weight: 600
                            }
                        },
                        grid: {
                            color: 'rgba(255, 255, 255, 0.1)',
                            borderColor: 'rgba(255, 255, 255, 0.2)'
                        },
                        ticks: { 
                            color: '#cbd5e1',
                            font: {
                                size: 11,
                                weight: 500
                            }
                        }
                    }
                },
                plugins: {
                    legend: {
                        display: false
                    },
                    tooltip: {
                        backgroundColor: 'rgba(30, 41, 59, 0.95)',
                        titleColor: '#22C55E',
                        bodyColor: '#ffffff',
                        borderColor: '#22C55E',
                        borderWidth: 1,
                        cornerRadius: 8,
                        displayColors: true,
                        titleFont: {
                            size: 14,
                            weight: 600
                        },
                        bodyFont: {
                            size: 13
                        }
                    },
                    datalabels: {
                        display: true,
                        color: (context) => context.dataset.borderColor,
                        align: 'top',
                        anchor: 'end',
                        offset: 10,
                        font: {
                            size: 12,
                            weight: 'bold'
                        },
                        formatter: (value, context) => {
                            const dataArr = context.dataset.data;
                            let lastValidIndex = dataArr.length - 1;
                            while (lastValidIndex >= 0 && (dataArr[lastValidIndex] === null || dataArr[lastValidIndex] === undefined)) {
                                lastValidIndex--;
                            }
                            if (context.dataIndex === lastValidIndex) {
                                return context.dataset.label.split(' ')[0]; // Só o mês
                            }
                            return '';
                        }
                    }
                }
            }
        });
    }

    // Preenche a tabela com os dados fornecidos
    function populateTable(elementId, data) {
        const tbody = document.getElementById(elementId);
        if (!tbody) return;
        
        tbody.innerHTML = '';
        if (data && data.length > 0) {
            data.forEach((item, index) => {
                const row = document.createElement('tr');
                // Adiciona classe para animação escalonada
                row.style.animationDelay = `${index * 100}ms`;
                row.style.animation = 'fadeInUp 0.5s ease forwards';
                
                const nome = item.fornecedora || item.região;
                const ativacoes = formatNumber(item.quantidade_registros);
                const kwh = formatLargeNumber(item.soma_consumo);
                const validados = item.registros_validados !== undefined ? formatNumber(item.registros_validados) : '';
                const kwhValidados = item.consumo_validados !== undefined ? formatLargeNumber(item.consumo_validados) : '';

                // Se a tabela tiver 5 colunas (com validados), preencher todas, senão manter 3 colunas
                const isFiveCols = tbody.parentElement && tbody.parentElement.querySelector('thead th:nth-child(5)');
                if (isFiveCols) {
                    row.innerHTML = `
                        <td>${nome}</td>
                        <td>${ativacoes}</td>
                        <td>${kwh}</td>
                        <td>${validados}</td>
                        <td>${kwhValidados}</td>
                    `;
                } else {
                    row.innerHTML = `
                        <td>${nome}</td>
                        <td>${ativacoes}</td>
                        <td>${kwh}</td>
                    `;
                }
                tbody.appendChild(row);
            });
        } else {
            // Descobrir número de colunas para o colspan
            const thCount = (tbody.parentElement && tbody.parentElement.querySelectorAll('thead th').length) || 3;
            tbody.innerHTML = `<tr><td colspan="${thCount}" class="loading-text">Nenhum dado encontrado.</td></tr>`;
        }
    }

    // Preenche a tabela de licenciados (com UF)
    function populateLicenciadoTable(elementId, data) {
        const tbody = document.getElementById(elementId);
        if (!tbody) return;
        
        tbody.innerHTML = '';
        if (data && data.length > 0) {
            data.forEach((item, index) => {
                const row = document.createElement('tr');
                row.style.animationDelay = `${index * 100}ms`;
                row.style.animation = 'fadeInUp 0.5s ease forwards';
                
                row.innerHTML = `
                    <td>${item.licenciado}</td>
                    <td>${item.uf}</td>
                    <td>${formatNumber(item.quantidade_registros)}</td>
                    <td>${formatLargeNumber(item.soma_consumo)}</td>
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
                
                // 1. Ativações com animação
                const ativacoes = data.ativacoes;
                if (ativacoes) {
                    const contagemAtual = ativacoes.contagem_mes_atual;
                    const contagemAnterior = ativacoes.contagem_mes_anterior;
                    
                    setTimeout(() => {
                        removeLoadingState('ativacoes-mes');
                        animateValue(
                            document.getElementById('ativacoes-mes'), 
                            0, 
                            contagemAtual, 
                            2000
                        );
                    }, 500);
                    
                    const percentualAtivacoes = calculatePercentage(contagemAtual, contagemAnterior);
                    const percentElement = document.getElementById('ativacoes-percentual');
                    if (percentElement) {
                        setTimeout(() => {
                            removeLoadingState('ativacoes-percentual');
                            percentElement.textContent = percentualAtivacoes.text;
                            percentElement.className = `percent-text ${percentualAtivacoes.className}`;
                        }, 1000);
                    }
                    
                    const mesAnteriorElement = document.getElementById('ativacoes-mes-anterior');
                    if (mesAnteriorElement) {
                        setTimeout(() => {
                            removeLoadingState('ativacoes-mes-anterior');
                            mesAnteriorElement.textContent = formatNumber(contagemAnterior);
                        }, 1500);
                    }
                }

                // 2. kWh com animação
                const kwh = data.kwh;
                if (kwh) {
                    const somaAtual = kwh.soma_consumo_mes_atual;
                    const somaAnterior = kwh.soma_consumo_mes_anterior;
                    
                    setTimeout(() => {
                        removeLoadingState('kwh-mes');
                        animateValue(
                            document.getElementById('kwh-mes'), 
                            0, 
                            somaAtual, 
                            2000, 
                            ' kWh',
                            formatLargeNumber
                        );
                    }, 700);
                    
                    const percentualKwh = calculatePercentage(somaAtual, somaAnterior);
                    const percentElement = document.getElementById('kwh-percentual');
                    if (percentElement) {
                        setTimeout(() => {
                            removeLoadingState('kwh-percentual');
                            percentElement.textContent = percentualKwh.text;
                            percentElement.className = `percent-text ${percentualKwh.className}`;
                        }, 1200);
                    }
                    
                    const mesAnteriorElement = document.getElementById('kwh-mes-anterior');
                    if (mesAnteriorElement) {
                        setTimeout(() => {
                            removeLoadingState('kwh-mes-anterior');
                            mesAnteriorElement.textContent = `${formatLargeNumber(somaAnterior)} kWh`;
                        }, 1700);
                    }
                }

                // 3. Card de Cadastros com animações escalonadas
                const cadastros = data.cadastros;
                if (cadastros) {
                    const cadastrosData = [
                        { id: 'cadastrados', qtd: cadastros.cadastrados_quantidade, consumo: cadastros.cadastrados_soma_consumo },
                        { id: 'a-validar', qtd: cadastros.a_validar_quantidade, consumo: cadastros.a_validar_soma_consumo },
                        { id: 'validados', qtd: cadastros.validados_quantidade, consumo: cadastros.validados_soma_consumo },
                        { id: 'cancelados', qtd: cadastros.cancelados_quantidade, consumo: cadastros.cancelados_soma_consumo }
                    ];

                    cadastrosData.forEach((item, index) => {
                        setTimeout(() => {
                            const qtdElement = document.getElementById(`${item.id}-quantidade`);
                            const consumoElement = document.getElementById(`${item.id}-soma-consumo`);
                            
                            if (qtdElement) {
                                qtdElement.classList.remove('loading-pulse');
                                animateValue(qtdElement, 0, item.qtd, 1500);
                            }
                            
                            if (consumoElement) {
                                consumoElement.classList.remove('loading-pulse');
                                animateValue(consumoElement, 0, item.consumo, 1500, ' kWh', formatLargeNumber);
                            }
                        }, 1000 + (index * 300));
                    });
                }

                // 4. Tabelas com delay
                setTimeout(() => {
                    populateTable('top-regioes-table', data.top_regioes);
                }, 2000);
                
                setTimeout(() => {
                    populateTable('top-fornecedoras-table', data.top_fornecedoras);
                }, 2300);
                
                setTimeout(() => {
                    populateLicenciadoTable('top-licenciados-table', data.top_licenciados);
                }, 2600);

                // 5. Gráfico
                setTimeout(() => {
                    renderChart(data.grafico_ativacoes_mes);
                }, 3000);

            } else {
                console.error('Erro na API:', result.message);
                showErrorState();
            }
        } catch (error) {
            console.error('Erro ao buscar dados:', error);
            showErrorState();
        }
    }

    // Mostra estado de erro
    function showErrorState() {
        const errorElements = [
            'ativacoes-mes',
            'kwh-mes',
            'cadastrados-quantidade',
            'a-validar-quantidade',
            'validados-quantidade', 
            'cancelados-quantidade'
        ];

        errorElements.forEach(id => {
            const element = document.getElementById(id);
            if (element) {
                element.textContent = 'Erro';
                element.classList.remove('loading-pulse');
                element.style.color = '#EF4444';
            }
        });
    }

    // Adiciona estilos para animações das tabelas
    const style = document.createElement('style');
    style.textContent = `
        @keyframes fadeInUp {
            from {
                opacity: 0;
                transform: translateY(10px);
            }
            to {
                opacity: 1;
                transform: translateY(0);
            }
        }
    `;
    document.head.appendChild(style);
    
    // Inicia o relógio
    updateClock();
    setInterval(updateClock, 1000);

    // Inicia a busca de dados e configura a atualização
    fetchDataAndRender();
    setInterval(fetchDataAndRender, 60000); // 60 segundos

    // Adiciona listener para visibilidade da página (pausa quando não visível)
    document.addEventListener('visibilitychange', function() {
        if (document.visibilityState === 'visible') {
            fetchDataAndRender();
        }
    });
});