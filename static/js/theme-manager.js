/**
 * Theme Manager para Fast BI iGreen
 * Gerencia alternância entre modo claro e escuro
 */
class ThemeManager {
    constructor() {
        // Remover chamada direta ao init, pois agora é async
        // this.init();
    }

    // MODIFICADO: init agora é async e tenta carregar do servidor primeiro
    async init() {
        // Tenta carregar tema do servidor primeiro
        const serverTheme = await this.loadThemeFromServer();
        if (serverTheme && ['light', 'dark'].includes(serverTheme)) {
            this.currentTheme = serverTheme;
        } else {
            // Fallback para localStorage ou padrão
            this.currentTheme = this.getStoredTheme() || 'light';
        }
        
        // Aplica o tema determinado
        this.applyTheme(this.currentTheme);
        
        // Configura o botão toggle
        this.setupToggleButton();
        
        // Configura listener para mudanças de tema em outros componentes
        this.setupEventListeners();
        
        console.log(`Theme Manager inicializado com tema: ${this.currentTheme}`);
    }

    getStoredTheme() {
        return localStorage.getItem('igreen-theme');
    }

    storeTheme(theme) {
        localStorage.setItem('igreen-theme', theme);
    }

    // MODIFICADO: applyTheme agora sincroniza com o servidor
    applyTheme(theme) {
        const body = document.body;
        const html = document.documentElement;
        
        if (theme === 'dark') {
            html.setAttribute('data-theme', 'dark');
            body.setAttribute('data-theme', 'dark');
        } else {
            html.removeAttribute('data-theme');
            body.removeAttribute('data-theme');
        }
        
        this.currentTheme = theme;
        this.storeTheme(theme);
        
        // Sincroniza com servidor (não bloqueia)
        this.syncThemeWithServer(theme);
        
        // Atualiza o ícone do botão
        this.updateToggleIcon();
        
        // Atualiza gráficos se existirem
        this.updateChartsTheme();
        
        // Dispara evento personalizado
        window.dispatchEvent(new CustomEvent('themeChanged', { 
            detail: { theme: theme }
        }));
    }

    toggleTheme() {
        const newTheme = this.currentTheme === 'light' ? 'dark' : 'light';
        this.applyTheme(newTheme);
        
        // Feedback visual
        this.showThemeChangeNotification(newTheme);
    }

    setupToggleButton() {
        const toggleButton = document.getElementById('theme-toggle');
        if (toggleButton) {
            toggleButton.addEventListener('click', (e) => {
                e.preventDefault();
                this.toggleTheme();
            });
            
            // Adiciona animação de hover
            toggleButton.addEventListener('mouseenter', () => {
                const icon = toggleButton.querySelector('i');
                if (icon) icon.style.transform = 'scale(1.1)';
            });
            
            toggleButton.addEventListener('mouseleave', () => {
                const icon = toggleButton.querySelector('i');
                if (icon) icon.style.transform = 'scale(1)';
            });
        }
    }

    updateToggleIcon() {
        const toggleButton = document.getElementById('theme-toggle');
        if (toggleButton) {
            const icon = toggleButton.querySelector('i');
            const text = toggleButton.querySelector('.theme-text');
            
            if (this.currentTheme === 'dark') {
                if (icon) {
                    icon.className = 'fas fa-sun';
                }
                if (text) text.textContent = 'Modo Claro';
                toggleButton.setAttribute('title', 'Alternar para Modo Claro');
            } else {
                if (icon) {
                    icon.className = 'fas fa-moon';
                }
                if (text) text.textContent = 'Modo Escuro';
                toggleButton.setAttribute('title', 'Alternar para Modo Escuro');
            }
        }
    }

    setupEventListeners() {
        // Listener para quando gráficos são criados/atualizados
        document.addEventListener('chartCreated', () => {
            setTimeout(() => this.updateChartsTheme(), 100);
        });
        
        // Listener para mudanças dinâmicas de conteúdo
        document.addEventListener('contentUpdated', () => {
            this.applyThemeToNewContent();
        });
    }

    // Configurações específicas para gráficos Chart.js
    getChartThemeConfig() {
        const isDark = this.currentTheme === 'dark';
        
        return {
            plugins: {
                legend: {
                    labels: { 
                        color: isDark ? '#f0f6fc' : '#212529'
                    }
                },
                tooltip: {
                    backgroundColor: isDark ? '#21262d' : '#ffffff',
                    titleColor: isDark ? '#f0f6fc' : '#212529',
                    bodyColor: isDark ? '#8b949e' : '#6c757d',
                    borderColor: isDark ? '#30363d' : '#dee2e6',
                    borderWidth: 1
                }
            },
            scales: {
                x: {
                    ticks: { color: isDark ? '#8b949e' : '#6c757d' },
                    grid: { color: isDark ? '#30363d' : '#e9ecef' }
                },
                y: {
                    ticks: { color: isDark ? '#8b949e' : '#6c757d' },
                    grid: { color: isDark ? '#30363d' : '#e9ecef' }
                }
            }
        };
    }

    // Configurações para gráficos Plotly.js (usado no mapa)
    getPlotlyThemeConfig() {
        const isDark = this.currentTheme === 'dark';
        
        return {
            paper_bgcolor: isDark ? '#0d1117' : '#ffffff',
            plot_bgcolor: isDark ? '#161b22' : '#f8f9fa',
            font: { color: isDark ? '#f0f6fc' : '#212529' },
            colorway: isDark ? 
                ['#00d946', '#3fb950', '#7ce38b', '#56d364', '#2ea043'] :
                ['#00b034', '#009a2e', '#28a745', '#20c997', '#17a2b8']
        };
    }

    updateChartsTheme() {
        // Atualiza gráficos Chart.js
        if (typeof Chart !== 'undefined' && Chart.instances) {
            const themeConfig = this.getChartThemeConfig();
            Object.values(Chart.instances).forEach(chart => {
                if (chart && chart.options) {
                    // Merge das configurações de tema
                    Object.assign(chart.options, themeConfig);
                    chart.update('none'); // Sem animação para mudança de tema
                }
            });
        }

        // Atualiza gráficos Plotly.js (mapa)
        const mapElements = document.querySelectorAll('[id*="map"], [id*="chart"]');
        mapElements.forEach(element => {
            if (element._fullLayout) {
                const plotlyTheme = this.getPlotlyThemeConfig();
                Plotly.relayout(element, plotlyTheme);
            }
        });
    }

    applyThemeToNewContent() {
        // Re-aplica o tema para conteúdo carregado dinamicamente
        const newCards = document.querySelectorAll('.card:not([data-themed])');
        newCards.forEach(card => {
            card.setAttribute('data-themed', 'true');
        });
    }

    showThemeChangeNotification(newTheme) {
        // Cria notificação visual discreta
        const notification = document.createElement('div');
        notification.className = 'theme-change-notification';
        notification.innerHTML = `
            <i class="fas fa-${newTheme === 'dark' ? 'moon' : 'sun'}"></i>
            Tema ${newTheme === 'dark' ? 'escuro' : 'claro'} ativado
        `;
        
        // Estilos inline para a notificação
        Object.assign(notification.style, {
            position: 'fixed',
            top: '20px',
            right: '20px',
            background: newTheme === 'dark' ? '#21262d' : '#ffffff',
            color: newTheme === 'dark' ? '#f0f6fc' : '#212529',
            padding: '12px 16px',
            borderRadius: '8px',
            boxShadow: '0 4px 12px rgba(0,0,0,0.2)',
            border: `1px solid ${newTheme === 'dark' ? '#30363d' : '#dee2e6'}`,
            fontSize: '14px',
            zIndex: '9999',
            transform: 'translateX(100%)',
            opacity: '0',
            transition: 'all 0.3s ease'
        });
        
        document.body.appendChild(notification);
        
        // Anima entrada
        setTimeout(() => {
            notification.style.transform = 'translateX(0)';
            notification.style.opacity = '1';
        }, 10);
        
        // Remove após 2 segundos
        setTimeout(() => {
            notification.style.transform = 'translateX(100%)';
            notification.style.opacity = '0';
            setTimeout(() => {
                if (notification.parentNode) {
                    notification.parentNode.removeChild(notification);
                }
            }, 300);
        }, 2000);
    }

    // Método público para outros scripts
    getCurrentTheme() {
        return this.currentTheme;
    }

    // Método para forçar um tema específico
    setTheme(theme) {
        if (['light', 'dark'].includes(theme)) {
            this.applyTheme(theme);
        }
    }

    // NOVO: Sincronização com backend
    async syncThemeWithServer(theme) {
        if (!theme) return;
        
        try {
            const response = await fetch('/api/user/theme', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-Requested-With': 'XMLHttpRequest'
                },
                body: JSON.stringify({ theme: theme })
            });
            
            if (!response.ok) {
                console.warn('Erro ao sincronizar tema com servidor:', response.status);
                return;
            }
            
            const data = await response.json();
            if (data.success) {
                console.log('Tema sincronizado com servidor:', data.message);
            }
        } catch (error) {
            console.warn('Erro na sincronização de tema:', error);
            // Não bloqueia a funcionalidade se o servidor não responder
        }
    }

    // NOVO: Carrega tema do servidor
    async loadThemeFromServer() {
        try {
            const response = await fetch('/api/user/theme');
            if (!response.ok) return null;
            
            const data = await response.json();
            return data.success ? data.theme : null;
        } catch (error) {
            console.warn('Erro ao carregar tema do servidor:', error);
            return null;
        }
    }
}

// MODIFICADO: Inicialização agora é async
document.addEventListener('DOMContentLoaded', async function() {
    // Verifica se já não foi inicializado
    if (!window.themeManager) {
        window.themeManager = new ThemeManager();
        // Como init agora é async, precisamos aguardar
        await window.themeManager.init();
    }
});

// Detecta preferência do sistema se não houver tema salvo
if (!localStorage.getItem('igreen-theme')) {
    const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
    if (prefersDark) {
        document.documentElement.setAttribute('data-theme', 'dark');
    }
}

// Listener para mudanças na preferência do sistema
window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (e) => {
    if (window.themeManager && !localStorage.getItem('igreen-theme')) {
        window.themeManager.setTheme(e.matches ? 'dark' : 'light');
    }
});