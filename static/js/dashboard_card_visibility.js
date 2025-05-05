// static/js/dashboard_card_visibility.js
// Este arquivo complementa o dashboard_controls.js com funcionalidades de visibilidade de cards

document.addEventListener('DOMContentLoaded', function() {
    // Configurações de visibilidade dos cards
    const STORAGE_KEY_VISIBILITY = 'dashboardCardVisibility';
    
    // Mapeamento de IDs para nomes legíveis
    const cardLabels = {
        'kpi-resultado-mes': 'Resultado do Mês (kWh vendidos)',
        'kpi-clientes-mes': 'Clientes do Mês (Registrados/Ativados)',
        'chart-evolucao-ativacoes': 'Evolução de Ativações (Gráfico)',
        'summary-fornecedora': 'Resumo por Fornecedora',
        'summary-concessionaria': 'Resumo por Região/Concessionária',
        'pie-fornecedora': 'Clientes Ativos por Fornecedora (Gráfico Pizza)',
        'bar-concessionaria': 'Top 8 Regiões/Concessionárias (Gráfico Barras)',
        'summary-fornecedora-no-rcb': 'Fornecedoras s/ RCB',
        'chart-vencidos': 'Pagamentos Vencidos s/ Baixa'
    };

    // Configuração do botão e modal
    const configBtn = document.getElementById('dashboard-config-btn');
    const configModal = document.getElementById('dashboardConfigModal');
    const cardsContainer = document.querySelector('.dashboard-grid');
    const cardsConfigContainer = document.getElementById('dashboard-cards-config');
    const saveConfigBtn = document.getElementById('save-dashboard-config');
    
    let bootstrapModal = null;

    // Função para carregar a configuração de visibilidade salva
    function loadCardVisibility() {
        const savedVisibility = localStorage.getItem(STORAGE_KEY_VISIBILITY);
        let visibility = {};
        
        // Se não houver configuração salva, todos os cards são visíveis por padrão
        if (!savedVisibility) {
            const cards = cardsContainer.querySelectorAll('.card[data-id]');
            cards.forEach(card => {
                visibility[card.dataset.id] = true;
            });
        } else {
            try {
                visibility = JSON.parse(savedVisibility);
            } catch (e) {
                console.error('Erro ao carregar configuração de visibilidade dos cards:', e);
                return {}; // Retorna objeto vazio em caso de erro
            }
        }
        
        return visibility;
    }

    // Função para aplicar a configuração de visibilidade
    function applyCardVisibility(visibility = null) {
        if (!visibility) {
            visibility = loadCardVisibility();
        }
        
        const cards = cardsContainer.querySelectorAll('.card[data-id]');
        cards.forEach(card => {
            const cardId = card.dataset.id;
            if (visibility[cardId] === false) {
                card.style.display = 'none';
            } else {
                card.style.display = ''; // Remove o estilo inline e volta ao padrão CSS
            }
        });
    }

    // Função para salvar a configuração de visibilidade
    function saveCardVisibility(visibility) {
        localStorage.setItem(STORAGE_KEY_VISIBILITY, JSON.stringify(visibility));
        applyCardVisibility(visibility);
    }

    // Função para preencher o modal com os checkboxes dos cards
    function populateConfigModal() {
        if (!cardsConfigContainer) return;
        
        const currentVisibility = loadCardVisibility();
        cardsConfigContainer.innerHTML = '';
        
        const cards = cardsContainer.querySelectorAll('.card[data-id]');
        
        cards.forEach(card => {
            const cardId = card.dataset.id;
            const cardLabel = cardLabels[cardId] || cardId;
            const isVisible = currentVisibility[cardId] !== false;
            
            const div = document.createElement('div');
            div.className = 'form-check mb-2';
            div.style.paddingLeft = '25px'; // Aumenta o padding esquerdo
            div.style.position = 'relative'; // Garante posicionamento adequado
            div.innerHTML = `
                <input class="form-check-input" type="checkbox" id="check-${cardId}" 
                       data-card-id="${cardId}" ${isVisible ? 'checked' : ''} 
                       style="border-color: #00b034; background-color: ${isVisible ? '#00b034' : ''}; margin-left: 5px;">
                <label class="form-check-label" for="check-${cardId}" style="padding-left: 10px;">
                    ${cardLabel}
                </label>
            `;
            
            // Adicionamos um evento de click diretamente no div
            div.onclick = function(event) {
                // Verifica se o clique não foi no checkbox (já tem comportamento padrão)
                if (event.target.tagName !== 'INPUT') {
                    const checkbox = div.querySelector('input[type="checkbox"]');
                    checkbox.checked = !checkbox.checked;
                    event.preventDefault(); // Previne comportamento padrão
                }
            };
            
            cardsConfigContainer.appendChild(div);
        });
    }

    // Função para inicializar o modal usando Bootstrap
    function initializeBootstrapModal() {
        try {
            bootstrapModal = new bootstrap.Modal(configModal, {
                backdrop: 'static', // Não fecha ao clicar fora
                keyboard: true      // Permite fechar com ESC
            });
            
            // Configura evento para abrir o modal
            configBtn.addEventListener('click', function() {
                populateConfigModal();
                bootstrapModal.show();
            });
            
            // Configura evento para salvar
            saveConfigBtn.addEventListener('click', function() {
                const checkboxes = cardsConfigContainer.querySelectorAll('input[type="checkbox"][data-card-id]');
                const newVisibility = {};
                
                checkboxes.forEach(checkbox => {
                    newVisibility[checkbox.dataset.cardId] = checkbox.checked;
                });
                
                saveCardVisibility(newVisibility);
                bootstrapModal.hide();
            });
            
            // Garante que o modal é interativo
            configModal.style.pointerEvents = 'auto';
            configModal.querySelector('.modal-content').style.pointerEvents = 'auto';
            
            return true;
        } catch (error) {
            console.error('Erro ao inicializar modal Bootstrap:', error);
            return false;
        }
    }

    // Função para inicializar o modal manualmente (fallback)
    function initializeCustomModal() {
        // Estilos para o modal
        configModal.style.display = 'none';
        configModal.style.position = 'fixed';
        configModal.style.top = '0';
        configModal.style.left = '0';
        configModal.style.width = '100%';
        configModal.style.height = '100%';
        configModal.style.backgroundColor = 'rgba(0,0,0,0.5)';
        configModal.style.zIndex = '1050';
        configModal.style.overflow = 'auto';
        configModal.style.pointerEvents = 'auto'; // Garante que os eventos de ponteiro funcionem
        
        // Estilos para o diálogo
        const modalDialog = configModal.querySelector('.modal-dialog');
        if (modalDialog) {
            modalDialog.style.margin = '1.75rem auto';
            modalDialog.style.maxWidth = '500px';
            modalDialog.style.pointerEvents = 'auto'; // Garante que os eventos de ponteiro funcionem
        }
        
        // Estilos para o conteúdo
        const modalContent = configModal.querySelector('.modal-content');
        if (modalContent) {
            modalContent.style.pointerEvents = 'auto'; // Garante que os eventos de ponteiro funcionem
        }
        
        // Configura evento para abrir modal
        configBtn.addEventListener('click', function(event) {
            event.preventDefault(); // Previne comportamento padrão
            populateConfigModal();
            configModal.style.display = 'block';
        });
        
        // Configura eventos para fechar modal
        const closeButtons = configModal.querySelectorAll('[data-bs-dismiss="modal"], .btn-close, .btn-secondary');
        closeButtons.forEach(button => {
            button.addEventListener('click', function() {
                configModal.style.display = 'none';
            });
        });
        
        // Fecha ao pressionar ESC
        document.addEventListener('keydown', function(event) {
            if (event.key === 'Escape' && configModal.style.display === 'block') {
                configModal.style.display = 'none';
            }
        });
        
        // Configura evento para salvar
        saveConfigBtn.addEventListener('click', function() {
            const checkboxes = cardsConfigContainer.querySelectorAll('input[type="checkbox"][data-card-id]');
            const newVisibility = {};
            
            checkboxes.forEach(checkbox => {
                newVisibility[checkbox.dataset.cardId] = checkbox.checked;
            });
            
            saveCardVisibility(newVisibility);
            configModal.style.display = 'none';
        });
    }

    // Inicializar os eventos
    function initializeCardVisibilityControls() {
        if (!configBtn || !configModal || !cardsContainer || !cardsConfigContainer || !saveConfigBtn) {
            console.error('Elementos necessários para configuração do dashboard não encontrados');
            return;
        }
        
        // Aplicar visibilidade salva ao carregar a página
        applyCardVisibility();
        
        // Tenta inicializar com Bootstrap, se falhar, usa o modal personalizado
        if (!initializeBootstrapModal()) {
            initializeCustomModal();
        }
    }
    
    // Iniciar a funcionalidade de visibilidade dos cards
    initializeCardVisibilityControls();
});