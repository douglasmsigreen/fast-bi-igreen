// static/js/script.js

// Adiciona um listener que espera o DOM (estrutura HTML) carregar completamente
document.addEventListener('DOMContentLoaded', function() {

    // --- Funcionalidade de Filtro da Tabela de Relatórios ---
    const searchInput = document.getElementById('tableSearch');
    const dataTable = document.getElementById('dataTable');
    const tableBody = dataTable ? dataTable.querySelector('tbody') : null;

    if (searchInput && tableBody) {
        searchInput.addEventListener('input', function() {
            const searchTerm = searchInput.value.toLowerCase();
            const rows = tableBody.querySelectorAll('tr');

            rows.forEach(row => {
                const cells = row.querySelectorAll('td');
                let match = false;
                cells.forEach(cell => {
                    // Verifica se o texto da célula (em minúsculas) inclui o termo de busca
                    if (cell.textContent.toLowerCase().includes(searchTerm)) {
                        match = true;
                    }
                });
                // Mostra ou oculta a linha com base na correspondência
                row.style.display = match ? '' : 'none';
            });
        });
    } else {
        // Avisa no console se algum elemento essencial não for encontrado
        if (!searchInput) console.warn("Elemento de busca #tableSearch não encontrado nesta página.");
        if (!tableBody) console.warn("Elemento tbody da tabela #dataTable não encontrado nesta página.");
    }

    // --- Funcionalidade para fechar Mensagens Flash Pop-up ---
    // Usa delegação de eventos no body para capturar cliques nos botões de fechar
    document.body.addEventListener('click', function(event) {
        // Verifica se o elemento clicado *ou* um de seus pais próximos tem a classe 'flash-close-btn'
        const closeButton = event.target.closest('.flash-close-btn');

        if (closeButton) {
            const alertElement = closeButton.closest('.alert'); // Encontra o .alert pai
            if (alertElement) {
                // 1. Adiciona classe para animação de fade-out (opcional)
                alertElement.classList.add('flash-fade-out');

                // 2. Remove o elemento do DOM após a animação terminar (300ms)
                //    Isso previne que o elemento desapareça abruptamente.
                setTimeout(() => {
                    alertElement.remove();
                }, 300); // Deve corresponder à duração da animação CSS 'fadeOut'
            }
        }
    });

    // Opcional: Remover mensagens flash automaticamente após um tempo
    const autoDismissTime = 3000; // Tempo em milissegundos (7 segundos). Defina como 0 para desativar.
    if (autoDismissTime > 0) {
        const flashMessages = document.querySelectorAll('.flash-popup-container .alert');
        flashMessages.forEach(message => {
            setTimeout(() => {
                // Verifica se a mensagem ainda existe no DOM (pode ter sido fechada manualmente)
                if (message && message.parentNode) {
                    message.classList.add('flash-fade-out');
                    setTimeout(() => {
                        // Verifica novamente antes de remover, caso algo tenha mudado
                        if (message && message.parentNode) {
                             message.remove();
                        }
                    }, 300); // Tempo da animação de saída
                }
            }, autoDismissTime);
        });
    }

}); // Fim do listener DOMContentLoaded