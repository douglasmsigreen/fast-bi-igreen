// static/js/reports_loader.js

document.addEventListener('DOMContentLoaded', function() {
    const tableContainer = document.querySelector('.table-responsive');
    const tableHeader = document.getElementById('dataTable').querySelector('thead');
    const tableBody = document.getElementById('dataTable').querySelector('tbody');
    const paginationNav = document.querySelector('nav.pagination');
    const tableInfo = document.querySelector('.table-info');
    const searchBox = document.getElementById('tableSearch');
    const exportLink = document.getElementById('export-link'); // Adicione um ID ao seu link de exportação

    // Elemento para mostrar o status de carregamento
    const loadingIndicator = document.createElement('div');
    loadingIndicator.className = 'loading-indicator';
    loadingIndicator.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Carregando dados...';
    if(tableContainer) tableContainer.parentNode.insertBefore(loadingIndicator, tableContainer);

    function buildTable(headers, data) {
        tableHeader.innerHTML = '';
        tableBody.innerHTML = '';

        if (!headers || headers.length === 0) return;

        const headerRow = document.createElement('tr');
        headers.forEach(headerText => {
            const th = document.createElement('th');
            th.textContent = headerText;
            headerRow.appendChild(th);
        });
        tableHeader.appendChild(headerRow);

        if (!data || data.length === 0) {
            const tr = document.createElement('tr');
            const td = document.createElement('td');
            td.textContent = 'Nenhum dado encontrado para os filtros selecionados.';
            td.colSpan = headers.length;
            td.style.textAlign = 'center';
            tr.appendChild(td);
            tableBody.appendChild(tr);
            return;
        }

        data.forEach(rowData => {
            const tr = document.createElement('tr');
            rowData.forEach(cellData => {
                const td = document.createElement('td');
                td.textContent = cellData;
                tr.appendChild(td);
            });
            tableBody.appendChild(tr);
        });
    }

    function buildPagination(page, total_pages, base_args) {
        paginationNav.innerHTML = '';
        if (total_pages <= 1) return;

        const ul = document.createElement('ul');

        // Botão Anterior
        const prevLi = document.createElement('li');
        if (page > 1) {
            base_args.set('page', page - 1);
            prevLi.innerHTML = `<a href="?${base_args.toString()}">&laquo; Anterior</a>`;
        } else {
            prevLi.className = 'disabled';
            prevLi.innerHTML = `<span>&laquo; Anterior</span>`;
        }
        ul.appendChild(prevLi);
        
        // Lógica de numeração (simplificada para o exemplo)
        let start_page = Math.max(1, page - 2);
        let end_page = Math.min(total_pages, page + 2);

        if (start_page > 1) {
             base_args.set('page', 1);
             ul.appendChild(createPageLink(1, base_args.toString()));
             if (start_page > 2) ul.appendChild(createPageSpan('...'));
        }

        for (let p = start_page; p <= end_page; p++) {
            base_args.set('page', p);
            const pageLi = (p === page) ? createActivePage(p) : createPageLink(p, base_args.toString());
            ul.appendChild(pageLi);
        }

        if (end_page < total_pages) {
            if (end_page < total_pages - 1) ul.appendChild(createPageSpan('...'));
            base_args.set('page', total_pages);
            ul.appendChild(createPageLink(total_pages, base_args.toString()));
        }

        // Botão Próximo
        const nextLi = document.createElement('li');
        if (page < total_pages) {
            base_args.set('page', page + 1);
            nextLi.innerHTML = `<a href="?${base_args.toString()}">Próximo &raquo;</a>`;
        } else {
            nextLi.className = 'disabled';
            nextLi.innerHTML = `<span>Próximo &raquo;</span>`;
        }
        ul.appendChild(nextLi);

        paginationNav.appendChild(ul);
    }
    
    // Funções auxiliares para paginação
    function createPageLink(pageNumber, url) { const li = document.createElement('li'); li.innerHTML = `<a href="?${url}">${pageNumber}</a>`; return li; }
    function createActivePage(pageNumber) { const li = document.createElement('li'); li.className = 'active'; li.innerHTML = `<span>${pageNumber}</span>`; return li; }
    function createPageSpan(text) { const li = document.createElement('li'); li.innerHTML = `<span>${text}</span>`; return li; }

    async function loadReportData() {
        // Pega os parâmetros da URL atual
        const urlParams = new URLSearchParams(window.location.search);
        const report_type = urlParams.get('report_type') || 'base_clientes';
        const fornecedora = urlParams.get('fornecedora') || 'Consolidado';
        const page = urlParams.get('page') || '1';

        // Mostra o indicador de carregamento
        loadingIndicator.style.display = 'block';
        tableContainer.style.display = 'none';
        paginationNav.style.display = 'none';
        tableInfo.style.display = 'none';

        try {
            const response = await fetch(`/api/reports/get-data?report_type=${report_type}&fornecedora=${fornecedora}&page=${page}`);
            if (!response.ok) {
                const errData = await response.json();
                throw new Error(errData.error || 'Erro ao buscar dados do relatório.');
            }
            const result = await response.json();

            // Constrói a tabela, paginação e info
            buildTable(result.headers, result.dados);
            
            const base_args_pagination = new URLSearchParams({ report_type: report_type });
            if (fornecedora !== 'Consolidado' && !['rateio_rzk', 'clientes_por_licenciado'].includes(report_type)) {
                base_args_pagination.set('fornecedora', fornecedora);
            }
            buildPagination(result.page, result.total_pages, base_args_pagination);

            tableInfo.textContent = `Exibindo ${result.dados?.length || 0} de ${result.total_items} registro(s). Página ${result.page} de ${result.total_pages}.`;

            // Atualiza o link de exportação
            const export_args = new URLSearchParams({ report_type: report_type });
            if (fornecedora && !['rateio_rzk', 'clientes_por_licenciado'].includes(report_type)) {
                 export_args.set('fornecedora', fornecedora);
            }
            exportLink.href = `/export?${export_args.toString()}`;
            exportLink.style.display = result.dados && result.dados.length > 0 ? 'inline-block' : 'none';

        } catch (error) {
            console.error('Erro ao carregar dados do relatório:', error);
            tableBody.innerHTML = `<tr><td colspan="1" style="text-align: center; color: red;">${error.message}</td></tr>`;
        } finally {
            // Esconde o indicador e mostra a tabela
            loadingIndicator.style.display = 'none';
            tableContainer.style.display = 'block';
            paginationNav.style.display = 'block';
            tableInfo.style.display = 'block';
        }
    }

    // Carrega os dados na primeira vez que a página é aberta
    loadReportData();
    
    // Garante que o filtro de busca funcione com a tabela carregada dinamicamente
    searchBox.addEventListener('input', function() {
        const searchTerm = searchBox.value.toLowerCase();
        const rows = tableBody.querySelectorAll('tr');
        rows.forEach(row => {
            row.style.display = row.textContent.toLowerCase().includes(searchTerm) ? '' : 'none';
        });
    });
});