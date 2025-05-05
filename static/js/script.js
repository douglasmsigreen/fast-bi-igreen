// static/js/script.js
document.addEventListener('DOMContentLoaded', function() {
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
       if (cell.textContent.toLowerCase().includes(searchTerm)) {
        match = true;
       }
      });
      // Mostra ou oculta a linha
      row.style.display = match ? '' : 'none';
     });
    });
    } else {
     if (!searchInput) console.error("Elemento #tableSearch não encontrado.");
     if (!tableBody) console.error("Elemento tbody da tabela #dataTable não encontrado.");
    }
  
    // Adiciona event listeners aos botões de fechar das mensagens flash
    const closeButtons = document.querySelectorAll('#flash-messages-container .btn-close');
  
    closeButtons.forEach(button => {
     button.addEventListener('click', function() {
      // Remove a mensagem flash quando o botão de fechar é clicado
      this.parentNode.remove();
     });
    });
   });