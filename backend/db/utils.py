# backend/db/utils.py
import logging
from typing import List
from .executor import execute_query # Import local
from .reports_specific import _get_recebiveis_clientes_fields # Importar função privada para fallback

logger = logging.getLogger(__name__)

# --- Funções Auxiliares para Relatórios (Cabeçalhos e Fornecedoras) ---

def get_fornecedoras() -> List[str]:
    """Busca lista única de fornecedoras."""
    query = 'SELECT DISTINCT fornecedora FROM public."CLIENTES" WHERE fornecedora IS NOT NULL AND fornecedora <> \'\' ORDER BY fornecedora;'
    try: results = execute_query(query); return sorted([str(f[0]) for f in results if f and f[0]]) if results else []
    except Exception as e: logger.error(f"Erro get_fornecedoras: {e}", exc_info=True); return []


def get_headers(report_type: str) -> List[str]:
     """Retorna cabeçalhos legíveis baseados no tipo de relatório."""
     # O header_map continua útil para outros relatórios
     header_map = {
         "c.idcliente": "ID Cliente", "c.nome": "Nome", "c.numinstalacao": "Instalação", "c.celular": "Celular",
         "regiao": "Região", "data_ativo": "Data Ativo", "qtdeassinatura": "Assinaturas", "c.consumomedio": "Consumo Médio",
         "c.status": "Status Cliente", "dtcad": "Data Cadastro", "c.\"cpf/cnpj\"": "CPF/CNPJ", "c.numcliente": "Num Cliente",
         "consultor_nome": "Licenciado",
         "devolutiva": "Devolutiva", "licenciado": "Licenciado RZK", "chave_contrato": "Chave Contrato",
         "nome_cliente_rateio": "Nome Cliente (RZK)",
         "c.idconsultor": "ID Licenciado", "c.cpf": "CPF Licenciado", "c.email": "Email Licenciado",
         "c.uf": "UF Licenciado", "quantidade_clientes_ativos": "Qtd Clientes Ativos",
         "dias_ativo": "Dias Ativo", "quantidade_registros_rcb": "Qtd Boletos",
         "rcb.idrcb": "Idrcb", "codigo_cliente": "Codigo Cliente", "cliente_nome": "Cliente",
         "rcb.valorseria": "Quanto Seria", "rcb.valorapagar": "Valor A Pagar",
         "rcb.valorcomcashback": "Valor Com Cashback", "data_referencia": "Data Referencia",
         "data_vencimento": "Data Vencimento", "data_pagamento": "Data Pagamento",
         "data_vencimento_original": "Data Vencimento Original", "status_financeiro_cliente": "Status Financeiro Cliente",
         "id_licenciado": "Id Licenciado", "nome_licenciado": "Licenciado", "celular_licenciado": "Celular Licenciado",
         "status_calculado": "Status Pagamento", "rcb.urldemonstrativo": "Url Demonstrativo", "rcb.urlboleto": "Url Boleto",
         "rcb.qrcode": "Qrcode Pix", "rcb.urlcontacemig": "Url Boleto Distribuidora",
         "valor_distribuidora": "Valor Distribuidora", "rcb.codigobarra": "Codigo Barra Boleto",
         "fornecedora_cliente": "Fornecedora Cliente", "c.concessionaria": "Concessionaria",
         "cpf_cnpj_cliente": "Cpf Cliente", "rcb.nrodocumento": "Numero Documento", "rcb.idcomerc": "Idcomerc",
         "rcb.idbomfuturo": "Idbomfuturo", "rcb.energiainjetada": "Energia Injetada",
         "rcb.energiacompensada": "Energia Compensada", "rcb.energiaacumulada": "Energia Acumulada",
         "rcb.energiaajuste": "Energia Ajuste", "rcb.energiafaturamento": "Energia Faturamento",
         "c.desconto_cliente": "Desconto Cliente", "qtd_rcb_cliente": "Qt de Rcb",
     }
     
     keys_order = {
         ### ALTERAÇÃO INICIADA: A lista de 'base_clientes' agora reflete as colunas da VIEW V_CUSTOMER ###
         "base_clientes": [
            "código", "nome", "instalacao", "celular", "cidade", "região", "data ativo", 
            "assinaturas", "Sequencia Assinaturas", "média consumo", "devolutiva", "data cadastro", 
            "cpf", "numero cliente", "data ult. alteração", "celular 2", "email", "rg", 
            "orgão emissor", "data injeção", "id licenciado", "licenciado", "celular consultor", 
            "cep", "endereco", "numero", "bairro", "complemento", "cnpj", "razao", "fantasia", 
            "UF consumo", "classificacao", "chave contrato", "chave assinatura cliente", 
            "chave solatio", "cashback", "codigo solatio", "enviado comerc", "obs", 
            "posvenda", "retido", "verificado", "rateio", "validado sucesso", "status sucesso", 
            "doc. enviado", "link Documento", "link Conta Energia", "link Cartão CNPJ", 
            "link Documento Frente", "link Documento Verso", "link Conta Energia 2", 
            "link Contrato Social", "link Comprovante de pagamento", "link Estatuto Convenção", 
            "senha pdf", "usuario ult alteracao", "elegibilidade", "id plano club pj", 
            "data cancelamento", "data ativação original", "fornecedora", "desconto cliente", 
            "data nascimento", "Origem", "Forma de pagamento", "Status Financeiro", 
            "Login Distribuidora", "Senha Distribuidora", "Cliente", "Representante", 
            "nacionalidade", "profissao", "estadocivil", "forma pagamento", 
            "Observação Compartilhada", "Auto Conexão", "Link assinatura"
         ],
         ### FIM DA ALTERAÇÃO ###

         # Ordens dos outros relatórios permanecem as mesmas
         "rateio": [ "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "regiao", "data_ativo", "c.consumomedio", "dtcad", "c.\"cpf/cnpj\"", "c.numcliente", "c.email", "c.rg", "c.emissor", "c.cep", "consultor_nome", "c.endereco", "c.numero", "c.bairro", "c.complemento", "c.cnpj", "c.razao", "c.fantasia", "c.ufconsumo", "c.classificacao", "c.link_documento", "c.caminhoarquivo", "c.caminhoarquivocnpj", "c.caminhoarquivodoc1", "c.caminhoarquivodoc2", "c.caminhoarquivoenergia2", "c.caminhocontratosocial", "c.caminhocomprovante", "c.caminhoarquivoestatutoconvencao", "c.senhapdf", "c.fornecedora", "c.desconto_cliente", "dtnasc", "c.logindistribuidora", "c.senhadistribuidora", "nome_cliente_rateio", "c.nacionalidade", "c.profissao", "c.estadocivil" ],
         "rateio_rzk": [ "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "regiao", "data_ativo", "c.consumomedio", "devolutiva", "dtcad", "c.\"cpf/cnpj\"", "c.numcliente", "c.email", "c.rg", "c.emissor", "licenciado", "c.cep", "c.endereco", "c.numero", "c.bairro", "c.complemento", "c.cnpj", "c.razao", "c.fantasia", "c.ufconsumo", "c.classificacao", "chave_contrato", "c.link_documento", "c.caminhoarquivo", "c.caminhoarquivocnpj", "c.caminhoarquivodoc1", "c.caminhoarquivodoc2", "c.caminhoarquivoenergia2", "c.caminhocontratosocial", "c.caminhocomprovante", "c.caminhoarquivoestatutoconvencao", "c.senhapdf", "c.fornecedora", "c.desconto_cliente", "dtnasc", "c.logindistribuidora", "c.senhadistribuidora", "nome_cliente_rateio", "c.nacionalidade", "c.profissao", "c.estadocivil" ],
         "clientes_por_licenciado": [ "c.idconsultor", "c.nome", "c.cpf", "c.email", "c.uf", "quantidade_clientes_ativos" ],
         "boletos_por_cliente": [ "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "regiao", "c.fornecedora", "data_ativo", "dias_ativo", "quantidade_registros_rcb" ],
         "recebiveis_clientes": [ "rcb.idrcb", "codigo_cliente", "cliente_nome", "rcb.numinstalacao", "rcb.valorseria", "rcb.valorapagar", "rcb.valorcomcashback", "data_referencia", "data_vencimento", "data_pagamento", "data_vencimento_original", "c.celular", "c.email", "status_financeiro_cliente", "c.numcliente", "id_licenciado", "nome_licenciado", "celular_licenciado", "status_calculado", "rcb.urldemonstrativo", "rcb.urlboleto", "rcb.qrcode", "rcb.urlcontacemig", "valor_distribuidora", "rcb.codigobarra", "c.ufconsumo", "fornecedora_cliente", "c.concessionaria", "c.cnpj", "cpf_cnpj_cliente", "rcb.nrodocumento", "rcb.idcomerc", "rcb.idbomfuturo", "rcb.energiainjetada", "rcb.energiacompensada", "rcb.energiaacumulada", "rcb.energiaajuste", "rcb.energiafaturamento", "c.desconto_cliente", "qtd_rcb_cliente" ]
     }

     report_keys = keys_order.get(report_type.lower())
     if not report_keys:
         logger.warning(f"Ordem de chaves não definida para '{report_type}' em get_headers.")
         if report_type.lower() == 'recebiveis_clientes':
              try:
                  report_keys = [f.split(' AS ')[-1].strip().replace('"', '') for f in _get_recebiveis_clientes_fields()]
                  logger.info(f"Usando ordem de campos da query como fallback para headers de '{report_type}'.")
              except Exception: return []
         else:
              return [] 

     headers_list = []
     missing_in_map = []
     for key in report_keys:
         header = header_map.get(key)
         if not header:
             base_key = key.split('.')[-1].replace('"', '') 
             header = header_map.get(base_key)
             if not header:
                 # O fallback agora simplesmente capitaliza a chave, o que funciona perfeitamente para as colunas da view.
                 header = key.replace('_', ' ').title()
                 missing_in_map.append(key)

         headers_list.append(header)

     if missing_in_map:
         logger.debug(f"Chaves/Aliases não encontrados em header_map (usado fallback) para '{report_type}': {missing_in_map}")

     return headers_list