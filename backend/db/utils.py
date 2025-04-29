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
     # (Mesma função que estava no database.py original, pode ser movida para cá)
     header_map = {
         # Mapeamentos para Base Clientes
         "c.idcliente": "ID Cliente", "c.nome": "Nome", "c.numinstalacao": "Instalação", "c.celular": "Celular", "c.cidade": "Cidade",
         "regiao": "Região", "data_ativo": "Data Ativo", "qtdeassinatura": "Assinaturas", "c.consumomedio": "Consumo Médio",
         "c.status": "Status Cliente", "dtcad": "Data Cadastro", "c.\"cpf/cnpj\"": "CPF/CNPJ", "c.numcliente": "Num Cliente",
         "c.email": "Email", "consultor_nome": "Licenciado", "c.fornecedora": "Fornecedora",
         # Mapeamentos para Rateio Geral (alguns são iguais a Base Clientes)
         # "c.idcliente": "ID Cliente", ...
         # Mapeamentos para Rateio RZK
         "devolutiva": "Devolutiva", "licenciado": "Licenciado RZK", "chave_contrato": "Chave Contrato",
         "nome_cliente_rateio": "Nome Cliente (RZK)", # Exemplo de alias específico
         # Mapeamentos para Clientes por Licenciado
         "c.idconsultor": "ID Licenciado", "c.cpf": "CPF Licenciado", "c.email": "Email Licenciado",
         "c.uf": "UF Licenciado", "quantidade_clientes_ativos": "Qtd Clientes Ativos",
         # Mapeamentos para Boletos por Cliente
         "dias_ativo": "Dias Ativo", "quantidade_registros_rcb": "Qtd Boletos",
         # Mapeamentos para Recebíveis Clientes
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
         # Adicione outros mapeamentos faltantes do seu database.py original aqui
          "c.rg": "RG", "c.emissor": "Emissor", "datainjecao": "Data Injeção",
          "consultor_celular": "Celular Licenciado", "c.cep": "CEP", "c.endereco": "Endereço", "c.numero": "Número",
          "c.bairro": "Bairro", "c.complemento": "Complemento", "c.cnpj": "CNPJ", "c.razao": "Razão Social",
          "c.fantasia": "Nome Fantasia", "c.ufconsumo": "UF Consumo", "c.classificacao": "Classificação",
          "c.keycontrato": "Key Contrato", "c.keysigner": "Key Signer", "c.leadidsolatio": "Lead ID Solatio",
          "c.indcli": "Indicação Cliente", "c.enviadocomerc": "Enviado Comercial", "c.obs": "Obs",
          "c.posvenda": "Pós Venda", "c.retido": "Retido", "c.contrato_verificado": "Contrato Verificado",
          "c.rateio": "Rateio", "c.validadosucesso": "Validado Sucesso", "status_sucesso": "Status Sucesso",
          "c.documentos_enviados": "Docs Enviados", "c.link_documento": "Link Documento",
          "c.caminhoarquivo": "Caminho Arquivo", "c.caminhoarquivocnpj": "Caminho CNPJ",
          "c.caminhoarquivodoc1": "Caminho Doc 1", "c.caminhoarquivodoc2": "Caminho Doc 2",
          "c.caminhoarquivoenergia2": "Caminho Energia 2", "c.caminhocontratosocial": "Caminho CS",
          "c.caminhocomprovante": "Caminho Comprovante", "c.caminhoarquivoestatutoconvencao": "Caminho Estatuto",
          "c.senhapdf": "Senha PDF", "c.codigo": "Código", "c.elegibilidade": "Elegibilidade",
          "c.idplanopj": "ID Plano PJ", "dtcancelado": "Data Cancelado", "data_ativo_original": "Data Ativo Original",
          "dtnasc": "Data Nascimento", "c.origem": "Origem", "c.cm_tipo_pagamento": "CM Tipo Pagamento",
          "c.status_financeiro": "Status Financeiro", "c.logindistribuidora": "Login Distribuidora",
          "c.senhadistribuidora": "Senha Distribuidora", "c.nacionalidade": "Nacionalidade",
          "c.profissao": "Profissão", "c.estadocivil": "Estado Civil",
          "c.obs_compartilhada": "Obs Compartilhada", "c.linkassinatura1": "Link Assinatura 1",
          "dtultalteracao": "Dt Ult Alteracao", "c.celular_2": "Celular 2",
     }
     # Define a ORDEM das colunas para cada relatório (copiado do database.py original)
     keys_order = {
         "base_clientes": [ "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "regiao", "data_ativo", "qtdeassinatura", "c.consumomedio", "c.status", "dtcad", "c.\"cpf/cnpj\"", "c.numcliente", "dtultalteracao", "c.celular_2", "c.email", "c.rg", "c.emissor", "datainjecao", "c.idconsultor", "consultor_nome", "consultor_celular", "c.cep", "c.endereco", "c.numero", "c.bairro", "c.complemento", "c.cnpj", "c.razao", "c.fantasia", "c.ufconsumo", "c.classificacao", "c.keycontrato", "c.keysigner", "c.leadidsolatio", "c.indcli", "c.enviadocomerc", "c.obs", "c.posvenda", "c.retido", "c.contrato_verificado", "c.rateio", "c.validadosucesso", "status_sucesso", "c.documentos_enviados", "c.link_documento", "c.caminhoarquivo", "c.caminhoarquivocnpj", "c.caminhoarquivodoc1", "c.caminhoarquivodoc2", "c.caminhoarquivoenergia2", "c.caminhocontratosocial", "c.caminhocomprovante", "c.caminhoarquivoestatutoconvencao", "c.senhapdf", "c.codigo", "c.elegibilidade", "c.idplanopj", "dtcancelado", "data_ativo_original", "c.fornecedora", "c.desconto_cliente", "dtnasc", "c.origem", "c.cm_tipo_pagamento", "c.status_financeiro", "c.logindistribuidora", "c.senhadistribuidora", "c.nacionalidade", "c.profissao", "c.estadocivil", "c.obs_compartilhada", "c.linkassinatura1" ],
         "rateio": [ "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "regiao", "data_ativo", "c.consumomedio", "dtcad", "c.\"cpf/cnpj\"", "c.numcliente", "c.email", "c.rg", "c.emissor", "c.cep", "consultor_nome", "c.endereco", "c.numero", "c.bairro", "c.complemento", "c.cnpj", "c.razao", "c.fantasia", "c.ufconsumo", "c.classificacao", "c.link_documento", "c.caminhoarquivo", "c.caminhoarquivocnpj", "c.caminhoarquivodoc1", "c.caminhoarquivodoc2", "c.caminhoarquivoenergia2", "c.caminhocontratosocial", "c.caminhocomprovante", "c.caminhoarquivoestatutoconvencao", "c.senhapdf", "c.fornecedora", "c.desconto_cliente", "dtnasc", "c.logindistribuidora", "c.senhadistribuidora", "nome_cliente_rateio", "c.nacionalidade", "c.profissao", "c.estadocivil" ],
         "rateio_rzk": [ "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "regiao", "data_ativo", "c.consumomedio", "devolutiva", "dtcad", "c.\"cpf/cnpj\"", "c.numcliente", "c.email", "c.rg", "c.emissor", "licenciado", "c.cep", "c.endereco", "c.numero", "c.bairro", "c.complemento", "c.cnpj", "c.razao", "c.fantasia", "c.ufconsumo", "c.classificacao", "chave_contrato", "c.link_documento", "c.caminhoarquivo", "c.caminhoarquivocnpj", "c.caminhoarquivodoc1", "c.caminhoarquivodoc2", "c.caminhoarquivoenergia2", "c.caminhocontratosocial", "c.caminhocomprovante", "c.caminhoarquivoestatutoconvencao", "c.senhapdf", "c.fornecedora", "c.desconto_cliente", "dtnasc", "c.logindistribuidora", "c.senhadistribuidora", "nome_cliente_rateio", "c.nacionalidade", "c.profissao", "c.estadocivil" ],
         "clientes_por_licenciado": [ "c.idconsultor", "c.nome", "c.cpf", "c.email", "c.uf", "quantidade_clientes_ativos" ],
         "boletos_por_cliente": [ "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "regiao", "c.fornecedora", "data_ativo", "dias_ativo", "quantidade_registros_rcb" ],
         "recebiveis_clientes": [ "rcb.idrcb", "codigo_cliente", "cliente_nome", "rcb.numinstalacao", "rcb.valorseria", "rcb.valorapagar", "rcb.valorcomcashback", "data_referencia", "data_vencimento", "data_pagamento", "data_vencimento_original", "c.celular", "c.email", "status_financeiro_cliente", "c.numcliente", "id_licenciado", "nome_licenciado", "celular_licenciado", "status_calculado", "rcb.urldemonstrativo", "rcb.urlboleto", "rcb.qrcode", "rcb.urlcontacemig", "valor_distribuidora", "rcb.codigobarra", "c.ufconsumo", "fornecedora_cliente", "c.concessionaria", "c.cnpj", "cpf_cnpj_cliente", "rcb.nrodocumento", "rcb.idcomerc", "rcb.idbomfuturo", "rcb.energiainjetada", "rcb.energiacompensada", "rcb.energiaacumulada", "rcb.energiaajuste", "rcb.energiafaturamento", "c.desconto_cliente", "qtd_rcb_cliente" ]
     }

     report_keys = keys_order.get(report_type.lower())
     if not report_keys:
         logger.warning(f"Ordem de chaves não definida para '{report_type}' em get_headers.")
         # Fallback específico para 'recebiveis_clientes' se a ordem não estiver definida
         if report_type.lower() == 'recebiveis_clientes':
              try:
                  report_keys = [f.split(' AS ')[-1].strip().replace('"', '') for f in _get_recebiveis_clientes_fields()]
                  logger.info(f"Usando ordem de campos da query como fallback para headers de '{report_type}'.")
              except Exception: return []
         else:
              return [] # Retorna vazio se não for 'recebiveis_clientes' e não houver ordem

     headers_list = []
     missing_in_map = []
     for key in report_keys:
         header = header_map.get(key)
         if not header:
             # Tenta mapear a parte base (ex: 'c.idcliente' -> 'idcliente')
             base_key = key.split('.')[-1].replace('"', '') # Remove prefixo e aspas
             header = header_map.get(base_key)
             if not header:
                 # Usa a chave original como fallback, formatando-a
                 header = key.split(' AS ')[-1].strip().replace('_', ' ').replace('"', '').title()
                 missing_in_map.append(key) # Adiciona à lista de não mapeados

         headers_list.append(header)

     if missing_in_map:
         logger.debug(f"Chaves/Aliases não encontrados em header_map (usado fallback) para '{report_type}': {missing_in_map}")

     return headers_list

# --- FIM DA FUNÇÃO get_headers ---