# backend/db/utils.py
import logging
from typing import List
from .executor import execute_query
from .reports_specific import _get_recebiveis_clientes_fields 

logger = logging.getLogger(__name__)

# --- Funções Auxiliares para Relatórios (Cabeçalhos e Fornecedoras) ---

def get_fornecedoras() -> List[str]:
    """Busca lista única de fornecedoras."""
    query = 'SELECT DISTINCT fornecedora FROM public."CLIENTES" WHERE fornecedora IS NOT NULL AND fornecedora <> \'\' ORDER BY fornecedora;'
    try: results = execute_query(query); return sorted([str(f[0]) for f in results if f and f[0]]) if results else []
    except Exception as e: logger.error(f"Erro get_fornecedoras: {e}", exc_info=True); return []


def get_headers(report_type: str) -> List[str]:
    """Retorna cabeçalhos legíveis baseados no tipo de relatório."""
    header_map = {
         "codigo": "Código", "nome": "Nome", "instalacao": "Instalação",
         "numero_cliente": "Nº Cliente", "cpf_cnpj": "CPF/CNPJ", "cidade": "Cidade",
         "ufconsumo": "UF", "concessionaria": "Concessionaria", "fornecedora": "Fornecedora",
         "data_ativo": "Data Ativo", "dias_desde_ativacao": "Dias Ativo",
         "validado_sucesso": "Validado", "devolutiva": "Devolutiva",
         "id_licenciado": "ID Licenciado", "licenciado": "Licenciado",
         "status_pro": "Status PRO", "data_graduacao_pro": "Data PRO",
         "quantidade_boletos": "Qtd. Boletos", "injecao": "Prazo Injeção",
         "atraso_na_injecao": "Atraso na Injeção",
         "dias_em_atraso": "Dias em Atraso",
         "c.idcliente": "ID Cliente", "c.numinstalacao": "Instalação", "c.celular": "Celular",
         "regiao": "Região", "qtdeassinatura": "Assinaturas", "c.consumomedio": "Consumo Médio",
         "consumomedio": "Consumo Médio", "c.status": "Status Cliente", "dtcad": "Data Cadastro", "c.\"cpf/cnpj\"": "CPF/CNPJ",
         "c.numcliente": "Num Cliente", "c.email": "Email", "consultor_nome": "Licenciado",
         "chave_contrato": "Chave Contrato", "nome_cliente_rateio": "Nome Cliente (RZK)",
         "c.idconsultor": "ID Licenciado", "c.nome": "Nome Licenciado",
         "data_ativo_formatada": "Data Ativo",
         "data_graduacao_formatada": "Data Graduação",
         "dias_para_graduacao": "Dias para Graduar",
         "c.cpf": "CPF Licenciado", "c.uf": "UF Licenciado",
         "quantidade_clientes_ativos": "Qtd Clientes Ativos", "rcb.idrcb": "Idrcb",
         "codigo_cliente": "Codigo Cliente", "cliente_nome": "Cliente", "rcb.valorseria": "Quanto Seria",
         "rcb.valorapagar": "Valor A Pagar", "rcb.valorcomcashback": "Valor Com Cashback",
         "data_referencia": "Data Referencia", "data_vencimento": "Data Vencimento",
         "data_pagamento": "Data Pagamento", "data_vencimento_original": "Data Vencimento Original",
         "status_financeiro_cliente": "Status Financeiro Cliente", "nome_licenciado": "Licenciado",
         "celular_licenciado": "Celular Licenciado", "status_calculado": "Status Pagamento",
         "rcb.urldemonstrativo": "Url Demonstrativo", "rcb.urlboleto": "Url Boleto",
         "rcb.qrcode": "Qrcode Pix", "rcb.urlcontacemig": "Url Boleto Distribuidora",
         "valor_distribuidora": "Valor Distribuidora", "rcb.codigobarra": "Codigo Barra Boleto",
         "fornecedora_cliente": "Fornecedora Cliente", "cpf_cnpj_cliente": "Cpf Cliente",
         "rcb.nrodocumento": "Numero Documento", "rcb.idcomerc": "Idcomerc",
         "rcb.idbomfuturo": "Idbomfuturo", "rcb.energiainjetada": "Energia Injetada",
         "rcb.energiacompensada": "Energia Compensada", "rcb.energiaacumulada": "Energia Acumulada",
         "rcb.energiaajuste": "Energia Ajuste", "rcb.energiafaturamento": "Energia Faturamento",
         "c.desconto_cliente": "Desconto Cliente", "qtd_rcb_cliente": "Qt de Rcb",
         "c.rg": "RG", "c.emissor": "Emissor", "datainjecao": "Data Injeção",
         "consultor_celular": "Celular Licenciado", "c.cep": "CEP", "c.endereco": "Endereço",
         "c.numero": "Número", "c.bairro": "Bairro", "c.complemento": "Complemento",
         "c.cnpj": "CNPJ", "c.razao": "Razão Social", "c.fantasia": "Nome Fantasia",
         "c.classificacao": "Classificação", "c.keycontrato": "Key Contrato",
         "c.keysigner": "Key Signer", "c.leadidsolatio": "Lead ID Solatio",
         "c.indcli": "Indicação Cliente", "c.enviadocomerc": "Enviado Comercial", "c.obs": "Obs",
         "c.posvenda": "Pós Venda", "c.retido": "Retido", "c.contrato_verificado": "Contrato Verificado",
         "c.rateio": "Rateio", "c.validadosucesso": "Validado Sucesso", "status_sucesso": "Status Sucesso",
         "retorno_fornecedora": "Retorno Fornecedora",
         "c.documentos_enviados": "Docs Enviados", "c.link_documento": "Link Documento",
         "c.caminhoarquivo": "Caminho Arquivo", "c.caminhoarquivocnpj": "Caminho CNPJ",
         "c.caminhoarquivodoc1": "Caminho Doc 1", "c.caminhoarquivodoc2": "Caminho Doc 2",
         "c.caminhoarquivoenergia2": "Caminho Energia 2", "c.caminhocontratosocial": "Caminho CS",
         "c.caminhocomprovante": "Caminho Comprovante",
         "c.caminhoarquivoestatutoconvencao": "Caminho Estatuto", "c.senhapdf": "Senha PDF",
         "c.codigo": "Código", "c.elegibilidade": "Elegibilidade", "c.idplanopj": "ID Plano PJ",
         "dtcancelado": "Data Cancelado", "data_ativo_original": "Data Ativo Original",
         "dtnasc": "Data Nascimento", "c.origem": "Origem", "c.cm_tipo_pagamento": "CM Tipo Pagamento",
         "c.status_financeiro": "Status Financeiro", "c.logindistribuidora": "Login Distribuidora",
         "c.senhadistribuidora": "Senha Distribuidora", "c.nacionalidade": "Nacionalidade",
         "c.profissao": "Profissão", "c.estadocivil": "Estado Civil",
         "c.obs_compartilhada": "Obs Compartilhada", "c.linkassinatura1": "Link Assinatura 1",
         "dtultalteracao": "Dt Ult Alteracao", "c.celular_2": "Celular 2",
    }
    keys_order = {
        "base_clientes": [ "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "regiao", "data_ativo", "qtdeassinatura", "c.consumomedio", "c.status", "dtcad", "c.\"cpf/cnpj\"", "c.numcliente", "dtultalteracao", "c.celular_2", "c.email", "c.rg", "c.emissor", "datainjecao", "c.idconsultor", "consultor_nome", "consultor_celular", "c.cep", "c.endereco", "c.numero", "c.bairro", "c.complemento", "c.cnpj", "c.razao", "c.fantasia", "c.ufconsumo", "c.classificacao", "c.keycontrato", "c.keysigner", "c.leadidsolatio", "c.indcli", "c.enviadocomerc", "c.obs", "c.posvenda", "c.retido", "c.contrato_verificado", "c.rateio", "c.validadosucesso", "status_sucesso", "c.documentos_enviados", "c.link_documento", "c.caminhoarquivo", "c.caminhoarquivocnpj", "c.caminhoarquivodoc1", "c.caminhoarquivodoc2", "c.caminhoarquivoenergia2", "c.caminhocontratosocial", "c.caminhocomprovante", "c.caminhoarquivoestatutoconvencao", "c.senhapdf", "c.codigo", "c.elegibilidade", "c.idplanopj", "dtcancelado", "data_ativo_original", "c.fornecedora", "c.desconto_cliente", "dtnasc", "c.origem", "c.cm_tipo_pagamento", "c.status_financeiro", "c.logindistribuidora", "c.senhadistribuidora", "c.nacionalidade", "c.profissao", "c.estadocivil", "c.obs_compartilhada", "c.linkassinatura1" ],
        "rateio": [ "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "regiao", "data_ativo", "c.consumomedio", "dtcad", "c.\"cpf/cnpj\"", "c.numcliente", "c.email", "c.rg", "c.emissor", "c.cep", "consultor_nome", "c.endereco", "c.numero", "c.bairro", "c.complemento", "c.cnpj", "c.razao", "c.fantasia", "c.ufconsumo", "c.classificacao", "c.link_documento", "c.caminhoarquivo", "c.caminhoarquivocnpj", "c.caminhoarquivodoc1", "c.caminhoarquivodoc2", "c.caminhoarquivoenergia2", "c.caminhocontratosocial", "c.caminhocomprovante", "c.caminhoarquivoestatutoconvencao", "c.senhapdf", "c.fornecedora", "c.desconto_cliente", "dtnasc", "c.logindistribuidora", "c.senhadistribuidora", "nome_cliente_rateio", "c.nacionalidade", "c.profissao", "c.estadocivil" ],
        "rateio_rzk": [ "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "regiao", "data_ativo", "c.consumomedio", "devolutiva", "dtcad", "c.\"cpf/cnpj\"", "c.numcliente", "c.email", "c.rg", "c.emissor", "licenciado", "c.cep", "c.endereco", "c.numero", "c.bairro", "c.complemento", "c.cnpj", "c.razao", "c.fantasia", "c.ufconsumo", "c.classificacao", "chave_contrato", "c.link_documento", "c.caminhoarquivo", "c.caminhoarquivocnpj", "c.caminhoarquivodoc1", "c.caminhoarquivodoc2", "c.caminhoarquivoenergia2", "c.caminhocontratosocial", "c.caminhocomprovante", "c.caminhoarquivoestatutoconvencao", "c.senhapdf", "c.fornecedora", "c.desconto_cliente", "dtnasc", "c.logindistribuidora", "c.senhadistribuidora", "nome_cliente_rateio", "c.nacionalidade", "c.profissao", "c.estadocivil" ],
        "clientes_por_licenciado": [ "c.idconsultor", "c.nome", "c.cpf", "c.email", "c.uf", "quantidade_clientes_ativos" ],
        "boletos_por_cliente": [
             "codigo", "nome", "instalacao", "numero_cliente", "cpf_cnpj", "cidade",
             "ufconsumo", "concessionaria", "fornecedora",
             "consumomedio",
             "data_ativo", 
             "dias_desde_ativacao",
             "injecao",
             "atraso_na_injecao",
             "dias_em_atraso",
             "validado_sucesso", "devolutiva", "retorno_fornecedora", "id_licenciado", "licenciado", "status_pro",
             "data_graduacao_pro", "quantidade_boletos"
        ],
        "recebiveis_clientes": [ "rcb.idrcb", "codigo_cliente", "cliente_nome", "rcb.numinstalacao", "rcb.valorseria", "rcb.valorapagar", "rcb.valorcomcashback", "data_referencia", "data_vencimento", "data_pagamento", "data_vencimento_original", "c.celular", "c.email", "status_financeiro_cliente", "c.numcliente", "id_licenciado", "nome_licenciado", "celular_licenciado", "status_calculado", "rcb.urldemonstrativo", "rcb.urlboleto", "rcb.qrcode", "rcb.urlcontacemig", "valor_distribuidora", "rcb.codigobarra", "c.ufconsumo", "fornecedora_cliente", "c.concessionaria", "c.cnpj", "cpf_cnpj_cliente", "rcb.nrodocumento", "rcb.idcomerc", "rcb.idbomfuturo", "rcb.energiainjetada", "rcb.energiacompensada", "rcb.energiaacumulada", "rcb.energiaajuste", "rcb.energiafaturamento", "c.desconto_cliente", "qtd_rcb_cliente" ],
        "graduacao_licenciado": [
            "c.idconsultor",
            "c.nome",
            "data_ativo_formatada",
            "data_graduacao_formatada",
            "dias_para_graduacao"
        ]
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
                header = key.split(' AS ')[-1].strip().replace('_', ' ').replace('"', '').title()
                missing_in_map.append(key)
        headers_list.append(header)

    if missing_in_map:
        logger.debug(f"Chaves/Aliases não encontrados em header_map (usado fallback) para '{report_type}': {missing_in_map}")

    return headers_list

# --- FIM DA FUNÇÃO get_headers ---