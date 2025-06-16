import streamlit as st
import pandas as pd
from datetime import datetime
import logging
import os
import subprocess
import sys
import io
import xlsxwriter
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from typing import Optional, Any, Dict, List, Union
import numpy as np # Importar numpy explicitamente
import base64 # Importar base64 explicitamente
import re # Importar o módulo 're' para expressões regulares

import followup_db_manager as db_manager # Importa o módulo db_manager

# Configura o logger
logger = logging.getLogger(__name__)

def set_background_image(image_path):
    """Define uma imagem de fundo para o aplicativo Streamlit com opacidade."""
    try:
        with open(image_path, "rb") as image_file:
            encoded_string = base64.b64encode(image_file.read()).decode()
        st.markdown(
            f"""
            <style>
            .stApp {{
                background-color: transparent !important; /* Garante que o fundo do app seja transparente */
            }}
            .stApp::before {{
                content: "";
                position: fixed;
                top: 0;
                left: 0;
                width: 100%;
                height: 100%;
                background-image: url("data:image/png;base64,{encoded_string}");
                background-size: cover;
                background-position: center;
                background-repeat: no-repeat;
                background-attachment: fixed;
                opacity: 0.50; /* Opacidade ajustada para 30% */
                z-index: -1; /* Garante que o pseudo-elemento fique atrás do conteúdo */
            }}
            </style>
            """,
            unsafe_allow_html=True
        )
    except FileNotFoundError:
        st.warning(f"A imagem de fundo não foi encontrada no caminho: {image_path}")
    except Exception as e:
        st.error(f"Erro ao carregar a imagem de fundo: {e}")
        
# Define a classe MockDbUtils globalmente, para evitar redeclarações
class MockDbUtils:
    """Classe Mock para simular funcionalidades do db_utils quando o módulo real não está disponível."""
    def get_db_path(self, db_name: str) -> str:
        _base_path = os.path.dirname(os.path.abspath(__file__))
        _app_root_path = os.path.dirname(_base_path) if os.path.basename(_base_path) == 'app_logic' else _base_path
        _DEFAULT_DB_FOLDER = "data"
        return os.path.join(_app_root_path, _DEFAULT_DB_FOLDER, f"{db_name}.db")
    
    def get_declaracao_by_id(self, di_id: int) -> Optional[dict]:
        """Função mock para simulação de obtenção de DI por ID."""
        if di_id == 999: # Exemplo de DI mock
            return {'numero_di': '9988776654', 'id': 999}
        return None 
    
    def get_declaracao_by_referencia(self, process_number: str) -> Optional[dict]:
        """Função mock para simulação de obtenção de DI por número de processo."""
        if process_number == "MOCK-DI-123": # Exemplo de DI mock
            return {'numero_di': '9988776654', 'id': 999}
        return None
    def get_ncm_item_by_ncm_code(self, ncm_code: str) -> Optional[dict]:
        """Função mock para simulação de obtenção de dados NCM por código."""
        # Mock para NCMs conhecidos
        if ncm_code == "85171231":
            return {
                'ncm_code': '85171231', 'descricao_item': 'Telefones celulares',
                'ii_aliquota': 16.0, 'ipi_aliquota': 5.0, 'pis_aliquota': 1.65,
                'cofins_aliquota': 7.6, 'icms_aliquota': 18.0
            }
        return None

    def selecionar_todos_ncm_itens(self) -> List[Dict[str, Any]]:
        """Função mock para simulação de obtenção de todos os itens NCM."""
        return [
            {'ID': 1, 'ncm_code': '85171231', 'descricao_item': 'Telefones celulares', 'ii_aliquota': 16.0, 'ipi_aliquota': 5.0, 'pis_aliquota': 1.65, 'cofins_aliquota': 7.6, 'icms_aliquota': 18.0},
            {'ID': 2, 'ncm_code': '84713012', 'descricao_item': 'Notebooks', 'ii_aliquota': 10.0, 'ipi_aliquota': 0.0, 'pis_aliquota': 1.65, 'cofins_aliquota': 7.6, 'icms_aliquota': 18.0},
        ]

# Importa db_utils real, ou usa o mock se houver erro
db_utils: Union[Any, MockDbUtils] 
try:
    import db_utils # type: ignore # Ignora o erro de importação se o módulo não for encontrado inicialmente
    if not hasattr(db_utils, 'get_declaracao_by_id') or \
       not hasattr(db_utils, 'get_declaracao_by_referencia') or \
       not hasattr(db_utils, 'get_ncm_item_by_ncm_code') or \
       not hasattr(db_utils, 'selecionar_todos_ncm_itens'):
        raise ImportError("db_utils real não contém todas as funções esperadas.")
    # Se a importação e verificação forem bem-sucedidas, db_utils será o módulo real.
except ImportError:
    # Se a importação falhar, usa o MockDbUtils
    db_utils = MockDbUtils()
    logging.warning("Módulo 'db_utils' não encontrado ou incompleto. Usando MockDbUtils.")
except Exception as e:
    # Fallback para MockDbUtils se qualquer outro erro ocorrer durante a importação real do db_utils
    db_utils = MockDbUtils() 
    logging.error(f"Erro ao importar ou inicializar 'db_utils': {e}. Usando MockDbUtils.")

# Importar pdf_analyzer_page.py e ncm_list_page para reuso de funções
try:
    from app_logic import pdf_analyzer_page
    import pdfplumber # Import pdfplumber here as it's used in this module directly
except ImportError:
    logging.warning("Módulo 'pdf_analyzer_page' ou 'pdfplumber' não encontrado. Funções de análise de PDF não estarão disponíveis.")
    pdf_analyzer_page = None # Define como None se não puder ser importado

try:
    from app_logic import ncm_list_page
except ImportError:
    logging.warning("Módulo 'ncm_list_page' não encontrado. Funções NCM não estarão disponíveis.")
    ncm_list_page = None # Define como None se não puder ser importado


# Configura o logger
logger = logging.getLogger(__name__)

# --- Funções Auxiliares para Formatação ---
def _format_di_number(di_number: Optional[str]) -> str:
    """Formata o número da DI para o padrão **/*******-*."""
    if di_number and isinstance(di_number, str) and len(di_number) == 10:
        return f"{di_number[0:2]}/{di_number[2:9]}-{di_number[9]}"
    return di_number if di_number is not None else ""

def _get_di_number_from_id(di_id: Optional[int]) -> str:
    """Obtém o número da DI a partir do seu ID no banco de dados de XML DI."""
    if di_id is None:
        return "N/A"
    try:
        di_data = db_utils.get_declaracao_by_id(di_id)
        if di_data:
            # Garante que di_data é um dicionário ou se comporta como um
            if isinstance(di_data, dict):
                return _format_di_number(str(di_data.get('numero_di')))
            else: # Assumindo que é um sqlite3.Row object
                return _format_di_number(str(di_data['numero_di']))
    except Exception as e:
        logger.error(f"Erro ao buscar DI por ID {di_id}: {e}")
    return "DI Não Encontrada"

def _display_message_box(message: str, type: str = "info"):
    """Exibe uma caixa de mensagem customizada (substitui alert())."""
    if type == "info":
        st.info(message)
    elif type == "success":
        st.success(message)
    elif type == "warning":
        st.warning(message)
    elif type == "error":
        st.error(message)

# --- Funções de Cálculo ---
def get_ncm_taxes(ncm_code: str) -> Dict[str, float]:
    """Busca as alíquotas de impostos para um dado NCM."""
    ncm_data_raw = db_utils.get_ncm_item_by_ncm_code(ncm_code)
    # Convert sqlite3.Row object to dictionary if it's not None
    ncm_data = dict(ncm_data_raw) if ncm_data_raw else None

    if ncm_data:
        return {
            'ii_aliquota': ncm_data.get('ii_aliquota', 0.0),
            'ipi_aliquota': ncm_data.get('ipi_aliquota', 0.0),
            'pis_aliquota': ncm_data.get('pis_aliquota', 0.0),
            'cofins_aliquota': ncm_data.get('cofins_aliquota', 0.0),
            'icms_aliquota': ncm_data.get('icms_aliquota', 0.0) # ICMS pode ser digitável ou buscado
        }
    return {'ii_aliquota': 0.0, 'ipi_aliquota': 0.0, 'pis_aliquota': 0.0, 'cofins_aliquota': 0.0, 'icms_aliquota': 0.0}

def calculate_item_taxes_and_values(item: Dict[str, Any], dolar_brl: float, total_invoice_value_usd: float, total_invoice_weight_kg: float, estimativa_frete_usd: float, estimativa_seguro_brl: float) -> Dict[str, Any]:
    """
    Calcula o VLMD, impostos e rateios para um item individual.
    Retorna o item com os campos de impostos atualizados e valores rateados.
    """
    item_qty = float(item.get('Quantidade', 0))
    item_unit_value_usd = float(item.get('Valor Unitário', 0))
    item_value_usd = item_qty * item_unit_value_usd
    item_weight_kg = float(item.get('Peso Unitário', 0)) * item_qty

    # Para evitar divisão por zero
    # Use max(1, ...) para garantir que o divisor nunca seja zero e evitar erros
    value_ratio = item_value_usd / max(1, total_invoice_value_usd)
    weight_ratio = item_weight_kg / max(1, total_invoice_weight_kg)

    # Rateio de frete e seguro
    frete_rateado_usd = estimativa_frete_usd * value_ratio
    seguro_rateado_brl = estimativa_seguro_brl * weight_ratio

    # NCM e impostos
    ncm_code = str(item.get('NCM', ''))
    ncm_taxes = get_ncm_taxes(ncm_code)

    # VLMD_Item (Valor da Mercadoria no Local de Desembaraço)
    # Considera o valor em USD, frete e seguro rateados convertidos para BRL
    vlmd_item = (item_unit_value_usd * item_qty * dolar_brl) + (frete_rateado_usd * dolar_brl) + seguro_rateado_brl
    
    # Cálculos de impostos
    item['Estimativa_II_BR'] = vlmd_item * (ncm_taxes['ii_aliquota'] / 100)
    item['Estimativa_IPI_BR'] = (vlmd_item + item['Estimativa_II_BR']) * (ncm_taxes['ipi_aliquota'] / 100)
    item['Estimativa_PIS_BR'] = vlmd_item * (ncm_taxes['pis_aliquota'] / 100)
    item['Estimativa_COFINS_BR'] = vlmd_item * (ncm_taxes['cofins_aliquota'] / 100)
    
    # ICMS é digitável, então, se já existir no item, mantém. Caso contrário, usa a alíquota do NCM.
    # IMPORTANT: The ICMS percentage from NCM is used for per-item calculation.
    # The global ICMS field on "Valores e Estimativas" is a manual override for the total, not a percentage override for items.
    item['Estimativa_ICMS_BR'] = (vlmd_item * (ncm_taxes['icms_aliquota'] / 100))

    item['VLMD_Item'] = vlmd_item # Adicionar para referência
    item['Frete_Rateado_USD'] = frete_rateado_usd
    item['Seguro_Rateado_BRL'] = seguro_rateado_brl

    return item

# --- Lógica para Salvar Processo ---
def _save_process_action(process_id_from_form_load: Optional[int], edited_data: dict, is_new_process_flag: bool, form_state_key: str) -> Optional[int]:
    """
    Lógica para salvar ou atualizar um processo.
    Retorna o ID do processo salvo/atualizado.
    """
    db_col_names_full = db_manager.obter_nomes_colunas_db()
    
    data_to_save_dict = {col: None for col in db_col_names_full if col != 'id'}

    for col_name, value in edited_data.items():
        if col_name in data_to_save_dict:
            if isinstance(value, datetime):
                data_to_save_dict[col_name] = value.strftime("%Y-%m-%d")
            elif isinstance(value, str) and value.strip() == '':
                data_to_save_dict[col_name] = None
            elif isinstance(value, (float, int)) and pd.isna(value):
                data_to_save_dict[col_name] = None
            else:
                data_to_save_dict[col_name] = value
        else:
            logger.warning(f"Campo '{col_name}' do formulário não corresponde a uma coluna no DB. Ignorado.")

    if 'Status_Arquivado' in db_col_names_full:
        if is_new_process_flag: # Use the flag
            data_to_save_dict['Status_Arquivado'] = 'Não Arquivado'
        else:
            original_process_data = db_manager.obter_processo_por_id(process_id_from_form_load if process_id_from_form_load is not None else -1)
            if original_process_data:
                original_status_arquivado = dict(original_process_data).get('Status_Arquivado')
                data_to_save_dict['Status_Arquivado'] = original_status_arquivado
            else:
                logger.warning(f"Processo ID {process_id_from_form_load} não encontrado ao tentar obter Status_Arquivado original para salvar.")
                data_to_save_dict['Status_Arquivado'] = 'Não Arquivado'
    
    # Tratamento para campo 'Caminho_da_pasta'
    if 'Caminho_da_pasta' in db_col_names_full:
        data_to_save_dict['Caminho_da_pasta'] = edited_data.get('Caminho_da_pasta')

    if 'DI_ID_Vinculada' in db_col_names_full:
        if 'DI_ID_Vinculada' in edited_data and edited_data['DI_ID_Vinculada'] is not None:
            data_to_save_dict['DI_ID_Vinculada'] = edited_data['DI_ID_Vinculada']
        elif not is_new_process_flag: # Use the flag
            original_process_data = db_manager.obter_processo_por_id(process_id_from_form_load if process_id_from_form_load is not None else -1)
            if original_process_data:
                data_to_save_dict['DI_ID_Vinculada'] = dict(original_process_data).get('DI_ID_Vinculada')
            else:
                data_to_save_dict['DI_ID_Vinculada'] = None
        else:
            data_to_save_dict['DI_ID_Vinculada'] = None

    user_info = st.session_state.get('user_info', {'username': 'Desconhecido'})
    data_to_save_dict['Ultima_Alteracao_Por'] = user_info.get('username')
    data_to_save_dict['Ultima_Alteracao_Em'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Adicionar Estimativa_Impostos_Total ao data_to_save_dict
    if 'Estimativa_Impostos_Total' in edited_data and 'Estimativa_Impostos_Total' in db_col_names_full:
        data_to_save_dict['Estimativa_Impostos_Total'] = edited_data['Estimativa_Impostos_Total']
    
    # Adicionar Quantidade_Containers ao data_to_save_dict
    if 'Quantidade_Containers' in edited_data and 'Quantidade_Containers' in db_col_names_full:
        data_to_save_dict['Quantidade_Containers'] = edited_data['Quantidade_Containers']

    # Adicionar Estimativa_Impostos_BR, se presente no edited_data (do form)
    if 'Estimativa_Impostos_BR' in edited_data and 'Estimativa_Impostos_BR' in db_col_names_full:
        data_to_save_dict['Estimativa_Impostos_BR'] = edited_data['Estimativa_Impostos_BR']

    # Ensure 'Processo_Novo' is handled correctly, especially for cloning
    if 'Processo_Novo' in edited_data and 'Processo_Novo' in db_col_names_full:
        data_to_save_dict['Processo_Novo'] = edited_data['Processo_Novo']
    else:
        # Fallback if Processo_Novo is missing, though it should always be present
        logger.warning("Processo_Novo não encontrado nos dados editados.")
        data_to_save_dict['Processo_Novo'] = "N/A" # or raise an error

    final_data_tuple = tuple(data_to_save_dict[col] for col in db_col_names_full if col != 'id')

    success = False
    actual_process_id_after_save: Optional[int] = None # Will store the final ID, whether new or existing

    try:
        if is_new_process_flag: # This branch is for NEW creation (including clones)
            # When cloning or creating new, always insert.
            # No need to use process_id_from_form_load here, as we are inserting a NEW record.
            # Check for duplicate 'Processo_Novo' before attempting insertion
            existing_process = db_manager.obter_processo_by_processo_novo(data_to_save_dict['Processo_Novo'])
            if existing_process:
                _display_message_box(f"Falha ao adicionar processo: Já existe um processo com a referência '{data_to_save_dict['Processo_Novo']}'. Por favor, altere a referência do processo clonado.", "error")
                return None # Return None as save failed
            
            success = db_manager.inserir_processo(data_to_save_dict) # Pass dictionary for Firestore compatibility
            if success:
                # Firestore returns the doc ID (which is Processo_Novo), SQLite returns lastrowid
                if db_manager._USE_FIRESTORE_AS_PRIMARY and db_manager.db_utils.db_firestore:
                    actual_process_id_after_save = data_to_save_dict['Processo_Novo'] # Processo_Novo is the ID for Firestore
                else:
                    actual_process_id_after_save = db_manager.obter_ultimo_processo_id()
                
                if not actual_process_id_after_save:
                    logger.error("Falha ao obter o ID do novo processo após a inserção.")
                    success = False # Mark as failed if new ID couldn't be retrieved
            
        else: # This branch is for UPDATING an EXISTING process
            if process_id_from_form_load is None:
                logger.error("Tentativa de atualizar processo existente sem ID de processo fornecido.")
                _display_message_box("Erro: ID do processo não encontrado para atualização.", "error")
                return None
            success = db_manager.atualizar_processo(process_id_from_form_load, data_to_save_dict) # Pass dictionary for Firestore compatibility
            actual_process_id_after_save = process_id_from_form_load # For update, the ID remains the same

    except Exception as e:
        logger.exception(f"Erro de banco de dados durante a operação de salvar/atualizar processo.")
        _display_message_box(f"Erro no banco de dados ao salvar processo: {e}", "error")
        success = False

    if success:
        # Save items related to the 'actual_process_id_after_save'
        if actual_process_id_after_save is not None and 'process_items_data' in st.session_state:
            db_manager.deletar_itens_processo(processo_id=actual_process_id_after_save)
            for item in st.session_state.process_items_data:
                db_manager.inserir_item_processo(
                    processo_id=actual_process_id_after_save, # Use the actual ID
                    codigo_interno=item.get('Código Interno'),
                    ncm=item.get('NCM'),
                    cobertura=item.get('Cobertura'),
                    sku=item.get('SKU'),
                    quantidade=item.get('Quantidade'),
                    peso_unitario=item.get('Peso Unitário'),
                    valor_unitario=item.get('Valor Unitário'),
                    valor_total_item=item.get('Valor total do item'), # O valor total do item que já foi calculado
                    estimativa_ii_br=item.get('Estimativa_II_BR'),
                    estimativa_ipi_br=item.get('Estimativa_IPI_BR'),
                    estimativa_pis_br=item.get('Estimativa_PIS_BR'),
                    estimativa_cofins_br=item.get('Estimativa_COFINS_BR'),
                    estimativa_icms_br=item.get('Estimativa_ICMS_BR'),
                    frete_rateado_usd=item.get('Frete_Rateado_USD'),
                    seguro_rateado_brl=item.get('Seguro_Rateado_BRL'),
                    vlmd_item=item.get('VLMD_Item'),
                    denominacao_produto=item.get('Denominação do produto'),
                    detalhamento_complementar_produto=item.get('Detalhamento complementar do produto')
                )
        
        _display_message_box(f"Processo {'adicionado' if is_new_process_flag else 'atualizado'} com sucesso!", "success") # Use the flag
        
        # Limpar o estado específico do formulário que acabou de ser processado
        if form_state_key in st.session_state:
            del st.session_state[form_state_key]
        
        # Resetar estados relacionados a itens/upload para uma nova interação do formulário
        st.session_state.show_add_item_popup = False
        st.session_state.process_items_data = []
        st.session_state.last_processed_upload_key = None
        st.session_state.process_items_loaded_for_id = None
        st.session_state.total_invoice_value_usd = 0.0
        st.session_state.total_invoice_weight_kg = 0.0

        return actual_process_id_after_save # Retorna o ID do processo salvo/atualizado
    else:
        # A mensagem de erro específica já foi exibida pelo bloco try-except ou pela validação de duplicidade.
        # Aqui apenas redefinimos flags para permitir uma nova tentativa.
        st.session_state.form_is_cloning = False
        st.session_state.last_cloned_from_id = None
        return None # Retorna None em caso de falha


# Define a standard schema for items
DEFAULT_ITEM_SCHEMA = {
    "Código Interno": None,
    "NCM": None,
    "Cobertura": "NÃO",
    "SKU": None,
    "Quantidade": 0,
    "Peso Unitário": 0.0,
    "Valor Unitário": 0.0, # Este será o 'Preço'
    "Valor total do item": 0.0,
    "Estimativa_II_BR": 0.0,
    "Estimativa_IPI_BR": 0.0,
    "Estimativa_PIS_BR": 0.0,
    "Estimativa_COFINS_BR": 0.0,
    "Estimativa_ICMS_BR": 0.0,
    "Frete_Rateado_USD": 0.0,
    "Seguro_Rateado_BRL": 0.0,
    "VLMD_Item": 0.0,
    "Denominação do produto": None,
    "Detalhamento complementar do produto": None,
    "Fornecedor": None,
    "Invoice N#": None
}

def _standardize_item_data(item_dict: Dict[str, Any], fornecedor: Optional[str] = None, invoice_n: Optional[str] = None) -> Dict[str, Any]:
    """
    Ensures an item dictionary conforms to the default schema,
    mapping database snake_case keys to schema Capitalized With Spaces keys.
    """
    db_to_schema_map = {
        "codigo_interno": "Código Interno",
        "ncm": "NCM",
        "cobertura": "Cobertura",
        "sku": "SKU",
        "quantidade": "Quantidade",
        "peso_unitario": "Peso Unitário",
        "valor_unitario": "Valor Unitário",
        "valor_total_item": "Valor total do item",
        "estimativa_ii_br": "Estimativa_II_BR",
        "estimativa_ipi_br": "Estimativa_IPI_BR",
        "estimativa_pis_br": "Estimativa_PIS_BR",
        "estimativa_cofins_br": "Estimativa_COFINS_BR",
        "estimativa_icms_br": "Estimativa_ICMS_BR",
        "frete_rateado_usd": "Frete_Rateado_USD",
        "seguro_rateado_brl": "Seguro_Rateado_BRL",
        "vlmd_item": "VLMD_Item",
        "denominacao_produto": "Denominação do produto",
        "detalhamento_complementar_produto": "Detalhamento complementar do produto",
    }

    standardized_item = DEFAULT_ITEM_SCHEMA.copy()

    # Apply mapping from DB keys to schema keys
    for db_key, schema_key in db_to_schema_map.items():
        if db_key in item_dict:
            standardized_item[schema_key] = item_dict[db_key]

    # Handle Fornecedor and Invoice N# which might come from the main process data
    if fornecedor is not None:
        standardized_item['Fornecedor'] = fornecedor
    if invoice_n is not None:
        standardized_item['Invoice N#'] = invoice_n

    # Ensure other keys from DEFAULT_ITEM_SCHEMA are present with their default values
    # if they were not explicitly mapped or provided.
    for key, default_value in DEFAULT_ITEM_SCHEMA.items():
        if key not in standardized_item:
            standardized_item[key] = default_value

    return standardized_item

def _import_items_from_excel(uploaded_file: Any, current_fornecedor_context: str, current_invoice_n_context: str) -> bool:
    """
    Importa itens de um arquivo Excel/CSV local e os adiciona à lista de itens do processo.
    """
    if uploaded_file is None:
        return False

    file_extension = os.path.splitext(uploaded_file.name)[1]
    df = None

    try:
        if file_extension.lower() in ('.csv'):
            try:
                df = pd.read_csv(uploaded_file, encoding='utf-8')
            except UnicodeDecodeError:
                df = pd.read_csv(uploaded_file, encoding='latin-1')
            except Exception:
                df = pd.read_csv(uploaded_file, sep=';')
        elif file_extension.lower() in ('.xlsx', '.xls'):
            df = pd.read_excel(uploaded_file)
        else:
            _display_message_box("Formato de arquivo não suportado. Por favor, use .csv, .xls ou .xlsx.", "error")
            return False

        if df.empty:
            _display_message_box("O arquivo importado está vazio.", "warning")
            return False

        # Mapeamento das colunas do Excel para o schema interno
        column_mapping_excel_to_internal = {
            "Cobertura": "Cobertura",
            "Codigo interno": "Código Interno",
            "Denominação": "Denominação do produto", # Ajustado para o nome da coluna no template
            "SKU": "SKU",
            "Quantidade": "Quantidade",
            "Preço": "Valor Unitário", # Mapeia 'Preço' do Excel para 'Valor Unitário'
            "NCM": "NCM",
        }
        
        # Renomeia as colunas do DataFrame para o formato interno
        df_renamed = df.rename(columns=column_mapping_excel_to_internal, errors='ignore')

        # Converte as colunas numéricas para o tipo correto e trata nulos ANTES de qualquer cálculo
        numeric_cols_to_convert = ["Quantidade", "Valor Unitário", "Peso Unitário"]
        for col in numeric_cols_to_convert:
            if col in df_renamed.columns:
                df_renamed[col] = pd.to_numeric(df_renamed[col], errors='coerce').fillna(0).astype(float)


        # Lista para armazenar os novos itens padronizados
        new_items_from_file = []

        # Itera sobre as linhas do DataFrame para padronizar e calcular
        for index, row in df_renamed.iterrows():
            item_data = row.to_dict()
            
            # Limpa o NCM de caracteres não numéricos
            if 'NCM' in item_data and item_data['NCM'] is not None:
                item_data['NCM'] = re.sub(r'\D', '', str(item_data['NCM']))

            # Padroniza o item com base no schema DEFAULT_ITEM_SCHEMA
            standardized_item = _standardize_item_data(item_data, current_fornecedor_context, current_invoice_n_context)
            
            # Garante que Quantidade e Valor Unitário são números antes de calcular Valor total do item
            qty = standardized_item.get('Quantidade', 0) # Já é float devido ao tratamento acima
            unit_val = standardized_item.get('Valor Unitário', 0.0) # Já é float
            standardized_item["Valor total do item"] = qty * unit_val
            
            new_items_from_file.append(standardized_item)
        
        # LIMPAR A LISTA DE ITENS ANTES DE ADICIONAR OS NOVOS DO ARQUIVO
        st.session_state.process_items_data = [] 
        st.session_state.process_items_data.extend(new_items_from_file)
        
        _display_message_box(f"{len(new_items_from_file)} itens importados com sucesso do arquivo!", "success")
        return True

    except Exception as e:
        _display_message_box(f"Erro ao processar o arquivo Excel/CSV: {e}", "error")
        logger.exception("Erro durante a importação de itens do arquivo.")
        return False

def _generate_items_excel_template():
    """Gera um arquivo Excel padrão para inserção de dados de itens de processo."""
    # Ajustada a coluna para "Denominação" para corresponder ao schema interno e facilitar o mapeamento
    template_columns = ["Cobertura", "Codigo interno", "Denominação", "SKU", "Quantidade", "Preço", "NCM"]
    df_template = pd.DataFrame(columns=template_columns)

    # Adicionar uma linha de exemplo
    example_row = {
        "Cobertura": "NÃO",
        "Codigo interno": "INT-001",
        "Denominação": "Processador Intel Core i7", # Corrigido para "Denominação"
        "SKU": "CPU-I7-12700K",
        "Quantidade": 5,
        "Preço": 350.00,
        "NCM": "84715010"
    }
    df_template = pd.concat([df_template, pd.DataFrame([example_row])], ignore_index=True)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_template.to_excel(writer, index=False, sheet_name='Template Itens')
    writer.close()
    output.seek(0)

    return output

# Mover a definição de campos_config_tabs para fora da função show_process_form_page
# para garantir que ela esteja sempre definida e acessível.
campos_config_tabs = {
    "Dados Gerais": {
        "col1": {
            "Processo_Novo": {"label": "Processo:", "type": "text"},
            "Fornecedor": {"label": "Fornecedor:", "type": "text"},
            "Tipos_de_item": {"label": "Tipos de item:", "type": "text"},
            "N_Invoice": {"label": "Nº Invoice:", "type": "text"},
            "Quantidade": {"label": "Quantidade:", "type": "number"},
            "N_Ordem_Compra": {"label": "Nº da Ordem de Compra:", "type": "text"},
            "Agente_de_Carga_Novo": {"label": "Agente de Carga:", "type": "text"},
        },
        "col2": {
            "Modal": {"label": "Modal:", "type": "dropdown", "values": ["", "Aéreo", "Maritimo"]},
            "Navio": {"label": "Navio:", "type": "text", "conditional_field": "Modal", "conditional_value": "Maritimo"}, 
            "Quantidade_Containers": {"label": "Quantidade de Containers:", "type": "number", "conditional_field": "Modal", "conditional_value": "Maritimo"}, # Novo campo
            "Origem": {"label": "Origem:", "type": "text"},
            "Destino": {"label": "Destino:", "type": "text"},
            "INCOTERM": {"label": "INCOTERM:", "type": "dropdown", "values": ["","EXW","FCA","FAS","FOB","CFR","CIF","CPT","CIP","DPU","DAP","DDP"]},
            "Comprador": {"label": "Comprador:", "type": "text"},
        }
    },
    "Itens": {}, # This tab will now only display the data_editor and totals
    "Valores e Estimativas": {
        "Estimativa_Dolar_BRL": {"label": "Cambio Estimado (R$):", "type": "currency_br"},
        "Valor_USD": {"label": "Valor (USD):", "type": "currency_usd", "disabled": True}, # Desabilitado
        "Pago": {"label": "Pago?:", "type": "dropdown", "values": ["Não", "Sim"]},
        "Estimativa_Frete_USD": {"label": "Estimativa de Frete (USD):", "type": "currency_usd"},
        "Estimativa_Seguro_BRL": {"label": "Estimativa Seguro (R$):", "type": "currency_br"},
        "Estimativa_II_BR": {"label": "Estimativa de II (R$):", "type": "currency_br", "disabled": True},
        "Estimativa_IPI_BR": {"label": "Estimativa de IPI (R$):", "type": "currency_br", "disabled": True},
        "Estimativa_PIS_BR": {"label": "Estimativa de PIS (R$):", "type": "currency_br", "disabled": True},
        "Estimativa_COFINS_BR": {"label": "Estimativa de COFINS (R$):", "type": "currency_br", "disabled": True},
        "Estimativa_ICMS_BR": {"label": "Estimativa de ICMS (R$):", "type": "currency_br"}, # Digitável
        "Estimativa_Impostos_Total": {"label": "Estimativa Impostos (R$):", "type": "currency_br", "disabled": True}, # Novo campo
        "Estimativa_Impostos_BR": {"label": "Estimativa Impostos (Antigo):", "type": "currency_br", "disabled": True}, # Adicionado para corresponder ao DB
    },
    "Status Operacional": {
        "Status_Geral": {"label": "Status Geral:", "type": "dropdown", "values": db_manager.STATUS_OPTIONS},
        "Data_Compra": {"label": "Data de Compra:", "type": "date"},
        "Data_Embarque": {"label": "Data de Embarque:", "type": "date"},
        "ETA_Recinto": {"label": "ETA no Recinto:", "type": "date"},
        "Data_Registro": {"label": "Data de Registro:", "type": "date"},
        "Previsao_Pichau": {"label": "Previsão na Pichau:", "type": "date"},
        "Documentos_Revisados": {"label": "Documentos Revisados:", "type": "dropdown", "values": ["Não", "Sim"]},
        "Conhecimento_Embarque": {"label": "Conhecimento de embarque:", "type": "dropdown", "values": ["Não", "Sim"]},
        "Descricao_Feita": {"label": "Descrição Feita:", "type": "dropdown", "values": ["Não", "Sim"]},
        "Descricao_Enviada": {"label": "Descrição Enviada:", "type": "dropdown", "values": ["Não", "Sim"]},
        "Nota_feita": {"label": "Nota feita?:", "type": "dropdown", "values": ["Não", "Sim"]},
        "Conferido": {"label": "Conferido?:", "type": "dropdown", "values": ["Não", "Sim"]},
    },
    "Documentação": {
        "Caminho_da_pasta": {"label": "Caminho da pasta:", "type": "folder_path", "placeholder": "Caminho ou URL para documentos"},
        "DI_ID_Vinculada": {"label": "DI Vinculada (ID):", "type": "text", "disabled": True, "help": "ID da Declaração de Importação vinculada a este processo."},
    }
}


def show_clonagem_processo_page(process_identifier: Optional[Any] = None, reload_processes_callback: Optional[callable] = None, is_cloning: bool = False):
    
    background_image_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'assets', 'logo_navio_atracado.png')
    set_background_image(background_image_path)
    """
    Exibe o formulário de edição/criação de processo em uma página dedicada.
    process_identifier: ID (int) ou Processo_Novo (str) do processo a ser editado. None para novo processo.
    reload_processes_callback: Função para chamar na página principal para recarregar os dados.
    is_cloning: Se True, indica que a operação é de clonagem.
    """
    if reload_processes_callback:
        st.session_state.form_reload_processes_callback = reload_processes_callback

    # Flag para saber se estamos em um "novo" processo (inclui clones)
    # Esta é a flag local da função, será usada para carregar os dados
    is_new_process = process_identifier is None or is_cloning

    # Inicializa process_data e process_id
    process_data: Dict[str, Any] = {}
    process_id: Optional[int] = None

    # Gerenciamento de estado do formulário para evitar recargas desnecessárias e manter dados
    # A chave do estado do formulário deve refletir a identidade única do que está sendo exibido.
    # Para edição, é o ID do processo. Para um novo processo (incluindo clones), pode ser uma string temporária.
    form_state_id_key_base = process_identifier if not is_cloning else f"new_clone_from_{process_identifier}"
    if process_identifier is None and not is_cloning: # Se é um novo processo em branco
        form_state_id_key_base = 'new_empty_process_form_instance'

    form_state_key = f"form_fields_process_{form_state_id_key_base}"

    # Reinicializa o estado do formulário SOMENTE se estivermos carregando um processo diferente
    # ou se for um novo processo/clone que ainda não foi inicializado.
    if (form_state_key not in st.session_state or 
        st.session_state.get('current_loaded_form_key') != form_state_key):
        
        st.session_state[form_state_key] = {}
        st.session_state.current_loaded_form_key = form_state_key # Armazena a chave do formulário carregado
        
        # Inicialização explícita de process_items_loaded_for_id
        st.session_state.process_items_loaded_for_id = None

        # A flag is_new_process_flag no session_state para esta instância do formulário
        st.session_state[f'{form_state_key}_is_new_process_flag'] = is_new_process

        if is_cloning and process_identifier is not None:
            # Se for clonagem, carrega os dados do processo original
            raw_data = None
            if isinstance(process_identifier, int):
                raw_data = db_manager.obter_processo_por_id(process_identifier)
            elif isinstance(process_identifier, str):
                raw_data = db_manager.obter_processo_by_processo_novo(process_identifier)
            
            if raw_data:
                process_data = dict(raw_data)
                # Modifica Processo_Novo para a versão clonada NO ESTADO DO FORMULÁRIO
                cloned_process_novo = f"{process_data.get('Processo_Novo', 'NovoProcesso')}_Clone"
                st.session_state[form_state_key]['Processo_Novo'] = cloned_process_novo
                
                # Carrega e padroniza os itens do processo original para a clonagem
                st.session_state.process_items_data = [
                    _standardize_item_data(dict(row), process_data.get("Fornecedor"), process_data.get("N_Invoice")) 
                    for row in db_manager.obter_itens_processo(process_identifier) # Usa o ID original para buscar os itens
                ]
                st.session_state.process_items_loaded_for_id = None # Limpa para que não haja confusão com o original
                
                # Popula os outros campos do formulário com os dados do processo original
                for tab_config in campos_config_tabs.values():
                    for col_group in ["col1", "col2"]:
                        if col_group in tab_config:
                            for field_name, config in tab_config[col_group].items():
                                if field_name != 'Processo_Novo': # Já tratamos Processo_Novo
                                    st.session_state[form_state_key][field_name] = process_data.get(field_name, config.get("default_value", None))
                    else: # para as outras tabs
                        for field_name, config in tab_config.items():
                             st.session_state[form_state_key][field_name] = process_data.get(field_name, config.get("default_value", None))
                st.session_state[form_state_key]["Observacao"] = process_data.get("Observacao", "")

            else:
                _display_message_box(f"Processo '{process_identifier}' não encontrado para clonagem.", "error")
                st.session_state.current_page = "Follow-up Importação"
                st.rerun()
                return
        elif not is_new_process: # Editando um processo existente
            raw_data = None
            if isinstance(process_identifier, int):
                raw_data = db_manager.obter_processo_por_id(process_identifier)
            elif isinstance(process_identifier, str):
                raw_data = db_manager.obter_processo_by_processo_novo(process_identifier)
            
            if raw_data:
                process_data = dict(raw_data) # Garante que seja um dicionário
                process_id = process_data.get('id')

                # Popula o estado do formulário com os dados do processo existente
                for tab_config in campos_config_tabs.values():
                    for col_group in ["col1", "col2"]:
                        if col_group in tab_config:
                            for field_name, config in tab_config[col_group].items():
                                st.session_state[form_state_key][field_name] = process_data.get(field_name, config.get("default_value", None))
                    else:
                        for field_name, config in tab_config.items():
                             st.session_state[form_state_key][field_name] = process_data.get(field_name, config.get("default_value", None))
                st.session_state[form_state_key]["Observacao"] = process_data.get("Observacao", "")

                # Carrega os itens do processo apenas se o ID for diferente do que foi carregado anteriormente
                if process_id is not None and process_id != st.session_state.process_items_loaded_for_id:
                    st.session_state.process_items_data = [
                        _standardize_item_data(dict(row), process_data.get("Fornecedor"), process_data.get("N_Invoice")) 
                        for row in db_manager.obter_itens_processo(process_id)
                    ]
                    st.session_state.process_items_loaded_for_id = process_id
            else:
                _display_message_box(f"Processo '{process_identifier}' não encontrado para edição.", "error")
                st.session_state.current_page = "Follow-up Importação"
                st.rerun()
                return
        else: # Novo processo em branco: inicializa com valores padrão
            st.session_state.process_items_data = []
            st.session_state.process_items_loaded_for_id = None
            process_data = {} # Garante que process_data esteja vazio para um novo processo
            
            # Define valores padrão para um novo processo em branco
            for tab_config in campos_config_tabs.values():
                for col_group in ["col1", "col2"]:
                    if col_group in tab_config:
                        for field_name, config in tab_config[col_group].items():
                            st.session_state[form_state_key][field_name] = config.get("default_value", None)
                    else:
                        for field_name, config in tab_config.items():
                            st.session_state[form_state_key][field_name] = config.get("default_value", None)
            st.session_state[form_state_key]["Observacao"] = ""

            # Sobrescreve valores específicos com padrões
            st.session_state[form_state_key]["Processo_Novo"] = ""
            st.session_state[form_state_key]["Quantidade"] = 0
            st.session_state[form_state_key]["Quantidade_Containers"] = 0
            st.session_state[form_state_key]["Modal"] = ""
            st.session_state[form_state_key]["INCOTERM"] = ""
            st.session_state[form_state_key]["Pago"] = "Não"
            st.session_state[form_state_key]["Status_Geral"] = ""
            st.session_state[form_state_key]["Documentos_Revisados"] = "Não"
            st.session_state[form_state_key]["Conhecimento_Embarque"] = "Não"
            st.session_state[form_state_key]["Descricao_Feita"] = "Não"
            st.session_state[form_state_key]["Descricao_Enviada"] = "Não"
            st.session_state[form_state_key]["Nota_feita"] = "Não"
            st.session_state[form_state_key]["Conferido"] = "Não"
            st.session_state[form_state_key]["Data_Compra"] = None
            st.session_state[form_state_key]["Data_Embarque"] = None
            st.session_state[form_state_key]["ETA_Recinto"] = None
            st.session_state[form_state_key]["Data_Registro"] = None
            st.session_state[form_state_key]["Previsao_Pichau"] = None
            st.session_state[form_state_key]["DI_ID_Vinculada"] = None
            st.session_state[form_state_key]["Estimativa_Impostos_Total"] = 0.0
            st.session_state[form_state_key]["Estimativa_Impostos_BR"] = 0.0 # Inicializa o campo
            st.session_state[form_state_key]["Estimativa_Dolar_BRL"] = 0.0
            st.session_state[form_state_key]["Valor_USD"] = 0.0
            st.session_state[form_state_key]["Estimativa_Frete_USD"] = 0.0
            st.session_state[form_state_key]["Estimativa_Seguro_BRL"] = 0.0
            st.session_state[form_state_key]["Estimativa_II_BR"] = 0.0
            st.session_state[form_state_key]["Estimativa_IPI_BR"] = 0.0
            st.session_state[form_state_key]["Estimativa_PIS_BR"] = 0.0
            st.session_state[form_state_key]["Estimativa_COFINS_BR"] = 0.0
            st.session_state[form_state_key]["Estimativa_ICMS_BR"] = 0.0
    
    # Se estamos editando, garantir que process_id seja o ID real do processo
    if not is_new_process and process_identifier is not None:
        if isinstance(process_identifier, int):
            process_id = process_identifier
        else: # Se for string, buscar o ID
            existing_process_data = db_manager.obter_processo_by_processo_novo(process_identifier)
            if existing_process_data:
                process_id = existing_process_data['id']
            else:
                process_id = None # Se não encontrar, trata como novo erro ou problema

    linked_di_id = st.session_state[form_state_key].get('DI_ID_Vinculada') # Use from session state
    linked_di_number = None
    if linked_di_id:
        linked_di_data = db_utils.get_declaracao_by_id(linked_di_id)
        if linked_di_data:
            # Garante que linked_di_data é um dicionário ou se comporta como um
            if isinstance(linked_di_data, dict):
                linked_di_number = _format_di_number(str(linked_di_data.get('numero_di')))
            else: # Assumindo que é um sqlite3.Row object
                linked_di_number = _format_di_number(str(linked_di_data['numero_di']))


    st.markdown(f"### {'Novo Processo' if is_new_process else f'Editar Processo: {st.session_state[form_state_key].get('Processo_Novo', '')}'}")

    # Sempre inicialize as flags de popup se não existirem
    if 'show_add_item_popup' not in st.session_state:
        st.session_state.show_add_item_popup = False
    if 'selected_item_indices' not in st.session_state:
        st.session_state.selected_item_indices = []
    if 'show_edit_item_popup' not in st.session_state:
        st.session_state.show_edit_item_popup = False
    if 'item_to_edit_index' not in st.session_state:
        st.session_state.item_to_edit_index = None
    if 'last_processed_upload_key' not in st.session_state:
        st.session_state.last_processed_upload_key = None

    # Tabs are now OUTSIDE the main Streamlit form
    tabs_names = list(campos_config_tabs.keys())
    tabs = st.tabs(tabs_names)

    for i, tab_name in enumerate(tabs_names):
        with tabs[i]:
            if tab_name == "Dados Gerais":
                col_left,  col_right, col_center = st.columns(3)

                with col_left:
                    for field_name, config in campos_config_tabs[tab_name]["col1"].items():
                        label = config["label"]
                        current_value = st.session_state[form_state_key].get(field_name) # Read from session state
                        
                        if config["type"] == "number":
                            # Modificado: Garante que o valor inicial seja um inteiro para Quantidade
                            # Força o valor a ser um inteiro, eliminando o aviso de float para %d
                            default_value_for_number_input = int(current_value) if (current_value is not None and pd.isna(current_value) == False) else 0
                            widget_value = st.number_input(label, value=default_value_for_number_input, format="%d", key=f"{form_state_key}_{field_name}", disabled=config.get("disabled", False))
                            st.session_state[form_state_key][field_name] = int(widget_value) if widget_value is not None else None # Update session state
                        else:
                            widget_value = st.text_input(label, value=current_value if current_value is not None else "", key=f"{form_state_key}_{field_name}", disabled=config.get("disabled", False))
                            st.session_state[form_state_key][field_name] = widget_value if widget_value else None # Update session state


                with col_right:
                    # Obter a seleção atual do Modal para controlar a visibilidade e editabilidade
                    current_modal_selection = st.session_state[form_state_key].get("Modal", "")

                    for field_name, config in campos_config_tabs[tab_name]["col2"].items():
                        label = config["label"]
                        current_value = st.session_state[form_state_key].get(field_name) # Read from session state

                        # Lógica para campos condicionais
                        is_conditional_field = "conditional_field" in config
                        is_editable_conditional = True # Por padrão, campos não condicionais são editáveis

                        if is_conditional_field:
                            conditional_field_name = config["conditional_field"]
                            conditional_value_required = config["conditional_value"]
                            
                            # A editabilidade depende da seleção do campo condicional
                            if current_modal_selection != conditional_value_required:
                                is_editable_conditional = False
                                # Se o campo não for editável, o valor deve ser None
                                st.session_state[form_state_key][field_name] = None
                        
                        is_disabled_overall = config.get("disabled", False) or (is_conditional_field and not is_editable_conditional)

                        if config["type"] == "dropdown":
                            options = config["values"]
                            default_index = 0
                            if current_value in options:
                                default_index = options.index(current_value)
                            elif current_value is not None and str(current_value).strip() != "" and current_value not in options:
                                options = [current_value] + options
                                default_index = 0
                            widget_value = st.selectbox(
                                label, 
                                options=options, 
                                index=default_index, 
                                key=f"{form_state_key}_{field_name}",
                                disabled=is_disabled_overall
                            )
                            st.session_state[form_state_key][field_name] = widget_value if widget_value else None
                        
                        elif config["type"] == "number": # Novo tipo para Quantidade_Containers
                            # Garante que current_value é um número antes de passar para int() e depois para float()
                            default_value_for_number_input = int(current_value) if (current_value is not None and pd.isna(current_value) == False) else 0
                            widget_value = st.number_input(
                                label, 
                                value=default_value_for_number_input, 
                                format="%d", 
                                key=f"{form_state_key}_{field_name}", 
                                disabled=is_disabled_overall
                            )
                            st.session_state[form_state_key][field_name] = int(widget_value) if widget_value is not None else None
                        
                        else: # Fallback para text_input se nenhum tipo específico for encontrado
                            widget_value = st.text_input(
                                label, 
                                value=current_value if current_value is not None else "", 
                                key=f"{form_state_key}_{field_name}", 
                                disabled=is_disabled_overall,
                                help="Selecione 'Maritimo' no campo Modal para habilitar." if is_conditional_field and not is_editable_conditional else None
                            )
                            st.session_state[form_state_key][field_name] = widget_value if widget_value else None


            elif tab_name == "Itens":
                st.subheader("Itens do Processo")
                
                current_fornecedor_context = st.session_state[form_state_key].get("Fornecedor", "N/A") # Read from session state
                current_invoice_n_context = st.session_state[form_state_key].get("N_Invoice", "N/A") # Read from session state
                

                col_add_item, col_edit_item, col_delete_item = st.columns([0.15, 0.15, 0.15])

                with col_add_item:
                    if st.button("Adicionar Item", key="add_item_button_in_items_tab"):
                        st.session_state.show_add_item_popup = True
                        st.session_state.show_edit_item_popup = False # Ensure edit popup is closed
                
                
                            
                if st.session_state.get('show_add_item_popup', False):
                    with st.popover("Adicionar Novo Item"):
                        with st.form("add_item_form_fixed", clear_on_submit=True):
                            new_item_codigo_interno = st.text_input("Código Interno", key="new_item_codigo_interno_popup")
                            
                            all_ncm_items = db_utils.selecionar_todos_ncm_itens()
                            ncm_options = [""] + sorted([ncm_list_page.format_ncm_code(item['ncm_code']) for item in all_ncm_items]) if ncm_list_page else [""]
                            new_item_ncm_display = st.selectbox("NCM", options=ncm_options, key="new_item_ncm_popup")
                            
                            new_item_cobertura = st.selectbox("Cobertura", options=["SIM", "NÃO"], key="new_item_cobertura_popup")
                            new_item_sku = st.text_input("SKU", key="new_item_sku_popup")
                            new_item_quantidade = st.number_input("Quantidade", min_value=0, value=0, step=1, key="new_item_quantidade_popup")
                            new_item_valor_unitario = st.number_input("Valor Unitário (USD)", min_value=0.0, format="%.2f", key="new_item_valor_unitario_popup")
                            # Adicionado campos faltantes ao popover de adicionar
                            new_item_peso_unitario = st.number_input("Peso Unitário (KG)", min_value=0.0, format="%.4f", key="new_item_peso_unitario_popup")
                            new_item_denominacao = st.text_input("Denominação do produto", key="new_item_denominacao_popup")
                            new_item_detalhamento = st.text_input("Detalhamento complementar do produto", key="new_item_detalhamento_popup")

                            
                            if st.form_submit_button("Adicionar Item"):
                                raw_new_item_data = {
                                    "Código Interno": new_item_codigo_interno,
                                    "NCM": re.sub(r'\D', '', new_item_ncm_display) if new_item_ncm_display else None,
                                    "Cobertura": new_item_cobertura,
                                    "SKU": new_item_sku,
                                    "Quantidade": new_item_quantidade, 
                                    "Valor Unitário": new_item_valor_unitario,
                                    "Peso Unitário": new_item_peso_unitario, # Adicionado
                                    "Denominação do produto": new_item_denominacao, # Adicionado
                                    "Detalhamento complementar do produto": new_item_detalhamento, # Adicionado
                                    "Fornecedor": current_fornecedor_context,
                                    "Invoice N#": current_invoice_n_context
                                }
                                standardized_new_item_data = _standardize_item_data(raw_new_item_data, current_fornecedor_context, current_invoice_n_context)
                                standardized_new_item_data["Valor total do item"] = standardized_new_item_data["Quantidade"] * standardized_new_item_data["Valor Unitário"]
                                
                                st.session_state.process_items_data.append(standardized_new_item_data)
                                _display_message_box("Item adicionado com sucesso!", "success")
                                st.session_state.show_add_item_popup = False
                                st.rerun()
                                
                # Botão para baixar template de itens
                col_download_template, col_upload_excel = st.columns([0.2, 0.8])
                with col_download_template:
                    excel_template_data = _generate_items_excel_template()
                    st.download_button(
                        label="Baixar Template Itens",
                        data=excel_template_data,
                        file_name="template_itens_processo.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="download_items_excel_template"
                    )
                with col_upload_excel:
                    uploaded_items_file = st.file_uploader("Upload Excel/CSV de Itens", type=["csv", "xls", "xlsx"], key="upload_items_file")
                    
                    current_upload_key = None
                    if uploaded_items_file is not None:
                        # Create a unique key for the current uploaded file
                        # Using name and size is a common way to identify a file upload
                        current_upload_key = (uploaded_items_file.name, uploaded_items_file.size)
                        
                    # Process only if a new file is uploaded or if it's a new session and file is pre-populated
                    if uploaded_items_file is not None and current_upload_key != st.session_state.last_processed_upload_key:
                        if _import_items_from_excel(uploaded_items_file, current_fornecedor_context, current_invoice_n_context):
                            st.session_state.last_processed_upload_key = current_upload_key # Update the key after successful processing
                            st.rerun()
                        else:
                            # If import fails, reset the key so user can try again (optional, but good for retries)
                            st.session_state.last_processed_upload_key = None            

                st.markdown("---") 

                # Ensure df_items is created with all expected columns from the schema
                df_items = pd.DataFrame(st.session_state.process_items_data)
                
                # Garanta que todas as colunas de DEFAULT_ITEM_SCHEMA estão presentes no DataFrame
                for col in DEFAULT_ITEM_SCHEMA.keys():
                    if col not in df_items.columns:
                        df_items[col] = None

                # Recalculate totals needed for tax calculation before displaying anything
                total_invoice_value_usd_for_calc = df_items["Valor total do item"].sum() if "Valor total do item" in df_items.columns else 0
                total_invoice_weight_kg_for_calc = 0
                if "Peso Unitário" in df_items.columns and "Quantidade" in df_items.columns:
                    df_items['Peso Total do Item Calculado'] = pd.to_numeric(df_items['Peso Unitário'], errors='coerce').fillna(0) * \
                                                            pd.to_numeric(df_items['Quantidade'], errors='coerce').fillna(0)
                    total_invoice_weight_kg_for_calc = df_items['Peso Total do Item Calculado'].sum()

                # Re-calculate taxes for all items in session state
                dolar_brl = st.session_state[form_state_key].get("Estimativa_Dolar_BRL", 0.0)
                updated_process_items_data = []
                for item in st.session_state.process_items_data:
                    updated_item = calculate_item_taxes_and_values(
                        item.copy(), # Pass a copy to ensure original is not modified during calculation
                        dolar_brl,
                        total_invoice_value_usd_for_calc,
                        total_invoice_weight_kg_for_calc,
                        st.session_state[form_state_key].get('Estimativa_Frete_USD', 0.0),
                        st.session_state[form_state_key].get('Estimativa_Seguro_BRL', 0.0)
                    )
                    updated_process_items_data.append(updated_item)
                st.session_state.process_items_data = updated_process_items_data
                
                # Re-create DataFrame from the updated session state for display
                df_items = pd.DataFrame(st.session_state.process_items_data)


                if not df_items.empty: # Only proceed if there are actual items to display or edit
                    st.markdown("#### Itens do Processo:")

                    # Adicionar coluna de seleção para edição/exclusão
                    df_items['Selecionar'] = False 

                    df_items['NCM Formatado'] = df_items['NCM'].apply(lambda x: ncm_list_page.format_ncm_code(str(x)) if ncm_list_page and x is not None else str(x) if x is not None else '')

                    # Definindo display_cols aqui
                    display_cols = [
                        "Selecionar", 
                        "Cobertura",
                        "Código Interno",
                        "Denominação do produto", 
                        "SKU",
                        "Quantidade", 
                        "Valor Unitário", 
                        "NCM Formatado", 
                        "Valor total do item", 
                        "Peso Unitário", 
                    ]
                    # Garante que as colunas a serem exibidas realmente existam no DataFrame
                    display_cols = [col for col in display_cols if col in df_items.columns]


                    column_config_items = {
                        "Selecionar": st.column_config.CheckboxColumn("Selecionar", default=False),
                        "Cobertura": st.column_config.SelectboxColumn("Cobertura", options=["SIM", "NÃO"], width="small", disabled=True), 
                        "Código Interno": st.column_config.TextColumn("Cód. Interno", width="small", disabled=True), 
                        "Denominação do produto": st.column_config.TextColumn("Denominação", width="medium", disabled=True), 
                        "SKU": st.column_config.TextColumn("SKU", width="small", disabled=True), 
                        "Quantidade": st.column_config.NumberColumn("Qtd.", format="%d", width="small", disabled=True), 
                        "Valor Unitário": st.column_config.NumberColumn("Preço (USD)", format="%.2f", width="small", disabled=True), 
                        "NCM Formatado": st.column_config.TextColumn("NCM", width="small", disabled=True), 
                        "Valor total do item": st.column_config.NumberColumn("Valor Total Item (USD)", format="%.2f", disabled=True, width="small"),
                        "Peso Unitário": st.column_config.NumberColumn("Peso Unit. (KG)", format="%.4f", width="small", disabled=True), 
                    }

                    selected_rows_data = st.data_editor(
                        df_items[display_cols], # Agora usando display_cols
                        column_config=column_config_items,
                        num_rows="fixed", 
                        hide_index=True,
                        use_container_width=True,
                        key="process_items_editor"
                    )
                    
                    # Store selected indices based on the 'Selecionar' column from the displayed DataFrame
                    st.session_state.selected_item_indices = [
                        idx for idx, selected in enumerate(selected_rows_data['Selecionar']) if selected # Usar selected_rows_data
                    ]

                    # Botões de Editar e Excluir Item
                    if st.session_state.selected_item_indices:
                        with col_edit_item:
                            if st.button("Editar Item", key="edit_selected_item_button"):
                                if len(st.session_state.selected_item_indices) == 1:
                                    st.session_state.item_to_edit_index = st.session_state.selected_item_indices[0]
                                    st.session_state.show_edit_item_popup = True
                                    st.session_state.show_add_item_popup = False 
                                else:
                                    _display_message_box("Selecione exatamente um item para editar.", "warning")
                        with col_delete_item:
                            if st.button("Excluir Item", key="delete_selected_item_button"):
                                # Excluir do maior índice para o menor para evitar problemas de índice
                                for idx in sorted(st.session_state.selected_item_indices, reverse=True):
                                    del st.session_state.process_items_data[idx]
                                st.session_state.selected_item_indices = [] 
                                _display_message_box("Itens selecionados excluídos com sucesso!", "success")
                                st.rerun()

                    # Popover para Edição de Item
                    if st.session_state.get('show_edit_item_popup', False) and st.session_state.item_to_edit_index is not None:
                        item_index = st.session_state.item_to_edit_index
                        item_data = st.session_state.process_items_data[item_index]

                        with st.popover(f"Editar Item: {item_data.get('Código Interno', 'N/A')}"):
                            with st.form("edit_item_form_fixed", clear_on_submit=False):
                                edited_codigo_interno = st.text_input("Código Interno", value=item_data.get("Código Interno", ""), key="edit_item_codigo_interno_popup")
                                
                                all_ncm_items = db_utils.selecionar_todos_ncm_itens()
                                ncm_options = [""] + sorted([ncm_list_page.format_ncm_code(item['ncm_code']) for item in all_ncm_items]) if ncm_list_page else [""]
                                current_ncm_display = ncm_list_page.format_ncm_code(str(item_data.get("NCM", ""))) if ncm_list_page else str(item_data.get("NCM", ""))
                                edited_ncm_display = st.selectbox("NCM", options=ncm_options, index=ncm_options.index(current_ncm_display) if current_ncm_display in ncm_options else 0, key="edit_item_ncm_popup")
                                
                                edited_cobertura = st.selectbox("Cobertura", options=["SIM", "NÃO"], index=0 if item_data.get("Cobertura", "NÃO") == "SIM" else 1, key="edit_item_cobertura_popup")
                                edited_sku = st.text_input("SKU", value=item_data.get("SKU", ""), key="edit_item_sku_popup")
                                edited_quantidade = st.number_input("Quantidade", min_value=0, value=int(item_data.get("Quantidade", 0)), step=1, key="edit_item_quantidade_popup")
                                edited_valor_unitario = st.number_input("Valor Unitário (USD)", min_value=0.0, value=float(item_data.get("Valor Unitário", 0.0)), format="%.2f", key="edit_item_valor_unitario_popup")
                                edited_peso_unitario = st.number_input("Peso Unitário (KG)", min_value=0.0, value=float(item_data.get("Peso Unitário", 0.0)), format="%.4f", key="edit_item_peso_unitario_popup")
                                edited_denominacao = st.text_input("Denominação do produto", value=item_data.get("Denominação do produto", ""), key="edit_item_denominacao_popup")
                                edited_detalhamento = st.text_input("Detalhamento complementar do produto", value=item_data.get("Detalhamento complementar do produto", ""), key="edit_item_detalhamento_popup")

                                if st.form_submit_button("Salvar Edição"):
                                    # Update item data in session state
                                    st.session_state.process_items_data[item_index]["Código Interno"] = edited_codigo_interno
                                    st.session_state.process_items_data[item_index]["NCM"] = re.sub(r'\D', '', edited_ncm_display) if edited_ncm_display else None
                                    st.session_state.process_items_data[item_index]["Cobertura"] = edited_cobertura
                                    st.session_state.process_items_data[item_index]["SKU"] = edited_sku
                                    st.session_state.process_items_data[item_index]["Quantidade"] = edited_quantidade
                                    st.session_state.process_items_data[item_index]["Valor Unitário"] = edited_valor_unitario
                                    st.session_state.process_items_data[item_index]["Peso Unitário"] = edited_peso_unitario
                                    st.session_state.process_items_data[item_index]["Denominação do produto"] = edited_denominacao
                                    st.session_state.process_items_data[item_index]["Detalhamento complementar do produto"] = edited_detalhamento
                                    st.session_state.process_items_data[item_index]["Valor total do item"] = edited_quantidade * edited_valor_unitario
                                    
                                    # Recalculate taxes for this item immediately after editing
                                    dolar_brl_form_state = st.session_state[form_state_key].get("Estimativa_Dolar_BRL", 0.0)
                                    
                                    # Recalculate total invoice value and weight from the current session state items
                                    temp_df_for_recalc = pd.DataFrame(st.session_state.process_items_data)
                                    total_invoice_value_usd_recalc = temp_df_for_recalc["Valor total do item"].sum() if "Valor total do item" in temp_df_for_recalc.columns else 0
                                    total_invoice_weight_kg_recalc = 0
                                    if "Peso Unitário" in temp_df_for_recalc.columns and "Quantidade" in temp_df_for_recalc.columns:
                                        total_invoice_weight_kg_recalc = (pd.to_numeric(temp_df_for_recalc['Peso Unitário'], errors='coerce').fillna(0) * \
                                                                            pd.to_numeric(temp_df_for_recalc['Quantidade'], errors='coerce').fillna(0)).sum()

                                    updated_item_after_recalc = calculate_item_taxes_and_values(
                                        st.session_state.process_items_data[item_index], # Pass reference to modify directly
                                        dolar_brl_form_state, 
                                        total_invoice_value_usd_recalc, 
                                        total_invoice_weight_kg_recalc, 
                                        st.session_state[form_state_key].get('Estimativa_Frete_USD', 0.0), 
                                        st.session_state[form_state_key].get('Estimativa_Seguro_BRL', 0.0)
                                    )
                                    # The item in session_state.process_items_data is already updated by passing reference

                                    _display_message_box("Item editado com sucesso!", "success")
                                    st.session_state.show_edit_item_popup = False
                                    st.session_state.item_to_edit_index = None
                                    st.session_state.selected_item_indices = [] # Limpar seleção
                                    st.rerun()
                                if st.form_submit_button("Cancelar"):
                                    st.session_state.show_edit_item_popup = False
                                    st.session_state.item_to_edit_index = None
                                    st.session_state.selected_item_indices = [] # Limpar seleção
                                    st.rerun()

                    # Recalculate total_invoice_value_usd e total_invoice_weight_kg com base nos dados ATUALIZADOS
                    # Estes totais são para exibição no resumo e para o cálculo de impostos globais.
                    df_items_for_summary_calc = pd.DataFrame(st.session_state.process_items_data)
                    
                    total_invoice_value_usd = df_items_for_summary_calc["Valor total do item"].sum() if "Valor total do item" in df_items_for_summary_calc.columns else 0
                    
                    total_invoice_weight_kg = 0
                    if "Peso Unitário" in df_items_for_summary_calc.columns and "Quantidade" in df_items_for_summary_calc.columns: 
                        total_invoice_weight_kg = (pd.to_numeric(df_items_for_summary_calc['Peso Unitário'], errors='coerce').fillna(0) * \
                                                   pd.to_numeric(df_items_for_summary_calc['Quantidade'], errors='coerce').fillna(0)).sum()


                    st.markdown("---")
                    st.subheader("Resumo de Itens para Cálculos")
                    st.write(f"Valor Total dos Itens (USD): **{total_invoice_value_usd:,.2f}**".replace('.', '#').replace(',', '.').replace('#', ','))
                    st.write(f"Peso Total dos Itens (KG): **{total_invoice_weight_kg:,.4f}**".replace('.', '#').replace(',', '.').replace('#', ','))

                    st.session_state.total_invoice_value_usd = total_invoice_value_usd
                    st.session_state.total_invoice_weight_kg = total_invoice_weight_kg

                else:
                    st.info("Nenhum item adicionado a este processo ainda. Use as opções acima para adicionar.")

            elif tab_name == "Valores e Estimativas":
                st.subheader("Valores e Estimativas")
                
                # Use o total_invoice_value_usd de st.session_state como valor padrão para Valor_USD
                total_itens_usd_from_session = st.session_state.get('total_invoice_value_usd', 0.0)
                
                # Garante que os valores numéricos são tratados como float (0.0 se None)
                dolar_brl_current = float(st.session_state[form_state_key].get("Estimativa_Dolar_BRL", 0.0) or 0.0)
                
                # ALTERAÇÃO: Valor_USD sempre reflete o total dos itens
                valor_usd_current = total_itens_usd_from_session 
                
                # Atualiza diretamente o valor no session_state do formulário
                st.session_state[form_state_key]["Valor_USD"] = valor_usd_current
                
                pago_current = st.session_state[form_state_key].get("Pago", "Não")
                frete_usd_current = float(st.session_state[form_state_key].get("Estimativa_Frete_USD", 0.0) or 0.0)
                seguro_brl_current = float(st.session_state[form_state_key].get("Estimativa_Seguro_BRL", 0.0) or 0.0)
                icms_br_manual_estimate_current = float(st.session_state[form_state_key].get("Estimativa_ICMS_BR", 0.0) or 0.0)
                
                col_1, col_2 = st.columns(2)

                with col_1:
                    st.session_state[form_state_key]["Estimativa_Dolar_BRL"] = st.number_input(
                        "Cambio Estimado (R$):", 
                        value=dolar_brl_current, 
                        format="%.2f", 
                        key=f"{form_state_key}_Estimativa_Dolar_BRL"
                    )
                    st.number_input( # Alterado para display apenas, pois o valor é calculado
                        "Valor (USD):", 
                        value=float(st.session_state[form_state_key]["Valor_USD"] or 0.0), # Usa o valor atualizado do session_state
                        format="%.2f", 
                        key=f"{form_state_key}_Valor_USD_display", # Nova chave para evitar conflito
                        disabled=True # Desabilita edição manual, pois é um valor calculado
                    )
                    st.session_state[form_state_key]["Pago"] = st.selectbox(
                        "Pago?:", 
                        options=["Não", "Sim"], 
                        index=0 if pago_current == "Não" else 1, 
                        key=f"{form_state_key}_Pago"
                    )
                    st.session_state[form_state_key]["Estimativa_Frete_USD"] = st.number_input(
                        "Estimativa de Frete (USD):", 
                        value=frete_usd_current, 
                        format="%.2f", 
                        key=f"{form_state_key}_Estimativa_Frete_USD"
                    )
                    st.session_state[form_state_key]["Estimativa_Seguro_BRL"] = st.number_input(
                        "Estimativa Seguro (R$):", 
                        value=seguro_brl_current, 
                        format="%.2f", 
                        key=f"{form_state_key}_Estimativa_Seguro_BRL"
                    )
                    
                    st.session_state[form_state_key]["Estimativa_ICMS_BR"] = st.number_input(
                        "Estimativa de ICMS (R$ - Manual):", 
                        value=icms_br_manual_estimate_current, 
                        format="%.2f",
                        key=f"{form_state_key}_Estimativa_ICMS_BR"
                    )

                    # Estimativa_Impostos_BR (campo antigo, agora desabilitado e que deve ser populado)
                    # No caso de um campo desabilitado que corresponde a uma coluna do DB,
                    # ele precisa ser populado de alguma forma antes de ser salvo.
                    # Por enquanto, se não for explicitamente calculado e salvo em outro lugar,
                    # ele pode ser mantido como o total de impostos, ou 0.0.
                    st.session_state[form_state_key]["Estimativa_Impostos_BR"] = st.number_input(
                        "Estimativa Impostos (Antigo):", 
                        value=float(st.session_state[form_state_key].get("Estimativa_Impostos_BR", 0.0) or 0.0), 
                        format="%.2f", 
                        key=f"{form_state_key}_Estimativa_Impostos_BR", 
                        disabled=True,
                        help="Campo de impostos para compatibilidade com versões antigas do DB."
                    )
                    
                    dolar_brl = st.session_state[form_state_key].get("Estimativa_Dolar_BRL", 0.0)
                    total_invoice_value_usd = st.session_state.get('total_invoice_value_usd', 0.0)
                    total_invoice_weight_kg = st.session_state.get('total_invoice_weight_kg', 0.0)
                    estimativa_frete_usd = st.session_state[form_state_key].get('Estimativa_Frete_USD', 0.0)
                    estimativa_seguro_brl = st.session_state[form_state_key].get('Estimativa_Seguro_BRL', 0.0)

                    total_ii = total_ipi = total_pis = total_cofins = total_icms_calculated_sum = 0.0
                    if st.session_state.process_items_data:
                        # Nao precisa recalcular itens inteiros aqui, eles ja foram atualizados na aba Itens
                        # Apenas somar os totais dos itens existentes
                        for item in st.session_state.process_items_data:
                            total_ii += item.get('Estimativa_II_BR', 0.0)
                            total_ipi += item.get('Estimativa_IPI_BR', 0.0)
                            total_pis += item.get('Estimativa_PIS_BR', 0.0)
                            total_cofins += item.get('Estimativa_COFINS_BR', 0.0)
                            total_icms_calculated_sum += item.get('Estimativa_ICMS_BR', 0.0)

                    st.session_state[form_state_key]['Estimativa_II_BR'] = total_ii
                    st.session_state[form_state_key]['Estimativa_IPI_BR'] = total_ipi
                    st.session_state[form_state_key]['Estimativa_PIS_BR'] = total_pis
                    st.session_state[form_state_key]['Estimativa_COFINS_BR'] = total_cofins
                    
                    # Calcular Estimativa Impostos (R$) - Soma de todos os impostos calculados
                    total_impostos_reais = total_ii + total_ipi + total_pis + total_cofins + total_icms_calculated_sum
                    st.session_state[form_state_key]['Estimativa_Impostos_Total'] = total_impostos_reais

                with col_2:
                    
                    st.number_input("Estimativa de II (R$ - Calculado):", value=total_ii, format="%.2f", disabled=True, key=f"display_{form_state_key}_II_BR_calc")
                    st.number_input("Estimativa de IPI (R$ - Calculado):", value=total_ipi, format="%.2f", disabled=True, key=f"display_{form_state_key}_IPI_BR_calc")
                    st.number_input("Estimativa de PIS (R$ - Calculado):", value=total_pis, format="%.2f", disabled=True, key=f"display_{form_state_key}_PIS_BR_calc")
                    st.number_input("Estimativa de COFINS (R$ - Calculado):", value=total_cofins, format="%.2f", disabled=True, key=f"display_{form_state_key}_COFINS_BR_calc")
                    st.number_input("Estimativa de ICMS (R$ - Calculado):", value=total_icms_calculated_sum, format="%.2f", disabled=True, key=f"display_{form_state_key}_ICMS_BR_calc")
                    st.number_input("Estimativa Impostos (R$):", value=total_impostos_reais, format="%.2f", disabled=True, key=f"display_{form_state_key}_Impostos_Total_calc") # Campo adicionado
                    st.caption("Os valores acima são a soma dos impostos calculados para cada item com base no NCM.")

            elif tab_name == "Status Operacional":
                st.subheader("Status Operacional")
                for field_name, config in campos_config_tabs[tab_name].items():
                    label = config["label"]
                    current_value = st.session_state[form_state_key].get(field_name)

                    if config["type"] == "date":
                        current_value_dt = None
                        if current_value:
                            try:
                                current_value_dt = datetime.strptime(str(current_value), "%Y-%m-%d")
                            except ValueError:
                                current_value_dt = None
                        widget_value = st.date_input(label, value=current_value_dt, key=f"{form_state_key}_{field_name}", format="DD/MM/YYYY")
                        st.session_state[form_state_key][field_name] = widget_value.strftime("%Y-%m-%d") if widget_value else None
                    elif config["type"] == "dropdown":
                        options = config["values"]
                        default_index = 0
                        if current_value in options:
                            default_index = options.index(current_value)
                        elif current_value is not None and str(current_value).strip() != "" and current_value not in options:
                            options = [current_value] + options
                            default_index = 0
                        widget_value = st.selectbox(label, options=options, index=default_index, key=f"{form_state_key}_{field_name}")
                        st.session_state[form_state_key][field_name] = widget_value if widget_value else None
                    else:
                        widget_value = st.text_input(label, value=current_value if current_value is not None else "", key=f"{form_state_key}_{field_name}")
                        st.session_state[form_state_key][field_name] = widget_value if widget_value else None

            elif tab_name == "Documentação":
                st.subheader("Documentação")
                st.info("(Tela em desenvolvimento)  A funcionalidade de upload de documentos ainda não está implementada.")
                for field_name, config in campos_config_tabs[tab_name].items():
                    label = config["label"]
                    current_value = st.session_state[form_state_key].get(field_name)

                    if config["type"] == "folder_path":
                        widget_value = st.text_input(label, value=current_value if current_value is not None else "", placeholder=config.get("placeholder", ""), key=f"{form_state_key}_{field_name}")
                        st.session_state[form_state_key][field_name] = widget_value if widget_value else None
                        st.info("Informações sobre o caminho da pasta onde os documentos do processo estão armazenados.")
                    elif config["type"] == "text" and config.get("disabled"):
                        di_vinculada_value = None
                        processo_novo_val = st.session_state[form_state_key].get("Processo_Novo") 
                        if processo_novo_val:
                            linked_di_data_by_process_number_raw = db_utils.get_declaracao_by_referencia(str(processo_novo_val)) 
                            # Convert sqlite3.Row to dictionary
                            linked_di_data_by_process_number = dict(linked_di_data_by_process_number_raw) if linked_di_data_by_process_number_raw else None

                            if linked_di_data_by_process_number:
                                di_vinculada_value = linked_di_data_by_process_number.get('id')
                                if di_vinculada_value:
                                    st.info(f"DI vinculada automaticamente: ID {di_vinculada_value} (Nº DI: {_format_di_number(str(linked_di_data_by_process_number.get('numero_di')))})")
                            else:
                                st.info(f"Nenhuma DI encontrada para o processo '{processo_novo_val}'.")

                        display_value = str(di_vinculada_value) if di_vinculada_value is not None else ""
                        st.text_input(label, value=display_value, key=f"{form_state_key}_{field_name}", disabled=True, help=config.get("help"))
                        st.session_state[form_state_key][field_name] = di_vinculada_value
                    else:
                        widget_value = st.text_input(label, value=current_value if current_value is not None else "", key=f"{form_state_key}_{field_name}")
                        st.session_state[form_state_key][field_name] = widget_value if widget_value else None
            
    st.markdown("---")
    st.markdown("##### Observação (Campo Dedicado)")
    st.session_state[form_state_key]["Observacao"] = st.text_area("Observação", value=st.session_state[form_state_key].get("Observacao", "") or "", height=150, key=f"{form_state_key}_Observacao_dedicated")
    st.session_state[form_state_key]["Observacao"] = st.session_state[form_state_key]["Observacao"] if st.session_state[form_state_key]["Observacao"] else None

    st.markdown("---")
    st.markdown("##### Histórico do Processo")
    if not is_new_process:
        history_data_raw = db_manager.obter_historico_processo(process_id if process_id is not None else -1) 
        if history_data_raw:
            history_df = pd.DataFrame(history_data_raw, columns=["Campo", "Valor Antigo", "Valor Novo", "Timestamp", "Usuário"])
            history_df["Timestamp"] = history_df["Timestamp"].apply(lambda x: datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y %H:%M:%S") if x else "")
            st.dataframe(history_df, use_container_width=True, hide_index=True)
        else:
            st.info("Nenhum histórico de alterações para este processo.")
    else:
        st.info("Histórico disponível apenas para processos existentes após a primeira gravação.")

    # Main form only for saving and canceling
    with st.form(key=f"followup_process_form_submit_buttons_{process_id}", clear_on_submit=False):
        col_save, col_cancel  = st.columns([0.03, 0.1])

        with col_save:
            if st.form_submit_button("Salvar Processo"):
                # Construct edited_data from session state before saving
                edited_data_to_save = {}
                for tab_name, tab_config in campos_config_tabs.items():
                    if "col1" in tab_config:
                        for field_name, config in tab_config["col1"].items():
                            edited_data_to_save[field_name] = st.session_state.get(f"{form_state_key}_{field_name}")
                    if "col2" in tab_config:
                        for field_name, config in tab_config["col2"].items():
                            edited_data_to_save[field_name] = st.session_state.get(f"{form_state_key}_{field_name}")
                    # For other tabs that have fields directly
                    if tab_name not in ["Dados Gerais", "Itens"]: # Garante que 'Itens' não é processada aqui
                        for field_name, config in tab_config.items():
                            # Se o campo é um campo calculado/desabilitado que não é diretamente um widget input,
                            # seu valor deve ser lido do st.session_state[form_state_key] onde foi salvo.
                            if config.get("disabled", False) and config.get("type") == "currency_br": # Ex: Estimativa_II_BR, Estimativa_Impostos_BR
                                edited_data_to_save[field_name] = st.session_state[form_state_key].get(field_name)
                            else:
                                edited_data_to_save[field_name] = st.session_state.get(f"{form_state_key}_{field_name}")
                # Ensure 'Observacao' is also included
                edited_data_to_save["Observacao"] = st.session_state.get(f"{form_state_key}_Observacao_dedicated")
                
                # Campos que são atualizados diretamente no st.session_state[form_state_key] mas não são widgets individuais
                # e precisam ser explicitamente incluídos no edited_data_to_save
                # Ex: Valor_USD, Estimativa_Impostos_Total
                edited_data_to_save['Valor_USD'] = st.session_state[form_state_key].get('Valor_USD')
                edited_data_to_save['Estimativa_Impostos_Total'] = st.session_state[form_state_key].get('Estimativa_Impostos_Total')
                edited_data_to_save['Estimativa_II_BR'] = st.session_state[form_state_key].get('Estimativa_II_BR')
                edited_data_to_save['Estimativa_IPI_BR'] = st.session_state[form_state_key].get('Estimativa_IPI_BR')
                edited_data_to_save['Estimativa_PIS_BR'] = st.session_state[form_state_key].get('Estimativa_PIS_BR')
                edited_data_to_save['Estimativa_COFINS_BR'] = st.session_state[form_state_key].get('Estimativa_COFINS_BR')


                # Logar edited_data_to_save para depuração
                logger.info(f"Dados coletados para salvar (process_form_page): {edited_data_to_save} (total de chaves: {len(edited_data_to_save)})")

                # Use the persistent flag for is_new_process from session state for the save operation
                is_new_process_for_save = st.session_state.get(f'{form_state_key}_is_new_process_flag', False)

                # Pass None for process_id_from_form_load if it's a new process creation (including cloning)
                process_id_arg_for_save_action = None if is_new_process_for_save else process_id

                saved_process_id = _save_process_action(process_id_arg_for_save_action, edited_data_to_save, is_new_process_for_save, form_state_key)
                
                # Após salvar, redireciona para a página de Follow-up Importação ou para o processo salvo
                if saved_process_id:
                    st.session_state.current_page = "Formulário Processo" # Redireciona para o próprio formulário
                    st.session_state.form_process_identifier = saved_process_id # Define o ID do processo recém-salvo para edição
                    st.session_state.form_is_cloning = False # Garante que a flag de clonagem seja limpa
                    st.session_state.last_cloned_from_id = None
                else: # Se o salvamento falhou, volta para a lista principal
                    st.session_state.current_page = "Follow-up Importação"
                    st.session_state.form_is_cloning = False
                    st.session_state.last_cloned_from_id = None

                st.session_state.form_reload_processes_callback() # Recarrega a lista na página principal
                st.rerun()

        with col_cancel:
            if st.form_submit_button("Cancelar"):
                # Clear form state and go back to main page
                st.session_state.current_page = "Follow-up Importação"
                # Clear the specific form state key to re-initialize on next load
                if form_state_key in st.session_state:
                    del st.session_state[form_state_key]
                st.session_state.show_add_item_popup = False # Also reset popover state
                st.session_state.process_items_data = [] # Limpa os itens ao cancelar
                st.session_state.last_processed_upload_key = None # Reseta a chave do upload
                # Reset the process_items_loaded_for_id as we are leaving this form
                st.session_state.process_items_loaded_for_id = None 
                # Reset the cloning flag
                st.session_state.form_is_cloning = False
                st.rerun()

        col_delete = st.columns([0.0000003, 0.01])[1]

        with col_delete:
            if not is_new_process:
                confirm_delete = st.checkbox("Confirmar exclusão", key=f"confirm_delete_process_{process_id}")
                if st.form_submit_button("Excluir Processo"):
                    if confirm_delete:
                        _display_message_box("A funcionalidade de exclusão direta por este formulário está temporariamente desabilitada. Por favor, use o botão de exclusão na tela principal de Follow-up.", "warning")
                    else:
                        st.warning("Marque a caixa de confirmação para excluir o processo.")
            else:
                st.info("Excluir disponível após salvar o processo.")
        
    if linked_di_id is not None and linked_di_number:
        st.markdown("---")
        st.markdown(f"**DI Vinculada:** {linked_di_number}")
        if st.button(f"Ver Detalhes da DI {linked_di_number}", key=f"view_linked_di_outside_form_{process_id}"):
            st.session_state.current_page = "Importar XML DI"
            st.session_state.selected_di_id = linked_di_id
            st.rerun()

    elif linked_di_id is not None and not linked_di_number: 
        st.markdown("---")
        st.warning(f"DI vinculada (ID: {linked_di_id}) não encontrada no banco de dados de Declarações de Importação.")
