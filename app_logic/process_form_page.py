import streamlit as st
import pandas as pd
from datetime import datetime, date
import logging
import os
import subprocess
import sys
import io
import xlsxwriter
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from typing import Optional, Any, Dict, List, Union
import numpy as np
import base64
import re
import uuid
import followup_db_manager as db_manager

# Configuração do logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG) # Manter DEBUG para logs detalhados durante o desenvolvimento

# --- Funções Auxiliares de UI e Estilo ---

def set_background_image(image_path: str):
    """Define uma imagem de fundo para o aplicativo Streamlit com opacidade."""
    try:
        with open(image_path, "rb") as image_file:
            encoded_string = base64.b64encode(image_file.read()).decode()
        st.markdown(
            f"""
            <style>
            .stApp {{
                background-color: transparent !important;
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
                opacity: 0.50;
                z-index: -1;
            }}
            </style>
            """,
            unsafe_allow_html=True
        )
    except FileNotFoundError:
        logger.warning(f"A imagem de fundo não foi encontrada no caminho: {image_path}")
        st.warning(f"A imagem de fundo não foi encontrada no caminho: {image_path}")
    except Exception as e:
        logger.error(f"Erro ao carregar a imagem de fundo: {e}")
        st.error(f"Erro ao carregar a imagem de fundo: {e}")

def _display_message_box(message: str, type: str = "info"):
    """Exibe uma caixa de mensagem customizada (substitui alert()/confirm())."""
    if type == "info":
        st.info(message)
    elif type == "success":
        st.success(message)
    elif type == "warning":
        st.warning(message)
    elif type == "error":
        st.error(message)

# --- Classes Mock para Dependências (se necessário) ---

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
    import db_utils # type: ignore
    # Verifica se as funções esperadas existem no db_utils real
    if not all(hasattr(db_utils, func) for func in [
        'get_declaracao_by_id', 'get_declaracao_by_referencia', 
        'get_ncm_item_by_ncm_code', 'selecionar_todos_ncm_itens'
    ]):
        raise ImportError("db_utils real não contém todas as funções esperadas.")
except ImportError:
    db_utils = MockDbUtils()
    logger.warning("Módulo 'db_utils' não encontrado ou incompleto. Usando MockDbUtils.")
except Exception as e:
    db_utils = MockDbUtils() 
    logger.error(f"Erro ao importar ou inicializar 'db_utils': {e}. Usando MockDbUtils.")

# Importar pdf_analyzer_page.py e ncm_list_page para reuso de funções
pdf_analyzer_page = None
try:
    from app_logic import pdf_analyzer_page
    import pdfplumber
except ImportError:
    logger.warning("Módulo 'pdf_analyzer_page' ou 'pdfplumber' não encontrado. Funções de análise de PDF não estarão disponíveis.")

ncm_list_page = None
try:
    from app_logic import ncm_list_page
except ImportError:
    logger.warning("Módulo 'ncm_list_page' não encontrado. Funções NCM não estarão disponíveis.")


# --- Funções Auxiliares para Formatação de Dados ---
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
            return _format_di_number(str(di_data.get('numero_di') if isinstance(di_data, dict) else di_data['numero_di']))
    except Exception as e:
        logger.error(f"Erro ao buscar DI por ID {di_id}: {e}")
    return "DI Não Encontrada"

# --- Funções de Cálculo de Impostos ---
@st.cache_data(ttl=3600) # Cache de 1 hora para dados NCM
def get_ncm_taxes(ncm_code: str) -> Dict[str, float]:
    """Busca as alíquotas de impostos para um dado NCM."""
    if not ncm_code:
        return {'ii_aliquota': 0.0, 'ipi_aliquota': 0.0, 'pis_aliquota': 0.0, 'cofins_aliquota': 0.0, 'icms_aliquota': 0.0}
    ncm_data_raw = db_utils.get_ncm_item_by_ncm_code(ncm_code)
    ncm_data = dict(ncm_data_raw) if ncm_data_raw else None

    if ncm_data:
        return {
            'ii_aliquota': ncm_data.get('ii_aliquota', 0.0),
            'ipi_aliquota': ncm_data.get('ipi_aliquota', 0.0),
            'pis_aliquota': ncm_data.get('pis_aliquota', 0.0),
            'cofins_aliquota': ncm_data.get('cofins_aliquota', 0.0),
            'icms_aliquota': ncm_data.get('icms_aliquota', 0.0)
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
    
    # Certifique-se de que 'Peso Unitário' é um float e lida com None/NaN
    item_unit_weight_kg = float(item.get('Peso Unitário', 0.0) if item.get('Peso Unitário') is not None else 0.0)
    item_weight_kg = item_qty * item_unit_weight_kg

    # Para evitar divisão por zero, use max(1, ...)
    value_ratio = item_value_usd / max(1, total_invoice_value_usd)
    weight_ratio = item_weight_kg / max(1, total_invoice_weight_kg)

    # Rateio de frete e seguro
    frete_rateado_usd = estimativa_frete_usd * value_ratio
    seguro_rateado_brl = estimativa_seguro_brl * weight_ratio

    # NCM e impostos
    ncm_code = str(item.get('NCM', ''))
    ncm_taxes = get_ncm_taxes(ncm_code) # Usa a função cacheada

    # VLMD_Item (Valor da Mercadoria no Local de Desembaraço)
    vlmd_item = (item_unit_value_usd * item_qty * dolar_brl) + (frete_rateado_usd * dolar_brl) + seguro_rateado_brl
    
    # Cálculos de impostos
    item['Estimativa_II_BR'] = vlmd_item * (ncm_taxes['ii_aliquota'] / 100)
    item['Estimativa_IPI_BR'] = (vlmd_item + item['Estimativa_II_BR']) * (ncm_taxes['ipi_aliquota'] / 100)
    item['Estimativa_PIS_BR'] = vlmd_item * (ncm_taxes['pis_aliquota'] / 100)
    item['Estimativa_COFINS_BR'] = vlmd_item * (ncm_taxes['cofins_aliquota'] / 100)
    item['Estimativa_ICMS_BR'] = (vlmd_item * (ncm_taxes['icms_aliquota'] / 100))

    item['VLMD_Item'] = vlmd_item
    item['Frete_Rateado_USD'] = frete_rateado_usd
    item['Seguro_Rateado_BRL'] = seguro_rateado_brl

    return item

# --- Lógica para Salvar Processo ---
def _save_process_action(process_id_from_form_load: Optional[Any], edited_data: dict, is_new_process_flag: bool, form_state_key: str) -> Optional[Any]:
    """
    Lógica para salvar ou atualizar um processo.
    Retorna o ID do processo salvo/atualizado (int para SQLite, str para Firestore).
    """
    db_col_names_full = db_manager.obter_nomes_colunas_db()
    data_to_save_dict = {col: None for col in db_col_names_full if col != 'id'}

    for col_name, value in edited_data.items():
        if col_name in data_to_save_dict:
            if isinstance(value, (datetime, date)):
                data_to_save_dict[col_name] = value.strftime("%Y-%m-%d")
            elif isinstance(value, str) and value.strip() == '':
                data_to_save_dict[col_name] = None
            elif pd.isna(value) if isinstance(value, (float, int, np.number)) else False: # Tratamento robusto para NaN
                data_to_save_dict[col_name] = None
            else:
                data_to_save_dict[col_name] = value
        else:
            logger.warning(f"Campo '{col_name}' do formulário não corresponde a uma coluna no DB. Ignorado.")

    # Trata campos específicos como 'Status_Arquivado', 'Caminho_da_pasta', 'DI_ID_Vinculada'
    # Esta lógica foi mantida para garantir a compatibilidade com o comportamento existente.
    if 'Status_Arquivado' in db_col_names_full:
        if is_new_process_flag:
            data_to_save_dict['Status_Arquivado'] = 'Não Arquivado'
        else:
            original_process_data_raw = db_manager.obter_processo_por_id(process_id_from_form_load if process_id_from_form_load is not None else -1)
            original_process_data = dict(original_process_data_raw) if original_process_data_raw else {}
            data_to_save_dict['Status_Arquivado'] = original_process_data.get('Status_Arquivado', 'Não Arquivado') # Fallback robusto
    
    if 'Caminho_da_pasta' in db_col_names_full:
        data_to_save_dict['Caminho_da_pasta'] = edited_data.get('Caminho_da_pasta')

    if 'DI_ID_Vinculada' in db_col_names_full:
        if 'DI_ID_Vinculada' in edited_data and edited_data['DI_ID_Vinculada'] is not None:
            data_to_save_dict['DI_ID_Vinculada'] = edited_data['DI_ID_Vinculada']
        elif not is_new_process_flag:
            original_process_data_raw = db_manager.obter_processo_por_id(process_id_from_form_load if process_id_from_form_load is not None else -1)
            original_process_data = dict(original_process_data_raw) if original_process_data_raw else {}
            data_to_save_dict['DI_ID_Vinculada'] = original_process_data.get('DI_ID_Vinculada')
        else:
            data_to_save_dict['DI_ID_Vinculada'] = None

    # Atualiza informações de auditoria
    user_info = st.session_state.get('user_info', {'username': 'Desconhecido'})
    current_username = user_info.get('username', 'Desconhecido')
    data_to_save_dict['Ultima_Alteracao_Por'] = current_username
    data_to_save_dict['Ultima_Alteracao_Em'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Incluir campos de estimativa de impostos no dicionário a ser salvo
    for field in ['Estimativa_Impostos_Total', 'Quantidade_Containers', 'Estimativa_Impostos_BR',
                   'Valor_USD', 'Estimativa_II_BR', 'Estimativa_IPI_BR', 'Estimativa_PIS_BR',
                   'Estimativa_COFINS_BR', 'Estimativa_Frete_USD', 'Estimativa_Seguro_BRL',
                   'Estimativa_Dolar_BRL', 'Estimativa_ICMS_BR',
                   'Nome_do_arquivo', 'Tipo_do_arquivo', 'Conteudo_do_arquivo']: # Adicionado campos de arquivo
        if field in edited_data and field in db_col_names_full:
            # Garante que números sejam salvos como float ou None se NaN
            val = edited_data[field]
            if isinstance(val, (float, int, np.number)):
                data_to_save_dict[field] = float(val) if not pd.isna(val) else None
            else:
                data_to_save_dict[field] = val # Mantém o tipo original se não for numérico

    if 'Processo_Novo' in edited_data and 'Processo_Novo' in db_col_names_full:
        data_to_save_dict['Processo_Novo'] = edited_data['Processo_Novo']
    else:
        logger.warning("Processo_Novo não encontrado nos dados editados.")
        _display_message_box("Erro: O nome do processo não foi fornecido. Não é possível salvar.", "error")
        return None


    success = False
    actual_process_id_after_save: Optional[Any] = None
    original_process_data_for_history = {} # Para coletar dados originais para histórico de forma mais robusta

    # Se for processo existente, carregar dados originais para histórico
    if not is_new_process_flag and process_id_from_form_load is not None:
        original_process_raw = db_manager.obter_processo_por_id(process_id_from_form_load if isinstance(process_id_from_form_load, int) else -1) if not db_manager._USE_FIRESTORE_AS_PRIMARY else db_manager.obter_processo_by_processo_novo(process_id_from_form_load)
        if original_process_raw:
            original_process_data_for_history = dict(original_process_raw)

    try:
        if is_new_process_flag:
            existing_process = db_manager.obter_processo_by_processo_novo(data_to_save_dict['Processo_Novo'])
            if existing_process:
                _display_message_box(f"Falha ao adicionar processo: Já existe um processo com a referência '{data_to_save_dict['Processo_Novo']}'. Por favor, altere a referência do processo clonado.", "error")
                return None
            
            success = db_manager.inserir_processo(data_to_save_dict)
            if success:
                # No Firestore, Processo_Novo é o ID. No SQLite, obtemos o último ID numérico.
                actual_process_id_after_save = data_to_save_dict['Processo_Novo'] if db_manager._USE_FIRESTORE_AS_PRIMARY else db_manager.obter_ultimo_processo_id()
                
                if not actual_process_id_after_save:
                    logger.error("Falha ao obter o ID do novo processo após a inserção (FireStore/SQLite).")
                    success = False
                
                if success:
                    db_type_for_history = "Firestore" if db_manager._USE_FIRESTORE_AS_PRIMARY else "SQLite"
                    db_manager.inserir_historico_processo(
                        actual_process_id_after_save, "Processo Criado", "N/A",
                        data_to_save_dict.get('Processo_Novo', 'Processo Criado'),
                        current_username, db_type=db_type_for_history
                    )
            
        else: # Atualizando processo existente
            success = db_manager.atualizar_processo(process_id_from_form_load, data_to_save_dict)
            actual_process_id_after_save = process_id_from_form_load

            if success and original_process_data_for_history:
                db_type_for_history = "Firestore" if db_manager._USE_FIRESTORE_AS_PRIMARY else "SQLite"
                for field_name, new_value in data_to_save_dict.items():
                    if field_name in ["Ultima_Alteracao_Por", "Ultima_Alteracao_Em"]:
                        continue

                    original_value = original_process_data_for_history.get(field_name)

                    # Normaliza valores para comparação, tratando None e NaN de forma consistente
                    normalized_original = None
                    if original_value is not None and not (isinstance(original_value, (float, np.number)) and pd.isna(original_value)):
                        normalized_original = str(original_value).strip() if isinstance(original_value, str) else original_value
                        # Para datas que podem ser strings
                        if isinstance(normalized_original, str) and field_name.startswith("Data_"):
                            try:
                                normalized_original = datetime.strptime(normalized_original, "%Y-%m-%d").date()
                            except ValueError:
                                pass # Não é uma data válido, mantém como string
                    
                    normalized_new = None
                    if new_value is not None and not (isinstance(new_value, (float, np.number)) and pd.isna(new_value)):
                        normalized_new = str(new_value).strip() if isinstance(new_value, str) else new_value
                        # Para datas que podem ser strings
                        if isinstance(normalized_new, str) and field_name.startswith("Data_"):
                            try:
                                normalized_new = datetime.strptime(normalized_new, "%Y-%m-%d").date()
                            except ValueError:
                                pass # Não é uma data válida, mantém como string


                    if normalized_original != normalized_new:
                        db_manager.inserir_historico_processo(
                            actual_process_id_after_save, field_name,
                            str(original_value) if original_value is not None and not (isinstance(original_value, (float, np.number)) and pd.isna(original_value)) else "Vazio",
                            str(new_value) if new_value is not None and not (isinstance(new_value, (float, np.number)) and pd.isna(new_value)) else "Vazio",
                            current_username, db_type=db_type_for_history
                        )

    except Exception as e:
        logger.exception(f"Erro de banco de dados durante a operação de salvar/atualizar processo.")
        _display_message_box(f"Erro no banco de dados ao salvar processo: {e}", "error")
        success = False

    if success:
        item_process_id_for_db_ops = None
        # Para salvar/deletar itens, usamos o ID do processo recém-salvo/atualizado
        # Que é o Processo_Novo no Firestore, ou o ID numérico no SQLite
        if db_manager._USE_FIRESTORE_AS_PRIMARY:
            item_process_id_for_db_ops = data_to_save_dict.get('Processo_Novo')
            if not item_process_id_for_db_ops: # Fallback se Processo_Novo não foi o ID de retorno
                item_process_id_for_db_ops = actual_process_id_after_save
        else:
            item_process_id_for_db_ops = actual_process_id_after_save

        logger.info(f"[_save_process_action] Processo salvo/atualizado com sucesso. ID do processo para itens: {item_process_id_for_db_ops}")
        
        if item_process_id_for_db_ops is not None:
            # Deleta todos os itens existentes e reinserir, estratégia mais simples por agora.
            # Em uma aplicação de grande escala, considerar delta updates ou UPSERT de itens.
            logger.info(f"[_save_process_action] Tentando deletar itens existentes para o processo ID {item_process_id_for_db_ops}.")
            try:
                db_manager.deletar_itens_processo(processo_id=item_process_id_for_db_ops)
                logger.info(f"[_save_process_action] Itens antigos deletados com sucesso para o processo ID {item_process_id_for_db_ops}.")
            except Exception as e:
                logger.error(f"[_save_process_action] ERRO ao deletar itens antigos para o processo ID {item_process_id_for_db_ops}: {e}")
                _display_message_box(f"Erro ao limpar itens antigos: {e}", "error")
            
            if 'process_items_data' in st.session_state and st.session_state.process_items_data:
                logger.info(f"[_save_process_action] Tentando inserir {len(st.session_state.process_items_data)} novos itens para o processo ID {item_process_id_for_db_ops}.")
                for i, item in enumerate(st.session_state.process_items_data):
                    try:
                        # Garante que os campos numéricos sejam convertidos corretamente para o DB
                        item_to_insert = item.copy()
                        for k_num in ['Quantidade', 'Peso Unitário', 'Valor Unitário', 'Valor total do item',
                                      'Estimativa_II_BR', 'Estimativa_IPI_BR', 'Estimativa_PIS_BR',
                                      'Estimativa_COFINS_BR', 'Estimativa_ICMS_BR', 'Frete_Rateado_USD',
                                      'Seguro_Rateado_BRL', 'VLMD_Item']:
                            if k_num in item_to_insert and item_to_insert[k_num] is not None:
                                try:
                                    item_to_insert[k_num] = float(item_to_insert[k_num])
                                except (ValueError, TypeError):
                                    item_to_insert[k_num] = 0.0 # Define um valor padrão se a conversão falhar
                            else:
                                item_to_insert[k_num] = 0.0 # Garante que None ou ausente virem 0.0
                        
                        logger.debug(f"[_save_process_action] Inserindo item {i} para processo ID {item_process_id_for_db_ops}: {item_to_insert}")
                        db_manager.inserir_item_processo(
                            processo_id=item_process_id_for_db_ops,
                            codigo_interno=item_to_insert.get('Código Interno'),
                            ncm=item_to_insert.get('NCM'),
                            cobertura=item_to_insert.get('Cobertura'),
                            sku=item_to_insert.get('SKU'),
                            quantidade=item_to_insert.get('Quantidade'),
                            peso_unitario=item_to_insert.get('Peso Unitário'),
                            valor_unitario=item_to_insert.get('Valor Unitário'),
                            valor_total_item=item_to_insert.get('Valor total do item'),
                            estimativa_ii_br=item_to_insert.get('Estimativa_II_BR'),
                            estimativa_ipi_br=item_to_insert.get('Estimativa_IPI_BR'),
                            estimativa_pis_br=item_to_insert.get('Estimativa_PIS_BR'),
                            estimativa_cofins_br=item_to_insert.get('Estimativa_COFINS_BR'),
                            estimativa_icms_br=item_to_insert.get('Estimativa_ICMS_BR'),
                            frete_rateado_usd=item_to_insert.get('Frete_Rateado_USD'),
                            seguro_rateado_brl=item_to_insert.get('Seguro_Rateado_BRL'),
                            vlmd_item=item_to_insert.get('VLMD_Item'),
                            denominacao_produto=item_to_insert.get('Denominação do produto'),
                            detalhamento_complementar_produto=item_to_insert.get('Detalhamento complementar do produto')
                        )
                        logger.debug(f"[_save_process_action] Item {i} inserido com sucesso para processo ID {item_process_id_for_db_ops}.")
                    except Exception as e:
                        logger.error(f"[_save_process_action] ERRO ao inserir item {i} para o processo ID {item_process_id_for_db_ops}: {e}")
                        _display_message_box(f"Erro ao inserir item {item.get('Código Interno', 'N/A')}: {e}", "error")
                logger.info(f"[_save_process_action] Tentativa de inserção de todos os itens concluída.")
            else:
                logger.info(f"[_save_process_action] Nenhum item para salvar em st.session_state.process_items_data para o processo ID {item_process_id_for_db_ops}.")

        # Limpar caches relacionados a itens do processo e aos próprios processos
        db_manager.obter_itens_processo.clear()
        db_manager.obter_historico_processo.clear()
        db_manager.obter_processo_por_id.clear()
        db_manager.obter_processo_by_processo_novo.clear()
        db_manager.obter_processos_filtrados.clear()
        db_manager.obter_todos_processos.clear()
        db_manager.obter_status_gerais_distintos.clear()
        logger.info(f"[_save_process_action] Caches de histórico, processos e itens limpos após salvamento/atualização.")

        _display_message_box(f"Processo {'adicionado' if is_new_process_flag else 'atualizado'} com sucesso!", "success")
        
        # Limpar estados relacionados a itens/upload para uma nova interação do formulário
        st.session_state.show_add_item_popup = False
        st.session_state.last_processed_upload_key = None
        st.session_state.process_items_loaded_for_id = None
        st.session_state.total_invoice_value_usd = 0.0
        st.session_state.total_invoice_weight_kg = 0.0

        return actual_process_id_after_save
    else:
        st.session_state.form_is_cloning = False
        st.session_state.last_cloned_from_id = None
        return None

# --- Esquema Padrão para Itens ---
DEFAULT_ITEM_SCHEMA = {
    "Código Interno": None, "NCM": None, "Cobertura": "NÃO", "SKU": None,
    "Quantidade": 0, "Peso Unitário": 0.0, "Valor Unitário": 0.0,
    "Valor total do item": 0.0, "Estimativa_II_BR": 0.0, "Estimativa_IPI_BR": 0.0,
    "Estimativa_PIS_BR": 0.0, "Estimativa_COFINS_BR": 0.0, "Estimativa_ICMS_BR": 0.0,
    "Frete_Rateado_USD": 0.0, "Seguro_Rateado_BRL": 0.0, "VLMD_Item": 0.0,
    "Denominação do produto": None, "Detalhamento complementar do produto": None,
    "Fornecedor": None, "Invoice N#": None
}

def _standardize_item_data(item_dict: Any, fornecedor: Optional[str] = None, invoice_n: Optional[str] = None) -> Dict[str, Any]:
    """
    Garante que um dicionário de item esteja em conformidade com o esquema padrão.
    Lida com chaves no formato snake_case (do DB) e "Capitalized With Spaces" (do formulário/Excel).
    """
    standardized_item = DEFAULT_ITEM_SCHEMA.copy()
    
    if not isinstance(item_dict, dict):
        logger.warning(f"[_standardize_item_data] Input item_dict não é um dicionário: {type(item_dict)}. Retornando esquema padrão.")
        standardized_item['Fornecedor'] = fornecedor
        standardized_item['Invoice N#'] = invoice_n
        return standardized_item

    # Mapeamento de chaves snake_case do DB para chaves 'Capitalized With Spaces' do schema/display
    db_to_schema_map = {
        "codigo_interno": "Código Interno", "ncm": "NCM", "cobertura": "Cobertura", "sku": "SKU",
        "quantidade": "Quantidade", "peso_unitario": "Peso Unitário", "valor_unitario": "Valor Unitário",
        "valor_total_item": "Valor total do item", "estimativa_ii_br": "Estimativa_II_BR",
        "estimativa_ipi_br": "Estimativa_IPI_BR", "estimativa_pis_br": "Estimativa_PIS_BR",
        "estimativa_cofins_br": "Estimativa_COFINS_BR", "estimativa_icms_br": "Estimativa_ICMS_BR",
        "frete_rateado_usd": "Frete_Rateado_USD", "seguro_rateado_brl": "Seguro_Rateado_BRL",
        "vlmd_item": "VLMD_Item", "denominacao_produto": "Denominação do produto",
        "detalhamento_complementar_produto": "Detalhamento complementar do produto",
    }

    # Primeiro, preencher com valores do item_dict usando o mapeamento para campos do DB
    for db_key, schema_key in db_to_schema_map.items():
        if db_key in item_dict:
            standardized_item[schema_key] = item_dict[db_key]
        elif schema_key in item_dict:
             standardized_item[schema_key] = item_dict[schema_key]

    # Campos que podem vir diretamente sem mapeamento
    for key in DEFAULT_ITEM_SCHEMA.keys():
        if key not in standardized_item and key in item_dict:
            standardized_item[key] = item_dict[key]

    # Lida com Fornecedor e Invoice N#
    if fornecedor is not None:
        standardized_item['Fornecedor'] = fornecedor
    if invoice_n is not None:
        standardized_item['Invoice N#'] = invoice_n

    # Converte tipos para garantir compatibilidade
    for k, v in standardized_item.items():
        if k in ["Quantidade"]:
            # Converte para numérico, se for NaN, vira 0, então para int
            numeric_val = pd.to_numeric(v, errors='coerce')
            standardized_item[k] = int(numeric_val if not pd.isna(numeric_val) else 0)
        elif k in ["Peso Unitário", "Valor Unitário", "Valor total do item",
                   "Estimativa_II_BR", "Estimativa_IPI_BR", "Estimativa_PIS_BR",
                   "Estimativa_COFINS_BR", "Estimativa_ICMS_BR", "Frete_Rateado_USD",
                   "Seguro_Rateado_BRL", "VLMD_Item"]:
            # Converte para numérico, se for NaN, vira 0.0, então para float
            numeric_val = pd.to_numeric(v, errors='coerce')
            standardized_item[k] = float(numeric_val if not pd.isna(numeric_val) else 0.0)
        elif isinstance(v, str) and v.strip() == '':
            standardized_item[k] = None # Converte strings vazias para None
    
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
        if file_extension.lower() == '.csv':
            try: df = pd.read_csv(uploaded_file, encoding='utf-8')
            except UnicodeDecodeError: df = pd.read_csv(uploaded_file, encoding='latin-1')
            except Exception: df = pd.read_csv(uploaded_file, sep=';')
        elif file_extension.lower() in ('.xlsx', '.xls'):
            df = pd.read_excel(uploaded_file)
        else:
            _display_message_box("Formato de arquivo não suportado. Por favor, use .csv, .xls ou .xlsx.", "error")
            return False

        if df.empty:
            _display_message_box("O arquivo importado está vazio.", "warning")
            return False

        column_mapping_excel_to_internal = {
            "Cobertura": "Cobertura", "Codigo interno": "Código Interno",
            "Denominação": "Denominação do produto", "SKU": "SKU",
            "Quantidade": "Quantidade", "Preço": "Valor Unitário", "NCM": "NCM",
            "Peso Unitário": "Peso Unitário" # Adicionado ao mapeamento para o template de itens
        }
        
        df_renamed = df.rename(columns=column_mapping_excel_to_internal, errors='ignore')

        # Converte apenas colunas numéricas que existem
        numeric_cols_to_convert = ["Quantidade", "Valor Unitário", "Peso Unitário"]
        for col in numeric_cols_to_convert:
            if col in df_renamed.columns:
                df_renamed[col] = pd.to_numeric(df_renamed[col], errors='coerce').fillna(0).astype(float)

        new_items_from_file = []
        for index, row in df_renamed.iterrows():
            item_data = row.to_dict()
            if 'NCM' in item_data and item_data['NCM'] is not None:
                item_data['NCM'] = re.sub(r'\D', '', str(item_data['NCM'])) # Limpa NCM para ter apenas dígitos

            standardized_item = _standardize_item_data(item_data, current_fornecedor_context, current_invoice_n_context)
            
            if not isinstance(standardized_item, dict):
                logger.error(f"CRITICAL ERROR: _standardize_item_data returned non-dict (type {type(standardized_item)}) for row {index}. Skipping this item.")
                continue

            qty = standardized_item.get('Quantidade', 0)
            unit_val = standardized_item.get('Valor Unitário', 0.0)
            standardized_item["Valor total do item"] = qty * unit_val
            
            new_items_from_file.append(standardized_item)
        
        # Substitui a lista de itens existente pelos itens importados
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
    template_columns = ["Cobertura", "Codigo interno", "Denominação", "SKU", "Quantidade", "Preço", "NCM", "Peso Unitário"]
    example_row = {
        "Cobertura": "NÃO", "Codigo interno": "INT-001", "Denominação": "Processador Intel Core i7",
        "SKU": "CPU-I7-12700K", "Quantidade": 5, "Preço": 350.00, "NCM": "84715010", "Peso Unitário": 0.5
    }
    df_template = pd.DataFrame([example_row], columns=template_columns) 

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_template.to_excel(writer, index=False, sheet_name='Template Itens')
    output.seek(0)
    return output

# NOVO: Função para gerar o template de dados gerais do processo
def _generate_process_excel_template():
    """Gera um arquivo Excel padrão para inserção de dados gerais do processo."""
    # Colunas na ordem e nomes solicitados pelo usuário
    template_columns = [
        "Process Reference", "Supplier", "Items", "PI / Invoice", "QTY", "Invoice Value USD",
        "PAY?", "OC", "Purchase Date", "DI Estimated R$", "Freight USD Est.",
        "Shipping Date", "AGENTE", "Status", "ETA Pichau", "Status para e-mail",
        "AIR or SEA", "Containers QTY", "Origin", "Dest.", "Terms", "Buyer", "Modal"
    ]
    example_row = {
        "Process Reference": "PR-2024-EXEMPLO",
        "Supplier": "Acme Corp",
        "Items": "Eletrônicos",
        "PI / Invoice": "INV-2024-001",
        "QTY": 100,
        "Invoice Value USD": 15000.00,
        "PAY?": "Sim",
        "OC": "OC-XYZ-001",
        "Purchase Date": "2024-01-10",
        "DI Estimated R$": 5000.00,
        "Freight USD Est.": 300.00,
        "Shipping Date": "2024-02-15",
        "AGENTE": "Agente ABC",
        "Status": "Desembaraço Aduaneiro",
        "ETA Pichau": "2024-03-05",
        "Status para e-mail": "Em Andamento",
        "AIR or SEA": "AIR", # Pode ser "AIR" ou "SEA"
        "Containers QTY": 0, # Preencher se "AIR or SEA" for "SEA"
        "Origin": "Shenzhen",
        "Dest.": "São Paulo",
        "Terms": "FOB",
        "Buyer": "João Silva",
        "Modal": "Aéreo" # Pode ser "Aéreo" ou "Maritimo"
    }
    df_template = pd.DataFrame([example_row], columns=template_columns) 

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_template.to_excel(writer, index=False, sheet_name='Template Dados Gerais')
    output.seek(0)
    return output

# NOVO: Função para importar dados gerais do processo de um arquivo Excel/CSV
def _import_process_from_excel(uploaded_file: Any, form_state_key: str) -> bool:
    """
    Importa dados gerais do processo de um arquivo Excel/CSV e os atualiza no st.session_state.
    """
    if uploaded_file is None:
        return False

    file_extension = os.path.splitext(uploaded_file.name)[1]
    df = None

    try:
        if file_extension.lower() == '.csv':
            try: df = pd.read_csv(uploaded_file, encoding='utf-8')
            except UnicodeDecodeError: df = pd.read_csv(uploaded_file, encoding='latin-1')
            except Exception: df = pd.read_csv(uploaded_file, sep=';')
        elif file_extension.lower() in ('.xlsx', '.xls'):
            df = pd.read_excel(uploaded_file)
        else:
            _display_message_box("Formato de arquivo não suportado. Por favor, use .csv, .xls ou .xlsx.", "error")
            return False

        if df.empty:
            _display_message_box("O arquivo importado está vazio.", "warning")
            return False

        # Mapeamento de colunas da planilha para os campos do formulário
        column_mapping_excel_to_form_fields = {
            "Process Reference": "Processo_Novo",
            "Supplier": "Fornecedor",
            "Items": "Tipos_de_item",
            "PI / Invoice": "N_Invoice",
            "QTY": "Quantidade",
            "Invoice Value USD": "Valor_USD",
            "PAY?": "Pago",
            "OC": "N_Ordem_Compra",
            "Purchase Date": "Data_Compra",
            "DI Estimated R$": "Estimativa_Impostos_Total",
            "Freight USD Est.": "Estimativa_Frete_USD",
            "Shipping Date": "Data_Embarque",
            "AGENTE": "Agente_de_Carga_Novo",
            "Status": "Observacao",
            "ETA Pichau": "Previsao_Pichau",
            "Status para e-mail": "Status_Geral",
            "AIR or SEA": "Modal_Air_Sea_Temp", # Usar um campo temporário para desambiguar com 'Modal'
            "Containers QTY": "Quantidade_Containers",
            "Origin": "Origem",
            "Dest.": "Destino",
            "Terms": "INCOTERM",
            "Buyer": "Comprador",
            "Modal": "Modal", # Pode estar presente, mas "AIR or SEA" tem prioridade
        }
        
        # Consideramos apenas a primeira linha para os dados do processo
        process_data_from_file = df.iloc[0].to_dict()

        updates_made = False
        for col_excel, form_field_name in column_mapping_excel_to_form_fields.items():
            if col_excel in process_data_from_file:
                value = process_data_from_file[col_excel]
                # Conversões de tipo e tratamento de valores
                if pd.isna(value):
                    value = None
                elif form_field_name in ["Quantidade", "Quantidade_Containers"]:
                    value = int(value) if value is not None else 0
                elif form_field_name in ["Valor_USD", "Estimativa_Impostos_Total", "Estimativa_Frete_USD"]:
                    value = float(value) if value is not None else 0.0
                elif "Date" in col_excel and value is not None:
                    try:
                        if isinstance(value, (datetime, date)):
                            value = value.strftime("%Y-%m-%d")
                        elif isinstance(value, str):
                            value = pd.to_datetime(value).strftime("%Y-%m-%d")
                    except Exception:
                        value = None # Fallback se a data não for reconhecida
                
                # Lógica especial para "AIR or SEA" vs "Modal"
                if form_field_name == "Modal_Air_Sea_Temp":
                    if value == "AIR":
                        st.session_state[form_state_key]["Modal"] = "Aéreo"
                    elif value == "SEA":
                        st.session_state[form_state_key]["Modal"] = "Maritimo"
                    # Se "Modal" também estiver na planilha, "AIR or SEA" tem prioridade
                    if "Modal" in process_data_from_file and "AIR or SEA" not in process_data_from_file:
                         st.session_state[form_state_key]["Modal"] = process_data_from_file["Modal"]

                elif form_field_name != "Modal" or ("AIR or SEA" not in process_data_from_file):
                    # Não sobrescreve "Modal" se "AIR or SEA" estiver presente e já tiver sido tratado
                    st.session_state[form_state_key][form_field_name] = value
                
                updates_made = True

        if updates_made:
            _display_message_box(f"Dados gerais do processo importados com sucesso do arquivo '{uploaded_file.name}'! Salve o processo para persistir.", "success")
            return True
        else:
            _display_message_box("Nenhum dado de processo reconhecível encontrado na planilha.", "warning")
            return False

    except Exception as e:
        _display_message_box(f"Erro ao processar o arquivo Excel/CSV de dados gerais: {e}", "error")
        logger.exception("Erro durante a importação de dados gerais do processo do arquivo.")
        return False


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
            "Origem": {"label": "Origem:", "type": "text"},
            "Destino": {"label": "Destino:", "type": "text"},
            "Comprador": {"label": "Comprador:", "type": "text"},
        },
        "col2": {
            "Modal": {"label": "Modal:", "type": "dropdown", "values": ["", "Aéreo", "Maritimo"]},
            "Navio": {"label": "Navio:", "type": "text", "conditional_field": "Modal", "conditional_value": "Maritimo"}, 
            "Quantidade_Containers": {"label": "Quantidade de Containers:", "type": "number", "conditional_field": "Modal", "conditional_value": "Maritimo"},
            "INCOTERM": {"label": "INCOTERM:", "type": "dropdown", "values": ["","EXW","FCA","FAS","FOB","CFR","CIF","CPT","CIP","DPU","DAP","DDP"]},
            "Pago": {"label": "Pago?:", "type": "dropdown", "values": ["Não", "Sim"]},
            "Data_Compra": {"label": "Data de Compra:", "type": "date"},
            "Data_Embarque": {"label": "Data de Embarque:", "type": "date"},
            "ETA_Recinto": {"label": "ETA no Recinto:", "type": "date"},
            "Previsao_Pichau": {"label": "Previsão na Pichau:", "type": "date"},
            "Status_Geral": {"label": "Status Geral (para e-mail):", "type": "dropdown", "values": db_manager.STATUS_OPTIONS},
        }
    },
    "Itens": {},
    "Valores e Estimativas": {
        "Estimativa_Dolar_BRL": {"label": "Cambio Estimado (R$):", "type": "currency_br"},
        "Valor_USD": {"label": "Valor (USD):", "type": "currency_usd", "disabled": True},
        "Estimativa_Frete_USD": {"label": "Estimativa de Frete (USD):", "type": "currency_usd"},
        "Estimativa_Seguro_BRL": {"label": "Estimativa Seguro (R$):", "type": "currency_br"},
        "Estimativa_II_BR": {"label": "Estimativa de II (R$):", "type": "currency_br", "disabled": True},
        "Estimativa_IPI_BR": {"label": "Estimativa de IPI (R$):", "type": "currency_br", "disabled": True},
        "Estimativa_PIS_BR": {"label": "Estimativa de PIS (R$):", "type": "currency_br", "disabled": True},
        "Estimativa_COFINS_BR": {"label": "Estimativa de COFINS (R$):", "type": "currency_br", "disabled": True},
        "Estimativa_ICMS_BR": {"label": "Estimativa de ICMS (R$):", "type": "currency_br"},
        "Estimativa_Impostos_Total": {"label": "Estimativa Impostos (R$):", "type": "currency_br", "disabled": True},
        "Estimativa_Impostos_BR": {"label": "Estimativa Impostos (Antigo):", "type": "currency_br", "disabled": True},
    },
    "Status Operacional": {
        # Campos de status operacional movidos para "Dados Gerais" ou mantidos aqui se forem muito específicos.
        # "Data_Registro": {"label": "Data de Registro:", "type": "date"}, # Movido para Dados Gerais
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
        # Novos campos para upload de arquivo
        "Nome_do_arquivo": {"label": "Nome do Arquivo:", "type": "text", "disabled": True},
        "Tipo_do_arquivo": {"label": "Tipo do Arquivo:", "type": "text", "disabled": True},
        "Conteudo_do_arquivo": {"label": "Conteúdo do Arquivo (Base64):", "type": "hidden"}, # Campo oculto para o conteúdo
    }
}

def _initialize_form_state(form_state_key: str, process_identifier: Optional[Any], is_cloning: bool):
    """Inicializa ou reinicializa o estado do formulário na session_state."""
    # Define a chave de carregamento para saber se os itens já foram carregados para este processo
    st.session_state.current_loaded_form_key = form_state_key
    st.session_state[f'{form_state_key}_is_new_process_flag'] = process_identifier is None or is_cloning
    
    process_data = {}
    items_loaded_successfully = False

    # Carregar dados do processo
    if not (process_identifier is None or is_cloning): # Editando um processo existente
        raw_data = db_manager.obter_processo_por_id(process_identifier) if isinstance(process_identifier, int) else db_manager.obter_processo_by_processo_novo(process_identifier)
        if raw_data:
            process_data = dict(raw_data)
        else:
            _display_message_box(f"Processo '{process_identifier}' não encontrado para edição.", "error")
            st.session_state.current_page = "Follow-up Importação"
            st.rerun()
            return {} # Retorna dicionário vazio em caso de erro

    # Lógica de clonagem
    if is_cloning and process_identifier is not None:
        raw_data = db_manager.obter_processo_por_id(process_identifier) if isinstance(process_identifier, int) else db_manager.obter_processo_by_processo_novo(process_identifier)
        if raw_data:
            process_data = dict(raw_data)
            cloned_process_novo = f"{process_data.get('Processo_Novo', 'NovoProcesso')}_Clone_{datetime.now().strftime('%H%M%S')}"
            process_data['Processo_Novo'] = cloned_process_novo # Altera para o nome clonado
            process_data['id'] = None # Garante que é um novo registro no DB
            # Limpa campos de auditoria para o clone
            process_data['Ultima_Alteracao_Por'] = None
            process_data['Ultima_Alteracao_Em'] = None
            # Os dados do arquivo não devem ser clonados, já que o arquivo é único por processo.
            process_data['Nome_do_arquivo'] = None
            process_data['Tipo_do_arquivo'] = None
            process_data['Conteudo_do_arquivo'] = None
            _display_message_box(f"Processo clonado de '{process_data.get('Processo_Novo', 'N/A')}' com sucesso. Por favor, edite a referência do novo processo.", "success")
        else:
            _display_message_box(f"Processo '{process_identifier}' não encontrado para clonagem.", "error")
            st.session_state.current_page = "Follow-up Importação"
            st.rerun()
            return {} # Retorna dicionário vazio em caso de erro
    
    # Preenche st.session_state[form_state_key] com os dados carregados ou padrões
    st.session_state[form_state_key] = {}
    for tab_name, tab_config in campos_config_tabs.items():
        if "col1" in tab_config: # Para tabs com colunas
            for field_name, config in tab_config["col1"].items():
                st.session_state[form_state_key][field_name] = process_data.get(field_name)
        if "col2" in tab_config: # Para tabs com colunas
            for field_name, config in tab_config["col2"].items():
                st.session_state[form_state_key][field_name] = process_data.get(field_name)
        if tab_config == campos_config_tabs["Valores e Estimativas"] or \
           tab_config == campos_config_tabs["Status Operacional"] or \
           tab_config == campos_config_tabs["Documentação"]: # Para tabs diretas
            for field_name, config in tab_config.items():
                st.session_state[form_state_key][field_name] = process_data.get(field_name)

    st.session_state[form_state_key]["Observacao"] = process_data.get("Observacao", "")

    # Define valores padrão para campos vazios, se não for clonagem e não tiver valor
    if is_cloning or (process_identifier is None):
        st.session_state[form_state_key].update({
            "Quantidade": st.session_state[form_state_key].get("Quantidade", 0), 
            "Quantidade_Containers": st.session_state[form_state_key].get("Quantidade_Containers", 0), 
            "Modal": st.session_state[form_state_key].get("Modal", ""),
            "INCOTERM": st.session_state[form_state_key].get("INCOTERM", ""), 
            "Pago": st.session_state[form_state_key].get("Pago", "Não"), 
            "Status_Geral": st.session_state[form_state_key].get("Status_Geral", ""), 
            "Documentos_Revisados": st.session_state[form_state_key].get("Documentos_Revisados", "Não"),
            "Conhecimento_Embarque": st.session_state[form_state_key].get("Conhecimento_Embarque", "Não"), 
            "Descricao_Feita": st.session_state[form_state_key].get("Descricao_Feita", "Não"), 
            "Descricao_Enviada": st.session_state[form_state_key].get("Descricao_Enviada", "Não"),
            "Nota_feita": st.session_state[form_state_key].get("Nota_feita", "Não"), 
            "Conferido": st.session_state[form_state_key].get("Conferido", "Não"), 
            "Data_Compra": st.session_state[form_state_key].get("Data_Compra", None), 
            "Data_Embarque": st.session_state[form_state_key].get("Data_Embarque", None),
            "ETA_Recinto": st.session_state[form_state_key].get("ETA_Recinto", None), 
            "Data_Registro": st.session_state[form_state_key].get("Data_Registro", None), 
            "Previsao_Pichau": st.session_state[form_state_key].get("Previsao_Pichau", None),
            "DI_ID_Vinculada": st.session_state[form_state_key].get("DI_ID_Vinculada", None), 
            "Estimativa_Impostos_Total": st.session_state[form_state_key].get("Estimativa_Impostos_Total", 0.0), 
            "Estimativa_Impostos_BR": st.session_state[form_state_key].get("Estimativa_Impostos_BR", 0.0),
            "Estimativa_Dolar_BRL": st.session_state[form_state_key].get("Estimativa_Dolar_BRL", 0.0), 
            "Valor_USD": st.session_state[form_state_key].get("Valor_USD", 0.0), 
            "Estimativa_Frete_USD": st.session_state[form_state_key].get("Estimativa_Frete_USD", 0.0),
            "Estimativa_Seguro_BRL": st.session_state[form_state_key].get("Estimativa_Seguro_BRL", 0.0), 
            "Estimativa_II_BR": st.session_state[form_state_key].get("Estimativa_II_BR", 0.0), 
            "Estimativa_IPI_BR": st.session_state[form_state_key].get("Estimativa_IPI_BR", 0.0), 
            "Estimativa_PIS_BR": st.session_state[form_state_key].get("Estimativa_PIS_BR", 0.0), 
            "Estimativa_COFINS_BR": st.session_state[form_state_key].get("Estimativa_COFINS_BR", 0.0), 
            "Estimativa_ICMS_BR": st.session_state[form_state_key].get("Estimativa_ICMS_BR", 0.0),
            "Nome_do_arquivo": st.session_state[form_state_key].get("Nome_do_arquivo", None),
            "Tipo_do_arquivo": st.session_state[form_state_key].get("Tipo_do_arquivo", None),
            "Conteudo_do_arquivo": st.session_state[form_state_key].get("Conteudo_do_arquivo", None),
        })

    # Carregamento de itens do processo
    process_id_for_items_load = process_data.get('id') # ID numérico para SQLite
    if db_manager._USE_FIRESTORE_AS_PRIMARY:
        process_id_for_items_load = process_data.get('Processo_Novo') # Processo_Novo para Firestore

    # Carrega itens apenas se o ID for válido e os itens ainda não foram carregados para este processo
    if process_id_for_items_load is not None and (st.session_state.get('process_items_loaded_for_id') != process_id_for_items_load or not st.session_state.get('process_items_data')):
        logger.info(f"[_initialize_form_state] Recarregando itens para processo ID: {process_id_for_items_load} (type: {type(process_id_for_items_load)})")
        retrieved_items = db_manager.obter_itens_processo(process_id_for_items_load)
        st.session_state.process_items_data = [
            _standardize_item_data(dict(row), process_data.get("Fornecedor"), process_data.get("N_Invoice")) 
            for row in retrieved_items
        ]
        logger.info(f"[_initialize_form_state] Obtidos {len(st.session_state.process_items_data)} itens após recarregar para ID: {process_id_for_items_load}.")
        st.session_state.process_items_loaded_for_id = process_id_for_items_load
        items_loaded_successfully = True
    elif process_identifier is None and not is_cloning: # Novo processo, sem itens
        st.session_state.process_items_data = []
        st.session_state.process_items_loaded_for_id = None
        items_loaded_successfully = True
    else: # Itens já carregados para este ID ou ID é None (clonagem inicial)
        if is_cloning and not st.session_state.get('process_items_data'): # Se é clone, e itens não foram preenchidos
             # Isso é tratado na clonagem, onde process_items_data é preenchido manualmente.
             pass
        elif not st.session_state.get('process_items_data'): # Caso não seja clone nem edição, e os dados estejam vazios
             st.session_state.process_items_data = []
        items_loaded_successfully = True # Considera que o estado atual dos itens está ok.

    # Garante que total_invoice_value_usd e total_invoice_weight_kg são calculados ao inicializar
    if items_loaded_successfully and st.session_state.get('process_items_data'):
        df_items_calc = pd.DataFrame(st.session_state.process_items_data)
        st.session_state.total_invoice_value_usd = df_items_calc["Valor total do item"].sum() if "Valor total do item" in df_items_calc.columns else 0.0
        
        total_invoice_weight_kg_calc = 0.0
        if "Peso Unitário" in df_items_calc.columns and "Quantidade" in df_items_calc.columns:
            # Garante que as colunas são numéricas antes de multiplicar
            peso_unitario_numeric = pd.to_numeric(df_items_calc['Peso Unitário'], errors='coerce').fillna(0)
            quantidade_numeric = pd.to_numeric(df_items_calc['Quantidade'], errors='coerce').fillna(0)
            total_invoice_weight_kg_calc = (peso_unitario_numeric * quantidade_numeric).sum()
        st.session_state.total_invoice_weight_kg = total_invoice_weight_kg_calc
    else:
        st.session_state.total_invoice_value_usd = 0.0
        st.session_state.total_invoice_weight_kg = 0.0

    return process_data # Retorna process_data para uso no display (importante para process_novo, etc.)

def show_process_form_page(process_identifier: Optional[Any] = None, reload_processes_callback: Optional[callable] = None, is_cloning: bool = False):
    """
    Exibe o formulário de edição/criação de processo em uma página dedicada.
    process_identifier: ID (int) ou Processo_Novo (str) do processo a ser editado. None para novo processo.
    reload_processes_callback: Função para chamar na página principal para recarregar os dados.
    is_cloning: Se True, indica que a operação é de clonagem.
    """
    background_image_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'assets', 'logo_navio_atracado.png')
    set_background_image(background_image_path)

    if reload_processes_callback:
        st.session_state.form_reload_processes_callback = reload_processes_callback

    # Inicializa process_items_data e process_items_loaded_for_id no início da função
    st.session_state.setdefault('process_items_data', [])
    st.session_state.setdefault('process_items_loaded_for_id', None)

    # Flag para saber se estamos em um "novo" processo (inclui clones)
    is_new_process = process_identifier is None or is_cloning

    # Gerenciamento de estado do formulário para evitar recargas desnecessárias e manter dados
    form_state_id_key_base = process_identifier if not is_cloning else f"new_clone_from_{process_identifier}"
    if process_identifier is None and not is_cloning:
        form_state_id_key_base = 'new_empty_process_form_instance'
    form_state_key = f"form_fields_process_{form_state_id_key_base}"

    # Inicializa o estado do formulário SOMENTE se estivermos carregando um processo diferente
    # ou se for um novo processo/clone que ainda não foi inicializado/processado.
    if (form_state_key not in st.session_state or 
        st.session_state.get('current_loaded_form_key') != form_state_key or
        (is_cloning and st.session_state.get(f'{form_state_key}_is_new_process_flag', False) and not st.session_state.get('process_items_data'))
        ): # Adiciona verificação de itens para clones
        
        logger.info(f"[show_process_form_page] Inicializando estado do formulário para key: {form_state_key}, process_id: {process_identifier}, is_cloning: {is_cloning}")
        process_data_from_init = _initialize_form_state(form_state_key, process_identifier, is_cloning)
        if is_cloning and not process_data_from_init and process_identifier is not None: 
            return # Sai da função, já exibe mensagem e reruns dentro de _initialize_form_state

    # Se estamos editando, garantir que process_id seja o ID real do processo
    process_id: Optional[Any] = None
    if not is_new_process and process_identifier is not None:
        if db_manager._USE_FIRESTORE_AS_PRIMARY:
            # No Firestore, o process_identifier já pode ser o Processo_Novo (string)
            process_id = process_identifier 
        else: # SQLite
            if isinstance(process_identifier, int):
                process_id = process_identifier
            else: # Se for string Processo_Novo, tenta buscar o ID numérico
                existing_process_data = db_manager.obter_processo_by_processo_novo(process_identifier)
                if existing_process_data:
                    process_id = existing_process_data['id']
                else:
                    process_id = None # Processo_Novo não encontrado no SQLite

    linked_di_id = st.session_state[form_state_key].get('DI_ID_Vinculada')
    linked_di_number = None
    if linked_di_id:
        linked_di_data = db_utils.get_declaracao_by_id(linked_di_id)
        if linked_di_data:
            linked_di_number = _format_di_number(str(linked_di_data.get('numero_di') if isinstance(linked_di_data, dict) else linked_di_data['numero_di']))

    st.markdown(f"### {'Novo Processo' if st.session_state[f'{form_state_key}_is_new_process_flag'] else f'Editar Processo: {st.session_state[form_state_key].get('Processo_Novo', '')}'}")

    # Sempre inicialize as flags de popup se não existirem
    st.session_state.setdefault('show_add_item_popup', False)
    st.session_state.setdefault('selected_item_indices', [])
    st.session_state.setdefault('show_edit_item_popup', False)
    st.session_state.setdefault('item_to_edit_index', None)
    st.session_state.setdefault('last_processed_upload_key', None)

    tabs_names = list(campos_config_tabs.keys())
    tabs = st.tabs(tabs_names)

    for i, tab_name in enumerate(tabs_names):
        with tabs[i]:
            if tab_name == "Dados Gerais":
                col_left, col_right = st.columns(2) 

                with col_left:
                    for field_name, config in campos_config_tabs[tab_name]["col1"].items():
                        current_value = st.session_state[form_state_key].get(field_name)
                        if config["type"] == "number":
                            default_value_for_number_input = int(current_value) if (current_value is not None and not pd.isna(current_value)) else 0
                            widget_value = st.number_input(config["label"], value=default_value_for_number_input, format="%d", key=f"{form_state_key}_{field_name}", disabled=config.get("disabled", False))
                            st.session_state[form_state_key][field_name] = int(widget_value) if widget_value is not None else None
                        else:
                            widget_value = st.text_input(config["label"], value=current_value if current_value is not None else "", key=f"{form_state_key}_{field_name}", disabled=config.get("disabled", False))
                            st.session_state[form_state_key][field_name] = widget_value if widget_value else None

                with col_right:
                    current_modal_selection = st.session_state[form_state_key].get("Modal", "")
                    for field_name, config in campos_config_tabs[tab_name]["col2"].items():
                        current_value = st.session_state[form_state_key].get(field_name)
                        is_conditional_field = "conditional_field" in config
                        is_editable_conditional = True

                        if is_conditional_field and current_modal_selection != config["conditional_value"]:
                            is_editable_conditional = False
                            # Se o campo condicional não está ativo, ele não deve ter valor persistido
                            if st.session_state[form_state_key][field_name] is not None:
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
                            widget_value = st.selectbox(config["label"], options=options, index=default_index, key=f"{form_state_key}_{field_name}", disabled=is_disabled_overall)
                            st.session_state[form_state_key][field_name] = widget_value if widget_value else None
                        elif config["type"] == "number":
                            default_value_for_number_input = int(current_value) if (current_value is not None and not pd.isna(current_value)) else 0
                            widget_value = st.number_input(config["label"], value=default_value_for_number_input, format="%d", key=f"{form_state_key}_{field_name}", disabled=is_disabled_overall)
                            st.session_state[form_state_key][field_name] = int(widget_value) if widget_value is not None else None
                        elif config["type"] == "date":
                            current_value_dt = None
                            if current_value:
                                try:
                                    if isinstance(current_value, str):
                                        current_value_dt = datetime.strptime(current_value, "%Y-%m-%d").date()
                                    elif isinstance(current_value, (datetime, date)):
                                        current_value_dt = current_value.date()
                                except ValueError:
                                    current_value_dt = None
                            widget_value = st.date_input(config["label"], value=current_value_dt, key=f"{form_state_key}_{field_name}", format="DD/MM/YYYY", disabled=is_disabled_overall)
                            st.session_state[form_state_key][field_name] = widget_value.strftime("%Y-%m-%d") if widget_value else None
                        else:
                            widget_value = st.text_input(config["label"], value=current_value if current_value is not None else "", key=f"{form_state_key}_{field_name}", disabled=is_disabled_overall, help="Selecione 'Maritimo' no campo Modal para habilitar." if is_conditional_field and not is_editable_conditional else None)
                            st.session_state[form_state_key][field_name] = widget_value if widget_value else None

                st.markdown("---")
                st.subheader("Importar/Exportar Dados do Processo")
                col_download_process_template, col_upload_process_excel = st.columns([0.25, 0.75])
                with col_download_process_template:
                    process_excel_template_data = _generate_process_excel_template()
                    st.download_button(
                        label="Baixar Template Processo", data=process_excel_template_data, file_name="template_dados_gerais_processo.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_process_excel_template_new"
                    )
                with col_upload_process_excel:
                    uploaded_process_file = st.file_uploader("Upload Excel/CSV de Dados do Processo", type=["csv", "xls", "xlsx"], key="upload_process_file_new")
                    current_process_upload_key = (uploaded_process_file.name, uploaded_process_file.size) if uploaded_process_file else None
                    
                    if uploaded_process_file is not None and current_process_upload_key != st.session_state.get('last_processed_process_upload_key'):
                        if _import_process_from_excel(uploaded_process_file, form_state_key):
                            st.session_state.last_processed_process_upload_key = current_process_upload_key
                            st.rerun()
                        else:
                            st.session_state.last_processed_process_upload_key = None

            elif tab_name == "Itens":
                st.subheader("Itens do Processo")
                
                current_fornecedor_context = st.session_state[form_state_key].get("Fornecedor", "N/A")
                current_invoice_n_context = st.session_state[form_state_key].get("N_Invoice", "N/A")
                
                col_add_item, col_edit_item, col_delete_item = st.columns([0.15, 0.15, 0.15])

                with col_add_item:
                    if st.button("Adicionar Item", key="add_item_button_in_items_tab"):
                        st.session_state.show_add_item_popup = True
                        st.session_state.show_edit_item_popup = False
                
                if st.session_state.get('show_add_item_popup', False):
                    # Forçando um uuid para a chave do popover para evitar reuso acidental se o conteúdo interno mudar
                    with st.popover("Adicionar Novo Item", key=f"add_item_popover_{uuid.uuid4()}"):
                        with st.form("add_item_form_fixed", clear_on_submit=True):
                            new_item_codigo_interno = st.text_input("Código Interno", key="new_item_codigo_interno_popup")
                            all_ncm_items = db_utils.selecionar_todos_ncm_itens()
                            # Garantir que ncm_options sempre tenha uma opção vazia no início
                            ncm_options = [""] + sorted([ncm_list_page.format_ncm_code(item['ncm_code']) for item in all_ncm_items]) if ncm_list_page else [""]
                            new_item_ncm_display = st.selectbox("NCM", options=ncm_options, key="new_item_ncm_popup")
                            new_item_cobertura = st.selectbox("Cobertura", options=["SIM", "NÃO"], key="new_item_cobertura_popup")
                            new_item_sku = st.text_input("SKU", key="new_item_sku_popup")
                            new_item_quantidade = st.number_input("Quantidade", min_value=0, value=0, step=1, key="new_item_quantidade_popup")
                            new_item_valor_unitario = st.number_input("Valor Unitário (USD)", min_value=0.0, format="%.2f", key="new_item_valor_unitario_popup")
                            new_item_peso_unitario = st.number_input("Peso Unitário (KG)", min_value=0.0, format="%.4f", key="new_item_peso_unitario_popup")
                            new_item_denominacao = st.text_input("Denominação do produto", key="new_item_denominacao_popup")
                            new_item_detalhamento = st.text_input("Detalhamento complementar do produto", key="new_item_detalhamento_popup")

                            if st.form_submit_button("Adicionar Item"):
                                raw_new_item_data = {
                                    "Código Interno": new_item_codigo_interno, "NCM": re.sub(r'\D', '', new_item_ncm_display) if new_item_ncm_display else None,
                                    "Cobertura": new_item_cobertura, "SKU": new_item_sku, "Quantidade": new_item_quantidade, 
                                    "Valor Unitário": new_item_valor_unitario, "Peso Unitário": new_item_peso_unitario,
                                    "Denominação do produto": new_item_denominacao, "Detalhamento complementar do produto": new_item_detalhamento,
                                    "Fornecedor": current_fornecedor_context, "Invoice N#": current_invoice_n_context
                                }
                                # Padronizar e recalcular valores do item
                                standardized_new_item_data = _standardize_item_data(raw_new_item_data, current_fornecedor_context, current_invoice_n_context)
                                standardized_new_item_data["Valor total do item"] = standardized_new_item_data["Quantidade"] * standardized_new_item_data["Valor Unitário"]
                                
                                # Anexa o novo item ao process_items_data
                                st.session_state.process_items_data.append(standardized_new_item_data)
                                
                                # Recalcular totais globais e impostos de todos os itens
                                # Criar um DataFrame temporário para os cálculos agregados
                                temp_df_for_calc = pd.DataFrame(st.session_state.process_items_data)
                                total_invoice_value_usd_recalc = temp_df_for_calc["Valor total do item"].sum() if "Valor total do item" in temp_df_for_calc.columns else 0.0
                                total_invoice_weight_kg_recalc = 0.0
                                if "Peso Unitário" in temp_df_for_calc.columns and "Quantidade" in temp_df_for_calc.columns:
                                    total_invoice_weight_kg_recalc = (pd.to_numeric(temp_df_for_calc['Peso Unitário'], errors='coerce').fillna(0) * pd.to_numeric(temp_df_for_calc['Quantidade'], errors='coerce').fillna(0)).sum()
                                
                                st.session_state.total_invoice_value_usd = total_invoice_value_usd_recalc
                                st.session_state.total_invoice_weight_kg = total_invoice_weight_kg_recalc

                                # Recalcular impostos para CADA item após a mudança de totais
                                dolar_brl = st.session_state[form_state_key].get("Estimativa_Dolar_BRL", 0.0)
                                frete_usd = st.session_state[form_state_key].get('Estimativa_Frete_USD', 0.0)
                                seguro_brl = st.session_state[form_state_key].get('Estimativa_Seguro_BRL', 0.0)

                                for item_in_list in st.session_state.process_items_data:
                                    calculate_item_taxes_and_values(
                                        item_in_list, dolar_brl, total_invoice_value_usd_recalc, total_invoice_weight_kg_recalc,
                                        frete_usd, seguro_brl
                                    )
                                
                                _display_message_box("Item adicionado com sucesso!", "success")
                                st.session_state.show_add_item_popup = False
                                st.rerun() # Força uma recarga para atualizar a tabela e totais
                                
                col_download_template, col_upload_excel = st.columns([0.2, 0.8])
                with col_download_template:
                    excel_template_data = _generate_items_excel_template()
                    st.download_button(
                        label="Baixar Template Itens", data=excel_template_data, file_name="template_itens_processo.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_items_excel_template"
                    )
                with col_upload_excel:
                    uploaded_items_file = st.file_uploader("Upload Excel/CSV de Itens", type=["csv", "xls", "xlsx"], key="upload_items_file")
                    # Corrigido: Usar uploaded_file.name e uploaded_file.size são robustos
                    current_upload_key = (uploaded_items_file.name, uploaded_items_file.size) if uploaded_items_file else None
                    
                    if uploaded_items_file is not None and current_upload_key != st.session_state.get('last_processed_upload_key'): # Usar .get()
                        if _import_items_from_excel(uploaded_items_file, current_fornecedor_context, current_invoice_n_context):
                            st.session_state.last_processed_upload_key = current_upload_key
                            st.rerun()
                        else:
                            st.session_state.last_processed_upload_key = None            

                st.markdown("---") 

                df_items = pd.DataFrame(st.session_state.process_items_data)
                # Garante que todas as colunas do schema estão presentes, preenchendo com None se ausentes
                for col in DEFAULT_ITEM_SCHEMA.keys():
                    if col not in df_items.columns:
                        df_items[col] = None

                # Não recalcula totais e impostos aqui, pois já é feito no _initialize_form_state e nos callbacks de adição/edição de item.
                # Apenas usa os dados já processados em st.session_state.process_items_data
                
                if not df_items.empty:
                    st.markdown("#### Itens do Processo:")
                    df_items['Selecionar'] = False 
                    df_items['NCM Formatado'] = df_items['NCM'].apply(lambda x: ncm_list_page.format_ncm_code(str(x)) if ncm_list_page and x is not None else str(x) if x is not None else '')

                    display_cols = [
                        "Selecionar", "Cobertura", "Código Interno", "Denominação do produto", "SKU",
                        "Quantidade", "Valor Unitário", "NCM Formatado", "Valor total do item", "Peso Unitário", 
                        "Estimativa_II_BR", "Estimativa_IPI_BR", "Estimativa_PIS_BR", "Estimativa_COFINS_BR", "Estimativa_ICMS_BR",
                        "Frete_Rateado_USD", "Seguro_Rateado_BRL", "VLMD_Item"
                    ]
                    # Filtra colunas para exibir apenas as que realmente existem no DataFrame
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
                        "Estimativa_II_BR": st.column_config.NumberColumn("II (R$)", format="%.2f", disabled=True, width="small"),
                        "Estimativa_IPI_BR": st.column_config.NumberColumn("IPI (R$)", format="%.2f", disabled=True, width="small"),
                        "Estimativa_PIS_BR": st.column_config.NumberColumn("PIS (R$)", format="%.2f", disabled=True, width="small"),
                        "Estimativa_COFINS_BR": st.column_config.NumberColumn("COFINS (R$)", format="%.2f", disabled=True, width="small"),
                        "Estimativa_ICMS_BR": st.column_config.NumberColumn("ICMS (R$)", format="%.2f", disabled=True, width="small"),
                        "Frete_Rateado_USD": st.column_config.NumberColumn("Frete Rat. (USD)", format="%.2f", disabled=True, width="small"),
                        "Seguro_Rateado_BRL": st.column_config.NumberColumn("Seguro Rat. (R$)", format="%.2f", disabled=True, width="small"),
                        "VLMD_Item": st.column_config.NumberColumn("VLMD (R$)", format="%.2f", disabled=True, width="small"),
                    }
                    # Filtra column_config_items para incluir apenas colunas que serão exibidas
                    column_config_items = {k:v for k,v in column_config_items.items() if k in display_cols}


                    selected_rows_data = st.data_editor(
                        df_items[display_cols], column_config=column_config_items, num_rows="fixed", 
                        hide_index=True, use_container_width=True, key="process_items_editor"
                    )
                    
                    st.session_state.selected_item_indices = [
                        idx for idx, selected in enumerate(selected_rows_data['Selecionar']) if selected
                    ]

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
                                for idx in sorted(st.session_state.selected_item_indices, reverse=True):
                                    del st.session_state.process_items_data[idx]
                                st.session_state.selected_item_indices = [] 
                                _display_message_box("Itens selecionados excluídos com sucesso!", "success")
                                st.rerun()

                    if st.session_state.get('show_edit_item_popup', False) and st.session_state.item_to_edit_index is not None:
                        item_index = st.session_state.item_to_edit_index
                        item_data = st.session_state.process_items_data[item_index]

                        # Usando uuid para a chave do popover para evitar conflitos
                        with st.popover(f"Editar Item: {item_data.get('Código Interno', 'N/A')}", key=f"edit_item_popover_{uuid.uuid4()}"):
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
                                    # Atualiza o item diretamente na lista
                                    st.session_state.process_items_data[item_index].update({
                                        "Código Interno": edited_codigo_interno,
                                        "NCM": re.sub(r'\D', '', edited_ncm_display) if edited_ncm_display else None,
                                        "Cobertura": edited_cobertura, "SKU": edited_sku, "Quantidade": edited_quantidade,
                                        "Valor Unitário": edited_valor_unitario, "Peso Unitário": edited_peso_unitario,
                                        "Denominação do produto": edited_denominacao, "Detalhamento complementar do produto": edited_detalhamento,
                                        "Valor total do item": edited_quantidade * edited_valor_unitario # Recalcula aqui
                                    })
                                    
                                    # Recalcular totais e impostos após edição de item
                                    temp_df_for_recalc = pd.DataFrame(st.session_state.process_items_data)
                                    total_invoice_value_usd_recalc = temp_df_for_recalc["Valor total do item"].sum() if "Valor total do item" in temp_df_for_recalc.columns else 0.0
                                    total_invoice_weight_kg_recalc = 0.0
                                    if "Peso Unitário" in temp_df_for_recalc.columns and "Quantidade" in temp_df_for_recalc.columns:
                                        total_invoice_weight_kg_recalc = (pd.to_numeric(temp_df_for_recalc['Peso Unitário'], errors='coerce').fillna(0) * pd.to_numeric(temp_df_for_recalc['Quantidade'], errors='coerce').fillna(0)).sum()
                                    
                                    st.session_state.total_invoice_value_usd = total_invoice_value_usd_recalc
                                    st.session_state.total_invoice_weight_kg = total_invoice_weight_kg_recalc

                                    # Recalcular impostos para CADA item (porque os totais podem ter mudado)
                                    dolar_brl = st.session_state[form_state_key].get("Estimativa_Dolar_BRL", 0.0)
                                    frete_usd = st.session_state[form_state_key].get('Estimativa_Frete_USD', 0.0)
                                    seguro_brl = st.session_state[form_state_key].get('Estimativa_Seguro_BRL', 0.0)
                                    
                                    for item_in_list in st.session_state.process_items_data:
                                        calculate_item_taxes_and_values(
                                            item_in_list, dolar_brl, total_invoice_value_usd_recalc, total_invoice_weight_kg_recalc,
                                            frete_usd, seguro_brl
                                        )

                                    _display_message_box("Item editado com sucesso!", "success")
                                    st.session_state.show_edit_item_popup = False
                                    st.session_state.item_to_edit_index = None
                                    st.session_state.selected_item_indices = []
                                    st.rerun() # Força uma recarga para atualizar a tabela e totais
                                    
                                if st.form_submit_button("Cancelar"):
                                    st.session_state.show_edit_item_popup = False
                                    st.session_state.item_to_edit_index = None
                                    st.session_state.selected_item_indices = []
                                    st.rerun()

                    # Recalcula totais a serem exibidos no resumo, usando os valores atuais no session state
                    total_itens_usd_current_display = st.session_state.get('total_invoice_value_usd', 0.0)
                    total_itens_weight_kg_current_display = st.session_state.get('total_invoice_weight_kg', 0.0)

                    st.markdown("---")
                    st.subheader("Resumo de Itens para Cálculos")
                    st.write(f"Valor Total dos Itens (USD): **{total_itens_usd_current_display:,.2f}**".replace('.', '#').replace(',', '.').replace('#', ','))
                    st.write(f"Peso Total dos Itens (KG): **{total_itens_weight_kg_current_display:,.4f}**".replace('.', '#').replace(',', '.').replace('#', ','))

                else:
                    st.info("Nenhum item adicionado a este processo ainda. Use as opções acima para adicionar.")

            elif tab_name == "Valores e Estimativas":
                st.subheader("Valores e Estimativas")
                
                # Obtém os totais dos itens do st.session_state, que já foram calculados em _initialize_form_state ou ao adicionar/editar itens.
                total_itens_usd_from_session = st.session_state.get('total_invoice_value_usd', 0.0)
                dolar_brl_current = float(st.session_state[form_state_key].get("Estimativa_Dolar_BRL", 0.0) or 0.0)
                
                # Atualiza o Valor_USD no estado do formulário com o total calculado
                st.session_state[form_state_key]["Valor_USD"] = total_itens_usd_from_session 
                
                # Campos de Valores e Estimativas
                frete_usd_current = float(st.session_state[form_state_key].get("Estimativa_Frete_USD", 0.0) or 0.0)
                seguro_brl_current = float(st.session_state[form_state_key].get("Estimativa_Seguro_BRL", 0.0) or 0.0)
                icms_br_manual_estimate_current = float(st.session_state[form_state_key].get("Estimativa_ICMS_BR", 0.0) or 0.0)
                
                col_1, col_2 = st.columns(2)

                with col_1:
                    # Input para Dólar/BRL, e os demais são exibição ou entrada
                    st.session_state[form_state_key]["Estimativa_Dolar_BRL"] = st.number_input(
                        "Cambio Estimado (R$):", value=dolar_brl_current, format="%.2f", key=f"{form_state_key}_Estimativa_Dolar_BRL"
                    )
                    st.number_input(
                        "Valor (USD):", value=float(st.session_state[form_state_key]["Valor_USD"] or 0.0), format="%.2f",
                        key=f"{form_state_key}_Valor_USD_display", disabled=True
                    )
                    st.session_state[form_state_key]["Estimativa_Frete_USD"] = st.number_input(
                        "Estimativa de Frete (USD):", value=frete_usd_current, format="%.2f", key=f"{form_state_key}_Estimativa_Frete_USD"
                    )
                    st.session_state[form_state_key]["Estimativa_Seguro_BRL"] = st.number_input(
                        "Estimativa Seguro (R$):", value=seguro_brl_current, format="%.2f", key=f"{form_state_key}_Estimativa_Seguro_BRL"
                    )
                    
                    st.session_state[form_state_key]["Estimativa_ICMS_BR"] = st.number_input(
                        "Estimativa de ICMS (R$ - Manual):", value=icms_br_manual_estimate_current, format="%.2f",
                        key=f"{form_state_key}_Estimativa_ICMS_BR"
                    )

                    st.session_state[form_state_key]["Estimativa_Impostos_BR"] = st.number_input(
                        "Estimativa Impostos (Antigo):", value=float(st.session_state[form_state_key].get("Estimativa_Impostos_BR", 0.0) or 0.0), 
                        format="%.2f", key=f"{form_state_key}_Estimativa_Impostos_BR", disabled=True,
                        help="Campo de impostos para compatibilidade com versões antigas do DB."
                    )
                    
                    # Soma os impostos dos itens que já estão calculados em st.session_state.process_items_data
                    total_ii = total_ipi = total_pis = total_cofins = total_icms_calculated_sum = 0.0
                    if st.session_state.process_items_data:
                        for item in st.session_state.process_items_data:
                            total_ii += item.get('Estimativa_II_BR', 0.0)
                            total_ipi += item.get('Estimativa_IPI_BR', 0.0)
                            total_pis += item.get('Estimativa_PIS_BR', 0.0)
                            total_cofins += item.get('Estimativa_COFINS_BR', 0.0)
                            total_icms_calculated_sum += item.get('Estimativa_ICMS_BR', 0.0)

                    # Atualiza os valores calculados no estado do formulário
                    st.session_state[form_state_key]['Estimativa_II_BR'] = total_ii
                    st.session_state[form_state_key]['Estimativa_IPI_BR'] = total_ipi
                    st.session_state[form_state_key]['Estimativa_PIS_BR'] = total_pis
                    st.session_state[form_state_key]['Estimativa_COFINS_BR'] = total_cofins
                    
                    # Soma total de impostos
                    total_impostos_reais = total_ii + total_ipi + total_pis + total_cofins + st.session_state[form_state_key].get("Estimativa_ICMS_BR", 0.0) # Usa o ICMS manual
                    st.session_state[form_state_key]['Estimativa_Impostos_Total'] = total_impostos_reais

                with col_2:
                    st.number_input("Estimativa de II (R$ - Calculado):", value=st.session_state[form_state_key].get('Estimativa_II_BR', 0.0), format="%.2f", disabled=True, key=f"display_{form_state_key}_II_BR_calc")
                    st.number_input("Estimativa de IPI (R$ - Calculado):", value=st.session_state[form_state_key].get('Estimativa_IPI_BR', 0.0), format="%.2f", disabled=True, key=f"display_{form_state_key}_IPI_BR_calc")
                    st.number_input("Estimativa de PIS (R$ - Calculado):", value=st.session_state[form_state_key].get('Estimativa_PIS_BR', 0.0), format="%.2f", disabled=True, key=f"display_{form_state_key}_PIS_BR_calc")
                    st.number_input("Estimativa de COFINS (R$ - Calculado):", value=st.session_state[form_state_key].get('Estimativa_COFINS_BR', 0.0), format="%.2f", disabled=True, key=f"display_{form_state_key}_COFINS_BR_calc")
                    st.number_input("Estimativa Impostos (R$):", value=st.session_state[form_state_key].get('Estimativa_Impostos_Total', 0.0), format="%.2f", disabled=True, key=f"display_{form_state_key}_Impostos_Total_calc")
                    st.caption("Os valores acima são a soma dos impostos calculados para cada item com base no NCM.")

            elif tab_name == "Status Operacional":
                st.subheader("Status Operacional")
                for field_name, config in campos_config_tabs[tab_name].items():
                    current_value = st.session_state[form_state_key].get(field_name)

                    if config["type"] == "date":
                        current_value_dt = None
                        if current_value:
                            try:
                                if isinstance(current_value, str):
                                    current_value_dt = datetime.strptime(current_value, "%Y-%m-%d").date()
                                elif isinstance(current_value, (datetime, date)):
                                    current_value_dt = current_value.date()
                            except ValueError:
                                current_value_dt = None
                        widget_value = st.date_input(config["label"], value=current_value_dt, key=f"{form_state_key}_{field_name}", format="DD/MM/YYYY")
                        st.session_state[form_state_key][field_name] = widget_value.strftime("%Y-%m-%d") if widget_value else None
                    elif config["type"] == "dropdown":
                        options = config["values"]
                        default_index = 0
                        if current_value in options:
                            default_index = options.index(current_value)
                        elif current_value is not None and str(current_value).strip() != "" and current_value not in options:
                            options = [current_value] + options
                            default_index = 0
                        widget_value = st.selectbox(config["label"], options=options, index=default_index, key=f"{form_state_key}_{field_name}")
                        st.session_state[form_state_key][field_name] = widget_value if widget_value else None
                    else:
                        widget_value = st.text_input(config["label"], value=current_value if current_value is not None else "", key=f"{form_state_key}_{field_name}")
                        st.session_state[form_state_key][field_name] = widget_value if widget_value else None

            elif tab_name == "Documentação":
                st.subheader("Documentação")
                st.info("A funcionalidade de upload e download de documentos está implementada. Para arquivos grandes, considere usar soluções de armazenamento em nuvem e salvar apenas o link no banco de dados, em vez de armazenar o arquivo diretamente.")

                # Exibe campos desabilitados para o nome e tipo do arquivo
                st.text_input("Nome do Arquivo Salvo:", value=st.session_state[form_state_key].get("Nome_do_arquivo", ""), disabled=True, key=f"{form_state_key}_Nome_do_arquivo_display")
                st.text_input("Tipo do Arquivo Salvo:", value=st.session_state[form_state_key].get("Tipo_do_arquivo", ""), disabled=True, key=f"{form_state_key}_Tipo_do_arquivo_display")

                uploaded_file = st.file_uploader(
                    "Upload de Documento (PDF ou Excel)",
                    type=["pdf", "xlsx", "xls"],
                    key=f"{form_state_key}_file_uploader"
                )

                # Processa o arquivo se um novo foi enviado e é diferente do último processado
                if uploaded_file is not None:
                    # Corrigido: Usar uploaded_file.name e uploaded_file.size são robustos
                    current_file_hash = uploaded_file.name + str(uploaded_file.size)
                    # Adiciona uuid para garantir unicidade mesmo se nome e tamanho forem iguais (ex: mesmo arquivo re-upload)
                    if st.session_state.get(f'{form_state_key}_last_uploaded_hash') != current_file_hash:
                        try:
                            # Lê o conteúdo do arquivo e o codifica em base64
                            file_content = uploaded_file.read()
                            encoded_content = base64.b64encode(file_content).decode('utf-8')

                            # Atualiza o session_state com os dados do novo arquivo
                            st.session_state[form_state_key]["Nome_do_arquivo"] = uploaded_file.name
                            st.session_state[form_state_key]["Tipo_do_arquivo"] = uploaded_file.type
                            st.session_state[form_state_key]["Conteudo_do_arquivo"] = encoded_content
                            st.session_state[f'{form_state_key}_last_uploaded_hash'] = current_file_hash
                            
                            _display_message_box(f"Arquivo '{uploaded_file.name}' carregado com sucesso! Salve o processo para persistir.", "success")
                            st.rerun() # Reruns para refletir o nome/tipo do arquivo na UI
                        except Exception as e:
                            _display_message_box(f"Erro ao processar o arquivo: {e}", "error")
                            logger.error(f"Erro ao processar o arquivo {uploaded_file.name}: {e}")
                            st.session_state[f'{form_state_key}_last_uploaded_hash'] = None # Reseta o hash para tentar novamente
                
                # Botão para remover o arquivo anexado (se houver)
                if st.session_state[form_state_key].get("Nome_do_arquivo"):
                    if st.button("Remover Arquivo Anexado", key=f"{form_state_key}_remove_file_button"):
                        st.session_state[form_state_key]["Nome_do_arquivo"] = None
                        st.session_state[form_state_key]["Tipo_do_arquivo"] = None
                        st.session_state[form_state_key]["Conteudo_do_arquivo"] = None
                        st.session_state[f'{form_state_key}_last_uploaded_hash'] = None
                        _display_message_box("Arquivo removido do processo. Salve para persistir a remoção.", "info")
                        st.rerun()

                # Botão de download para o arquivo (se houver)
                if st.session_state[form_state_key].get("Conteudo_do_arquivo"):
                    file_name_to_download = st.session_state[form_state_key]["Nome_do_arquivo"] or "documento_anexado"
                    file_type_to_download = st.session_state[form_state_key]["Tipo_do_arquivo"] or "application/octet-stream"
                    decoded_content = base64.b64decode(st.session_state[form_state_key]["Conteudo_do_arquivo"])
                    
                    st.download_button(
                        label=f"Baixar {file_name_to_download}",
                        data=decoded_content,
                        file_name=file_name_to_download,
                        mime=file_type_to_download,
                        key=f"{form_state_key}_download_file_button"
                    )
            
    st.markdown("---")
    st.markdown("##### Observação (Campo Dedicado)")
    st.session_state[form_state_key]["Observacao"] = st.text_area("Observação", value=st.session_state[form_state_key].get("Observacao", "") or "", height=150, key=f"{form_state_key}_Observacao_dedicated")
    st.session_state[form_state_key]["Observacao"] = st.session_state[form_state_key]["Observacao"] if st.session_state[form_state_key]["Observacao"] else None

    with st.form(key=f"followup_process_form_submit_buttons_{process_id}", clear_on_submit=False):
        col_save, col_cancel = st.columns([0.03, 0.1]) # Ajusta largura das colunas
        col_delete = st.columns([0.0000003, 0.01])[1] # Mantém a coluna de exclusão separada

        with col_save:
            if st.form_submit_button("Salvar Processo"):
                edited_data_to_save = {}
                for tab_name, tab_config in campos_config_tabs.items():
                    if "col1" in tab_config:
                        for field_name, config in tab_config["col1"].items():
                            edited_data_to_save[field_name] = st.session_state.get(f"{form_state_key}_{field_name}")
                    if "col2" in tab_config:
                        for field_name, config in tab_config["col2"].items():
                            edited_data_to_save[field_name] = st.session_state.get(f"{form_state_key}_{field_name}")
                    if tab_name not in ["Dados Gerais", "Itens"]:
                        for field_name, config in tab_config.items():
                            # Se o campo é desabilitado e do tipo moeda BR, usa o valor do st.session_state[form_state_key] (já calculado)
                            if config.get("disabled", False) and config.get("type") == "currency_br":
                                edited_data_to_save[field_name] = st.session_state[form_state_key].get(field_name)
                            # Para os campos de arquivo, pegamos diretamente do session_state[form_state_key]
                            elif field_name in ["Nome_do_arquivo", "Tipo_do_arquivo", "Conteudo_do_arquivo"]:
                                edited_data_to_save[field_name] = st.session_state[form_state_key].get(field_name)
                            else:
                                edited_data_to_save[field_name] = st.session_state.get(f"{form_state_key}_{field_name}")
                edited_data_to_save["Observacao"] = st.session_state.get(f"{form_state_key}_Observacao_dedicated")
                
                # Garante que todos os campos de cálculo e totais sejam passados corretamente
                # Eles já estão atualizados em st.session_state[form_state_key] devido aos cálculos anteriores
                edited_data_to_save.update({
                    'Valor_USD': st.session_state[form_state_key].get('Valor_USD', 0.0),
                    'Estimativa_Impostos_Total': st.session_state[form_state_key].get('Estimativa_Impostos_Total', 0.0),
                    'Estimativa_II_BR': st.session_state[form_state_key].get('Estimativa_II_BR', 0.0),
                    'Estimativa_IPI_BR': st.session_state[form_state_key].get('Estimativa_IPI_BR', 0.0),
                    'Estimativa_PIS_BR': st.session_state[form_state_key].get('Estimativa_PIS_BR', 0.0),
                    'Estimativa_COFINS_BR': st.session_state[form_state_key].get('Estimativa_COFINS_BR', 0.0),
                    'Estimativa_Frete_USD': st.session_state[form_state_key].get('Estimativa_Frete_USD', 0.0),
                    'Estimativa_Seguro_BRL': st.session_state[form_state_key].get('Estimativa_Seguro_BRL', 0.0),
                    'Estimativa_Dolar_BRL': st.session_state[form_state_key].get('Estimativa_Dolar_BRL', 0.0),
                    'Estimativa_ICMS_BR': st.session_state[form_state_key].get('Estimativa_ICMS_BR', 0.0),
                })

                logger.info(f"Dados coletados para salvar (process_form_page): {edited_data_to_save} (total de chaves: {len(edited_data_to_save)})")

                is_new_process_for_save = st.session_state.get(f'{form_state_key}_is_new_process_flag', False)
                process_id_arg_for_save_action = None if is_new_process_for_save else process_id

                saved_process_id = _save_process_action(process_id_arg_for_save_action, edited_data_to_save, is_new_process_for_save, form_state_key)
                
                if saved_process_id:
                    st.session_state.current_page = "Follow-up Importação"
                    st.session_state.form_process_identifier = saved_process_id
                    st.session_state.form_is_cloning = False
                    st.session_state.last_cloned_from_id = None
                else:
                    st.session_state.current_page = "Follow-up Importação"
                    st.session_state.form_is_cloning = False
                    st.session_state.last_cloned_from_id = None

                st.session_state.form_reload_processes_callback() # Callback para recarregar a lista principal
                st.rerun()

        with col_cancel:
            if st.form_submit_button("Cancelar"):
                st.session_state.current_page = "Follow-up Importação"
                # Limpa apenas os estados de sessão específicos deste formulário
                if form_state_key in st.session_state:
                    del st.session_state[form_state_key]
                st.session_state.show_add_item_popup = False
                st.session_state.process_items_data = [] # Garante que itens sejam limpos
                st.session_state.last_processed_upload_key = None
                st.session_state.process_items_loaded_for_id = None 
                st.session_state.form_is_cloning = False
                st.rerun()

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

