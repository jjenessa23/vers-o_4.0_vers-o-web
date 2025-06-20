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
import base64
import warnings
import followup_db_manager as db_manager # Importa o módulo db_manager
from app_logic import process_form_page # Importa a página do formulário
from app_logic import process_query_page # Importa a página de consulta
import uuid # Importar uuid para gerar chaves únicas

# Configura o logger
logger = logging.getLogger(__name__)

# Define a classe MockDbUtils globalmente, para evitar redeclarações
class MockDbUtils:
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

# Importa db_utils real, ou usa o mock se houver erro
db_utils: Union[Any, MockDbUtils] 
try:
    import db_utils # type: ignore # Ignora o erro de importação se o módulo não for encontrado inicialmente
    if not hasattr(db_utils, 'get_declaracao_by_id') or \
       not hasattr(db_utils, 'get_declaracao_by_referencia'):
        logger.warning("Módulo 'db_utils' real não contém funções esperadas. Usando MockDbUtils.")
        db_utils = MockDbUtils()
except ImportError:
    logger.warning("Módulo 'db_utils' não encontrado. Usando MockDbUtils.")
    db_utils = MockDbUtils()
except Exception as e:
    logger.error(f"Erro ao importar ou inicializar 'db_utils': {e}. Usando MockDbUtils.")

# --- Função para definir imagem de fundo com opacidade ---
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
                opacity: 0.20;
                z-index: -1;
            }}
            </style>
            """,
            unsafe_allow_html=True
        )
    except FileNotFoundError:
        st.warning(f"A imagem de fundo não foi encontrada no caminho: {image_path}")
    except Exception as e:
        st.error(f"Erro ao carregar a imagem de fundo: {e}")


# --- Funções Auxiliares de Formatação ---
def _format_date_display(date_str: Optional[str]) -> str:
    """Formata uma string de data (YYYY-MM-DD) para exibição (DD/MM/YYYY)."""
    if date_str and isinstance(date_str, str):
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").strftime("%d/%m/%Y")
        except ValueError:
            return date_str
    return ""

def _format_currency_display(value: Any) -> str:
    """Formata um valor numérico para o formato de moeda R$ X.XXX,XX."""
    try:
        val = float(value)
        return f"R$ {val:,.2f}".replace('.', '#').replace(',', '.').replace('#', ',')
    except (ValueError, TypeError):
        return "R$ 0,00"

def _format_usd_display(value: Any) -> str:
    """Formata um valor numérico para o formato de moeda US$ X.XXX,XX."""
    try:
        val = float(value)
        return f"US$ {val:,.2f}".replace('.', '#').replace(',', '.').replace('#', ',')
    except (ValueError, TypeError):
        return "US$ 0,00"

def _format_int_display(value: Any) -> str:
    """Formata um valor para inteiro."""
    try:
        val = int(value)
        return str(val)
    except (ValueError, TypeError):
        return ""

def _format_di_number(di_number: Optional[str]) -> str:
    """Formata o número da DI para o padrão **/*******-*."""
    if di_number and isinstance(di_number, str) and len(di_number) == 10:
        return f"{di_number[0:2]}/{di_number[2:9]}-{di_number[9]}"
    return di_number if di_number is not None else ""

def _get_di_number_from_id(di_id: Optional[int]) -> str:
    """Obtém o número da DI a partir do seu ID no banco de dados de XML DI."""
    if di_id is None:
        return "N/A"
    di_data = db_utils.get_declaracao_by_id(di_id)
    if di_data:
        return _format_di_number(str(di_data.get('numero_di')))
    return "DI Não Encontrada"

# --- Funções Auxiliares de UI (Popups e Ações) ---
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

def _display_delete_confirm_popup():
    """Exibe um pop-up de confirmação antes de excluir/arquivar um processo."""
    if not st.session_state.get('show_delete_confirm_popup', False):
        return

    process_id_to_delete = st.session_state.get('delete_process_id_to_confirm')
    process_name_to_delete = st.session_state.get('delete_process_name_to_confirm')

    if process_id_to_delete is None:
        st.session_state.show_delete_confirm_popup = False
        return

    with st.form(key=f"delete_confirm_form_{process_id_to_delete}"):
        st.markdown(f"### Confirmar Arquivamento")
        st.warning(f"Tem certeza que deseja arquivar o processo {process_name_to_delete} ? Ele não será excluído do banco de dados, mas não aparecerá na tela principal.")
        
        col_yes, col_no = st.columns(2)
        with col_yes:
            # O botão agora reflete a ação de arquivar
            if st.form_submit_button("Sim, Arquivar"):
                _delete_process_action(process_id_to_delete) # Esta função será modificada para arquivar
        with col_no:
            if st.form_submit_button("Não, Cancelar"):
                st.session_state.show_delete_confirm_popup = False
                st.session_state.delete_process_id_to_confirm = None
                st.session_state.delete_process_name_to_confirm = None
                st.rerun()

def _on_status_multiselect_change():
    """Callback para mudança no multiselect de status."""
    selected_options_with_counts = st.session_state.main_followup_status_multiselect
    new_selected_raw_statuses = []
    if not selected_options_with_counts or 'Todos' in selected_options_with_counts:
        new_selected_raw_statuses.append('Todos')
    else:
        for opt in selected_options_with_counts:
            # Extrai o nome do status antes de ' ('
            new_selected_raw_statuses.append(opt.split(' (')[0])
    st.session_state.followup_selected_statuses = new_selected_raw_statuses
    _load_processes() # Recarrega os processos com o novo filtro

def _on_process_search_change():
    """Callback para mudança no campo de pesquisa de processo principal."""
    st.session_state.followup_main_process_search_term = st.session_state.main_followup_search_processo_novo
    _load_processes() # Recarrega os processos com o novo termo de pesquisa

def _load_processes():
    """Carrega os processos do DB aplicando filtros e termos de pesquisa."""
    if db_manager._USE_FIRESTORE_AS_PRIMARY and not st.session_state.get('firebase_ready', False):
        st.error("Conexão com Firestore não estabelecida. Não é possível carregar os processos de Follow-up.")
        st.session_state.followup_processes_data = []
        return
    
    # A função criar_tabela_followup agora só lida com Firestore
    if not db_manager.criar_tabela_followup():
        st.error(f"Não foi possível verificar/criar as coleções do banco de dados de Follow-up. Verifique sua configuração e logs.")
        st.session_state.followup_processes_data = []
        return
        
    # Combina termos de pesquisa da tela principal e do popup
    combined_search_terms = st.session_state.get('followup_popup_search_terms', {}).copy()
    main_process_search = st.session_state.get('followup_main_process_search_term', '').strip()
    
    if main_process_search:
        combined_search_terms['Processo_Novo'] = main_process_search
    else:
        # Garante que 'Processo_Novo' não persista se o campo principal for limpo
        combined_search_terms.pop('Processo_Novo', None) 

    # Buscar TODOS os processos (arquivados e não arquivados) que correspondem aos termos de pesquisa de texto
    # e filtros de data. A filtragem de status de arquivamento será feita em memória.
    processes_raw = db_manager.obter_processos_filtrados('Todos', combined_search_terms)
    df_processes_all_unfiltered = pd.DataFrame([dict(row) for row in processes_raw])

    # Garante que as colunas essenciais existam para o filtro
    df_processes_all_unfiltered['Status_Geral'] = df_processes_all_unfiltered.get('Status_Geral', pd.Series('Sem Status', index=df_processes_all_unfiltered.index)).fillna('Sem Status')
    df_processes_all_unfiltered['Modal'] = df_processes_all_unfiltered.get('Modal', pd.Series('Sem Modal', index=df_processes_all_unfiltered.index)).fillna('Sem Modal')

    # Adicionado: Garante que 'Status_Arquivado' exista com um valor padrão
    if 'Status_Arquivado' not in df_processes_all_unfiltered.columns:
        df_processes_all_unfiltered['Status_Arquivado'] = 'Não Arquivado' # Ou outro valor padrão desejado

    # Adicionado: Garante que 'Processo_Novo' exista no DataFrame.
    if 'Processo_Novo' not in df_processes_all_unfiltered.columns and 'id' in df_processes_all_unfiltered.columns:
        df_processes_all_unfiltered['Processo_Novo'] = df_processes_all_unfiltered['id']
    elif 'Processo_Novo' not in df_processes_all_unfiltered.columns:
        df_processes_all_unfiltered['Processo_Novo'] = 'UNKNOWN_PROCESS_' + pd.Series(range(len(df_processes_all_unfiltered))).astype(str)

    # --- Filtragem em memória aprimorada baseada nos requisitos do usuário ---
    selected_statuses_from_multiselect = st.session_state.get('followup_selected_statuses', ['Todos'])
    df_filtered_in_memory = df_processes_all_unfiltered.copy()

    # Determinar se a pesquisa principal por nome do processo está ativa
    is_main_process_name_search_active = bool(st.session_state.get('followup_main_process_search_term', '').strip())

    # Condição para exibir processos arquivados:
    # 1. Se "Arquivados" está explicitamente selecionado no multiselect.
    # 2. OU se uma pesquisa principal por nome de processo está ativa (para mostrar resultados da pesquisa).
    condition_show_archived_in_view = (
        'Arquivados' in selected_statuses_from_multiselect or
        is_main_process_name_search_active
    )

    # Se NÃO estamos explicitamente exibindo processos arquivados, então filtre-os.
    # Isso cobre o caso "Todos" (padrão) e outros status específicos sem pesquisa ativa.
    if not condition_show_archived_in_view:
        df_filtered_in_memory = df_filtered_in_memory[
            df_filtered_in_memory['Status_Arquivado'].isin([None, "Não Arquivado"])
        ]
    
    # Agora, aplique o filtro de Status_Geral se "Todos" não estiver selecionado.
    if 'Todos' not in selected_statuses_from_multiselect:
        if 'Arquivados' in selected_statuses_from_multiselect:
            # Se "Arquivados" e outros status gerais estão selecionados, combine as condições.
            temp_general_statuses_to_filter = [s for s in selected_statuses_from_multiselect if s != 'Todos' and s != 'Arquivados']
            if temp_general_statuses_to_filter:
                df_filtered_in_memory = df_filtered_in_memory[
                    (df_filtered_in_memory['Status_Geral'].isin(temp_general_statuses_to_filter)) |
                    (df_filtered_in_memory['Status_Arquivado'] == 'Arquivado') # Mantenha arquivados se "Arquivados" foi selecionado
                ]
            else: # Apenas "Arquivados" foi selecionado
                df_filtered_in_memory = df_filtered_in_memory[df_filtered_in_memory['Status_Arquivado'] == 'Arquivado']
        else: # Apenas status gerais específicos (não "Todos", não "Arquivados")
            df_filtered_in_memory = df_filtered_in_memory[df_filtered_in_memory['Status_Geral'].isin(selected_statuses_from_multiselect)]

    # Aplica outros termos de pesquisa do popup (N_Invoice, Fornecedor, etc.)
    if combined_search_terms:
        for col, term in combined_search_terms.items():
            if col not in ['Processo_Novo', 'ETA_Recinto_Start', 'ETA_Recinto_End', 'Data_Registro_Start', 'Data_Registro_End'] and term:
                df_filtered_in_memory = df_filtered_in_memory[
                    df_filtered_in_memory[col].astype(str).str.lower().str.contains(str(term).lower(), na=False)
                ]

    df_processes_filtered_by_status = df_filtered_in_memory
    # --- Fim da filtragem em memória aprimorada ---

    # Ordena o DataFrame para exibição
    custom_status_order = [
        'Encerrado','Chegada Pichau', 'Agendado', 'Liberado', 'Registrado',
        'Chegada Recinto', 'Embarcado', 'Verificando','Limbo Consolidado','Limbo Saldo', 'Pré Embarque',
        'Em produção', 'Processo Criado', 
        'Sem Status', 'Status Desconhecido', 'Arquivados' # Adicionado Arquivados para ordenação
    ]
    # Adiciona quaisquer status que possam existir nos dados mas não na lista predefinida
    for status_val in df_processes_filtered_by_status['Status_Geral'].unique():
        if status_val not in custom_status_order:
            custom_status_order.append(status_val)

    df_processes_filtered_by_status['Status_Geral'] = pd.Categorical(
        df_processes_filtered_by_status['Status_Geral'],
        categories=custom_status_order,
        ordered=True
    )
    df_processes_filtered_by_status = df_processes_filtered_by_status.sort_values(by=['Status_Geral', 'Modal', 'Processo_Novo'])

    st.session_state.followup_processes_data = df_processes_filtered_by_status.to_dict(orient='records')

def _update_status_filter_options(df_all_processes_for_options: pd.DataFrame):
    """Atualiza as opções do filtro de status com base nos status do DB, incluindo contagens."""
    # Usa o DataFrame passado (que deve conter todos os processos) para obter todos os status possíveis
    status_counts = df_all_processes_for_options['Status_Geral'].fillna('Sem Status').value_counts().to_dict()
    
    # Incluir contagem para "Arquivados" separadamente
    arquivados_count = df_all_processes_for_options[
        (df_all_processes_for_options['Status_Arquivado'] == 'Arquivado') | 
        (df_all_processes_for_options['Status_Geral'] == 'Arquivados') # Caso 'Arquivados' seja um Status_Geral
    ].shape[0]
    
    all_raw_status_options = list(status_counts.keys())
    
    # Define a ordem personalizada dos status para as opções do filtro
    custom_order_for_options = [
        'Encerrado','Chegada Pichau', 'Agendado', 'Liberado', 'Registrado',
        'Chegada Recinto', 'Embarcado', 'Verificando','Limbo Consolidado','Limbo Saldo', 'Pré Embarque',
        'Em produção', 'Processo Criado', 
        'Sem Status', 'Status Desconhecido'
    ]
    
    # Adiciona quaisquer status dos dados que não estão na ordem personalizada, e então ordena o restante
    sorted_status_options = sorted([s for s in all_raw_status_options if s not in custom_order_for_options and s != "Arquivados"])
    final_ordered_status_options = [s for s in custom_order_for_options if s in all_raw_status_options] + sorted_status_options

    # Formata as opções com as contagens
    formatted_options_with_counts = []
    for status in final_ordered_status_options:
        count = status_counts.get(status, 0)
        formatted_options_with_counts.append(f"{status} ({count})")
    
    # Adiciona "Arquivados" no final da lista de opções se houver algum
    if arquivados_count > 0:
        formatted_options_with_counts.append(f"Arquivados ({arquivados_count})")
        # Garante que 'Arquivados' esteja na lista de status brutos também para o default do multiselect
        if 'Arquivados' not in db_manager.STATUS_OPTIONS: # Evita duplicar se já estiver lá
             st.session_state.followup_raw_status_options_for_multiselect = ["Todos"] + final_ordered_status_options + ["Arquivados"]
        else:
            st.session_state.followup_raw_status_options_for_multiselect = ["Todos"] + final_ordered_status_options
    else:
        st.session_state.followup_raw_status_options_for_multiselect = ["Todos"] + final_ordered_status_options

    st.session_state.followup_all_status_options = ["Todos"] + formatted_options_with_counts


def _import_file_action(uploaded_file: Any) -> bool:
    """Importa itens de um arquivo Excel/CSV local e os adiciona à lista de itens do processo."""
    if uploaded_file is None:
        st.warning("Nenhum arquivo selecionado para importação.")
        return False

    file_extension = os.path.splitext(uploaded_file.name)[1]
    df = None

    try:
        if file_extension.lower() in ('.csv'):
            try: df = pd.read_csv(uploaded_file, encoding='utf-8')
            except UnicodeDecodeError: df = pd.read_csv(uploaded_file, encoding='latin-1')
            except Exception as e:
                logger.warning(f"Tentativa de ler CSV com delimitador padrão falhou, tentando com ';': {e}")
                df = pd.read_csv(uploaded_file, sep=';')
        elif file_extension.lower() in ('.xlsx', '.xls'):
            df = pd.read_excel(uploaded_file)
        else:
            st.error("Formato de arquivo não suportado. Por favor, use .csv, .xls ou .xlsx.")
            return False

        if df.empty:
            st.warning("O arquivo importado está vazio ou não contém dados.")
            return False

        processed_records = _preprocess_dataframe_for_db(df)

        if processed_records is None or not processed_records:
            st.error("Falha no pré-processamento dos dados do arquivo local ou nenhum dado válido restante após o processamento.")
            return False

        import_success_count = 0
        total_rows = len(processed_records)
        
        st.info(f"Iniciando importação/atualização de {total_rows} processos...")
        progress_bar = st.progress(0)

        for index, row_dict in enumerate(processed_records):
            process_name = row_dict.get("Processo_Novo")
            if not process_name:
                st.warning(f"Linha {index+2} ignorada: 'Processo_Novo' está vazio ou inválido.")
                continue

            if db_manager.upsert_processo(row_dict):
                import_success_count += 1
                logger.info(f"Processo '{process_name}' upserted (inserido/atualizado) via importação.")
            else:
                st.error(f"Falha ao fazer upsert do processo '{process_name}' na linha {index+2}. Verifique os dados.")
            
            progress_bar.progress((index + 1) / total_rows)
        
        progress_bar.empty()

        if import_success_count == total_rows:
            st.success("Dados do arquivo local importados/atualizados com sucesso! A tabela foi recarregada.")
            _load_processes()
            return True
        elif import_success_count > 0:
            st.warning(f"Importação/atualização do Google Sheets concluída com {import_success_count} de {total_rows} processos bem-sucedidos. Verifique os erros acima para os processos que falharam.")
            _load_processes()
            return True
        else:
            st.error("Falha total ao importar/atualizar dados do arquivo local para o banco de dados.")
            return False

    except Exception as e:
        st.error(f"Erro ao processar o arquivo local: {e}")
        logger.exception("Erro durante a importação do arquivo local.")
        return False

def _open_edit_process_popup(process_identifier: Optional[Any] = None, is_cloning: bool = False):
    """Navega para a página dedicada de formulário de processo."""
    st.session_state.form_process_identifier = process_identifier
    st.session_state.form_is_cloning = is_cloning
    st.session_state.form_reload_processes_callback = _load_processes
    st.session_state.current_page = "Formulário Processo"
    st.session_state.show_filter_search_popup = False
    st.session_state.show_import_popup = False
    st.session_state.show_delete_confirm_popup = False
    st.session_state.show_mass_edit_popup = False
    st.session_state.show_change_status_popup = False # Fechar status popup
    st.rerun()

def _open_process_query_page(process_identifier: Any):
    """Navega para a nova página de consulta de processo."""
    st.session_state.query_process_identifier = process_identifier
    st.session_state.current_page = "Consulta de Processo"
    st.session_state.show_filter_search_popup = False
    st.session_state.show_import_popup = False
    st.session_state.show_delete_confirm_popup = False
    st.session_state.show_mass_edit_popup = False
    st.session_state.show_change_status_popup = False # Fechar status popup
    st.rerun()

def _delete_process_action(process_id: Any):
    """Arquiva um processo no banco de dados (não exclui permanentemente)."""
    # Ação de arquivar em vez de excluir
    if db_manager.arquivar_processo(process_id):
        st.success(f"Processo ID {process_id} arquivado com sucesso! Ele não aparecerá mais na tela principal por padrão.")
    else:
        st.error(f"Falha ao arquivar processo ID {process_id}.")
    
    st.session_state.show_delete_confirm_popup = False
    st.session_state.delete_process_id_to_confirm = None
    st.session_state.delete_process_name_to_confirm = None
    st.session_state.selected_process_data = None # Limpa a seleção após arquivamento
    _load_processes() # Recarrega para remover o processo arquivado da lista
    st.rerun()

def _change_process_status_action(process_id: Any, new_status: str):
    """Altera o status de um processo no banco de dados."""
    user_info = st.session_state.get('user_info', {'username': 'Desconhecido'})
    current_username = user_info.get('username', 'Desconhecido')

    original_process_data_raw = db_manager.obter_processo_por_id(process_id) if isinstance(process_id, int) else db_manager.obter_processo_by_processo_novo(process_id)
    if not original_process_data_raw:
        st.error(f"Processo ID {process_id} não encontrado para alteração de status.")
        return

    original_status = original_process_data_raw.get('Status_Geral')

    if db_manager.atualizar_processo(process_id, {"Status_Geral": new_status}):
        db_manager.inserir_historico_processo(
            process_id, "Status_Geral", original_status, new_status,
            current_username, db_type="Firestore" if db_manager._USE_FIRESTORE_AS_PRIMARY else "SQLite"
        )
        st.success(f"Status do processo ID {process_id} alterado para '{new_status}' com sucesso!")
    else:
        st.error(f"Falha ao alterar status do processo ID {process_id}.")
    
    st.session_state.show_change_status_popup = False
    st.session_state.process_id_to_change_status = None
    st.session_state.process_name_to_change_status = None
    _load_processes()
    st.rerun()


def _display_change_status_popup():
    """Exibe um pop-up para alterar o status de um processo."""
    if not st.session_state.get('show_change_status_popup', False):
        return

    process_id = st.session_state.get('process_id_to_change_status')
    process_name = st.session_state.get('process_name_to_change_status')

    if process_id is None:
        st.session_state.show_change_status_popup = False
        return

    with st.form(key=f"change_status_form_{process_id}"):
        st.markdown(f"### Alterar Status do Processo")
        st.info(f"Selecione o novo status para o processo: **{process_name}**")

        # Obter o status atual do processo
        current_process_data = db_manager.obter_processo_por_id(process_id) if isinstance(process_id, int) else db_manager.obter_processo_by_processo_novo(process_id)
        current_status = current_process_data.get('Status_Geral') if current_process_data else None

        # Opções de status, com o status atual pré-selecionado se existir
        status_options = db_manager.STATUS_OPTIONS
        default_index = 0
        if current_status in status_options:
            default_index = status_options.index(current_status)

        new_status = st.selectbox("Novo Status:", options=status_options, index=default_index, key="new_status_selectbox")

        col_apply, col_cancel = st.columns(2)
        with col_apply:
            if st.form_submit_button("Aplicar Status"):
                _change_process_status_action(process_id, new_status)
        with col_cancel:
            if st.form_submit_button("Cancelar"):
                st.session_state.show_change_status_popup = False
                st.session_state.process_id_to_change_status = None
                st.session_state.process_name_to_change_status = None
                st.rerun()

def _preprocess_dataframe_for_db(df: pd.DataFrame) -> Optional[List[Dict[str, Any]]]:
    """Realiza o pré-processamento e padronização dos dados do DataFrame."""
    df_processed = df.copy()

    column_mapping_to_db = {
        "Processo": "Processo_Novo", "Fornecedor": "Fornecedor", "Tipo de Item": "Tipos_de_item",
        "Nº Invoice": "N_Invoice", "Quantidade": "Quantidade", "Valor (USD)": "Valor_USD",
        "Pago?": "Pago", "Nº Ordem Compra": "N_Ordem_Compra", "Data Compra": "Data_Compra",
        "Estimativa Impostos (Antigo)": "Estimativa_Impostos_BR", "Estimativa Frete (USD)": "Estimativa_Frete_USD",
        "Data Embarque": "Data_Embarque", "Agente de Carga": "Agente_de_Carga_Novo", 
        "Status Geral": "Status_Geral", "Modal": "Modal", "Navio": "Navio", "Origem": "Origem",
        "Destino": "Destino", "INCOTERM": "INCOTERM", "Comprador": "Comprador",
        "Docs Revisados": "Documentos_Revisados", "Conhecimento Embarque": "Conhecimento_Embarque",
        "Descricao_Feita": "Descricao_Feita", "Descricao_Enviada": "Descricao_Enviada",
        "Caminho da Pasta": "Caminho_da_pasta", "ETA Recinto": "ETA_Recinto", "Data Registro": "Data_Registro",
        "Observação": "Observacao", "DI Vinculada ID": "DI_ID_Vinculada", "Nota Feita": "Nota_feita",
        "Conferido": "Conferido", "Imp. Totais (R$)": "Estimativa_Impostos_Total",
        "Quantidade de Containers": "Quantidade_Containers", "Câmbio Estimado (R$)": "Estimativa_Dolar_BRL",
        "Estimativa Seguro (R$)": "Estimativa_Seguro_BRL", "Estimativa II (R$)": "Estimativa_II_BR",
        "Estimativa IPI (R$)": "Estimativa_IPI_BR", "Estimativa PIS (R$)": "Estimativa_PIS_BR",
        "Estimativa COFINS (R$)": "Estimativa COFINS (R$)", "Estimativa ICMS (R$)": "Estimativa_ICMS_BR",
        "Previsão na Pichau": "Previsao_Pichau", # Adicionado
    }
    df_processed = df_processed.rename(columns=column_mapping_to_db, errors='ignore')

    if "Processo_Novo" in df_processed.columns:
        df_processed["Processo_Novo"] = df_processed["Processo_Novo"].fillna("")
        df_processed = df_processed[df_processed["Processo_Novo"] != ""].copy()
    else:
        st.error("Coluna 'Processo_Novo' não encontrada no arquivo importado após renomeação. Verifique o cabeçalho.")
        return None

    records = df_processed.to_dict(orient='records')
    
    final_processed_records = []
    for record in records:
        cleaned_record = {}
        for key, value in record.items():
            if key in ["Quantidade", "DI_ID_Vinculada", "Quantidade_Containers"]:
                numeric_value = pd.to_numeric(value, errors='coerce').fillna(0)
                cleaned_record[key] = int(numeric_value)
            elif key in ["Valor_USD", "Estimativa_Impostos_BR", "Estimativa_Frete_USD", 
                          "Estimativa_Impostos_Total", "Estimativa_Dolar_BRL", "Estimativa_Seguro_BRL", 
                          "Estimativa_II_BR", "Estimativa_IPI_BR", "Estimativa_PIS_BR", 
                          "Estimativa_COFINS_BR", "Estimativa_ICMS_BR"]:
                if isinstance(value, str):
                    value = value.replace('.', '').replace(',', '.')
                numeric_value = pd.to_numeric(value, errors='coerce').fillna(0)
                cleaned_record[key] = float(numeric_value)
            elif key in ["Data_Compra", "Data_Embarque", "Previsao_Pichau", "ETA_Recinto", "Data_Registro"]:
                try:
                    date_obj = pd.to_datetime(value, errors='coerce', dayfirst=True)
                    cleaned_record[key] = date_obj.strftime('%Y-%m-%d') if pd.notna(date_obj) else None
                except Exception:
                    cleaned_record[key] = None
            elif key in ["Pago", "Documentos_Revisados", "Conhecimento_Embarque",
                          "Descricao_Feita", "Descricao_Enviada", "Nota_feita", "Conferido"]:
                str_value = str(value).strip().lower()
                if str_value in ["sim", "s"]:
                    cleaned_record[key] = "Sim"
                elif str_value in ["nao", "não", "n"]:
                    cleaned_record[key] = "Não"
                else:
                    cleaned_record[key] = None
            else:
                if pd.isna(value) or str(value).strip().lower() == 'nan' or str(value).strip() == '':
                    cleaned_record[key] = None
                else:
                    cleaned_record[key] = str(value)
        final_processed_records.append(cleaned_record)
    return final_processed_records

def _open_filter_search_popup():
    """Abre um pop-up para a seleção de filtros e termos de pesquisa."""
    st.session_state.show_filter_search_popup = True
    st.rerun()

def _display_filter_search_popup():
    """Exibe o pop-up de filtros e pesquisa."""
    if not st.session_state.get('show_filter_search_popup', False):
        return

    with st.form(key="filter_search_form"):
        st.markdown("### Mais Filtros e Pesquisa de Processos")

        # Os filtros de Status e Pesquisa de Processo foram movidos para a página principal.
        # Este pop-up agora contém apenas os filtros adicionais.

        col_left, col_right = st.columns(2)

        with col_left:
            st.text_input("Pesquisar N. Invoice:", key="popup_followup_search_n_invoice",
                          value=st.session_state.get('followup_popup_search_terms', {}).get('N_Invoice', '') or "")
            st.text_input("Pesquisar Modal:", key="popup_followup_search_Modal",
                          value=st.session_state.get('followup_popup_search_terms', {}).get('Modal', '') or "")
            st.text_input("Pesquisar Origem:", key="popup_followup_search_Origem",
                          value=st.session_state.get('followup_popup_search_terms', {}).get('Origem', '') or "")
            
            current_eta_recinto_start = st.session_state.get('followup_popup_search_terms', {}).get('ETA_Recinto_Start', None)
            current_eta_recinto_end = st.session_state.get('followup_popup_search_terms', {}).get('ETA_Recinto_End', None)
            st.date_input("Data no Recinto (Início):", value=current_eta_recinto_start, key="popup_followup_search_eta_recinto_start", format="DD/MM/YYYY")
            st.date_input("Data no Recinto (Fim):", value=current_eta_recinto_end, key="popup_followup_search_eta_recinto_end", format="DD/MM/YYYY")


        with col_right:
            st.text_input("Pesquisar Fornecedor:", key="popup_followup_search_fornecedor",
                          value=st.session_state.get('followup_popup_search_terms', {}).get('Fornecedor', '') or "")
            st.text_input("Pesquisar Tipos de Item:", key="popup_followup_search_Tipos_de_item",
                          value=st.session_state.get('followup_popup_search_terms', {}).get('Tipos_de_item', '') or "")
            st.text_input("Pesquisar Navio:", key="popup_followup_search_Navio",        
                          value=st.session_state.get('followup_popup_search_terms', {}).get('Navio', '') or "")
            st.text_input("Pesquisar Comprador:", key="popup_followup_search_Comprador",
                          value=st.session_state.get('followup_popup_search_terms', {}).get('Comprador', '') or "")

            current_data_registro_start = st.session_state.get('followup_popup_search_terms', {}).get('Data_Registro_Start', None)
            current_data_registro_end = st.session_state.get('followup_popup_search_terms', {}).get('Data_Registro_End', None)
            st.date_input("Data de Registro (Início):", value=current_data_registro_start, key="popup_followup_search_data_registro_start", format="DD/MM/YYYY")
            st.date_input("Data de Registro (Fim):", value=current_data_registro_end, key="popup_followup_search_data_registro_end", format="DD/MM/YYYY")


        col_buttons_popup = st.columns(2)
        with col_buttons_popup[0]:
            if st.form_submit_button("Aplicar Mais Filtros"):
                # Atualiza apenas os termos de pesquisa que estão no popup
                search_terms_to_apply = {
                    "N_Invoice": st.session_state.popup_followup_search_n_invoice,
                    "Fornecedor": st.session_state.popup_followup_search_fornecedor,
                    "Tipos_de_item": st.session_state.popup_followup_search_Tipos_de_item,
                    "Modal": st.session_state.popup_followup_search_Modal,
                    "Navio": st.session_state.popup_followup_search_Navio,
                    "Origem": st.session_state.popup_followup_search_Origem,
                    "Comprador": st.session_state.popup_followup_search_Comprador
                }

                if st.session_state.popup_followup_search_eta_recinto_start:
                    search_terms_to_apply['ETA_Recinto_Start'] = st.session_state.popup_followup_search_eta_recinto_start.strftime("%Y-%m-%d")
                else:
                    search_terms_to_apply['ETA_Recinto_Start'] = None
                
                if st.session_state.popup_followup_search_eta_recinto_end:
                    search_terms_to_apply['ETA_Recinto_End'] = st.session_state.popup_followup_search_eta_recinto_end.strftime("%Y-%m-%d")
                else:
                    search_terms_to_apply['ETA_Recinto_End'] = None

                if st.session_state.popup_followup_search_data_registro_start:
                    search_terms_to_apply['Data_Registro_Start'] = st.session_state.popup_followup_search_data_registro_start.strftime("%Y-%m-%d")
                else:
                    search_terms_to_apply['Data_Registro_Start'] = None

                if st.session_state.popup_followup_search_data_registro_end:
                    search_terms_to_apply['Data_Registro_End'] = st.session_state.popup_followup_search_data_registro_end.strftime("%Y-%m-%d")
                else:
                    search_terms_to_apply['Data_Registro_End'] = None
                
                st.session_state.followup_popup_search_terms = {k: v for k, v in search_terms_to_apply.items() if v} # Guarda apenas valores não vazios
                _load_processes()
                st.session_state.show_filter_search_popup = False
                st.rerun()
        with col_buttons_popup[1]:
            if st.form_submit_button("Limpar Mais Filtros"):
                st.session_state.followup_popup_search_terms = {} # Limpa apenas os filtros do popup
                _load_processes()
                st.session_state.show_filter_search_popup = False
                st.rerun()
        
        if st.form_submit_button("Fechar"):
            st.session_state.show_filter_search_popup = False
            st.rerun()

def _generate_excel_template():
    """Gera um arquivo Excel padrão para inserção de dados de Follow-up."""
    template_columns_map = {
        "Processo_Novo": "Processo", "Fornecedor": "Fornecedor", "Tipos_de_item": "Tipo de Item",
        "Observacao": "Observação", "Data_Embarque": "Data Embarque", "ETA_Recinto": "ETA Recinto",
        "Previsao_Pichau": "Previsão Pichau", "Documentos_Revisados": "Docs Revisados",
        "Conhecimento_Embarque": "Conhecimento Embarque", "Descricao_Feita": "Descrição Feita",
        "Descricao_Enviada": "Descrição Enviada", "Nota_feita": "Nota Feita", "N_Invoice": "Nº Invoice",
        "Quantidade": "Quantidade", "Valor (USD)": "Valor (USD)", "Pago?": "Pago?",
        "Nº Ordem Compra": "Nº Ordem Compra", "Data Compra": "Data Compra",
        "Estimativa Impostos (Antigo)": "Estimativa Impostos (Antigo)", "Estimativa Frete (USD)": "Estimativa Frete (USD)",
        "Agente de Carga": "Agente de Carga", "Status Geral": "Status Geral", "Modal": "Modal",
        "Navio": "Navio", "Origem": "Origem", "Destino": "Destino", "INCOTERM": "INCOTERM",
        "Comprador": "Comprador", "Caminho_da_pasta": "Caminho da Pasta", "ETA_Recinto": "ETA Recinto",
        "Data_Registro": "Data Registro", "DI_ID_Vinculada": "DI Vinculada ID",
        "Estimativa_Impostos_Total": "Imp. Totais (R$)", "Quantidade_Containers": "Quantidade de Containers",
        "Câmbio Estimado (R$)": "Câmbio Estimado (R$)", "Estimativa Seguro (R$)": "Estimativa Seguro (R$)",
        "Estimativa II (R$)": "Estimativa II (R$)", "Estimativa IPI (R$)": "Estimativa IPI (R$)",
        "Estimativa PIS (R$)": "Estimativa PIS (R$)", "Estimativa COFINS (R$)": "Estimativa COFINS (R$)",
        "Estimativa ICMS (R$)": "Estimativa ICMS (R$)",
    }

    df_template = pd.DataFrame(columns=list(template_columns_map.values()))
    example_row = {
        "Processo": "EXEMPLO-001", "Fornecedor": "Exemplo Fornecedor Ltda.", "Tipo de Item": "Eletrônicos",
        "Observação": "Observação de exemplo para o processo.", "Data Embarque": "2023-02-01",
        "ETA Recinto": "2023-03-05", "Previsão Pichau": "2023-03-10", "Docs Revisados": "Não",
        "Conhecimento Embarque": "Sim", "Descrição Feita": "Não", "Descrição Enviada": "Não",
        "Nota Feita": "Não", "Nº Invoice": "INV-2023-001", "Quantidade": 100,
        "Valor (USD)": 15000.00, "Pago?": "Não", "Nº Ordem Compra": "PO-XYZ-456",
        "Data Compra": "2023-01-15", "Estimativa Impostos (Antigo)": 5000.00,
        "Estimativa Frete (USD)": 1200.00, "Agente de Carga": "Agente ABC",
        "Caminho da Pasta": "C:\\Exemplo\\Pasta\\Processo_EXEMPLO-001", "Origem": "China",
        "Destino": "Brasil", "INCOTERM": "FOB", "Comprador": "Comprador X", "Navio": "Navio Exemplo",
        "Data Registro": "2023-03-08", "Imp. Totais (R$)": 5000.00, "Status Geral": "Processo Criado",
        "Modal": "Maritimo", "Quantidade de Containers": 1, "Câmbio Estimado (R$)": 5.00,
        "Estimativa Seguro (R$)": 100.00, "Estimativa II (R$)": 500.00, "Estimativa IPI (R$)": 200.00,
        "Estimativa PIS (R$)": 150.00, "Estimativa COFINS (R$)": 700.00, "Estimativa ICMS (R$)": 900.00,
        "DI Vinculada ID": "",
    }
    df_template = pd.DataFrame(columns=list(template_columns_map.values()))
    df_template = pd.concat([df_template, pd.DataFrame([example_row])], ignore_index=True)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_template.to_excel(writer, index=False, sheet_name='Follow-up Template')
    writer.close()
    output.seek(0)

    st.download_button(
        label="Baixar Template Excel",
        data=output,
        file_name="followup_template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="download_excel_template"
    )

def _get_gspread_client():
    """Autentica e retorna um cliente gspread para interagir com o Google Sheets."""
    try:
        if "gcp_service_account" not in st.secrets:
            st.error("Credenciais do Google Cloud (gcp_service_account) não encontradas em .streamlit/secrets.toml. Por favor, configure.")
            return None
        creds_json = st.secrets["gcp_service_account"]
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        logger.error(f"Erro ao autenticar gspread: {e}")
        st.error(f"Erro de autenticação com Google Sheets. Verifique suas credenciais em .streamlit/secrets.toml e as permissões da conta de serviço. Detalhes: {e}")
        return None

def _import_from_google_sheets(sheet_url_or_id: str, worksheet_name: str) -> bool:
    """Importa dados de uma planilha Google Sheets para o banco de dados."""
    client = _get_gspread_client()
    if not client: return False
    try:
        spreadsheet = client.open_by_url(sheet_url_or_id) if "https://" in sheet_url_or_id else client.open_by_key(sheet_url_or_id)
        worksheet = spreadsheet.worksheet(worksheet_name)
        data = worksheet.get_all_records(value_render_option='UNFORMATTED_VALUE', head=1)
        
        if not data:
            st.warning(f"A aba '{worksheet_name}' na planilha '{sheet_url_or_id}' está vazia.")
            return False

        df_from_gsheets = pd.DataFrame(data)
        processed_records = _preprocess_dataframe_for_db(df_from_gsheets)

        if processed_records is None or not processed_records:
            st.error("Falha no pré-processamento dos dados do Google Sheets ou nenhum dado válido restante.")
            return False

        import_success_count = 0
        total_rows = len(processed_records)

        st.info(f"Iniciando importação/atualização de {total_rows} processos...")
        progress_bar = st.progress(0)

        for index, row_dict in enumerate(processed_records):
            process_name = row_dict.get("Processo_Novo")
            if not process_name:
                st.warning(f"Linha {index+2} ignorada (Google Sheets): 'Processo_Novo' está vazio ou inválido.")
                continue

            if db_manager.upsert_processo(row_dict):
                import_success_count += 1
                logger.info(f"Processo '{process_name}' upserted (inserido/atualizado) via Google Sheets.")
            else:
                st.error(f"Falha ao fazer upsert do processo '{process_name}' na linha {index+2}. Verifique os dados.")
            
            progress_bar.progress((index + 1) / total_rows)
        
        progress_bar.empty()

        if import_success_count == total_rows:
            st.success("Dados do Google Sheets importados/atualizados com sucesso! A tabela foi recarregada.")
            _load_processes()
            return True
        elif import_success_count > 0:
            st.warning(f"Importação/atualização do Google Sheets concluída com {import_success_count} de {total_rows} processos bem-sucedidos. Verifique os erros acima para os processos que falharam.")
            _load_processes()
            return True
        else:
            st.error("Falha total ao importar/atualizar dados do Google Sheets para o banco de dados.")
            return False
    
    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"Planilha Google Sheets não encontrada com ID/URL: {sheet_url_or_id}. Verifique o ID/URL e as permissões.")
        logger.error(f"SpreadsheetNotFound: {sheet_url_or_id}")
        return False
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"Aba '{worksheet_name}' não encontrada na planilha. Verifique o nome da aba.")
        logger.error(f"WorksheetNotFound: {worksheet_name}")
        return False
    except Exception as e:
        st.error(f"Erro ao ler ou importar dados do Google Sheets: {e}")
        logger.exception("Erro inesperado ao importar do Google Sheets.")
        return False

def _display_import_popup():
    """Exibe o pop-up unificado para importação via Google Sheets ou Excel/CSV."""
    if not st.session_state.get('show_import_popup', False):
        return

    with st.form(key="import_popup_form"):
        st.markdown("### Opções de Importação")
        st.markdown("#### Importar do Google Sheets")
        st.info("Insira a URL ou ID da planilha e o nome da aba.")
        st.session_state.gsheets_url_id = st.text_input("URL ou ID da Planilha:", value=st.session_state.gsheets_url_id, key="popup_gsheets_url_id")
        st.session_state.gsheets_worksheet_name = st.text_input("Nome da Aba:", value=st.session_state.gsheets_worksheet_name, key="popup_gsheets_worksheet_name")
        confirm_gsheets_overwrite = st.checkbox("Confirmar substituição de dados no DB (Google Sheets)", key="popup_confirm_gsheets_overwrite")

        if st.form_submit_button("Importar Planilha do Google Sheets"):
            if st.session_state.popup_gsheets_url_id and st.session_state.popup_gsheets_worksheet_name:
                if confirm_gsheets_overwrite:
                    if _import_from_google_sheets(st.session_state.popup_gsheets_url_id, st.session_state.popup_gsheets_worksheet_name):
                        st.session_state.show_import_popup = False
                        st.rerun()
                else:
                    st.warning("Marque a caixa de confirmação para importar do Google Sheets.")
            else:
                st.warning("Por favor, forneça a URL/ID da Planilha e o Nome da Aba para Google Sheets.")

        st.markdown("---")
        st.markdown("#### Importar de Arquivo Excel/CSV Local")
        uploaded_file = st.file_uploader("Escolha um arquivo (.csv, .xls, .xlsx)", type=["csv", "xls", "xlsx"], key="file_uploader_local")
        confirm_local_overwrite = st.checkbox("Confirmar substituição de dados no DB (Arquivo Local)", key="popup_confirm_local_overwrite")

        if st.form_submit_button("Importar Arquivo Local"):
            if uploaded_file is not None:
                if confirm_local_overwrite:
                    if _import_file_action(uploaded_file):
                        st.session_state.show_import_popup = False
                        st.rerun()
                else:
                    st.warning("Marque a caixa de confirmação para importar o arquivo local.")
            else:
                st.warning("Por favor, selecione um arquivo para importação local.")
        
        if st.form_submit_button("Fechar Opções de Importação"):
            st.session_state.show_import_popup = False
            st.rerun()

    st.markdown("---")
    _generate_excel_template()

def _open_mass_edit_popup():
    """Abre o pop-up para edição em massa de processos."""
    st.session_state.show_mass_edit_popup = True
    if 'mass_edit_process_names_input' not in st.session_state:
        st.session_state.mass_edit_process_names_input = ""
    if 'mass_edit_found_processes' not in st.session_state:
        st.session_state.mass_edit_found_processes = []
    st.rerun()

def _display_mass_edit_popup():
    """Exibe o pop-up para edição em massa de processos."""
    if not st.session_state.get('show_mass_edit_popup', False):
        return

    with st.form(key="mass_edit_form"):
        st.markdown("### Editar Múltiplos Processos")
        st.markdown("#### 1. Inserir Processos para Edição")
        process_names_input = st.text_area(
            "Insira os nomes dos processos (um por linha):",
            value=st.session_state.mass_edit_process_names_input,
            height=150,
            key="mass_edit_process_names_textarea"
        )
        
        if st.form_submit_button("Buscar Processos"):
            st.session_state.mass_edit_process_names_input = process_names_input
            
            st.session_state.mass_edit_found_processes = []
            if process_names_input:
                names_to_search = [name.strip() for name in process_names_input.split('\n') if name.strip()]
                for name in names_to_search:
                    process_data_row = db_manager.obter_processo_by_processo_novo(name)
                    
                    found_entry = {
                        'Processo_Novo': name, 'ID': 'Não encontrado', 'Status da Busca': 'Não encontrado',
                        'Status_Geral': 'N/A', 'Observacao': 'N/A', 'Previsao_Pichau': 'N/A',
                        'Data_Embarque': 'N/A', 'ETA_Recinto': 'N/A', 'Data_Registro': 'N/A',
                        'Estimativa_Impostos_Total': 'N/A', 'Nota_feita': 'N/A',
                    }

                    if process_data_row:
                        process_data = dict(process_data_row) 
                        found_entry.update({
                            'Processo_Novo': process_data['Processo_Novo'], 'ID': process_data['id'],
                            'Status da Busca': 'Encontrado', 'Status_Geral': process_data.get('Status_Geral', 'N/A'),
                            'Observacao': process_data.get('Observacao', 'N/A'),
                            'Previsao_Pichau': _format_date_display(process_data.get('Previsao_Pichau')),
                            'Data_Embarque': _format_date_display(process_data.get('Data_Embarque')),
                            'ETA_Recinto': _format_date_display(process_data.get('ETA_Recinto')),
                            'Data_Registro': _format_date_display(process_data.get('Data_Registro')),
                            'Estimativa_Impostos_Total': _format_currency_display(process_data.get('Estimativa_Impostos_Total')),
                            'Nota_feita': process_data.get('Nota_feita', 'N/A'),
                        })
                    st.session_state.mass_edit_found_processes.append(found_entry)
            st.rerun()

        if st.session_state.mass_edit_found_processes:
            st.markdown("#### Resultados da Busca:")
            df_found_processes = pd.DataFrame(st.session_state.mass_edit_found_processes)
            
            display_cols_search_results = [
                "Processo_Novo", "Status_Geral", "Observacao", "Previsao_Pichau", "Data_Embarque", 
                "ETA_Recinto", "Data_Registro", "ID", "Status da Busca", "Estimativa_Impostos_Total", "Nota_feita"
            ]
            
            display_col_names_map = {
                "Processo_Novo": "Processo", "Status_Geral": "Status Geral", "Observacao": "Observação",
                "Previsao_Pichau": "Previsão na Pichau", "Data_Embarque": "Data do Embarque",
                "ETA_Recinto": "ETA no Recinto", "Data de Registro": "Data de Registro", "ID": "ID do DB",
                "Status da Busca": "Status da Busca", "Imp. Totais (R$)": "Imp. Totais (R$)", "Nota feita": "Nota feita",
            }
            
            df_display_search_results = df_found_processes[[col for col in display_cols_search_results if col in df_found_processes.columns]].rename(columns=display_col_names_map)

            st.dataframe(
                df_display_search_results, hide_index=True, use_container_width=True,
                column_config={
                    "Processo": st.column_config.TextColumn("Processo", width="medium"),
                    "Status Geral": st.column_config.TextColumn("Status Geral", width="small"),
                    "Observação": st.column_config.TextColumn("Observação", width="medium"),
                    "Previsão na Pichau": st.column_config.TextColumn("Previsão na Pichau", width="small"),
                    "Data do Embarque": st.column_config.TextColumn("Data do Embarque", width="small"),
                    "ETA no Recinto": st.column_config.TextColumn("ETA no Recinto", width="small"),
                    "Data de Registro": st.column_config.TextColumn("Data de Registro", width="small"),
                    "ID do DB": st.column_config.TextColumn("ID do DB", width="small"),
                    "Status da Busca": st.column_config.TextColumn("Status da Busca", width="small"),
                    "Imp. Totais (R$)": st.column_config.TextColumn("Imp. Totais (R$)", width="small"),
                    "Nota feita": st.column_config.TextColumn("Nota feita", width="small"),
                }
            )
            
            processes_to_edit_ids = [p['ID'] for p in st.session_state.mass_edit_found_processes if p['ID'] != 'Não encontrado']
            
            if not processes_to_edit_ids:
                st.warning("Nenhum processo válido encontrado para edição. Por favor, corrija os nomes e tente novamente.")
                st.session_state.mass_edit_can_proceed = False
            else:
                st.session_state.mass_edit_can_proceed = True
                st.markdown("---")
                st.markdown("#### 2. Selecionar Novos Valores")

                new_status_geral = st.selectbox(
                    "Novo Status Geral:", options=[""] + db_manager.STATUS_OPTIONS, key="mass_edit_new_status_value"
                )
                if new_status_geral == "": new_status_geral = None

                if 'mass_edit_observacao_touched' not in st.session_state:
                    st.session_state.mass_edit_observacao_touched = False

                new_observacao_input = st.text_area(
                    "Nova Observação:", value="", key="mass_edit_new_observacao_value"
                )

                if new_observacao_input != "":
                    st.session_state.mass_edit_observacao_touched = True
                elif st.session_state.mass_edit_observacao_touched and new_observacao_input == "":
                    pass
                else:
                    st.session_state.mass_edit_observacao_touched = False

                new_previsao_pichau_date = st.date_input(
                    "Nova Previsão na Pichau:", value=None, key="mass_edit_new_previsao_pichau_value", format="DD/MM/YYYY"
                )
                new_previsao_pichau = new_previsao_pichau_date.strftime("%Y-%m-%d") if new_previsao_pichau_date else None

                new_data_embarque_date = st.date_input(
                    "Nova Data do Embarque:", value=None, key="mass_edit_new_data_embarque_value", format="DD/MM/YYYY"
                )
                new_data_embarque = new_data_embarque_date.strftime("%Y-%m-%d") if new_data_embarque_date else None

                new_eta_recinto_date = st.date_input(
                    "Nova ETA no Recinto:", value=None, key="mass_edit_new_eta_recinto_value", format="DD/MM/YYYY"
                )
                new_eta_recinto = new_eta_recinto_date.strftime("%Y-%m-%d") if new_eta_recinto_date else None

                new_data_registro_date = st.date_input(
                    "Nova Data de Registro:", value=None, key="mass_edit_new_data_registro_value", format="DD/MM/YYYY"
                )
                new_data_registro = new_data_registro_date.strftime("%Y-%m-%d") if new_data_registro_date else None

                new_nota_feita = st.selectbox(
                    "Nova Nota feita?:", options=["", "Não", "Sim"], key="mass_edit_new_nota_feita_value"
                )
                if new_nota_feita == "": new_nota_feita = None

                col_save, col_cancel = st.columns(2)

                with col_save:
                    if st.form_submit_button("Aplicar Alterações", disabled=not st.session_state.mass_edit_can_proceed):
                        if not processes_to_edit_ids:
                            st.warning("Nenhum processo válido selecionado para edição.")
                        else:
                            user_info = st.session_state.get('user_info', {'username': 'Desconhecido'})
                            username = user_info.get('username')

                            successful_updates_count = 0
                            for p_id in processes_to_edit_ids:
                                original_process_data_row = db_manager.obter_processo_por_id(p_id)
                                if original_process_data_row:
                                    original_process_data = dict(original_process_data_row)
                                    
                                    changes_to_apply = {}
                                    if new_status_geral is not None: changes_to_apply["Status_Geral"] = new_status_geral
                                    if st.session_state.mass_edit_observacao_touched: changes_to_apply["Observacao"] = new_observacao_input if new_observacao_input != "" else None
                                    if new_previsao_pichau is not None: changes_to_apply["Previsao_Pichau"] = new_previsao_pichau
                                    if new_data_embarque is not None: changes_to_apply["Data_Embarque"] = new_data_embarque
                                    if new_eta_recinto is not None: changes_to_apply["ETA_Recinto"] = new_eta_recinto
                                    if new_data_registro is not None: changes_to_apply["Data_Registro"] = new_data_registro
                                    if new_nota_feita is not None: changes_to_apply["Nota_feita"] = new_nota_feita

                                    if not changes_to_apply:
                                        st.info(f"Nenhuma alteração detectada para o processo {original_process_data.get('Processo_Novo', 'N/A')} (ID: {p_id}).")
                                        continue

                                    if db_manager.atualizar_processo(p_id, changes_to_apply):
                                        successful_updates_count += 1
                                        for field_name, new_val in changes_to_apply.items():
                                            # db_type_to_use é sempre "Firestore" agora
                                            db_manager.inserir_historico_processo(p_id, field_name, original_process_data.get(field_name), new_val, username, db_type="Firestore")
                                    else:
                                        st.error(f"Falha ao atualizar processo ID {p_id}.")
                                else:
                                    st.error(f"Processo ID {p_id} não encontrado para atualização.")

                            if successful_updates_count > 0:
                                st.success(f"{successful_updates_count} processos atualizados com sucesso!")
                                st.session_state.show_mass_edit_popup = False
                                st.session_state.mass_edit_process_names_input = ""
                                st.session_state.mass_edit_found_processes = []
                                st.session_state.mass_edit_observacao_touched = False
                                _load_processes()
                                st.rerun()
                            else:
                                st.warning("Nenhum processo foi atualizado ou nenhuma alteração foi detectada para aplicar.")

                with col_cancel:
                    if st.form_submit_button("Cancelar"):
                        st.session_state.show_mass_edit_popup = False
                        st.session_state.mass_edit_process_names_input = ""
                        st.session_state.mass_edit_found_processes = []
                        st.session_state.mass_edit_observacao_touched = False
                        st.rerun()
        else:
            col_empty, col_cancel_only = st.columns([0.7, 0.3])
            with col_cancel_only:
                if st.form_submit_button("Fechar"):
                    st.session_state.show_mass_edit_popup = False
                    st.session_state.mass_edit_process_names_input = ""
                    st.session_state.mass_edit_found_processes = []
                    st.session_state.mass_edit_observacao_touched = False
                    st.rerun()

def _export_processes_to_excel(df_data: pd.DataFrame):
    """Exporta os dados do DataFrame para um arquivo Excel em memória."""
    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')

    column_display_names = {
        "Processo_Novo": "Processo", "Fornecedor": "Fornecedor", "Tipos_de_item": "Tipo de Item",
        "Observacao": "Observação", "Data_Embarque": "Data Embarque", "ETA_Recinto": "ETA Recinto",
        "Previsao_Pichau": "Previsão Pichau", "Documentos_Revisados": "Docs Revisados",
        "Conhecimento_Embarque": "Conhecimento Embarque", "Descricao_Feita": "Descrição Feita",
        "Descricao_Enviada": "Descrição Enviada", "Nota_feita": "Nota Feita", "N_Invoice": "Nº Invoice",
        "Quantidade": "Quantidade", "Valor (USD)": "Valor (USD)", "Pago": "Pago?",
        "Nº Ordem Compra": "Nº Ordem Compra", "Data Compra": "Data Compra",
        "Estimativa_Frete_USD": "Estimativa Frete (USD)", "Agente_de_Carga_Novo": "Agente de Carga",
        "Status_Geral": "Status Geral", "Modal": "Modal", "Navio": "Navio", "Origem": "Origem",
        "Destino": "Destino", "INCOTERM": "INCOTERM", "Comprador": "Comprador", "Navio": "Navio",
        "Quantidade_Containers": "Qtd. Containers", "Data_Registro": "Data Registro",
        "Estimativa_Impostos_Total": "Imp. Totais (R$)", "Estimativa_Dolar_BRL": "Câmbio Estimado (R$)", 
        "Estimativa_Seguro_BRL": "Estimativa Seguro (R$)", "Estimativa_II_BR": "Estimativa II (R$)", 
        "Estimativa_IPI_BR": "Estimativa IPI (R$)", "Estimativa PIS (R$)": "Estimativa PIS (R%))", 
        "Estimativa_COFINS_BR": "Estimativa COFINS (R$)", "Estimativa_ICMS_BR": "Estimativa ICMS (R$)", 
        "id": "ID do Processo"
    }
    
    cols_to_export = [col for col in column_display_names.keys() if col in df_data.columns]
    df_export = df_data[cols_to_export].copy()
    df_export = df_export.rename(columns=column_display_names)

    date_cols = ["Data Embarque", "ETA Recinto", "Previsão Pichau", "Data Compra", "Data Registro"]
    currency_usd_cols = ["Valor (USD)", "Estimativa Frete (USD)"]
    currency_brl_cols = ["Imp. Totais (R$)", "Câmbio Estimado (R$)", "Estimativa Seguro (R$)", "Estimativa II (R$)", "Estimativa IPI (R$)", "Estimativa PIS (R$)", "Estimativa COFINS (R$)", "Estimativa ICMS (R$)"]

    for col in date_cols:
        if col in df_export.columns:
            df_export[col] = df_export[col].apply(lambda x: _format_date_display(x) if pd.notna(x) else '')
            
    for col in currency_usd_cols:
        if col in df_export.columns:
            df_export[col] = pd.to_numeric(df_export[col], errors='coerce').fillna(0).apply(lambda x: f"{x:,.2f}".replace('.', '#').replace(',', '.').replace('#', ','))

    for col in currency_brl_cols:
        if col in df_export.columns:
            df_export[col] = pd.to_numeric(df_export[col], errors='coerce').fillna(0).apply(lambda x: f"R$ {x:,.2f}".replace('.', '#').replace(',', '.').replace('#', ','))
    
    df_export.to_excel(writer, index=False, sheet_name='Processos de Importação')
    writer.close()
    output.seek(0)
    return output

# Cores para os status (revisadas para um visual mais agradável e claro)
STATUS_COLORS_HEX = {
    'Encerrado': "#FFFDFD",        # Cinza Neutro
    'Chegada Pichau': "#636464",   # Azul Padrão
    'Agendado': "#888888",         # Roxo Suave
    'Liberado': '#28A745',         # Verde Sucesso
    'Registrado': "#CFE600",       # Azul Ciano Claro
    'Chegada Recinto': "#0787FF",  # Amarelo Alerta
    'Embarcado': '#DC3545',        # Vermelho Erro/Perigo
    'Limbo Consolidado': '#6C757D',# Cinza Chumbo
    'Limbo Saldo': "#0A6D05",      # Cinza Chumbo
    'Pré Embarque': '#20C997',     # Verde Água Suave
    'Verificando': '#FD7E14',      # Laranja Alerta
    'Em produção': '#6F42C1',      # Roxo Médio
    'Processo Criado': '#A9A9A9',  # Cinza Claro para o padrão
    'Arquivados': '#DC3545',       # Vermelho para status "Arquivado"
    'Sem Status': '#343A40',       # Cinza Escuro para campos vazios/desconhecidos
    'Status Desconhecido': '#343A40', # Cinza Escuro
}

def _get_text_color(background_hex_color: str) -> str:
    """Determina a cor do texto (branco ou preto) com base na cor de fundo para melhor contraste."""
    # Para o propósito atual, onde a cor é aplicada apenas ao texto do status
    # A cor do texto será sempre a cor do status para manter a consistência visual
    # Se o fundo da célula mudar, esta função precisaria ser mais complexa.
    return background_hex_color # A cor do texto é a própria cor do status.


def _reset_main_filters():
    """Reseta os filtros principais de status e pesquisa de processo."""
    st.session_state.followup_selected_statuses = ['Todos']
    st.session_state.followup_main_process_search_term = ''
    _load_processes() # Recarrega os processos com os filtros resetados

def _display_followup_list_page():
    """Função para exibir a página da lista de Follow-up de Importação."""
    background_image_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'assets', 'logo_navio_atracado.png')
    set_background_image(background_image_path)

    st.subheader("Follow-up Importação")

    # Inicialização de todos os estados da sessão para a página de follow-up
    st.session_state.setdefault('followup_processes_data', [])
    st.session_state.setdefault('selected_process_data', None)
    st.session_state.setdefault('followup_search_terms', {}) # Mantido para compatibilidade se houver outros usos
    st.session_state.setdefault('followup_all_status_options', []) # Vai ser populado por _update_status_filter_options
    st.session_state.setdefault('followup_raw_status_options_for_multiselect', []) # Vai ser populado
    st.session_state.setdefault('followup_selected_statuses', ['Todos']) # Novo estado para multiselect (status brutos)
    st.session_state.setdefault('followup_main_process_search_term', '') # Novo estado para pesquisa de processo na tela principal
    st.session_state.setdefault('followup_popup_search_terms', {}) # Termos de pesquisa do popup
    st.session_state.setdefault('show_filter_search_popup', False)
    st.session_state.setdefault('gsheets_url_id', "")
    st.session_state.setdefault('gsheets_worksheet_name', "Sheet1")
    st.session_state.setdefault('show_delete_confirm_popup', False)
    st.session_state.setdefault('delete_process_id_to_confirm', None)
    st.session_state.setdefault('delete_process_name_to_confirm', None)
    st.session_state.setdefault('show_import_popup', False)
    st.session_state.setdefault('show_mass_edit_popup', False)
    st.session_state.setdefault('mass_edit_process_names_input', "")
    st.session_state.setdefault('mass_edit_found_processes', [])
    st.session_state.setdefault('mass_edit_can_proceed', False)
    st.session_state.setdefault('form_is_cloning', False)
    st.session_state.setdefault('show_change_status_popup', False) # Novo: controlar popup de status
    st.session_state.setdefault('process_id_to_change_status', None)
    st.session_state.setdefault('process_name_to_change_status', None)


    # _load_processes() é chamado antes dos popups para garantir que as opções de filtro estejam atualizadas
    _load_processes() 

    # Exibe pop-ups modais se estiverem ativos
    _display_filter_search_popup()
    _display_import_popup()
    _display_delete_confirm_popup()
    _display_mass_edit_popup()
    _display_change_status_popup() # Exibir o novo popup de status

    # Se um pop-up está aberto, impede a renderização do restante da página principal
    if st.session_state.get('show_filter_search_popup', False) or \
       st.session_state.get('show_import_popup', False) or \
       st.session_state.get('show_delete_confirm_popup', False) or \
       st.session_state.get('show_mass_edit_popup', False) or \
       st.session_state.get('show_change_status_popup', False): # Adicionado o novo popup
        return

    st.markdown("---")
    
    # Filtros e pesquisa na tela principal
    col_main_filters_1, col_main_filters_2 = st.columns([0.5, 0.5])
    with col_main_filters_1:
        # Pega as opções de status formatadas e as opções de status brutas para o default
        all_status_options_formatted = st.session_state.get('followup_all_status_options', ["Todos"])
        current_selected_statuses_raw = st.session_state.get('followup_selected_statuses', ['Todos'])

        # Mapeia os status brutos selecionados para as opções formatadas para o default
        default_multiselect_value = []
        for raw_s in current_selected_statuses_raw:
            if raw_s == 'Todos':
                if 'Todos' in all_status_options_formatted:
                    default_multiselect_value.append("Todos")
            else:
                found_formatted_opt = next((opt for opt in all_status_options_formatted if opt.startswith(raw_s + ' (')), None)
                if found_formatted_opt:
                    default_multiselect_value.append(found_formatted_opt)
        
        # Multiselect para filtrar por Status
        selected_options_with_counts = st.multiselect(
            "Filtrar por Status:",
            options=all_status_options_formatted,
            default=default_multiselect_value,
            key="main_followup_status_multiselect",
            on_change=_on_status_multiselect_change # Usa o novo callback
        )

    with col_main_filters_2:
        # Campo de pesquisa de processo na tela principal
        st.text_input(
            "Pesquisar Processo:", 
            value=st.session_state.get('followup_main_process_search_term', ''),
            key="main_followup_search_processo_novo",
            on_change=_on_process_search_change # Usa o novo callback
        )
    
    # Botões de ação globais (agora com "Mais Filtros" e "Limpar Filtros")
    with st.popover("Mais Opções"):
        if st.button("Adicionar Novo Processo +", key="add_new_process_button"):
            _open_edit_process_popup(None)
        if st.button("Editar Múltiplos Processos ✍🏻", key="mass_edit_processes_button"):
            _open_mass_edit_popup()
        if st.button("Mais Filtros ⌨", key="open_filter_search_popup_button"):
            _open_filter_search_popup()
        if st.session_state.followup_processes_data:
            df_to_export = pd.DataFrame(st.session_state.followup_processes_data)
            excel_data = _export_processes_to_excel(df_to_export)
            st.download_button(
                label="Exportar Excel 📊",
                data=excel_data,
                file_name="processos_importacao_filtrados.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="export_excel_button"
            )
        else:
            st.info("Nenhum dado para exportar.")
        if st.button("Limpar Filtros �", key="clear_main_filters_button"):
            _reset_main_filters()
            st.rerun() # Necessário para forçar a UI a refletir o reset dos filtros
        
        if st.button("Pesquisar Item 🔎 ", key="search_item_button"):
            st.session_state.current_page = "Produtos"
            st.rerun()
            
    

    st.markdown("---")
    st.markdown("#### Processos de Importação")

    # Estilo CSS para os cards
    st.markdown("""
        <style>
        .process-card-container {
            background-color: #333; /* Fundo escuro para os cards */
            border-radius: 10px;
            padding: 15px;
            margin-bottom: 15px;
            box-shadow: 2px 2px 8px rgba(0, 0, 0, 0.3);
            transition: transform 0.2s ease-in-out, box-shadow 0.2s ease-in-out;
            cursor: pointer; /* Indica que o card é clicável */
            border: 1px solid #444; /* Borda sutil */
        }
        .process-card-container:hover {
            transform: translateY(-5px); /* Efeito de elevação ao passar o mouse */
            box-shadow: 3px 3px 12px rgba(0, 0, 0, 0.5); /* Sombra mais forte */
        }
        .process-card-row {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 10px;
        }
        .process-card-col {
            flex: 1;
            padding: 0 5px;
            color: #E0E0E0; /* Cor do texto padrão para o card */
        }
        .process-card-col strong {
            color: #F8F8F8; /* Cor mais clara para títulos */
        }
        .process-card-status-text {
            font-weight: bold;
            padding: 2px 8px;
            border-radius: 5px;
            display: inline-block;
        }
        /* Ajustes para alinhar emojis de check/cross */
        .process-card-doc-status {
            font-size: 1.1em; /* Ajusta o tamanho dos emojis */
        }

        /* Estilo para o botão de três pontos e menu */
        /* Alvo mais específico para o botão do popover */
        div[data-testid^="stVerticalBlock"] > div > div > div > div > .stButton > button {
            font-size: 1.5em;
            padding: 0.2em 0.5em;
            border-radius: 50%;
            width: 40px;
            height: 40px;
            display: flex;
            justify-content: center;
            align-items: center;
            background-color: #555;
            color: white;
            border: none;
            box-shadow: 1px 1px 3px rgba(0,0,0,0.3);
        }
        div[data-testid^="stVerticalBlock"] > div > div > div > div > .stButton > button:hover {
            background-color: #777;
        }

        /* Estilo para os botões dentro do popover */
        div[data-testid="stPopover"] .stButton > button {
            width: 100%;
            margin-bottom: 5px;
            justify-content: flex-start; /* Alinha o texto à esquerda */
            padding-left: 10px;
            border-radius: 5px; /* Volta para cantos arredondados normais para botões de menu */
            background-color: #444; /* Cor de fundo para botões de menu */
            color: white;
            box-shadow: none; /* Remove sombra extra */
        }
        div[data-testid="stPopover"] .stButton > button:hover {
            background-color: #666; /* Escurece no hover */
        }
        </style>
    """, unsafe_allow_html=True)

    # Initialize column variables outside the loop to satisfy Pylance's static analysis
    # These assignments are primarily for linter satisfaction and don't affect runtime logic,
    # as the actual columns are defined dynamically inside the loop.
    col_main_info = None
    col_dates_status = None
    col_docs_status = None
    col_actions = None

    if st.session_state.followup_processes_data:
        
        for row_index, row_dict in enumerate(st.session_state.followup_processes_data):
            # Garante que process_id seja uma string única para a chave do widget
            # Adiciona UUID como fallback caso 'id' seja None ou vazio para robustez
            unique_id_for_key = str(row_dict.get('id')) if row_dict.get('id') is not None and str(row_dict.get('id')).strip() != '' else str(uuid.uuid4())
            processo_novo = row_dict.get('Processo_Novo', 'N/A')
            status_geral = row_dict.get('Status_Geral', 'Sem Status')
            modal = row_dict.get('Modal', 'Sem Modal')
            fornecedor = row_dict.get('Fornecedor', 'N/A')
            n_invoice = row_dict.get('N_Invoice', 'N/A')
            quantidade = row_dict.get('Quantidade', 0)
            valor_usd = row_dict.get('Valor_USD', 0.0)
            data_compra = _format_date_display(row_dict.get('Data_Compra'))
            data_embarque = _format_date_display(row_dict.get('Data_Embarque'))
            eta_recinto = _format_date_display(row_dict.get('ETA_Recinto'))
            previsao_pichau = _format_date_display(row_dict.get('Previsao_Pichau'))
            observacao = row_dict.get('Observacao', 'N/A')

            # **LÓGICA DE ESTILO PARA STATUS ARQUIVADO**
            status_arquivado = row_dict.get('Status_Arquivado', 'Não Arquivado')
            display_status = status_geral # O status a ser exibido
            status_display_color = STATUS_COLORS_HEX.get(status_geral, STATUS_COLORS_HEX['Sem Status'])

            if status_arquivado == 'Arquivado':
                display_status = "Arquivado" # Altera o texto para "Arquivado"
                status_display_color = STATUS_COLORS_HEX['Arquivados'] # Usa a cor vermelha definida para "Arquivados"
            # FIM DA LÓGICA

            # Conversão para emojis
            pago = "✅" if str(row_dict.get('Pago', '')).lower() == "sim" else ("❌" if str(row_dict.get('Pago', '')).lower() == "não" else "➖")
            docs_revisados = "✅" if str(row_dict.get('Documentos_Revisados', '')).lower() == "sim" else ("❌" if str(row_dict.get('Documentos_Revisados', '')).lower() == "não" else "➖")
            conhecimento_embarque = "✅" if str(row_dict.get('Conhecimento_Embarque', '')).lower() == "sim" else ("❌" if str(row_dict.get('Conhecimento_Embarque', '')).lower() == "não" else "➖")
            descricao_feita = "✅" if str(row_dict.get('Descricao_Feita', '')).lower() == "sim" else ("❌" if str(row_dict.get('Descricao_Feita', '')).lower() == "não" else "➖")
            descricao_enviada = "✅" if str(row_dict.get('Descricao_Enviada', '')).lower() == "sim" else ("❌" if str(row_dict.get('Descricao_Enviada', '')).lower() == "não" else "➖")
            nota_feita = "✅" if str(row_dict.get('Nota_feita', '')).lower() == "sim" else ("❌" if str(row_dict.get('Nota_feita', '')).lower() == "não" else "➖")
            conferido = "✅" if str(row_dict.get('Conferido', '')).lower() == "sim" else ("❌" if str(row_dict.get('Conferido', '')).lower() == "não" else "➖")

            modal_icon = '✈️' if modal == 'Aéreo' else ('🚢' if modal == 'Maritimo' else '➖')
            
            # Use status_display_color e display_status aqui
            col_main_info, col_dates_status, col_docs_status, col_actions = st.columns([0.15, 0.25, 0.35, 0.05])
            with col_main_info:
                st.markdown(f"<div style='font-size: 2.5em; text-align: center; color: #F8F8F8;'>{modal_icon}</div>", unsafe_allow_html=True)
                st.markdown(f"""
                    <div style='color: #E0E0E0; text-align: center;'>
                        <strong>{processo_novo}</strong><br>
                        <small>{fornecedor}</small><br>
                    </div>
                """, unsafe_allow_html=True)
            
            with col_dates_status:
                st.markdown(f"""
                    <div style='color: #E0E0E0;'>
                        <span class="process-card-status-text" style="color: {status_display_color}; font-size: 1.2em;">{display_status}</span><br>
                        <strong>Qtd:</strong> {quantidade} | <strong>Valor (US$):</strong> {_format_usd_display(valor_usd).replace('US$', '')}<br>
                        <small>Nº Invoice: {n_invoice}</small><br>
                        <strong>Observação:</strong> <span style="color:{'#FF0000' if observacao != 'N/A' and observacao != 'None' else '#E0E0E0'}">{observacao if observacao not in ['N/A', 'None'] else 'Nenhuma'}</span><br>

                    </div>
                """, unsafe_allow_html=True)

            with col_docs_status:
                # Revertendo para exibição em uma linha para as informações 'Sim/Não'
                
                    st.markdown(f"""
                        <div style='color: #E0E0E0;'>
                            <br><strong>Data Compra:</strong> {data_compra}<br>
                            <strong>Data Emb.:</strong> {data_embarque} |
                            <strong>Prev. Pichau:</strong> {previsao_pichau}
                        </div>
                    """, unsafe_allow_html=True)

            with col_actions:
                # NOVO: Remover o 'key' do st.popover
                with st.popover("📂"):
                    if st.button("Consultar Processo 🔎", key=f"menu_query_{unique_id_for_key}"):
                        st.session_state.selected_process_data = row_dict 
                        _open_process_query_page(unique_id_for_key)
                    if st.button("Alterar Status do Processo 🔄", key=f"menu_change_status_{unique_id_for_key}"):
                        st.session_state.show_change_status_popup = True
                        st.session_state.process_id_to_change_status = unique_id_for_key
                        st.session_state.process_name_to_change_status = processo_novo
                        st.rerun()
                    if st.button("Editar Processo ✏️", key=f"menu_edit_{unique_id_for_key}"):
                        st.session_state.selected_process_data = row_dict
                        _open_edit_process_popup(unique_id_for_key)
                    if st.button("Clonar Processo 🗒️", key=f"menu_clone_{unique_id_for_key}"):
                        st.session_state.selected_process_data = row_dict
                        _open_edit_process_popup(unique_id_for_key, is_cloning=True)
                    if st.button("Arquivar Processo 🗑️", key=f"menu_archive_{unique_id_for_key}"):
                        st.session_state.show_delete_confirm_popup = True
                        st.session_state.delete_process_id_to_confirm = unique_id_for_key
                        st.session_state.delete_process_name_to_confirm = processo_novo
                        st.rerun()
                    # Nova opção: Alterar Status do Processo
                    

            col1_empty, col1_docs_status,col3_empty = st.columns([0.08, 0.32, 0.11])
            with col1_docs_status:
                st.markdown(f"""
                            <div style='display: flex; justify-content: space-around; font-size: 0.9em; color: #E0E0E0;'>
                                <span>Pago: <span class="process-card-doc-status">{pago}</span></span>
                                <span>Docs Rev.: <span class="process-card-doc-status">{docs_revisados}</span></span>
                                <span>Conh. Emb.: <span class="process-card-doc-status">{conhecimento_embarque}</span></span>                  
                                <span>Desc. Feita: <span class="process-card-doc-status">{descricao_feita}</span></span>
                                <span>Nota feita: <span class="process-card-doc-status">{nota_feita}</span></span>
                                <span>Conferido: <span class="process-card-doc-status">{conferido}</span></span>
                            </div>
                        """, unsafe_allow_html=True)
            st.markdown("---") # Separador entre os cards
    else:
        st.info("Nenhum processo de importação encontrado. Adicione um novo ou importe via arquivo.")

    st.markdown("---")
    col_import_data_btn, _ = st.columns([0.2, 0.8])
    with col_import_data_btn:
        if st.button("Importação de Dados", key="open_import_options_button_bottom"):
            st.session_state.show_import_popup = True
            st.rerun()

    st.write("Esta tela permite gerenciar o follow-up de processos de importação.")
    st.markdown("---")

# Nova função principal para rotear entre as páginas
def show_page():
    
    
    """Função principal para rotear entre as páginas do Follow-up e Formulário de Processo."""
    if 'current_page' not in st.session_state:
        st.session_state.current_page = "Follow-up Importação"

    # Define a função de callback para recarregar processos no formulário
    st.session_state.form_reload_processes_callback = _load_processes 
    
    if st.session_state.current_page == "Formulário Processo":
        process_form_page.show_process_form_page(
            process_identifier=st.session_state.get('form_process_identifier'),
            reload_processes_callback=st.session_state.form_reload_processes_callback,
            is_cloning=st.session_state.get('form_is_cloning', False)
        )
    elif st.session_state.current_page == "Consulta de Processo":
        process_query_page.show_process_query_page(
            process_identifier=st.session_state.get('query_process_identifier'),
            return_callback=lambda: setattr(st.session_state, 'current_page', "Follow-up Importação")
        )
    else: # Default para "Follow-up Importação"
        _display_followup_list_page()
