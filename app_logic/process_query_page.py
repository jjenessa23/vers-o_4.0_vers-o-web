import streamlit as st
import pandas as pd
from datetime import datetime
import logging
import os
import base64
from typing import Optional, Any, Dict, List, Union

import followup_db_manager as db_manager
# Importa db_utils real, ou usa o mock se houver erro
db_utils: Any # Declaração para tipo-checking, o import real/mock acontece em db_manager

# Configura o logger para esta página
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) # Nível INFO para esta página, para logs de alto nível

# Tenta importar db_utils real, ou usa o mock se houver erro
try:
    import db_utils
    # Verifica se as funções essenciais estão presentes, caso contrário, usa o Mock
    if not hasattr(db_utils, 'get_declaracao_by_id') or \
       not hasattr(db_utils, 'get_declaracao_by_referencia') or \
       not hasattr(db_utils, 'get_ncm_item_by_ncm_code') or \
       not hasattr(db_utils, 'selecionar_todos_ncm_itens'):
        logger.warning("Módulo 'db_utils' real não contém funções esperadas. Usando MockDbUtils.")
        raise ImportError # Força o uso do Mock
except ImportError:
    # Define uma classe MockDbUtils simplificada localmente, se db_utils não puder ser importado
    class MockDbUtils:
        def get_db_path(self, db_name: str) -> str:
            _base_path = os.path.dirname(os.path.abspath(__file__))
            _app_root_path = os.path.dirname(_base_path) if os.path.basename(_base_path) == 'app_logic' else _base_path
            _DEFAULT_DB_FOLDER = "data"
            return os.path.join(_app_root_path, _DEFAULT_DB_FOLDER, f"{db_name}.db")
        
        def get_declaracao_by_id(self, di_id: int) -> Optional[dict]:
            # Mock de dados da DI
            if di_id == 1:
                return {'numero_di': '1234567890', 'id': 1}
            return None 

        def get_declaracao_by_referencia(self, process_number: str) -> Optional[dict]:
            if process_number == "MOCK-DI-123":
                return {'numero_di': '9988776654', 'id': 999}
            return None
        
        def get_ncm_item_by_ncm_code(self, ncm_code: str) -> Optional[dict]:
            return {'ii_aliquota': 0.0, 'ipi_aliquota': 0.0, 'pis_aliquota': 0.0, 'cofins_aliquota': 0.0, 'icms_aliquota': 0.0}

        def seleccionar_todos_ncm_itens(self) -> List[Dict[str, Any]]:
            return [] # Mock vazio

    db_utils = MockDbUtils()
except Exception as e:
    logger.error(f"Erro inesperado ao importar ou inicializar 'db_utils': {e}. Usando MockDbUtils.")
    class MockDbUtils:
        def get_db_path(self, db_name: str) -> str:
            _base_path = os.path.dirname(os.path.abspath(__file__))
            _app_root_path = os.path.dirname(_base_path) if os.path.basename(_base_path) == 'app_logic' else _base_path
            _DEFAULT_DB_FOLDER = "data"
            return os.path.join(_app_root_path, _DEFAULT_DB_FOLDER, f"{db_name}.db")
        def get_declaracao_by_id(self, di_id: int) -> Optional[dict]: return None
        def get_declaracao_by_referencia(self, process_number: str) -> Optional[dict]: return None
        def get_ncm_item_by_ncm_code(self, ncm_code: str) -> Optional[dict]: return {'ii_aliquota': 0.0, 'ipi_aliquota': 0.0, 'pis_aliquota': 0.0, 'cofins_aliquota': 0.0, 'icms_aliquota': 0.0}
        def seleccionar_todos_ncm_itens(self) -> List[Dict[str, Any]]: return []
    db_utils = MockDbUtils()


# Função para definir imagem de fundo com opacidade
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
        logger.warning(f"A imagem de fundo não foi encontrada no caminho: {image_path}")
    except Exception as e:
        logger.error(f"Erro ao carregar a imagem de fundo: {e}")

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

def show_process_query_page(process_identifier: Any, return_callback: callable):
    """
    Exibe a tela de consulta de um processo específico.
    process_identifier: ID (int) ou Processo_Novo (str) do processo a ser consultado.
    return_callback: Função para chamar para retornar à página anterior.
    """
    background_image_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'assets', 'logo_navio_atracado.png')
    set_background_image(background_image_path)

    
    process_data = None
    process_history = []

    if isinstance(process_identifier, int):
        process_data_raw = db_manager.obter_processo_por_id(process_identifier)
    elif isinstance(process_identifier, str):
        process_data_raw = db_manager.obter_processo_by_processo_novo(process_identifier)
    else:
        st.error("Identificador de processo inválido para consulta.")
        if st.button("Voltar"):
            return_callback()
            st.rerun()
        return

    if process_data_raw:
        process_data = dict(process_data_raw)
        # O ID para o histórico deve ser consistente com o tipo de ID que o db_manager.obter_historico_processo espera
        # Para Firestore, é a string Processo_Novo. Para SQLite, é o ID numérico.
        history_id = process_data.get('id') # 'id' já deve ser a chave correta para o DB primário
        if db_manager._USE_FIRESTORE_AS_PRIMARY: # Se Firestore é primário, use o Processo_Novo
            history_id = process_data.get('Processo_Novo')
            
        process_history = db_manager.obter_historico_processo(history_id)
    else:
        st.error(f"Processo '{process_identifier}' não encontrado para consulta.")
        if st.button("Voltar"):
            return_callback()
            st.rerun()
        return

    st.subheader(f"Detalhes do Processo: {process_data.get('Processo_Novo', 'N/A')}")
    st.markdown(f"**Status Geral:** {process_data.get('Status_Geral', 'N/A')}")

    st.markdown("---")

    col_left, col_right = st.columns(2)

    with col_left:
        st.markdown(f"**Fornecedor:** {process_data.get('Fornecedor', 'N/A')}")
        st.markdown(f"**Nº Invoice:** {process_data.get('N_Invoice', 'N/A')}")
        st.markdown(f"**Quantidade:** {process_data.get('Quantidade', 0)}")
        st.markdown(f"**Valor (USD):** {_format_usd_display(process_data.get('Valor_USD', 0.0))}")

    with col_right:
        st.markdown(f"**Estimativa Impostos (BRL):** {_format_currency_display(process_data.get('Estimativa_Impostos_Total', 0.0))}")
        st.markdown(f"**INCOTERM:** {process_data.get('INCOTERM', 'N/A')}")
        st.markdown(f"**Comprador:** {process_data.get('Comprador', 'N/A')}")
        st.markdown(f"**Modal:** {process_data.get('Modal', 'N/A')}")

    st.markdown("---")

    # Adicionar seção de download de documento
    st.markdown("#### Documento Anexado")
    file_name = process_data.get("Nome_do_arquivo")
    file_type = process_data.get("Tipo_do_arquivo")
    file_content_base64 = process_data.get("Conteudo_do_arquivo")

    if file_name and file_content_base64:
        try:
            decoded_content = base64.b64decode(file_content_base64)
            st.download_button(
                label=f"Baixar Documento: {file_name}",
                data=decoded_content,
                file_name=file_name,
                mime=file_type if file_type else "application/octet-stream",
                key="download_attached_document"
            )
        except Exception as e:
            st.error(f"Erro ao decodificar ou preparar o download do arquivo: {e}")
            logger.error(f"Erro ao decodificar ou preparar o download do arquivo '{file_name}': {e}")
    else:
        st.info("Nenhum documento anexado a este processo.")

    st.markdown("---")

    st.markdown("#### Histórico de Status")
    
    # Definir a ordem dos status para a barra de progresso
    ordered_statuses = [
        "Processo Criado", "Em produção", "Pré Embarque", "Embarcado",
        "Chegada Recinto", "Registrado", "Chegada Pichau", "Encerrado"
    ]

    # Criar um dicionário de histórico de status para fácil acesso (Status -> {timestamp, usuario})
    status_history_map = {}
    if process_history:
        # Preencher o status_history_map com a data e hora da primeira vez que cada status foi atingido
        # Isso garante que a data na timeline corresponda ao momento em que o processo *entrou* naquele status
        for entry in sorted(process_history, key=lambda x: x.get('timestamp', ''), reverse=False): # Ordenar por timestamp ascendente
            timestamp_to_use = entry.get('status_change_timestamp') if entry.get('status_change_timestamp') else entry['timestamp']
            
            # Helper function to parse timestamp safely
            def parse_timestamp_safely(ts_str):
                if not ts_str or str(ts_str).lower() == 'nan':
                    return datetime.min # Use a very old date for invalid/missing timestamps
                try:
                    return datetime.strptime(str(ts_str).split('.')[0], "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    logger.warning(f"Could not parse timestamp '{ts_str}'. Using datetime.min for comparison.")
                    return datetime.min

            parsed_timestamp = parse_timestamp_safely(timestamp_to_use)

            if entry.get('campo_alterado') == 'Status_Geral' and entry.get('valor_novo'):
                status_key = entry.get('valor_novo')
                if status_key and (status_key not in status_history_map or parsed_timestamp > parse_timestamp_safely(status_history_map[status_key]['timestamp'])):
                    status_history_map[status_key] = {
                        'timestamp': timestamp_to_use,
                        'usuario': entry['usuario']
                    }
            elif entry.get('campo_alterado') == 'Processo Criado': # Captura a data de criação do processo
                status_key = "Processo Criado"
                if status_key not in status_history_map or parsed_timestamp > parse_timestamp_safely(status_history_map[status_key]['timestamp']):
                    status_history_map[status_key] = {
                        'timestamp': timestamp_to_use,
                        'usuario': entry['usuario']
                    }
    
    # === INÍCIO DO AJUSTE DA TIMELINE, CORES E ALINHAMENTO ===
    # CSS para o estilo da timeline
    st.markdown("""
    <style>
    /* Importa Font Awesome */
    @import url('https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css');

    .status-timeline-container {
        position: relative;
        width: 100%;
        margin-top: 20px;
        margin-bottom: 20px;
        padding-bottom: 20px; /* Espaço para o texto abaixo da linha */
    }

    .status-timeline-line {
        position: absolute;
        top: 70px; /* Alinha a linha no meio dos círculos */
        left: 0;
        right: 0;
        height: 2px;
        background-color: #007bff; /* Linha azul */
        z-index: 0;
    }

    .status-point-wrapper {
        display: flex;
        flex-direction: column;
        align-items: center;
        text-align: center;
        position: relative;
        z-index: 1; /* Garante que os pontos fiquem acima da linha */
        flex: 1; /* Distribui o espaço igualmente entre os pontos */
    }

    .status-circle {
        width: 40px;
        height: 40px;
        border-radius: 50%;
        background-color: #888; /* Cor padrão cinza */
        display: flex;
        align-items: center;
        justify-content: center;
        color: white;
        font-size: 1.2em;
        font-weight: bold;
        border: 2px solid #888;
        box-shadow: 0 0 5px rgba(0,0,0,0.5);
        margin-bottom: 5px; /* Espaço entre o círculo e o label */
    }
    .status-circle.completed {
        background-color: #007bff; /* Azul para status concluídos */
        border-color: #007bff;
    }
    .status-circle i {
        color: white;
        font-size: 1.2em;
    }
    .status-label {
        font-size: 0.85em;
        color: #ddd;
        white-space: normal; /* Permite quebra de linha */
        word-break: break-word;
    }
    .status-date {
        font-size: 0.65em;
        color: #aaa;
        white-space: nowrap; /* Impede quebra de linha na data */
    }
    </style>
    """, unsafe_allow_html=True)

    current_status = process_data.get('Status_Geral', 'N/A')

    # Dicionário de ícones para cada status
    status_icons = {
        "Processo Criado": "fa-solid fa-pen",
        "Em produção": "fa-solid fa-industry",
        "Pré Embarque": "fa-solid fa-box",
        "Embarcado": "fa-solid fa-ship",
        "Chegada Recinto": "fa-solid fa-warehouse",
        "Registrado": "fa-solid fa-clipboard-check",
        "Chegada Pichau": "fa-solid fa-truck-ramp-box",
        "Encerrado": "fa-solid fa-circle-check"
    }

    # Obter o índice do status atual
    current_status_index = -1
    try:
        current_status_index = ordered_statuses.index(current_status)
    except ValueError:
        pass

    # Cria as colunas para cada ponto de status
    cols = st.columns(len(ordered_statuses))

    # Renderiza cada ponto de status dentro de sua coluna
    for i, status in enumerate(ordered_statuses):
        with cols[i]:
            circle_class = ""
            if i <= current_status_index:
                circle_class = "completed"
            
            icon_class = status_icons.get(status, "fa-solid fa-circle") 

            # Ajustar a obtenção das datas do status_history_map
            timestamp_info = status_history_map.get(status, {}).get('timestamp')
            
            display_date = ''
            display_time = ''
            if timestamp_info:
                try:
                    # Garantir que a string esteja no formato correto antes de tentar o split e parse
                    # Removendo milissegundos se presentes (ex: '2024-01-01 12:30:45.123')
                    dt_object = datetime.strptime(str(timestamp_info).split('.')[0], "%Y-%m-%d %H:%M:%S")
                    display_date = dt_object.strftime("%d/%m/%Y")
                    display_time = dt_object.strftime("%H:%M") # Formata apenas hora e minuto
                    
                except ValueError:
                    logger.warning(f"Erro ao formatar timestamp da timeline para status '{status}': {timestamp_info}")
                    display_date = "N/A"
                    display_time = "N/A"


            st.markdown(f"""
            <div class="status-point-wrapper">
                <div class="status-circle {circle_class}">
                    <i class="{icon_class}"></i>
                </div>
                <div class="status-label">{status}</div>
                <div class="status-date">{display_date}</div>
                <div class="status-date">{display_time}</div>
            </div>
            """, unsafe_allow_html=True)

    # A linha do tempo é uma superposição, então a renderizamos separadamente
    # e ajustamos seu `top` para que fique alinhada com o centro dos círculos
    st.markdown(f"""
    <div class="status-timeline-container">
        <div class="status-timeline-line"></div>
    </div>
    """, unsafe_allow_html=True)

    # === FIM DO AJUSTE DA TIMELINE, CORES E ALINHAMENTO ===

    st.markdown("---")
    
    # Início do expander para o Histórico do Processo
    with st.expander("Ver Histórico Detalhado do Processo"):
        # Use o ID correto para buscar o histórico (Processo_Novo para Firestore, ID int para SQLite)
        history_fetch_id = process_data.get('id')
        if db_manager._USE_FIRESTORE_AS_PRIMARY and not isinstance(history_fetch_id, str):
            # Se Firestore é primário, mas o ID é int (do SQLite, por exemplo),
            # precisamos da string 'Processo_Novo' do processo para buscar no Firestore.
            if process_data and 'Processo_Novo' in process_data:
                history_fetch_id = process_data['Processo_Novo']

        process_history = db_manager.obter_historico_processo(history_fetch_id) 
        logger.debug(f"Raw process_history fetched for detailed history: {process_history}") # Log para depuração

        if process_history: # Verifica se há qualquer histórico para processar
            # Agrupar por data para exibição em blocos
            history_by_date = {}
            for entry in process_history:
                logger.debug(f"Processando entrada de histórico para visualização detalhada: {entry}") # Log para depuração
                # Tratar valores que podem vir como None ou NaN
                timestamp_str = str(entry.get("timestamp")) if entry.get("timestamp") is not None else ""
                
                if timestamp_str.lower() == 'nan' or not timestamp_str.strip():
                    date_part = "Data Desconhecida"
                    time_part = ""
                else:
                    try:
                        # Garantir que a string esteja formatada corretamente antes do parse
                        # Removendo milissegundos se presentes (ex: '2024-01-01 12:30:45.123')
                        dt_object = datetime.strptime(timestamp_str.split('.')[0], "%Y-%m-%d %H:%M:%S")
                        date_part = dt_object.strftime("%d/%m/%Y")
                        time_part = dt_object.strftime("%H:%M:%S")
                    except ValueError:
                        date_part = "Data Inválida"
                        time_part = timestamp_str # Mantém o original para depuração se o parse falhar
                        logger.warning(f"Erro ao parsear timestamp do histórico detalhado: {timestamp_str}. Entrada: {entry}")

                if date_part not in history_by_date:
                    history_by_date[date_part] = []
                
                history_by_date[date_part].append({
                    "campo_alterado": entry.get("campo_alterado", "N/A"),
                    "valor_antigo": entry.get("valor_antigo", "Vazio"),
                    "valor_novo": entry.get("valor_novo", "Vazio"),
                    "timestamp_full": timestamp_str, # Mantém o timestamp completo para ordenação
                    "timestamp": time_part, # Agora é só a parte da hora
                    "usuario": entry.get("usuario", "Desconhecido")
                })
        
            # Ordenar as datas para exibição (mais recente primeiro)
            # Se 'Data Desconhecida' ou 'Data Inválida' for um valor, colocá-lo no final
            sorted_dates = sorted(history_by_date.keys(), key=lambda x: datetime.strptime(x, "%d/%m/%Y") if x not in ["Data Desconhecida", "Data Inválida"] else datetime.min, reverse=True)

            for date_key in sorted_dates:
                st.markdown(f"**Data: {date_key}**")
                
                # Ordenar os registros dentro de cada data do mais recente para o mais antigo
                sorted_records_for_date = sorted(
                    history_by_date[date_key], 
                    key=lambda x: datetime.strptime(str(x.get('timestamp_full')).split('.')[0], "%Y-%m-%d %H:%M:%S") if x.get('timestamp_full') and str(x.get('timestamp_full')).lower() != 'nan' else datetime.min, 
                    reverse=True
                )

                for record in sorted_records_for_date:
                    # Adicionar CSS para um bloco de histórico mais visual
                    st.markdown(f"""
                    <div style="
                        border-left: 3px solid #007bff; 
                        padding: 10px; 
                        margin-bottom: 5px; 
                        background-color: #f0f2f6; 
                        border-radius: 5px;
                        color: #333; /* Cor do texto para melhor contraste */
                    ">
                        <small style="color: #555;"><strong>Hora:</strong> {record['timestamp']} | <strong>Usuário:</strong> {record['usuario']}</small><br>
                        <small style="color: #333;"><strong>Campo:</strong> {record['campo_alterado']}</small><br>
                        <small style="color: #333;"><strong>De:</strong> {record['valor_antigo']}</small><br>
                        <small style="color: #333;"><strong>Para:</strong> {record['valor_novo']}</small>
                    </div>
                    """, unsafe_allow_html=True)
                st.markdown("---") # Separador entre os dias
        else: # Este else está no mesmo nível do 'if process_history:'
             st.info("Nenhum histórico de alterações para este processo.")
    # Fim do expander
    st.markdown("---")

    # Botão de retorno
    if st.button("Voltar para Follow-up Importação"):
        return_callback()
        st.rerun()

    st.markdown("---")
    st.write("Esta tela apresenta uma visão detalhada do processo selecionado.")
