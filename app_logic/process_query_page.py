import streamlit as st # Importa a biblioteca Streamlit para construir a interface web.
import pandas as pd # Importa Pandas para manipmanipulação e exibição de dados em formato de DataFrame.
from datetime import datetime # Importa datetime para trabalhar com datas e horas.
import logging # Importa logging para registrar eventos e depurar o código.
import os # Importa os para interagir com o sistema operacional, como caminhos de arquivos.
import base64 # Importa base64 para codificar/decodificar dados (usado para imagens de fundo).
from typing import Optional, Any, Dict, List, Union, Tuple # Importa tipos para type hinting, melhorando a legibilidade e robustez.
import io # Importa io para manipulação de streams de I/O (usado para PDFs).

# Importa o módulo db_manager para interagir com o banco de dados de processos.
# A importação deve refletir a estrutura do seu projeto. Se db_manager.py
# estiver na mesma pasta 'app_logic' que db_utils.py, a importação seria:
# from . import followup_db_manager as db_manager
# Mantenho a importação atual 'import followup_db_manager as db_manager'
# assumindo que o sistema de módulos Python consegue encontrá-lo,
# ou que ele está no mesmo nível que a pasta 'app_logic'.
import followup_db_manager as db_manager 

# IMPORTAÇÃO CENTRALIZADA DE db_utils.
# Tenta importar todas as funções e a classe db_utils diretamente de app_logic.db_utils.
# NÃO há fallback para MockDbUtils. Se a importação ou a inicialização do Firestore falhar,
# a aplicação irá lançar um erro.
# Configura o logger para esta página
logger = logging.getLogger(__name__) # Obtém uma instância de logger para este módulo.
logger.setLevel(logging.INFO) # Define o nível de log para INFO, registrando informações importantes.

try:
    # Importa db_utils como um todo, permitindo acessar suas funções como db_utils.funcao()
    from app_logic import db_utils 
    # Verifica se o Firestore client foi inicializado com sucesso em db_utils.
    # Se db_firestore for None, indica que a conexão falhou.
    if db_utils.db_firestore is None:
        raise ImportError("Erro: O cliente Firestore em db_utils não foi inicializado. Verifique suas credenciais.")
    logger.info("process_query_page.py: db_utils real importado com sucesso e Firestore client está pronto.")
except ImportError as e:
    logger.critical(f"process_query_page.py: Erro CRÍTICO: Não foi possível importar 'app_logic.db_utils' ou o Firestore client falhou: {e}. A aplicação não pode continuar sem uma conexão válida com o banco de dados. Por favor, verifique a estrutura do seu projeto e as credenciais do Firestore em 'secrets.toml'.", exc_info=True)
    st.error("Erro crítico na conexão com o banco de dados. Por favor, contate o suporte e verifique as configurações do Firestore.")
    st.stop() # Interrompe a execução do Streamlit se o DB não puder ser conectado.
except Exception as e:
    logger.critical(f"process_query_page.py: Erro INESPERADO ao importar ou inicializar 'db_utils': {e}. A aplicação não pode continuar sem uma conexão válida com o banco de dados.", exc_info=True)
    st.error("Erro inesperado ao iniciar a conexão com o banco de dados. Por favor, contate o suporte.")
    st.stop() # Interrompe a execução do Streamlit se o DB não puder ser conectado.

# NOVA IMPORTAÇÃO PARA WEASYPRINT
from weasyprint import HTML, CSS # Importa as bibliotecas para gerar PDFs a partir de HTML/CSS.




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

def _format_di_number(di_number):
    """Formata o número da DI para o padrão **/*******-*."""
    if di_number and isinstance(di_number, str) and len(di_number) == 10:
        return f"{di_number[0:2]}/{di_number[2:9]}-{di_number[9]}"
    return di_number


# FUNÇÃO DE GERAÇÃO DE PDF AGORA USA WEASYPRINT
def _generate_process_summary_pdf(process_data: Dict[str, Any], process_history: List[Dict[str, Any]]) -> Tuple[io.BytesIO, str]:
    """
    Gera um PDF com as informações gerais do processo e a linha do tempo de status visual
    utilizando WeasyPrint para renderização HTML/CSS.
    """
    file_name = f"Resumo_Processo_{process_data.get('Processo_Novo', 'N_A')}.pdf"
    pdf_buffer = io.BytesIO()

    # Dicionário de ícones para cada status (usando Font Awesome para o HTML)
    status_icons_html = {
        "Processo Criado": "fa-solid fa-pen",
        "Em produção": "fa-solid fa-industry",
        "Verificando": "fa-solid fa-magnifying-glass",
        "Pré Embarque": "fa-solid fa-box",
        "Limbo Saldo": "fa-solid fa-hourglass-half",
        "Limbo Consolidado": "fa-solid fa-boxes-packing",
        "Embarcado": "fa-solid fa-ship",
        "Chegada Recinto": "fa-solid fa-warehouse",
        "Registrado": "fa-solid fa-clipboard-check",
        "Liberado": "fa-solid fa-unlock",
        "Agendado": "fa-solid fa-calendar-check",
        "Chegada Pichau": "fa-solid fa-truck-ramp-box",
        "Encerrado": "fa-solid fa-circle-check"
    }

    # Define a lista completa ordenada de todos os status possíveis
    all_possible_statuses = [
        "Processo Criado", "Em produção", "Verificando", "Pré Embarque", "Limbo Saldo",
        "Limbo Consolidado", "Embarcado", "Chegada Recinto", "Registrado", "Liberado",
        "Agendado", "Chegada Pichau", "Encerrado"
    ]

    # Define o subconjunto de status que terão círculos visíveis na linha do tempo
    displayed_timeline_statuses_for_pdf = [
        "Processo Criado", "Em produção", "Pré Embarque", "Embarcado",
        "Chegada Recinto", "Registrado", "Chegada Pichau", "Encerrado"
    ]

    status_history_map = {}
    assigned_statuses = set()

    # Primeira passada: coletar todas as datas de status do histórico real
    if process_history:
        for entry in sorted(process_history, key=lambda x: x.get('timestamp', ''), reverse=False):
            timestamp_to_use = entry.get('status_change_timestamp') if entry.get('status_change_timestamp') else entry['timestamp']
            
            def parse_timestamp_safely(ts_str):
                if not ts_str or str(ts_str).lower() == 'nan':
                    return datetime.min
                try:
                    return datetime.strptime(str(ts_str).split('.')[0], "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    return datetime.min

            parsed_timestamp = parse_timestamp_safely(timestamp_to_use)

            status_key = None
            if entry.get('campo_alterado') == 'Status_Geral' and entry.get('valor_novo'):
                status_key = entry.get('valor_novo')
            elif entry.get('campo_alterado') == 'Processo Criado':
                status_key = "Processo Criado"
            
            if status_key and status_key not in assigned_statuses:
                status_history_map[status_key] = {
                    'timestamp': timestamp_to_use,
                    'usuario': entry['usuario']
                }
                assigned_statuses.add(status_key)
    
    # Encontra o timestamp mais antigo de todo o histórico do processo
    first_known_timestamp_overall = None
    if process_history:
        for entry in process_history:
            ts_str = entry.get('status_change_timestamp') or entry.get('timestamp')
            if ts_str and str(ts_str).lower() != 'nan':
                try:
                    current_ts = datetime.strptime(str(ts_str).split('.')[0], "%Y-%m-%d %H:%M:%S")
                    if first_known_timestamp_overall is None or current_ts < first_known_timestamp_overall:
                        first_known_timestamp_overall = current_ts
                except ValueError:
                    pass

    # Segunda passada: preencher status ausentes que precedem ou são iguais ao status atual
    current_status = process_data.get('Status_Geral', 'N/A')
    current_status_overall_index = -1
    try:
        current_status_overall_index = all_possible_statuses.index(current_status)
    except ValueError:
        logger.info(f"O status atual '{current_status}' não foi encontrado na lista completa de status possíveis.")

    if first_known_timestamp_overall:
        for status in all_possible_statuses:
            # Preenche o status se ele precede ou é igual ao status atual, E não tem um timestamp já definido
            if all_possible_statuses.index(status) <= current_status_overall_index and \
               status not in status_history_map:
                status_history_map[status] = {
                    'timestamp': first_known_timestamp_overall.strftime("%Y-%m-%d %H:%M:%S"),
                    'usuario': 'Sistema (Data de Criação/Primeiro Registro)'
                }


    # CONSTRUÇÃO DO HTML PARA O PDF
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Resumo do Processo: {process_data.get('Processo_Novo', 'N_A')}</title>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
        <style>
            body {{
                font-family: 'Helvetica', 'Arial', sans-serif;
                margin: 0.5in; /* Margens padrão para PDF */
                color: #333;
            }}
            h1 {{
                font-size: 16pt;
                text-align: center;
                margin-bottom: 14pt;
                color: #000;
            }}
            h2 {{
                font-size: 12pt;
                margin-top: 20pt;
                margin-bottom: 8pt;
                color: #000;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-bottom: 20pt;
            }}
            th, td {{
                border: 1px solid #ddd;
                padding: 8pt;
                text-align: left;
                font-size: 10pt;
            }}
            th {{
                background-color: #555; /* Cor de fundo para cabeçalhos de tabela */
                color: white;
                font-weight: bold;
            }}
            td {{
                background-color: #f8f8f8; /* Cor de fundo para células de dados */
            }}

            /* Estilos da Timeline */
            .status-timeline-container {{
                display: flex;
                justify-content: space-between;
                align-items: flex-start; /* Alinha os itens ao topo */
                margin-top: 20pt;
                position: relative;
                padding-bottom: 40pt; /* Espaço para o texto abaixo */
            }}
            /* REMOVIDO: .status-timeline-line para o PDF, pois será segmentado */
            .status-point-wrapper {{
                display: flex;
                flex-direction: column;
                align-items: center;
                text-align: center;
                position: relative;
                z-index: 1;
                flex: 1;
                margin: 0 5pt; /* Adiciona um pequeno espaçamento horizontal */
            }}
            .status-circle {{
                width: 40pt;
                height: 40pt;
                border-radius: 50%;
                background-color: #888;
                display: flex;
                align-items: center;
                justify-content: center;
                color: white;
                font-size: 14pt; /* Ajustado para ícones */
                font-weight: bold;
                border: 2pt solid #888;
                box-shadow: 0 0 5pt rgba(0,0,0,0.5);
                margin-bottom: 5pt;
            }}
            .status-circle.completed {{
                background-color: #007bff;
                border-color: #007bff;
            }}
            .status-circle i {{
                color: white;
                font-size: 14pt;
            }}
            .status-label {{
                font-size: 8pt;
                color: #333;
                white-space: normal;
                word-break: break-word;
                margin-top: 5pt;
            }}
            .status-date {{
                font-size: 7pt;
                color: #555;
                white-space: nowrap;
            }}
            .timeline-segment {{ /* Novo estilo para os segmentos de linha */
                position: absolute;
                top: 25pt; /* Alinha no meio do círculo */
                height: 2pt;
                background-color: #888; /* Cor padrão cinza */
                z-index: 0;
                width: calc(100% - 80pt); /* Largura para preencher o espaço entre os círculos */
                left: 40pt; /* Deslocamento para começar após o círculo anterior */
            }}
            .timeline-segment.completed-segment {{
                background-color: #007bff; /* Azul para segmentos concluídos */
            }}
        </style>
    </head>
    <body>
        <h1>Resumo do Processo: {process_data.get('Processo_Novo', 'N_A')}</h1>

        <h2>Dados Gerais do Processo:</h2>
        <table>
            <tr><th>Processo Novo:</th><td>{str(process_data.get('Processo_Novo', 'N/A'))}</td></tr>
            <tr><th>Status Geral:</th><td>{str(process_data.get('Status_Geral', 'N/A'))}</td></tr>
            <tr><th>Fornecedor:</th><td>{str(process_data.get('Fornecedor', 'N/A'))}</td></tr>
            <tr><th>Nº Invoice:</th><td>{str(process_data.get('N_Invoice', 'N/A'))}</td></tr>
            <tr><th>Quantidade:</th><td>{str(process_data.get('Quantidade', 0))}</td></tr>
            <tr><th>Valor (USD):</th><td>{str(_format_usd_display(process_data.get('Valor_USD', 0.0)))}</td></tr>
            <tr><th>Estimativa Impostos (BRL):</th><td>{str(_format_currency_display(process_data.get('Estimativa_Impostos_Total', 0.0)))}</td></tr>
            <tr><th>INCOTERM:</th><td>{str(process_data.get('INCOTERM', 'N/A'))}</td></tr>
            <tr><th>Comprador:</th><td>{str(process_data.get('Comprador', 'N/A'))}</td></tr>
            <tr><th>Modal:</th><td>{str(process_data.get('Modal', 'N/A'))}</td></tr>
        </table>

        <h2>Progresso do Processo:</h2>
        <div class="status-timeline-container">
            <!-- Segmentos de linha e pontos de status são gerados dinamicamente -->
    """
    
    # Adiciona os pontos da timeline ao HTML
    for i, status in enumerate(displayed_timeline_statuses_for_pdf):
        circle_class = ""
        # Um status é 'concluído' se seu índice na lista completa for <= ao índice do status atual na lista completa
        if current_status_overall_index != -1 and \
           all_possible_statuses.index(status) <= current_status_overall_index:
            circle_class = "completed"
        
        icon_class = status_icons_html.get(status, "fa-solid fa-circle")
        
        timestamp_info = status_history_map.get(status, {}).get('timestamp')
        display_date = ''
        display_time = ''
        if timestamp_info:
            try:
                dt_object = datetime.strptime(str(timestamp_info).split('.')[0], "%Y-%m-%d %H:%M:%S")
                display_date = dt_object.strftime("%d/%m/%Y")
                display_time = dt_object.strftime("%H:%M")
            except ValueError:
                display_date = "N/A"
                display_time = "N/A"

        # Adiciona o segmento de linha antes de cada ponto, exceto o primeiro
        if i > 0:
            # Verifica se o segmento atual deve ser concluído (se o status anterior ou atual estiver completo)
            prev_status = displayed_timeline_statuses_for_pdf[i-1]
            segment_completed_class = ""
            if (current_status_overall_index != -1 and \
                all_possible_statuses.index(prev_status) <= current_status_overall_index) and \
               (current_status_overall_index != -1 and \
                all_possible_statuses.index(status) <= current_status_overall_index):
                segment_completed_class = "completed-segment"

            html_content += f"""
            <div class="timeline-segment {segment_completed_class}"></div>
            """

        html_content += f"""
            <div class="status-point-wrapper">
                <div class="status-circle {circle_class}">
                    <i class="{icon_class}"></i>
                </div>
                <div class="status-label">{status}</div>
                <div class="status-date">{display_date}</div>
                <div class="status-date">{display_time}</div>
            </div>
        """
    html_content += """
        </div>
    </body>
    </html>
    """

    try:
        HTML(string=html_content).write_pdf(pdf_buffer)
        pdf_buffer.seek(0)
        logger.info(f"PDF '{file_name}' gerado com sucesso usando WeasyPrint.")
        return pdf_buffer, file_name
    except Exception as e:
        logger.error(f"Erro ao gerar PDF de resumo do processo com WeasyPrint: {e}", exc_info=True)
        st.error(f"Erro interno ao gerar o PDF com WeasyPrint. Detalhes: {e}")
        return None, None


# Funções para manipulação de upload de arquivos e XML
# A função _handle_general_document_upload e sua chamada foram removidas
# já que a seção de upload de documentos gerais foi removida.
def _handle_xml_di_upload(process_data_in_session_state: Dict[str, Any], uploaded_file: Any, unique_key_prefix: str):
    """Lida com o upload de um XML da DI, parseia, salva e tenta vincular ao processo."""
    if uploaded_file is not None:
        # Gerar um hash único para o arquivo XML
        current_file_hash = uploaded_file.name + str(uploaded_file.size) + uploaded_file.type
        
        if st.session_state.get(f'{unique_key_prefix}_last_uploaded_xml_di_hash') != current_file_hash:
            try:
                xml_content = uploaded_file.getvalue().decode("utf-8")
                di_data_parsed, itens_data_parsed_raw = db_utils.parse_xml_data_to_dict(xml_content)
                itens_data_parsed = itens_data_parsed_raw if itens_data_parsed_raw is not None else []

                if di_data_parsed:
                    # Limpa a referência do processo antes de usá-la
                    processo_novo_ref = db_utils._clean_reference_string(process_data_in_session_state.get('Processo_Novo'))
                    if not processo_novo_ref:
                        st.error("Não foi possível vincular a DI: O processo atual não tem uma referência válida ('Processo Novo').")
                        st.session_state[f'{unique_key_prefix}_last_uploaded_xml_di_hash'] = None
                        return

                    di_numero_from_xml = di_data_parsed.get('numero_di')
                    if not di_numero_from_xml:
                        st.error("Não foi possível extrair o número da DI do arquivo XML. Verifique o formato do arquivo.")
                        st.session_state[f'{unique_key_prefix}_last_uploaded_xml_di_hash'] = None
                        return
                    
                    # Tentar buscar a DI existente pelo número da DI
                    existing_di_by_num = db_utils.get_declaracao_by_id(di_numero_from_xml)
                    if existing_di_by_num:
                        st.warning(f"Uma DI com o número '{_format_di_number(di_numero_from_xml)}' já existe no banco de dados. Importação não realizada.")
                        st.session_state[f'{unique_key_prefix}_last_uploaded_xml_di_hash'] = None
                        return

                    # Vincular a DI ao processo atual pela referência (informacao_complementar)
                    di_data_parsed['informacao_complementar'] = processo_novo_ref
                    
                    # Salvar a DI e seus itens
                    # save_parsed_di_data agora também tenta atualizar o link no processo
                    success = db_utils.save_parsed_di_data(di_data_parsed, itens_data_parsed)

                    if success:
                        st.success(f"XML da DI '{_format_di_number(di_numero_from_xml)}' importado e vinculado ao processo '{processo_novo_ref}' com sucesso!")
                        
                        # Atualizar DI_ID_Vinculada no processo atual
                        # É necessário buscar o ID real da DI recém-salva.
                        # Já foi feito dentro de save_parsed_di_data, mas para garantir a UI:
                        newly_saved_di_data = db_utils.get_declaracao_by_referencia(processo_novo_ref) # Ou por numero_di
                        if newly_saved_di_data and 'id' in newly_saved_di_data:
                            process_data_in_session_state['DI_ID_Vinculada'] = newly_saved_di_data['id']
                            # O processo já foi atualizado por save_parsed_di_data,
                            # então não precisamos chamar _save_process_changes aqui novamente.
                            st.session_state[f'{unique_key_prefix}_last_uploaded_xml_di_hash'] = current_file_hash # Marca como processado
                            st.rerun() # Recarregar para exibir o ID da DI vinculada e o botão de salvar processo.
                        else:
                            st.warning("DI importada, mas não foi possível obter o ID para vincular automaticamente ao processo.")
                            st.session_state[f'{unique_key_prefix}_last_uploaded_xml_di_hash'] = None # Não marca como totalmente processado se não vinculou
                    else:
                        st.error("Falha ao salvar a DI importada no banco de dados.")
                        st.session_state[f'{unique_key_prefix}_last_uploaded_xml_di_hash'] = None
                else:
                    st.error("Não foi possível extrair dados válidos do arquivo XML da DI. Verifique o formato do arquivo.")
                    st.session_state[f'{unique_key_prefix}_last_uploaded_xml_di_hash'] = None
            except Exception as e:
                st.error(f"Erro ao processar o arquivo XML da DI: {e}")
                logger.error(f"Erro ao processar o arquivo XML da DI: {e}", exc_info=True)
                st.session_state[f'{unique_key_prefix}_last_uploaded_xml_di_hash'] = None

# Função para salvar o processo (incluindo documentos/DI vinculada)
def _save_process_changes(process_data_dict: Dict[str, Any]):
    """Salva as alterações do processo no banco de dados."""
    if not process_data_dict.get('id') and not db_manager._USE_FIRESTORE_AS_PRIMARY:
        st.error("Não é possível salvar um processo sem ID. Este formulário é para consulta.")
        return False
    
    # Prepara os dados para atualização no DB
    data_to_update = process_data_dict.copy()
    user_info = st.session_state.get('user_info', {'username': 'Desconhecido'})
    current_username = user_info.get('username', 'Desconhecido')
    data_to_update['Ultima_Alteracao_Por'] = current_username
    data_to_update['Ultima_Alteracao_Em'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    process_id_to_update = data_to_update.get('id')
    if db_manager._USE_FIRESTORE_AS_PRIMARY:
        process_id_to_update = data_to_update.get('Processo_Novo') # Firestore usa Processo_Novo como ID

    if not process_id_to_update:
        st.error("ID do processo ou Referência não encontrada para salvar as alterações.")
        return False

    success = db_manager.atualizar_processo(process_id_to_update, data_to_update)
    if success:
        # Limpar caches relacionados a este processo após o salvamento
        db_manager.obter_processo_por_id.clear()
        db_manager.obter_processo_by_processo_novo.clear()
        db_manager.obter_todos_processos.clear()
        db_manager.obter_processos_filtrados.clear()
        return True
    else:
        st.error("Falha ao salvar as alterações no processo.")
        return False


def show_process_query_page(process_identifier: Any, return_callback: callable):
    """
    Exibe a tela de consulta de um processo específico.
    process_identifier: ID (int) ou Processo_Novo (str) do processo a ser consultado.
    return_callback: Função para chamar para retornar à página anterior.
    """
    background_image_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'assets', 'logo_navio_atracado.png')
    set_background_image(background_image_path)

    # Inicializa process_data no session_state para carregar automaticamente
    if 'current_process_data' not in st.session_state:
        st.session_state.current_process_data = None
    if 'current_process_history' not in st.session_state:
        st.session_state.current_process_history = []
    if 'current_declaracao_di_data' not in st.session_state:
        st.session_state.current_declaracao_di_data = None

    process_data = None
    process_history = []
    declaracao_di_data = None

    # Gerar uma chave única para os widgets de upload e seus hashes
    # Baseado no ID do processo, para que o estado persista se o processo mudar
    process_key_for_state = f"query_page_{process_identifier}"
    st.session_state.setdefault(f'{process_key_for_state}_last_uploaded_general_doc_hash', None)
    st.session_state.setdefault(f'{process_key_for_state}_last_uploaded_xml_di_hash', None)


    # Carrega os dados automaticamente ao entrar na página
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
        st.session_state.current_process_data = process_data # Armazena no session_state

        history_id = process_data.get('id')
        if db_manager._USE_FIRESTORE_AS_PRIMARY:
            history_id = process_data.get('Processo_Novo')
            
        process_history = db_manager.obter_historico_processo(history_id)
        st.session_state.current_process_history = process_history # Armazena no session_state

        # Lógica de busca da DI associada:
        # Primeiro, tentar pelo ID vinculado diretamente no processo (se existir)
        # Segundo, se não encontrar ou o ID não existir, tentar pela referência do processo (Processo_Novo)
        
        declaracao_di_data_found = None
        linked_di_id = process_data.get('DI_ID_Vinculada')
        
        # Garante que a referência do processo seja limpa antes de qualquer busca.
        processo_novo_ref_limpo = db_utils._clean_reference_string(process_data.get('Processo_Novo'))

        # Tenta buscar pelo ID vinculado primeiro
        if linked_di_id:
            declaracao_di_data_found = db_utils.get_declaracao_by_id(linked_di_id)
        
        # Se não encontrou pelo ID vinculado ou não havia ID vinculado, tenta pela referência do processo (limpa)
        if not declaracao_di_data_found and processo_novo_ref_limpo:
            declaracao_di_data_found = db_utils.get_declaracao_by_referencia(processo_novo_ref_limpo)
            # Se encontrou pela referência e não tinha ID vinculado antes, atualiza o processo
            if declaracao_di_data_found and not linked_di_id:
                process_data['DI_ID_Vinculada'] = declaracao_di_data_found['id']
                if _save_process_changes(process_data):
                    logger.info(f"DI vinculada automaticamente ao processo por referência: {_format_di_number(declaracao_di_data_found.get('numero_di'))} e processo salvo.")
                    # st.info(f"DI vinculada automaticamente ao processo por referência e processo salvo.") # Descomentar se quiser mensagem na UI
                else:
                    logger.warning("DI vinculada automaticamente, mas falha ao salvar o processo automaticamente.")
                    # st.warning("DI vinculada automaticamente, mas falha ao salvar o processo automaticamente.") # Descomentar se quiser mensagem na UI
        
        st.session_state.current_declaracao_di_data = declaracao_di_data_found


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

    st.markdown("#### Histórico de Status")
    
    # Define a lista completa ordenada de todos os status possíveis
    all_possible_statuses = [
        "Processo Criado", "Em produção", "Verificando", "Pré Embarque", "Limbo Saldo",
        "Limbo Consolidado", "Embarcado", "Chegada Recinto", "Registrado", "Liberado",
        "Agendado", "Chegada Pichau", "Encerrado"
    ]

    # Define o subconjunto de status que terão círculos visíveis na linha do tempo
    displayed_timeline_statuses = [
        "Processo Criado", "Em produção", "Pré Embarque", "Embarcado",
        "Chegada Recinto", "Registrado", "Chegada Pichau", "Encerrado"
    ]

    # Cria um dicionário de histórico de status para fácil acesso (Status -> {timestamp, usuario})
    status_history_map = {}
    assigned_statuses = set()

    # Primeira passada: coletar todas as datas de status do histórico real
    if process_history:
        for entry in sorted(process_history, key=lambda x: x.get('timestamp', ''), reverse=False):
            timestamp_to_use = entry.get('status_change_timestamp') if entry.get('status_change_timestamp') else entry['timestamp']
            
            # Helper function to parse timestamp safely
            def parse_timestamp_safely(ts_str):
                if not ts_str or str(ts_str).lower() == 'nan':
                    return datetime.min # Usa uma data muito antiga para timestamps inválidos/ausentes
                try:
                    # Remove milissegundos se presentes (ex: '2024-01-01 12:30:45.123')
                    return datetime.strptime(str(ts_str).split('.')[0], "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    logger.warning(f"Não foi possível analisar o timestamp '{ts_str}'. Usando datetime.min para comparação.")
                    return datetime.min

            parsed_timestamp = parse_timestamp_safely(timestamp_to_use)

            status_key = None
            if entry.get('campo_alterado') == 'Status_Geral' and entry.get('valor_novo'):
                status_key = entry.get('valor_novo')
            elif entry.get('campo_alterado') == 'Processo Criado': # Captura a data de criação do processo
                status_key = "Processo Criado"
            
            # Registra apenas o primeiro timestamp para cada status encontrado
            if status_key and status_key not in assigned_statuses:
                status_history_map[status_key] = {
                    'timestamp': timestamp_to_use,
                    'usuario': entry['usuario']
                }
                assigned_statuses.add(status_key)

    # Encontra o timestamp mais antigo de todo o histórico do processo
    first_known_timestamp_overall = None
    if process_history:
        for entry in process_history:
            ts_str = entry.get('status_change_timestamp') or entry.get('timestamp')
            if ts_str and str(ts_str).lower() != 'nan':
                try:
                    current_ts = datetime.strptime(str(ts_str).split('.')[0], "%Y-%m-%d %H:%M:%S")
                    if first_known_timestamp_overall is None or current_ts < first_known_timestamp_overall:
                        first_known_timestamp_overall = current_ts
                except ValueError:
                    pass

    # Obtém o índice do status atual na lista completa de todos os status possíveis
    current_status = process_data.get('Status_Geral', 'N/A')
    current_status_overall_index = -1
    try:
        current_status_overall_index = all_possible_statuses.index(current_status)
    except ValueError:
        logger.info(f"O status atual '{current_status}' não foi encontrado na lista completa de status possíveis.")

    # Segunda passada: preencher status ausentes que precedem ou são iguais ao status atual
    if first_known_timestamp_overall:
        for status in all_possible_statuses:
            # Preenche o status se ele precede ou é igual ao status atual, E não tem um timestamp já definido
            if all_possible_statuses.index(status) <= current_status_overall_index and \
               status not in status_history_map:
                status_history_map[status] = {
                    'timestamp': first_known_timestamp_overall.strftime("%Y-%m-%d %H:%M:%S"),
                    'usuario': 'Sistema (Data de Criação/Primeiro Registro)'
                }
    
    # === INÍCIO DO AJUSTE DA TIMELINE, CORES E ALINHAMENTO ===
    # CSS para o estilo da timeline
    st.markdown("""
    <style>
    /* Importa Font Awesome */
    @import url('https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css');

    .status-timeline-overall-container { /* Nova classe de container para todo o bloco da timeline */
        position: relative;
        width: 100%;
        margin-top: 20px;
        margin-bottom: 20px;
        padding-bottom: 20px; /* Espaço para o texto abaixo da linha */
        display: flex; /* Usa flexbox para o layout horizontal dos pontos de status */
        justify-content: space-between;
        align-items: flex-start;
    }

    .status-timeline-line { /* Esta classe não será mais usada para a linha principal */
        display: none; /* Esconde a linha contínua */
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
    .timeline-segment { /* Estilo para os segmentos de linha entre os círculos */
        position: relative; /* Ajustado para relative para posicionar no fluxo flexbox */
        height: 2px;
        background-color: #888; /* Cor padrão cinza */
        z-index: 0;
        flex-grow: 1; /* Faz com que o segmento preencha o espaço disponível */
        margin-top: 20px; /* Alinha verticalmente com o centro dos círculos */
    }
    .timeline-segment.completed-segment {
        background-color: #007bff; /* Azul para segmentos concluídos */
    }
    </style>
    """, unsafe_allow_html=True)

    current_status = process_data.get('Status_Geral', 'N/A')

    # Dicionário de ícones para cada status (para a UI do Streamlit)
    status_icons = {
        "Processo Criado": "fa-solid fa-pen",
        "Em produção": "fa-solid fa-industry",
        "Verificando": "fa-solid fa-magnifying-glass",
        "Pré Embarque": "fa-solid fa-box",
        "Limbo Saldo": "fa-solid fa-hourglass-half",
        "Limbo Consolidado": "fa-solid fa-boxes-packing",
        "Embarcado": "fa-solid fa-ship",
        "Chegada Recinto": "fa-solid fa-warehouse",
        "Registrado": "fa-solid fa-clipboard-check",
        "Liberado": "fa-solid fa-unlock",
        "Agendado": "fa-solid fa-calendar-check",
        "Chegada Pichau": "fa-solid fa-truck-ramp-box",
        "Encerrado": "fa-solid fa-circle-check"
    }

    # Obtém o índice do status atual na lista completa de todos os status possíveis
    current_status_overall_index = -1
    try:
        current_status_overall_index = all_possible_statuses.index(current_status)
    except ValueError:
        logger.info(f"O status atual '{current_status}' não foi encontrado na lista completa de status possíveis.")

    # Constrói o HTML da timeline inteira dentro de um único bloco de markdown
    timeline_elements = []
    timeline_elements.append('<div class="status-timeline-overall-container">')

    for i, status in enumerate(displayed_timeline_statuses):
        # Adiciona o segmento de linha *antes* de cada ponto, exceto o primeiro
        if i > 0:
            # Verifica se o segmento atual deve ser concluído
            # Ele é concluído se o status ANTERIOR a este ponto (na lista all_possible_statuses)
            # e o status ATUAL deste ponto já foram alcançados.
            prev_displayed_status = displayed_timeline_statuses[i-1]
            
            is_prev_status_completed = (current_status_overall_index != -1 and \
                                       all_possible_statuses.index(prev_displayed_status) <= current_status_overall_index)
            is_current_status_completed = (current_status_overall_index != -1 and \
                                          all_possible_statuses.index(status) <= current_status_overall_index)
            
            segment_completed_class = "completed-segment" if is_prev_status_completed and is_current_status_completed else ""
            timeline_elements.append(f"""<div class="timeline-segment {segment_completed_class}"></div>""")

        circle_class = ""
        if current_status_overall_index != -1 and \
           all_possible_statuses.index(status) <= current_status_overall_index:
            circle_class = "completed"
        
        icon_class = status_icons.get(status, "fa-solid fa-circle") 

        timestamp_info = status_history_map.get(status, {}).get('timestamp')
        
        display_date = ''
        display_time = ''
        if timestamp_info:
            try:
                dt_object = datetime.strptime(str(timestamp_info).split('.')[0], "%Y-%m-%d %H:%M:%S")
                display_date = dt_object.strftime("%d/%m/%Y")
                display_time = dt_object.strftime("%H:%M")
                
            except ValueError:
                logger.warning(f"Erro ao formatar timestamp da timeline para status '{status}': {timestamp_info}")
                display_date = "N/A"
                display_time = "N/A"

        # Constrói o HTML para cada ponto de status
        timeline_elements.append(f"""<div class="status-point-wrapper"><div class="status-circle {circle_class}"><i class="{icon_class}"></i></div><div class="status-label">{status}</div><div class="status-date">{display_date}</div><div class="status-date">{display_time}</div></div>""")
    
    timeline_elements.append('</div>') # Fecha status-timeline-overall-container
    timeline_html = "".join(timeline_elements) # Junta todas as partes em uma única string

    st.markdown(timeline_html, unsafe_allow_html=True) # Renderiza o HTML da timeline

    # === FIM DO AJUSTE DA TIMELINE, CORES E ALINHAMENTO ===

    st.markdown("---")
    
    st.markdown("---")

    # Botão para Gerar PDF do Resumo do Processo
    if st.button("Gerar PDF Resumo do Processo", key="btn_generate_summary_pdf"):
        pdf_buffer, pdf_filename = _generate_process_summary_pdf(process_data, process_history)
        if pdf_buffer:
            st.download_button(
                label="Baixar Resumo do Processo (PDF)",
                data=pdf_buffer,
                file_name=pdf_filename,
                mime="application/pdf",
                key="download_summary_pdf"
            )
        else:
            st.error("Erro ao gerar o PDF de resumo do processo. Por favor, verifique os logs para mais detalhes.")

    st.markdown("---")

    # --- Seção de Importação de XML DI ---
    st.markdown("#### Importar Documento de Declaração de Importação (DI)")
    
    st.subheader("XML da DI")
    
    # A mensagem de "Declaração de Importação associada encontrada" ou "Ao importar um XML..."
    # será exibida aqui, antes do uploader
    if st.session_state.current_declaracao_di_data:
        di_data_display_info = st.session_state.current_declaracao_di_data
        st.info(f"Declaração de Importação associada encontrada: **{_format_di_number(di_data_display_info.get('numero_di', 'N/A'))}**")
    else:
        st.info(f"Ao importar um XML, a DI será salva e vinculada a esta referência de processo: **{process_data.get('Processo_Novo', 'N/A')}**")

    uploaded_xml_di_file = st.file_uploader(
        "Carregar XML da DI",
        type=["xml"],
        key=f"{process_key_for_state}_xml_di_uploader"
    )

    # Chama a função de tratamento de upload de XML DI
    _handle_xml_di_upload(st.session_state.current_process_data, uploaded_xml_di_file, process_key_for_state)

    st.markdown("---")

    # Display da DI Vinculada (se houver) - esta seção permanece para exibir os detalhes
    if st.session_state.current_declaracao_di_data:
        di_data_display = st.session_state.current_declaracao_di_data
        st.markdown(f"#### Declaração de Importação Vinculada: **{_format_di_number(di_data_display.get('numero_di', 'N/A'))}**")
        st.markdown(f"Referência DI: `{di_data_display.get('informacao_complementar', 'N/A')}`")
        st.markdown(f"Valor VMLD: {_format_currency_display(di_data_display.get('vmld', 0.0))}")
        
        # Botões "Pagamentos" e "Acessar Custo do Processo" para a DI vinculada
        col_di_buttons = st.columns(2)
        with col_di_buttons[0]:
            if st.button("Pagamentos da DI", key="btn_pagamentos_di"):
                st.session_state.current_page = "Pagamentos"
                # Garante que a referência do processo é usada se o numero_di não for ideal para a tela de pagamentos
                # Agora, passa a informacao_complementar do processo para a página de detalhes/cálculos
                st.session_state.detalhes_di_input_text = di_data_display.get('informacao_complementar') # <--- ALTERAÇÃO AQUI
                st.rerun()

        with col_di_buttons[1]:
            if st.button("Acessar Custo do Processo da DI", key="btn_acessar_custo_di"):
                st.session_state.current_page = "Custo do Processo"
                st.session_state.custo_search_ref_input = di_data_display.get('informacao_complementar') # Passa a referência do processo
                st.rerun()

        st.markdown("---")
        st.markdown("##### Status das Despesas da DI")

        # Checklist de despesas com cores e emojis
        frete_internacional_data = db_utils.get_frete_internacional_by_referencia(di_data_display.get('informacao_complementar', ''))
        frete_internacional_valor = 0.0
        if frete_internacional_data:
            if frete_internacional_data.get('tipo_frete') == 'Aéreo':
                frete_internacional_valor = frete_internacional_data.get('total_aereo_brl', 0.0)
            elif frete_internacional_data.get('tipo_frete') == 'Marítimo':
                frete_internacional_valor = frete_internacional_data.get('total_maritimo_brl', 0.0)

        frete_nacional_valor = di_data_display.get('frete_nacional', 0.0)
        armazenagem_valor = di_data_display.get('armazenagem', 0.0)

        col_checklist = st.columns(3)
        with col_checklist[0]:
            if frete_internacional_valor > 0:
                st.markdown(f"<span style='color:green;'>✔ Frete Internacional</span>", unsafe_allow_html=True)
            else:
                st.markdown(f"<span style='color:red;'>✖ Frete Internacional</span>", unsafe_allow_html=True)
        
        with col_checklist[1]:
            if frete_nacional_valor > 0:
                st.markdown(f"<span style='color:green;'>✔ Frete Nacional</span>", unsafe_allow_html=True)
            else:
                st.markdown(f"<span style='color:red;'>✖ Frete Nacional</span>", unsafe_allow_html=True)

        with col_checklist[2]:
            if armazenagem_valor > 0:
                st.markdown(f"<span style='color:green;'>✔ Armazenagem</span>", unsafe_allow_html=True)
            else:
                st.markdown(f"<span style='color:red;'>✖ Armazenagem</span>", unsafe_allow_html=True)
        st.markdown("---")
    else: # Esta mensagem só será exibida se _current_declaracao_di_data for None APÓS TODAS as tentativas de busca.
        st.warning(f"Nenhuma Declaração de Importação vinculada ou encontrada para este processo.")


    # Adicionar o expander para o histórico detalhado
    st.markdown("---")
    st.markdown("#### Histórico Detalhado (Log de Eventos)")

    if process_history:
        # Converter process_history para um DataFrame para melhor exibição
        df_history = pd.DataFrame(process_history)
        
        # Formatar timestamps para legibilidade
        if 'timestamp' in df_history.columns:
            df_history['timestamp'] = df_history['timestamp'].apply(
                lambda x: datetime.strptime(str(x).split('.')[0], "%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y %H:%M:%S") if x and str(x).lower() != 'nan' else 'N/A'
            )
        if 'status_change_timestamp' in df_history.columns:
            df_history['status_change_timestamp'] = df_history['status_change_timestamp'].apply(
                lambda x: datetime.strptime(str(x).split('.')[0], "%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y %H:%M:%S") if x and str(x).lower() != 'nan' else 'N/A'
            )
        
        # Selecionar e reordenar colunas para exibição
        display_cols = [
            'timestamp', 'usuario', 'campo_alterado', 'valor_antigo', 'valor_novo',
            'status_change_timestamp', 'detalhes_adicionais'
        ]
        
        # Filtrar para colunas existentes para evitar erros
        display_df = df_history[[col for col in display_cols if col in df_history.columns]]

        with st.expander("Ver Histórico Completo de Eventos", expanded=False):
            st.dataframe(display_df, use_container_width=True, hide_index=True)
    else:
        st.info("Nenhum histórico detalhado de eventos encontrado para este processo.")

    # Botão de retorno
    if st.button(label="Voltar para Follow-up Importação"):
        return_callback()
        st.rerun()

    st.markdown("---")
    st.write("Esta tela apresenta uma visão detalhada do processo selecionado.")
