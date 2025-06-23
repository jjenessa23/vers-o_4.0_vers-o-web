import streamlit as st
import pandas as pd
from datetime import datetime
import logging
import os
import base64

# Importar funções do módulo de utilitários de banco de dados
from db_utils import (
    get_declaracao_by_id,
    get_declaracao_by_referencia,
    get_all_declaracoes,
    get_frete_internacional_by_referencia
)
# Importar a função _clean_reference_string do db_utils
try:
    from db_utils import _clean_reference_string
except ImportError:
    def _clean_reference_string(s: str) -> str:
        if not isinstance(s, str):
            return str(s) if s is not None else ""
        return s.strip().upper()

# Importar as páginas de cálculo Streamlit (mantidas como estão)
from app_logic import calculo_portonave_page
from app_logic import calculo_futura_page
from app_logic import calculo_paclog_elo_page
from app_logic import calculo_fechamento_page
from app_logic import calculo_fn_transportes_page
from app_logic import calculo_frete_internacional_page
# NOVO: Importar o módulo de gerenciamento de banco de dados para follow-up
from app_logic import followup_db_manager

logger = logging.getLogger(__name__)

# --- Função para definir imagem de fundo com opacidade ---
def set_background_image(image_path):
    """Define uma imagem de fundo para o aplicativo Streamlit com opacidade."""
    try:
        with open(image_path, "rb") as image_file:
            encoded_string = base64.b64encode(image_file.read()).decode()
        st.markdown(
            f"""
            <style>
            /* Definir variáveis CSS para cores dos cards e bordas */
            :root {{
                --card-bg-start: #2c3e50; /* Cinza azulado escuro */
                --card-bg-end: #34495e;   /* Cinza azulado um pouco mais claro */
                --border-color: #00FF00; /* Borda VERDE NEON para visibilidade máxima */
                --text-color-primary: #ecf0f1; /* Cor primária do texto */
                --text-color-secondary: #bdc3c7; /* Cor secundária do texto */
                --button-bg-color: #3498db; /* Azul vibrante para botões */
                --button-border-color: #2980b9; /* Azul mais escuro para borda do botão */
                --button-hover-bg-color: #2980b9; /* Azul para hover do botão */
                --disabled-bg-color: #7f8c8d; /* Cinza para botões desabilitados */
                --disabled-border-color: #546a76; /* Cinza escuro para borda de desabilitados */
            }}

            /* Garante que o fundo do app seja transparente e aplica a imagem de fundo */
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

            /* Estilo principal para os "cards" (baseado em st.container e st.columns) */
            .main .block-container [data-testid="stVerticalBlock"] {{
                border-radius: 12px !important;
                background: linear-gradient(135deg, var(--card-bg-start), var(--card-bg-end)) !important;
                border: 3px solid var(--border-color) !important; /* Borda visível */
                box-shadow: 0 8px 24px rgba(0, 0, 0, 0.8) !important;
                padding: 20px !important; /* Padding para o conteúdo do card */
                margin-bottom: 20px !important; /* Espaçamento entre cards */
                position: relative !important;
                overflow: hidden !important;
                transition: all 0.3s ease-in-out !important;
            }}

            /* Estilo para st.expander, st.alert, st.popover (cards secundários) */
            .main .block-container [data-testid="stExpander"],
            .main .block-container [data-testid="stAlert"],
            .main .block-container .stPopover > div > div {{
                border-radius: 12px !important;
                background: linear-gradient(135deg, var(--card-bg-start), var(--card-bg-end)) !important;
                border: 1px solid rgba(255, 255, 255, 0.1) !important; /* Borda mais discreta */
                box-shadow: 0 4px 12px rgba(0, 0, 0, 0.6) !important;
                padding: 15px !important; 
                margin-bottom: 15px !important; 
                transition: all 0.3s ease-in-out !important;
            }}
            
            /* Removendo borda dos st.metric e ajustando padding para não serem cards isolados */
            /* Eles devem estar dentro de um container que é um card, mas eles próprios não são cards */
            .main .block-container [data-testid="stMetric"] {{
                border: none !important; 
                background: none !important;
                box-shadow: none !important;
                padding: 0px !important; 
                margin-bottom: 0px !important; 
            }}

            /* Efeitos de Hover para os cards principais e secundários */
            .main .block-container [data-testid="stVerticalBlock"]:hover, /* Para containers e colunas */
            .main .block-container [data-testid="stExpander"]:hover,
            .main .block-container [data-testid="stAlert"]:hover,
            .main .block-container .stPopover > div > div:hover {{
                border-color: var(--button-bg-color) !important; /* Cor de hover para border */
                box-shadow: 0 12px 30px rgba(0, 0, 0, 0.9), 0 0 20px var(--button-bg-color) !important;
                transform: translateY(-8px) !important; /* Mais elevação */
            }}

            /* Ajustes para texto em dark mode no conteúdo principal */
            .main .block-container .stMarkdown, 
            .main .block-container .stText, 
            .main .block-container .stTextInput > label > div, 
            .main .block-container .stSelectbox > label > div {{
                color: var(--text-color-primary) !important; /* Forçar cor do texto */
            }}
            .main .block-container h1, 
            .main .block-container h2, 
            .main .block-container h3, 
            .main .block-container h4, 
            .main .block-container h5, 
            .main .block-container h6 {{
                color: var(--text-color-primary) !important; /* Forçar cor do título */
            }}

            /* Estilo dos botões no conteúdo principal (incluindo o popover) */
            .main .block-container .stButton > button {{
                border-radius: 8px !important;
                border: 1px solid var(--button-border-color) !important;
                color: #FFFFFF !important;
                background-color: var(--button-bg-color) !important;
                padding: 10px 15px !important;
                font-size: 16px !important;
                transition: all 0.2s ease-in-out !important;
                display: flex !important;
                align-items: center !important;
                justify-content: center !important;
                gap: 8px !important;
                box-shadow: 0 4px 8px rgba(0,0,0,0.3) !important;
                width: 100% !important; /* Garante que os botões dentro do popover preencham o espaço */
            }}
            .main .block-container .stButton > button:hover:not(:enabled) {{
                background-color: var(--button-hover-bg-color) !important;
                border-color: var(--button-bg-color) !important;
                box-shadow: 0 6px 12px rgba(0,0,0,0.4) !important;
                transform: translateY(-2px) !important;
            }}
            .main .block-container .stButton > button:disabled {{
                opacity: 0.6 !important;
                cursor: not-allowed !important;
                background-color: var(--disabled-bg-color) !important;
                border-color: var(--disabled-border-color) !important;
                box-shadow: none !important;
            }}

            /* Ajustes para inputs de texto para evitar padding excessivo de "card" */
            .main .block-container [data-testid="stTextInput"] {{
                padding: 0px !important; /* Remove padding de card */
                margin-bottom: 10px !important; /* Espaçamento padrão para inputs */
                border: none !important; /* Remove borda de card se aplicada */
                background: none !important; /* Remove fundo de card se aplicado */
                box-shadow: none !important; /* Remove sombra de card se aplicada */
            }}
            .main .block-container [data-testid="stTextInput"] > div > label {{
                color: var(--text-color-primary) !important; /* Força a cor do label do input */
            }}
            .main .block-container [data-testid="stTextInput"] input {{
                background-color: rgba(255, 255, 255, 0.1) !important; /* Fundo do input */
                color: var(--text-color-primary) !important; /* Cor do texto do input */
                border-radius: 8px !important;
                border: 1px solid rgba(255, 255, 255, 0.3) !important;
                padding: 8px 12px !important;
            }}
            .main .block-container [data-testid="stTextInput"] input:focus {{
                border-color: var(--button-bg-color) !important;
                box-shadow: 0 0 0 0.15rem rgba(52, 152, 219, 0.25) !important;
            }}

            /* Estilo para DataFrame dentro do expander para evitar que seja muito grande */
            .main .block-container [data-testid="stDataFrame"] {{
                max-height: 700px !important;
                overflow-y: auto !important;
                border: 1px solid rgba(255, 255, 255, 0.1) !important;
                border-radius: 8px !important;
                box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3) !important;
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
def _format_di_number(di_number):
    """Formata o número da DI para o padrão **/*******-*."""
    if di_number and isinstance(di_number, str) and len(di_number) == 10:
        return f"{di_number[0:2]}/{di_number[2:9]}-{di_number[9]}"
    return di_number

def _format_currency(value, prefix="R$ "): # Adicionado o parâmetro 'prefix' com valor padrão
    """Formata um valor numérico para o formato de moeda com um prefixo."""
    try:
        val = float(value)
        return f"{prefix}{val:,.2f}".replace('.', '#').replace(',', '.').replace('#', ',')
    except (ValueError, TypeError):
        return f"{prefix}0,00" # Garante que o prefixo seja usado também para valores padrão

def _format_date(date_str):
    """Formata uma string de data AAAA-MM-DD para DD/MM/AAAA."""
    if date_str:
        try:
            # Tenta converter de YYYY-MM-DD HH:MM:SS ou YYYY-MM-DD
            if ' ' in date_str:
                return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y")
            else:
                return datetime.strptime(date_str, "%Y-%m-%d").strftime("%d/%m/%Y")
        except ValueError:
            return date_str # Retorna original se formato for diferente
    return "N/A"

# --- Função auxiliar para criar botões com ícones ---
def icon_button(label, emoji_icon, key, disabled=False, use_container_width=True):
    """Cria um botão com um emoji como ícone."""
    return st.button(
        f"{emoji_icon} {label}",
        key=key,
        disabled=disabled,
        use_container_width=use_container_width # Mantido para controle individual se necessário
    )

# --- Funções de Ação ---

def _perform_di_loading(input_value):
    """
    Função auxiliar que contém a lógica de carregamento da DI.
    Atualiza st.session_state directamente, não contém st.rerun().
    """
    # NOVO: Limpa TODO o cache de dados do Streamlit para forçar recarregamento.
    st.cache_data.clear()
    # E também limpa o cache da função específica do followup_db_manager.
    followup_db_manager.obter_processo_by_processo_novo.clear() 
    logger.info("Cache de dados do Streamlit e de obter_processo_by_processo_novo.clear() limpos.")

    st.session_state.detalhes_di_data = None # Limpa dados anteriores
    st.session_state.frete_internacional_calculado = 0.0 # Limpa o frete internacional calculado
    st.session_state.processo_data = None # NOVO: Limpa dados do processo anterior

    if not input_value:
        st.info("Digite uma Referência ou ID da DI para carregar os detalhes.")
        return False # Indica que nenhum dado foi carregado

    if get_declaracao_by_id is None or get_declaracao_by_referencia is None:
        st.error("Serviço de banco de dados não disponível.")
        return False

    di_data_row = None
    
    # Tenta carregar por ID (se for numérico)
    try:
        declaracao_id = int(input_value)
        logger.info(f"Tentando carregar DI por ID: {declaracao_id}")
        di_data_row = get_declaracao_by_id(declaracao_id)
    except ValueError:
        # Se não for um ID numérico, tenta carregar por Referência
        cleaned_input_value = _clean_reference_string(input_value)
        logger.info(f"Valor '{input_value}' não é um ID numérico. Tentando buscar por Referência (normalizada): '{cleaned_input_value}'.")
        di_data_row = get_declaracao_by_referencia(cleaned_input_value)
    
    if di_data_row:
        st.session_state.detalhes_di_data = dict(di_data_row)
        st.success(f"DI {_format_di_number(st.session_state.detalhes_di_data.get('numero_di', ''))} carregada com sucesso!")
        logging.info(f"Detalhes da DI '{input_value}' carregados. DI_ID: {st.session_state.detalhes_di_data.get('id')}")
        
        # Tenta carregar o frete internacional associado
        referencia_processo = st.session_state.detalhes_di_data.get('informacao_complementar')
        if referencia_processo:
            frete_internacional_data = get_frete_internacional_by_referencia(referencia_processo)
            if frete_internacional_data:
                if frete_internacional_data['tipo_frete'] == 'Aéreo':
                    st.session_state.frete_internacional_calculado = frete_internacional_data.get('total_aereo_brl', 0.0)
                elif frete_internacional_data['tipo_frete'] == 'Marítimo':
                    st.session_state.frete_internacional_calculado = frete_internacional_data.get('total_maritimo_brl', 0.0)
                logger.info(f"Frete internacional de R$ {st.session_state.frete_internacional_calculado:.2f} carregado para referência '{referencia_processo}'.")
            else:
                logger.info(f"Nenhum frete internacional encontrado para a referência '{referencia_processo}'.")
        
        # NOVO: Tenta carregar os detalhes do processo (Follow-up)
        if referencia_processo:
            logger.info(f"DEBUG: Buscando processo de follow-up para referência: '{referencia_processo}' (após limpeza e carga da DI).")
            processo_data = followup_db_manager.obter_processo_by_processo_novo(referencia_processo)
            if processo_data:
                st.session_state.processo_data = processo_data
                logger.info(f"Dados do processo '{referencia_processo}' carregados do Follow-up. Processo_Novo: {processo_data.get('Processo_Novo')}, Status: {processo_data.get('Status_Geral')}")
            else:
                logger.info(f"Nenhum dado de processo encontrado no Follow-up para a referência '{referencia_processo}'.")
        
        return True
    else:
        st.error(f"Nenhum dado encontrado para a DI: '{input_value}'. Verifique o ID ou a Referência.")
        logging.warning(f"Tentativa de carregar DI '{input_value}' falhou: não encontrada por ID ou Referência.")
        return False


def load_di_details_manual(input_value):
    _perform_di_loading(input_value)


def load_di_details():
    if _perform_di_loading(st.session_state.detalhes_di_input_text):
        st.rerun()


def navigate_to_calc_page(page_name, di_id_session_key):
    if 'detalhes_di_data' in st.session_state and st.session_state.detalhes_di_data:
        # Limpa os dados da DI da página de cálculo específica para forçar um refresh
        if page_name == "Cálculo Futura":
            st.session_state.futura_di_data = None
        elif page_name == "Cálculo Pac Log - Elo":
            st.session_state.elo_di_data = None
        elif page_name == "Cálculo Fechamento":
            st.session_state.fechamento_di_data = None
        elif page_name == "Cálculo FN Transportes":
            st.session_state.fn_transportes_di_data = None
        elif page_name == "Cálculo Frete Internacional":
            st.session_state.frete_internacional_di_data = None


        st.session_state.current_page = page_name
        st.session_state[di_id_session_key] = st.session_state.detalhes_di_data['id']
        st.rerun()
    else:
        st.warning("Por favor, carregue uma DI antes de ir para o cálculo.")


# --- Tela Principal do Streamlit para Detalhes DI e Cálculos ---
def show_page():
    background_image_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'assets', 'logo_navio_atracado.png')
    set_background_image(background_image_path)

    # Botão "Voltar para Follow-up Importação" envolto em um container
    with st.container(): # Este container vai receber o estilo de card automaticamente
        if st.button("Voltar para Follow-up Importação"):
            st.session_state.current_page = "Follow-up Importação"
            st.rerun()

    if 'detalhes_di_data' not in st.session_state:
        st.session_state.detalhes_di_data = None
    if 'detalhes_di_input_text' not in st.session_state: 
        st.session_state.detalhes_di_input_text = "" 
    if 'frete_internacional_calculado' not in st.session_state: 
        st.session_state.frete_internacional_calculado = 0.0
    if 'processo_data' not in st.session_state: # NOVO: Inicializa o estado para os dados do processo
        st.session_state.processo_data = None

    if 'last_processed_di_reference' not in st.session_state:
        st.session_state.last_processed_di_reference = None
    
    current_input_ref = st.session_state.detalhes_di_input_text
    current_loaded_di_ref = st.session_state.detalhes_di_data.get('informacao_complementar') if st.session_state.detalhes_di_data else None

    if current_input_ref and (
        current_input_ref != st.session_state.last_processed_di_reference or
        current_loaded_di_ref is None or
        _clean_reference_string(current_input_ref) != _clean_reference_string(current_loaded_di_ref)
    ):
        logger.info(f"Detectada nova referência '{current_input_ref}'. Tentando carregamento inicial da DI.")
        if _perform_di_loading(current_input_ref):
            st.session_state.last_processed_di_reference = current_input_ref


    # --- SEÇÃO: Carregar Declaração de Importação ---
    with st.container(): # Este container vai ser o card principal da seção de carregamento
        st.markdown("### Carregar Declaração de Importação")
        st.text_input(
            "Referência para Carregar (ID ou Processo)",
            value=st.session_state.detalhes_di_input_text,
            key="detalhes_di_input_text",
            on_change=load_di_details
        )
    st.markdown("---") # Linha separadora

    # --- SEÇÃO: Exibir detalhes da DI carregada ---
    if st.session_state.detalhes_di_data:
        di_data = st.session_state.detalhes_di_data
        
        # O cabeçalho do processo como um container que é um card
        with st.container():
            st.markdown(f"## Processo: **{di_data.get('informacao_complementar', 'N/A')}**")
            st.write("") # Espaçamento

        # Contêiner para as métricas chave (será um card)
        with st.container():
            st.markdown("#### Resumo da DI")
            col_vmle, col_vmld, col_frete = st.columns(3) 
            
            with col_vmle:
                st.metric("VMLE", _format_currency(di_data.get('vmle', 0.0)))
                st.metric("Armazenagem (DB)", _format_currency(di_data.get('armazenagem', 0.0)))
            with col_vmld:
                st.metric("VMLD", _format_currency(di_data.get('vmld', 0.0)))
                st.metric("Frete Nacional (DB)", _format_currency(di_data.get('frete_nacional', 0.0)))
            with col_frete:
                st.metric("Frete (DI)", _format_currency(di_data.get('frete', 0.0)))
                st.metric("Frete Intl. (Calc)", _format_currency(st.session_state.frete_internacional_calculado))
            
        
        st.write("") # Espaçamento
        
        # Contêiner principal para detalhes e cálculos (colunas)
        main_layout_cols = st.columns([3, 1]) # Proporção ajustada para mais detalhes

        with main_layout_cols[0]: # Coluna da esquerda para detalhes (o expander já é um card pelo seletor CSS)
            with st.expander("##### Detalhes Completos da Declaração de Importação", expanded=False):
                details_to_display = {
                    "REFERENCIA": str(di_data.get('informacao_complementar') or "N/A"),
                    "Data do Registro": _format_date(di_data.get('data_registro')),
                    "VMLE": _format_currency(di_data.get('vmle')),
                    "Frete (DI)": _format_currency(di_data.get('frete')),
                    "Seguro": _format_currency(di_data.get('seguro')),
                    "VMLD": _format_currency(di_data.get('vmld')),
                    "II": _format_currency(di_data.get('imposto_importacao')),
                    "IPI": _format_currency(di_data.get('ipi')),
                    "Pis/Pasep": _format_currency(di_data.get('pis_pasep')),
                    "Cofins": _format_currency(di_data.get('cofins')),
                    "ICMS-SC": str(di_data.get('icms_sc') or "N/A"),
                    "Taxa Cambial (USD)": str(di_data.get('taxa_cambial_usd') or "N/A"),
                    "Taxa SISCOMEX": _format_currency(di_data.get('taxa_siscomex')),
                    "Nº Invoice": str(di_data.get('numero_invoice') or "N/A"),
                    "Peso Bruto (KG)": str(di_data.get('peso_bruto') or "N/A"),
                    "Peso Líquido (KG)": str(di_data.get('peso_liquido') or "N/A"),
                    "CNPJ Importador": str(di_data.get('cnpj_importador') or "N/A"),
                    "Importador Nome": str(di_data.get('importador_nome') or "N/A"),
                    "Recinto": str(di_data.get('recinto') or "N/A"),
                    "Embalagem": str(di_data.get('embalagem') or "N/A"),
                    "Quantidade Volumes": str(di_data.get('quantidade_volumes') or "N/A"),
                    "Acréscimo": _format_currency(di_data.get('acrescimo')),
                    "Armazenagem (DB)": _format_currency(di_data.get('armazenagem')),
                    "Frete Nacional (DB)": _format_currency(di_data.get('frete_nacional')),
                    "Frete Internacional (Calculado)": _format_currency(st.session_state.frete_internacional_calculado),
                    "Arquivo Origem": str(di_data.get('arquivo_origem') or "N/A"),
                    "Data Importação": _format_date(di_data.get('data_importacao', '').split(' ')[0])
                }
                
                df_details = pd.DataFrame.from_dict(details_to_display, orient='index', columns=['Valor'])
                st.dataframe(
                    df_details, 
                    use_container_width=True,
                    height=min(len(details_to_display) * 35 + 38, 700) # Altura ajustável, mas com limite
                )

            # NOVO: Exibir detalhes completos do processo
            if st.session_state.processo_data:
                processo_data = st.session_state.processo_data
                with st.expander("##### Detalhes Completos do Processo", expanded=True): # Alterado para True para abrir por padrão
                    process_details_to_display = {
                        "Processo Novo": str(processo_data.get('Processo_Novo', 'N/A')),
                        "Status Geral": str(processo_data.get('Status_Geral', 'N/A')),
                        "Modal": str(processo_data.get('Modal', 'N/A')),
                        "Origem": str(processo_data.get('Origem', 'N/A')),
                        "Destino": str(processo_data.get('Destino', 'N/A')),
                        "INCOTERM": str(processo_data.get('INCOTERM', 'N/A')),
                        "Fornecedor": str(processo_data.get('Fornecedor', 'N/A')),
                        "Comprador": str(processo_data.get('Comprador', 'N/A')),
                        "Nº Ordem Compra": str(processo_data.get('N_Ordem_Compra', 'N/A')),
                        "Nº Invoice": str(processo_data.get('N_Invoice', 'N/A')),
                        "Quantidade": str(processo_data.get('Quantidade', 'N/A')),
                        "Valor (US$)": _format_currency(processo_data.get('Valor_USD', 0.0), prefix='US$ '),
                        "Pago": str(processo_data.get('Pago', 'N/A')),
                        "Data Compra": _format_date(processo_data.get('Data_Compra', '')),
                        "Estimativa Impostos BR": _format_currency(processo_data.get('Estimativa_Impostos_BR', 0.0)),
                        "Estimativa Frete USD": _format_currency(processo_data.get('Estimativa_Frete_USD', 0.0), prefix='US$ '),
                        "Agente de Carga Novo": str(processo_data.get('Agente_de_Carga_Novo', 'N/A')),
                        "Estimativa Dólar BRL": str(processo_data.get('Estimativa_Dolar_BRL', 'N/A')),
                        "Estimativa Seguro BRL": _format_currency(processo_data.get('Estimativa_Seguro_BRL', 0.0)),
                        "Observação": str(processo_data.get('Observacao', 'N/A')),
                        "Tipos de Item": str(processo_data.get('Tipos_de_item', 'N/A')),
                        "Data Embarque": _format_date(processo_data.get('Data_Embarque', '')),
                        "Previsão Pichau": _format_date(processo_data.get('Previsao_Pichau', '')),
                        "Documentos Revisados": str(processo_data.get('Documentos_Revisados', 'N/A')),
                        "Conhecimento Embarque": str(processo_data.get('Conhecimento_Embarque', 'N/A')),
                        "Descrição Feita": str(processo_data.get('Descricao_Feita', 'N/A')),
                        "Descrição Enviada": str(processo_data.get('Descricao_Enviada', 'N/A')),
                        "Status Arquivado": str(processo_data.get('Status_Arquivado', 'Não Arquivado')),
                        "Caminho da Pasta": str(processo_data.get('Caminho_da_pasta', 'N/A')),
                        "Estimativa II BR": _format_currency(processo_data.get('Estimativa_II_BR', 0.0)),
                        "Estimativa IPI BR": _format_currency(processo_data.get('Estimativa_IPI_BR', 0.0)),
                        "Estimativa PIS BR": _format_currency(processo_data.get('Estimativa_PIS_BR', 0.0)),
                        "Estimativa COFINS BR": _format_currency(processo_data.get('Estimativa_COFINS_BR', 0.0)),
                        "Estimativa ICMS BR": _format_currency(processo_data.get('Estimativa_ICMS_BR', 0.0)),
                        "Nota Feita": str(processo_data.get('Nota_feita', 'N/A')),
                        "Conferido": str(processo_data.get('Conferido', 'N/A')),
                        "Última Alteração Por": str(processo_data.get('Ultima_Alteracao_Por', 'N/A')),
                        "Última Alteração Em": _format_date(processo_data.get('Ultima_Alteracao_Em', '')),
                        "Estimativa Impostos Total": _format_currency(processo_data.get('Estimativa_Impostos_Total', 0.0)),
                        "Quantidade Containers": str(processo_data.get('Quantidade_Containers', 'N/A')),
                        "ETA Recinto": _format_date(processo_data.get('ETA_Recinto', '')),
                        "Data Registro": _format_date(processo_data.get('Data_Registro', '')),
                        "DI ID Vinculada": str(processo_data.get('DI_ID_Vinculada', 'N/A')),
                        "Nome do Arquivo": str(processo_data.get('Nome_do_arquivo', 'N/A')),
                        "Tipo do Arquivo": str(processo_data.get('Tipo_do_arquivo', 'N/A')),
                        "Consolidado": str(processo_data.get('Consolidado', 'N/A')),
                        "LCL Processos Quantidade": str(processo_data.get('LCL_Processos_Quantidade', 'N/A')),
                        "LCL Processos Vinculados": ", ".join(processo_data.get('LCL_Processos_Vinculados', [])) if processo_data.get('LCL_Processos_Vinculados') else 'N/A'
                    }
                    
                    df_process_details = pd.DataFrame.from_dict(process_details_to_display, orient='index', columns=['Valor'])
                    st.dataframe(
                        df_process_details, 
                        use_container_width=True,
                        height=min(len(process_details_to_display) * 35 + 38, 700) # Altura ajustável, mas com limite
                    )
            else:
                st.info("Nenhum dado de processo de Follow-up encontrado para esta DI.")


        with main_layout_cols[1]: # Coluna da direita para botões de cálculo
            with st.container(): # Este container será o card que agrupa os botões de cálculo
                with st.popover("##### Acessar Cálculos", use_container_width=True):
                    st.markdown("###### Despachantes")
                    if icon_button("Futura", "📝", "calc_futura_button", use_container_width=True): # Força o uso da largura do contêiner para o botão
                        navigate_to_calc_page("Cálculo Futura", "selected_di_id_futura")
                    st.markdown("---")

                    st.markdown("###### Portos")
                    if icon_button("Portonave", "🚢", "calc_portonave_button", use_container_width=True):
                        navigate_to_calc_page("Cálculo Portonave", "portonave_selected_di_id")
                    icon_button("Itapoá", "🚢", "calc_itapoa_button", disabled=True, use_container_width=True)
                    st.markdown("---")

                    st.markdown("###### Aeroportos")
                    if icon_button("Pac Log - Elo", "✈️", "calc_paclog_button", use_container_width=True):
                        navigate_to_calc_page("Cálculo Pac Log - Elo", "selected_di_id_paclog")
                    icon_button("Ponta Negra", "✈️", "calc_pontanegra_button", disabled=True, use_container_width=True)
                    icon_button("Floripa Air", "✈️", "calc_floripaair_button", disabled=True, use_container_width=True)
                    st.markdown("---")

                    st.markdown("###### Fretes")
                    if icon_button("FN Transportes", "🚚", "calc_fntransportes_button", disabled=False, use_container_width=True):
                        navigate_to_calc_page("Cálculo FN Transportes", "selected_di_id_fn_transportes")
                    if icon_button("Frete Internacional", "🌍", "calc_frete_internacional_button", disabled=False, use_container_width=True):
                        navigate_to_calc_page("Cálculo Frete Internacional", "selected_di_id_frete_internacional")
                    st.markdown("---")

                    st.markdown("###### Seguro")
                    icon_button("Ação", "🛡️", "calc_acao_button", disabled=True, use_container_width=True)
                    st.markdown("---")

                    st.markdown("###### Conferências")
                    icon_button("Seguro", "✅", "calc_seguro_button", disabled=True, use_container_width=True)
                    if icon_button("Fechamento", "📊", "calc_fechamento_button", use_container_width=True):
                        navigate_to_calc_page("Cálculo Fechamento", "selected_di_id_fechamento")
                    st.markdown("---")

    else:
        # Garante que a mensagem de "Nenhuma Declaração..." também seja um card
        with st.container():
            st.info("Nenhuma Declaração de Importação carregada. Por favor, digite uma Referência ou ID para começar.")

    st.markdown("---")
    st.write("Esta tela permite visualizar os detalhes de uma Declaração de Importação e navegar para telas de cálculo específicas.")
