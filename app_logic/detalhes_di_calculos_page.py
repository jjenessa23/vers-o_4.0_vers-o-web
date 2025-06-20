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
    get_frete_internacional_by_referencia # NOVO: Importa a função para buscar frete internacional
)
# Importar a função _clean_reference_string do db_utils
try:
    from db_utils import _clean_reference_string
except ImportError:
    # Fallback simples se não puder ser importado (apenas para compatibilidade)
    def _clean_reference_string(s: str) -> str:
        if not isinstance(s, str):
            return str(s) if s is not None else ""
        return s.strip().upper()


# Importar as páginas de cálculo Streamlit
from app_logic import calculo_portonave_page
from app_logic import calculo_futura_page
from app_logic import calculo_paclog_elo_page
from app_logic import calculo_fechamento_page
from app_logic import calculo_fn_transportes_page
from app_logic import calculo_frete_internacional_page


logger = logging.getLogger(__name__)

# --- Função para definir imagem de fundo com opacidade (copiada de app_main.py) ---
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
                opacity: 0.20; /* Opacidade ajustada para 20% */
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


# --- Funções Auxiliares de Formatação ---
def _format_di_number(di_number):
    """Formata o número da DI para o padrão **/*******-*."""
    if di_number and isinstance(di_number, str) and len(di_number) == 10:
        return f"{di_number[0:2]}/{di_number[2:9]}-{di_number[9]}"
    return di_number

def _format_currency(value):
    """Formata um valor numérico para o formato de moeda R$ X.XXX,XX."""
    try:
        val = float(value)
        return f"R$ {val:,.2f}".replace('.', '#').replace(',', '.').replace('#', ',')
    except (ValueError, TypeError):
        return "R$ 0,00"

def _format_date(date_str):
    """Formata uma string de data AAAA-MM-DD para DD/MM/AAAA."""
    if date_str:
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").strftime("%d/%m/%Y")
        except ValueError:
            return date_str # Retorna original se formato for diferente
    return "N/A"

# --- Função auxiliar para criar botões com ícones ---
def icon_button(label, emoji_icon, key, disabled=False, use_container_width=True):
    """Cria um botão com um emoji como ícone."""
    st.markdown("""
        <style>
        /* Estilo específico para os botões da coluna de cálculos */
        div[data-testid="column"] .stButton > button {
            min-width: 150px !important;
            max-width: 150px !important;
            width: 150px !important;  /* Força largura fixa */
            margin-left: 0 !important;
            margin-right: 0 !important;
            display: block !important;
            padding: 0.5rem !important;
        }
        
        /* Mantém o alinhamento do texto e ícone */
        div[data-testid="column"] .stButton > button > div {
            display: flex !important;
            align-items: center !important;
            justify-content: center !important;
            gap: 0.5rem !important;
        }

        /* Ajusta o container da coluna de cálculos */
        div[data-testid="column"]:nth-child(2) {
            min-width: 150px !important;
            max-width: 150px !important;
            width: 150px !important;
            padding: 0 !important;
            margin: 0 !important;
        }

        /* Remove margens extras */
        div[data-testid="column"] .stButton {
            margin: 0 !important;
            padding: 0 !important;
            width: 150px !important;
        }

        /* Ajusta a altura da tabela de detalhes */
        [data-testid="stDataFrame"] {
            height: calc(100vh - 100px) !important;  /* Ajusta a altura para ocupar toda a altura disponível menos 100px */
            min-height: 800px !important;  /* Define uma altura mínima */
        }
        </style>
    """, unsafe_allow_html=True)
    
    return st.button(
        f"{emoji_icon} {label}",
        key=key,
        disabled=disabled,
        use_container_width=False
    )

# --- Funções de Ação ---

def _perform_di_loading(input_value):
    """
    Função auxiliar que contém a lógica de carregamento da DI.
    Atualiza st.session_state diretamente, não contém st.rerun().
    """
    st.session_state.detalhes_di_data = None # Limpa dados anteriores
    st.session_state.frete_internacional_calculado = 0.0 # Limpa o frete internacional calculado

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
        cleaned_input_value = _clean_reference_string(input_value) # Usando a função de limpeza do db_utils
        logger.info(f"Valor '{input_value}' não é um ID numérico. Tentando buscar por Referência (normalizada): '{cleaned_input_value}'.")
        di_data_row = get_declaracao_by_referencia(cleaned_input_value)
    
    if di_data_row:
        st.session_state.detalhes_di_data = dict(di_data_row)
        st.success(f"DI {_format_di_number(st.session_state.detalhes_di_data.get('numero_di', ''))} carregada com sucesso!")
        logging.info(f"Detalhes da DI '{input_value}' carregados.")
        
        # Tenta carregar o frete internacional associado
        referencia_processo = st.session_state.detalhes_di_data.get('informacao_complementar')
        if referencia_processo:
            frete_internacional_data = get_frete_internacional_by_referencia(referencia_processo)
            if frete_internacional_data:
                # Usa o total calculado dependendo do tipo de frete
                if frete_internacional_data['tipo_frete'] == 'Aéreo':
                    st.session_state.frete_internacional_calculado = frete_internacional_data.get('total_aereo_brl', 0.0)
                elif frete_internacional_data['tipo_frete'] == 'Marítimo':
                    st.session_state.frete_internacional_calculado = frete_internacional_data.get('total_maritimo_brl', 0.0)
                logger.info(f"Frete internacional de R$ {st.session_state.frete_internacional_calculado:.2f} carregado para referência '{referencia_processo}'.")
            else:
                logger.info(f"Nenhum frete internacional encontrado para a referência '{referencia_processo}'.")
        return True # Indica sucesso no carregamento
    else:
        st.error(f"Nenhum dado encontrado para a DI: '{input_value}'. Verifique o ID ou a Referência.")
        logging.warning(f"Tentativa de carregar DI '{input_value}' falhou: não encontrada por ID ou Referência.")
        return False # Indica falha no carregamento


def load_di_details_manual(input_value):
    """
    Carrega os detalhes de uma DI do banco de dados, aceitando ID ou Referência.
    Esta função é chamada explicitamente (e.g., ao navegar para a página).
    """
    _perform_di_loading(input_value)
    # Não há st.rerun() aqui, pois a reexecução do script já está em andamento.


def load_di_details():
    """
    Callback para o on_change do st.text_input.
    Lê o valor do widget e chama a lógica de carregamento.
    """
    if _perform_di_loading(st.session_state.detalhes_di_input_text):
        # Se o carregamento for bem-sucedido, force um rerun para atualizar a UI.
        # Este rerun é geralmente seguro aqui pois é o fim do callback.
        st.rerun()


def navigate_to_calc_page(page_name, di_id_session_key):
    """
    Navega para a tela de cálculo especificada, passando o ID da DI carregada.
    """
    if 'detalhes_di_data' in st.session_state and st.session_state.detalhes_di_data:
        # Limpa os dados da DI da página de cálculo específica para forçar um refresh
        if page_name == "Cálculo Futura":
            st.session_state.futura_di_data = None
        elif page_name == "Cálculo Pac Log - Elo":
            st.session_state.elo_di_data = None # Corrigido para 'elo_di_data'
        elif page_name == "Cálculo Fechamento":
            st.session_state.fechamento_di_data = None
        elif page_name == "Cálculo FN Transportes": # NOVO: Limpeza para FN Transportes
            st.session_state.fn_transportes_di_data = None
        elif page_name == "Cálculo Frete Internacional": # NOVO: Limpeza para Frete Internacional
            st.session_state.frete_internacional_di_data = None


        st.session_state.current_page = page_name
        # Armazena o ID da DI selecionada para que a tela de cálculo possa carregá-la
        st.session_state[di_id_session_key] = st.session_state.detalhes_di_data['id']
        st.rerun()
    else:
        st.warning("Por favor, carregue uma DI antes de ir para o cálculo.")


# --- Tela Principal do Streamlit para Detalhes DI e Cálculos ---
def show_page():
    # --- Configuração da Imagem de Fundo para a página Detalhes DI e Cálculos ---
    background_image_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'assets', 'logo_navio_atracado.png')
    set_background_image(background_image_path)
    # --- Fim da Configuração da Imagem de Fundo ---

    if st.button("Voltar para Follow-up Importação"):
        st.session_state.current_page = "Follow-up Importação"
        st.rerun()
    # Inicializa o estado da sessão para esta página
    # O valor inicial do text_input, se a página for recarregada sem navegação externa
    # será o último valor que o usuário digitou ou que foi preenchido.
    if 'detalhes_di_data' not in st.session_state:
        st.session_state.detalhes_di_data = None
    if 'detalhes_di_input_text' not in st.session_state: 
        st.session_state.detalhes_di_input_text = "" 
    if 'frete_internacional_calculado' not in st.session_state: 
        st.session_state.frete_internacional_calculado = 0.0

    # NOVO: Estado para rastrear o valor da referência que já foi processada no carregamento inicial.
    # Isso evita reprocessar a mesma DI múltiplas vezes se o valor do input_text não mudou.
    if 'last_processed_di_reference' not in st.session_state:
        st.session_state.last_processed_di_reference = None
    
    # Lógica de carregamento inicial da DI ao entrar na página
    # Esta lógica é acionada se:
    # 1. Há um valor em detalhes_di_input_text (passado da página anterior).
    # 2. Esse valor é diferente da última referência que já foi processada (evita loops).
    # 3. st.session_state.detalhes_di_data ainda não está populado para a referência atual
    #    OU se o input_text mudou e a DI carregada não corresponde mais.
    
    current_input_ref = st.session_state.detalhes_di_input_text
    current_loaded_di_ref = st.session_state.detalhes_di_data.get('informacao_complementar') if st.session_state.detalhes_di_data else None

    # Condição para tentar o carregamento inicial:
    # - Se o input_text não está vazio
    # - E o input_text é diferente da última referência que tentamos carregar
    # - OU se não há DI carregada atualmente
    if current_input_ref and (
        current_input_ref != st.session_state.last_processed_di_reference or
        current_loaded_di_ref is None or
        _clean_reference_string(current_input_ref) != _clean_reference_string(current_loaded_di_ref) # Verifica se a referência mudou, ignorando case/espaços
    ):
        logger.info(f"Detectada nova referência '{current_input_ref}'. Tentando carregamento inicial da DI.")
        if _perform_di_loading(current_input_ref):
            st.session_state.last_processed_di_reference = current_input_ref # Marca como processado com sucesso/tentativa
        # else:
            # st.session_state.last_processed_di_reference = None # Se falhou, resetar para tentar novamente na próxima
        # IMPORTANTE: Se o perform_di_loading falhar, ele já exibe um st.error.
        # Não precisamos de um st.rerun() aqui, pois o script continua a re-renderizar.

    # Seção para carregar DI
    st.markdown("#### Carregar Declaração de Importação")
    
    col_1 = st.columns(2)
    with col_1[0]:
        # st.text_input com carregamento automático no on_change
        # O valor do value é lido do session_state, garantindo que o valor persista entre reruns
        st.text_input(
            "Referência para Carregar (ID ou Processo)",
            value=st.session_state.detalhes_di_input_text, 
            key="detalhes_di_input_text", 
            on_change=load_di_details # Callback automático ao mudar o texto
        )
        
    # O botão "Limpar Campos" foi removido conforme solicitado.
            
    # Exibir detalhes da DI carregada
    if st.session_state.detalhes_di_data:
        di_data = st.session_state.detalhes_di_data
        st.markdown(f"#### Processo: **{di_data.get('informacao_complementar', 'N/A')}**")

        st.markdown("---")
        main_content_container = st.container()
        with main_content_container:
            col_details, col_3, col_calculations = st.columns([3, 1, 2])

            with col_details:
                st.markdown("##### Detalhes da Declaração de Importação")
                details_to_display = {
                    "REFERENCIA": di_data.get('informacao_complementar'),
                    "Data do Registro": _format_date(di_data.get('data_registro')),
                    "VMLE": _format_currency(di_data.get('vmle')),
                    "Frete (DI)": _format_currency(di_data.get('frete')), # Rotulado como Frete (DI) para clareza
                    "Seguro": _format_currency(di_data.get('seguro')),
                    "VMLD": _format_currency(di_data.get('vmld')),
                    "II": _format_currency(di_data.get('imposto_importacao')),
                    "IPI": _format_currency(di_data.get('ipi')),
                    "Pis/Pasep": _format_currency(di_data.get('pis_pasep')),
                    "Cofins": _format_currency(di_data.get('cofins')),
                    "ICMS-SC": di_data.get('icms_sc'),
                    "Taxa Cambial (USD)": di_data.get('taxa_cambial_usd'),
                    "Taxa SISCOMEX": _format_currency(di_data.get('taxa_siscomex')),
                    "Nº Invoice": di_data.get('numero_invoice'),
                    "Peso Bruto (KG)": di_data.get('peso_bruto'),
                    "Peso Líquido (KG)": di_data.get('peso_liquido'),
                    "CNPJ Importador": di_data.get('cnpj_importador'),
                    "Importador Nome": di_data.get('importador_nome'),
                    "Recinto": di_data.get('recinto'),
                    "Embalagem": di_data.get('embalagem'),
                    "Quantidade Volumes": di_data.get('quantidade_volumes'),
                    "Acréscimo": _format_currency(di_data.get('acrescimo')),
                    "Armazenagem (DB)": _format_currency(di_data.get('armazenagem')),
                    "Frete Nacional (DB)": _format_currency(di_data.get('frete_nacional')),
                    "Frete Internacional (Calculado)": _format_currency(st.session_state.frete_internacional_calculado), # NOVO: Exibe o frete internacional calculado
                    "Arquivo Origem": di_data.get('arquivo_origem'),
                    "Data Importação": _format_date(di_data.get('data_importacao', '').split(' ')[0])
                }
                
                df_details = pd.DataFrame.from_dict(details_to_display, orient='index', columns=['Valor'])
                st.dataframe(
                    df_details, 
                    use_container_width=True, 
                    height=800  # Altura fixa para corresponder à altura dos botões
                )

            with col_calculations:
                with st.popover("Pagamentos",):
                
                    # --- Categoria: Despachantes ---
                    st.markdown("###### Despachantes")
                    if icon_button("Futura", "📝", "calc_futura_button"):
                        navigate_to_calc_page("Cálculo Futura", "selected_di_id_futura")
                    st.markdown("---")

                    # --- Categoria: Portos ---
                    st.markdown("###### Portos")
                    if icon_button("Portonave", "🚢", "calc_portonave_button"):
                        navigate_to_calc_page("Cálculo Portonave", "portonave_selected_di_id")
                    icon_button("Itapoá", "🚢", "calc_itapoa_button", disabled=True)
                    st.markdown("---")

                    # --- Categoria: Aeroportos ---
                    st.markdown("###### Aeroportos")
                    if icon_button("Pac Log - Elo", "✈️", "calc_paclog_button"):
                        navigate_to_calc_page("Cálculo Pac Log - Elo", "selected_di_id_paclog")
                    icon_button("Ponta Negra", "✈️", "calc_pontanegra_button", disabled=True)
                    icon_button("Floripa Air", "✈️", "calc_floripaair_button", disabled=True)
                    st.markdown("---")

                    # --- Categoria: Fretes ---
                    st.markdown("###### Fretes")
                    # Habilitado o botão FN Transportes
                    if icon_button("FN Transportes", "🚚", "calc_fntransportes_button", disabled=False):
                        navigate_to_calc_page("Cálculo FN Transportes", "selected_di_id_fn_transportes")
                    # Habilita o botão Cálculo Frete Internacional
                    if icon_button("Frete Internacional", "🌍", "calc_frete_internacional_button", disabled=False):
                        navigate_to_calc_page("Cálculo Frete Internacional", "selected_di_id_frete_internacional")
                    
                    st.markdown("---")

                    # --- Categoria: Seguro ---
                    st.markdown("###### Seguro")
                    icon_button("Ação", "🛡️", "calc_acao_button", disabled=True)
                    st.markdown("---")

                    # --- Categoria: Conferências ---
                    st.markdown("###### Conferências")
                    icon_button("Seguro", "✅", "calc_seguro_button", disabled=True)
                    if icon_button("Fechamento", "�", "calc_fechamento_button"):
                        navigate_to_calc_page("Cálculo Fechamento", "selected_di_id_fechamento")
                    st.markdown("---")

    else:
        st.info("Nenhuma Declaração de Importação carregada. Por favor, digite uma Referência ou ID para começar.")

    st.markdown("---")
    st.write("Esta tela permite visualizar os detalhes de uma Declaração de Importação e navegar para telas de cálculo específicas.")
