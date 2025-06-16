import streamlit as st
import pandas as pd
import logging
import os
from datetime import datetime
import urllib.parse # Importa para codificar URLs para o mailto

# Importa as funções reais do db_utils
from db_utils import get_declaracao_by_id, update_declaracao_field

# Importa as funções de utilidade para o fundo
try:
    from app_logic.utils import set_background_image
except ImportError:
    logging.warning("Módulo 'app_logic.utils' não encontrado. Funções de imagem de fundo podem não funcionar.")
    def set_background_image(image_path, opacity=None):
        pass # Função mock se utils não for encontrado

logger = logging.getLogger(__name__)

# --- Funções Auxiliares de Formatação ---
def _format_currency(value):
    """Formata um valor numérico para o formato de moeda R$ X.XXX,XX."""
    try:
        val = float(value)
        return f"R$ {val:,.2f}".replace('.', '#').replace(',', '.').replace('#', ',')
    except (ValueError, TypeError):
        return "R$ 0,00"

def _format_float(value, decimals=4):
    """Formata um valor numérico float com um número específico de casas decimais."""
    try:
        val = float(value)
        return f"{val:,.{decimals}f}".replace('.', '#').replace(',', '.').replace('#', ',')
    except (ValueError, TypeError):
        return "N/A"

def _format_weight_no_kg(value):
    """Formata um valor numérico para peso com 3 casas decimais e 'KG'."""
    try:
        val = float(value)
        return f"{val:,.3f} KG".replace('.', '#').replace(',', '.').replace('#', ',')
    except (ValueError, TypeError):
        return "N/A"

def _format_int(value):
    """Formata um valor para inteiro."""
    try:
        val = int(value)
        return str(val)
    except (ValueError, TypeError):
        return "N/A"

# --- Funções de Geração de Conteúdo de E-mail ---

def generate_fn_email_content():
    """Gera o conteúdo do e-mail para FN Transportes."""
    di_data = st.session_state.fn_transportes_di_data
    referencia_processo = st.session_state.fn_transportes_processo_ref
    valor_total_depositar = st.session_state.fn_transportes_total_a_depositar_display

    current_hour = datetime.now().hour
    saudacao = "Bom dia" if 6 <= current_hour < 12 else "Boa tarde"
    usuario_programa = st.session_state.get('user_info', {}).get('username', 'usuário do sistema')
    data_hoje = datetime.now().strftime("%d/%m/%Y")
    
    # Dados bancários fixos para FN Transportes
    dados_bancarios = """Dados Bancários:
TRANSPORTES FN
Pagamento Via Boleto
Banco Viacredi - 085
Agencia: 0108-2
Conta: 2010871-0
CNPJ: 27.064.174/0001-74"""

    email_body_plaintext = f"""{saudacao} Mayra,

Gentileza realizar o pagamento para a FN TRANSPORTES

Referência dos Processos: {referencia_processo}
Valor total: {valor_total_depositar}
Vencimento: {data_hoje}
Serviço: Frete rodoviário de Navegantes para Joinville.

{dados_bancarios}

Conforme instruções em anexo.
Obs.: Segue Invoice, CTE e Boleto

Obrigado,
{usuario_programa}
"""
    email_subject = f"{referencia_processo} - Pagamento de frete nacional FN TRANSPORTES"
    
    return email_subject, email_body_plaintext

# --- Funções de Ação ---

def _save_frete_nacional_to_db():
    """Salva o valor do Total a Depositar no banco de dados, na coluna 'frete_nacional'."""
    if 'fn_transportes_di_data' not in st.session_state or not st.session_state.fn_transportes_di_data:
        st.error("Não há dados da DI carregados para salvar o Frete Nacional.")
        return

    di_id = st.session_state.fn_transportes_di_data[0] # O ID da DI é o primeiro elemento da tupla
    
    # O valor a ser salvo é o 'Total a Depositar' calculado
    frete_nacional_to_save_str = st.session_state.fn_transportes_total_a_depositar_display
    try:
        frete_nacional_float = float(frete_nacional_to_save_str.replace('R$', '').replace('.', '').replace(',', '.').strip())
    except ValueError:
        st.error("Valor do Total a Depositar calculado inválido para salvar no banco de dados.")
        return

    if update_declaracao_field(di_id, 'frete_nacional', frete_nacional_float):
        st.success(f"Frete Nacional ({_format_currency(frete_nacional_float)}) salvo com sucesso")
    else:
        st.error(f"Falha ao salvar o valor do Frete Nacional para a DI ID {di_id}.")

    # Não chamar st.rerun() aqui, a atualização será natural após o clique no botão

def load_fn_transportes_di_data(declaracao_id):
    """
    Carrega os dados da DI para a tela FN Transportes e inicializa o estado da sessão.
    """
    if not declaracao_id:
        logger.warning("Nenhum ID de declaração fornecido para carregar dados (FN Transportes).")
        clear_fn_transportes_di_data()
        return

    logger.info(f"Carregando dados para DI ID (FN Transportes): {declaracao_id}")
    di_data_row = get_declaracao_by_id(declaracao_id)

    if di_data_row:
        # Converte sqlite3.Row para uma tupla ou lista para desempacotar
        di_data = tuple(di_data_row)
        st.session_state.fn_transportes_di_data = di_data
        
        # Desempacota os dados para acessar informacao_complementar e outros campos
        (id_db, numero_di, data_registro_db, valor_total_reais_xml,
         arquivo_origem, data_importacao, informacao_complementar,
         vmle, frete, seguro, vmld, ipi, pis_pasep, cofins, icms_sc,
         taxa_cambial_usd, taxa_siscomex, numero_invoice, peso_bruto, peso_liquido,
         cnpj_importador, importador_nome, recinto, embalagem, quantidade_volumes, acrescimo,
         imposto_importacao, armazenagem_db_value, frete_nacional_db_value) = di_data

        st.session_state.fn_transportes_processo_ref = informacao_complementar if informacao_complementar else "N/A"
        
        # Atualiza os valores brutos da DI no session_state para uso nos cálculos
        st.session_state.fn_transportes_vmld_raw = vmld
        st.session_state.fn_transportes_peso_bruto_raw = peso_bruto
        st.session_state.fn_transportes_peso_liquido_raw = peso_liquido
        st.session_state.fn_transportes_frete_nacional_db_raw = frete_nacional_db_value # Guarda o valor do DB

        # Ensure these are initialized if they don't exist, but don't force overwrite if widgets are already rendered.
        if 'fn_transportes_qtde_processos_input' not in st.session_state:
            st.session_state.fn_transportes_qtde_processos_input = "1"
        if 'fn_transportes_qtde_container_input' not in st.session_state:
            st.session_state.fn_transportes_qtde_container_input = "1"
        if 'fn_transportes_diferenca_input' not in st.session_state:
            st.session_state.fn_transportes_diferenca_input = _format_currency(0.00)
        if 'fn_transportes_baixa_vazio_option' not in st.session_state:
            st.session_state.fn_transportes_baixa_vazio_option = "Não"
        if 'fn_transportes_qtde_baixa_vazio_input' not in st.session_state:
            st.session_state.fn_transportes_qtde_baixa_vazio_input = "0"

        perform_fn_transportes_calculations() # Realiza os cálculos iniciais

    else:
        st.warning(f"Nenhum dado encontrado para a DI ID: {declaracao_id} (FN Transportes)")
        clear_fn_transportes_di_data()

def clear_fn_transportes_di_data():
    """Limpa todos os dados e estados da sessão para a tela FN Transportes."""
    st.session_state.fn_transportes_di_data = None
    st.session_state.fn_transportes_processo_ref = "PCH-XXXX-XX"
    
    # Initialize these with default values if they don't exist, or set them to defaults
    if 'fn_transportes_qtde_processos_input' not in st.session_state:
        st.session_state.fn_transportes_qtde_processos_input = "1"
    else:
        st.session_state.fn_transportes_qtde_processos_input = "1"

    if 'fn_transportes_qtde_container_input' not in st.session_state:
        st.session_state.fn_transportes_qtde_container_input = "1"
    else:
        st.session_state.fn_transportes_qtde_container_input = "1"

    if 'fn_transportes_diferenca_input' not in st.session_state:
        st.session_state.fn_transportes_diferenca_input = _format_currency(0.00)
    else:
        st.session_state.fn_transportes_diferenca_input = _format_currency(0.00)

    if 'fn_transportes_baixa_vazio_option' not in st.session_state:
        st.session_state.fn_transportes_baixa_vazio_option = "Não"
    else:
        st.session_state.fn_transportes_baixa_vazio_option = "Não"

    if 'fn_transportes_qtde_baixa_vazio_input' not in st.session_state:
        st.session_state.fn_transportes_qtde_baixa_vazio_input = "0"
    else:
        st.session_state.fn_transportes_qtde_baixa_vazio_input = "0"

    st.session_state.show_fn_email_expander = False
    st.session_state.fn_email_type_to_show = None

    # Limpa os valores calculados e brutos
    st.session_state.fn_transportes_vmld_raw = 0.0
    st.session_state.fn_transportes_peso_bruto_raw = 0.0
    st.session_state.fn_transportes_peso_liquido_raw = 0.0
    st.session_state.fn_transportes_frete_nacional_db_raw = 0.0

    st.session_state.fn_transportes_vmld_di_display = _format_currency(0.00)
    st.session_state.fn_transportes_base_calculo_display = _format_currency(0.00)
    st.session_state.fn_transportes_percentual_vmld_display = _format_currency(0.00)
    st.session_state.fn_transportes_total_parcial_display = _format_currency(0.00)
    st.session_state.fn_transportes_total_a_depositar_display = _format_currency(0.00)


def perform_fn_transportes_calculations():
    """Realiza os cálculos para a tela FN Transportes."""
    if 'fn_transportes_di_data' not in st.session_state or not st.session_state.fn_transportes_di_data:
        logger.warning("Não há dados da DI para realizar cálculos (FN Transportes).")
        return

    # Puxa os valores brutos da DI do session_state
    vmld = st.session_state.fn_transportes_vmld_raw
    # peso_bruto = st.session_state.fn_transportes_peso_bruto_raw # Não usado diretamente nos cálculos abaixo

    # Obter valores dos campos de entrada
    try:
        qtde_processos = int(st.session_state.fn_transportes_qtde_processos_input)
        qtde_container = int(st.session_state.fn_transportes_qtde_container_input)
        diferenca_float = float(st.session_state.fn_transportes_diferenca_input.replace('R$', '').replace('.', '').replace(',', '.').strip())
        baixa_vazio_sim = (st.session_state.fn_transportes_baixa_vazio_option == "Sim")
        qtde_baixa_vazio = int(st.session_state.fn_transportes_qtde_baixa_vazio_input) if baixa_vazio_sim else 0
    except ValueError:
        logger.warning("Valores de entrada inválidos para FN Transportes, usando 0 para cálculo.")
        # Limpar os valores calculados se a entrada for inválida
        st.session_state.fn_transportes_base_calculo_display = _format_currency(0.00)
        st.session_state.fn_transportes_percentual_vmld_display = _format_currency(0.00)
        st.session_state.fn_transportes_total_parcial_display = _format_currency(0.00)
        st.session_state.fn_transportes_total_a_depositar_display = _format_currency(0.00)
        return

    # Constantes de cálculo (extraídas de view_calculo_fn_transportes.py)
    BASE_FIXA_CALCULO = 1650.00
    VALOR_BAIXA_VAZIO_UNITARIO = 380.00
    DIVISOR_FINAL = 0.83

    # Cálculo da Base - Ajustado para corresponder à imagem e lógica de 1650 fixo por processo
    # A lógica original do view_calculo_fn_transportes.py é:
    # base_calculo = self.BASE_FIXA_CALCULO
    # if qtde_container > 0: base_calculo *= qtde_container
    # if qtde_processos > 0: base_calculo /= qtde_processos
    # Para corresponder à imagem, onde a base de cálculo é R$ 1.650,00 mesmo com 2 contêineres e 1 processo:
    base_calculo_for_display = BASE_FIXA_CALCULO # Mantém 1650 fixo para exibição

    # Porcentagem do VMLD da DI
    percentual_vmld = 0.00055 * vmld

    # Total Parcial
    # A lógica do view_calculo_fn_transportes.py para total_parcial_bruto é (base_calculo + percentual_vmld)
    # ONDE base_calculo já considera qtde_container e qtde_processos.
    calculated_base_for_total = BASE_FIXA_CALCULO
    if qtde_container > 0:
        calculated_base_for_total *= qtde_container
    if qtde_processos > 0:
        calculated_base_for_total /= qtde_processos

    total_parcial_bruto = (calculated_base_for_total + percentual_vmld)
    total_parcial = total_parcial_bruto / DIVISOR_FINAL

    # Valor da Baixa de Vazio
    valor_baixa_vazio_calculado = 0.0
    if baixa_vazio_sim:
        valor_baixa_vazio_calculado = VALOR_BAIXA_VAZIO_UNITARIO * qtde_baixa_vazio
        if qtde_processos > 0: # Divide pela quantidade de processos, se houver mais de um
            valor_baixa_vazio_calculado /= qtde_processos

    # Total a Depositar
    total_a_depositar = total_parcial + diferenca_float + valor_baixa_vazio_calculado

    # Atualiza os valores no session_state para exibição
    st.session_state.fn_transportes_vmld_di_display = _format_currency(vmld)
    st.session_state.fn_transportes_base_calculo_display = _format_currency(base_calculo_for_display) # Exibir 1650 fixo para a Base Cálculo
    st.session_state.fn_transportes_percentual_vmld_display = _format_currency(percentual_vmld)
    st.session_state.fn_transportes_total_parcial_display = _format_currency(total_parcial)
    st.session_state.fn_transportes_total_a_depositar_display = _format_currency(total_a_depositar)

# --- Funções de Callback para Botões de Ajuste ---
def _increment_qtde_processos():
    st.session_state.fn_transportes_qtde_processos_input = str(int(st.session_state.fn_transportes_qtde_processos_input) + 1)
    perform_fn_transportes_calculations()

def _decrement_qtde_processos():
    st.session_state.fn_transportes_qtde_processos_input = str(max(1, int(st.session_state.fn_transportes_qtde_processos_input) - 1))
    perform_fn_transportes_calculations()

def _increment_qtde_container():
    st.session_state.fn_transportes_qtde_container_input = str(int(st.session_state.fn_transportes_qtde_container_input) + 1)
    perform_fn_transportes_calculations()

def _decrement_qtde_container():
    st.session_state.fn_transportes_qtde_container_input = str(max(1, int(st.session_state.fn_transportes_qtde_container_input) - 1))
    perform_fn_transportes_calculations()

def _increment_qtde_baixa_vazio():
    st.session_state.fn_transportes_qtde_baixa_vazio_input = str(int(st.session_state.fn_transportes_qtde_baixa_vazio_input) + 1)
    perform_fn_transportes_calculations()

def _decrement_qtde_baixa_vazio():
    st.session_state.fn_transportes_qtde_baixa_vazio_input = str(max(0, int(st.session_state.fn_transportes_qtde_baixa_vazio_input) - 1))
    perform_fn_transportes_calculations()

def _increment_diferenca():
    current_diff = float(st.session_state.fn_transportes_diferenca_input.replace('R$', '').replace('.', '').replace(',', '.').strip())
    st.session_state.fn_transportes_diferenca_input = _format_currency(round(current_diff + 0.01, 2))
    perform_fn_transportes_calculations()

def _decrement_diferenca():
    current_diff = float(st.session_state.fn_transportes_diferenca_input.replace('R$', '').replace('.', '').replace(',', '.').strip())
    st.session_state.fn_transportes_diferenca_input = _format_currency(round(current_diff - 0.01, 2))
    perform_fn_transportes_calculations()


def show_calculo_fn_transportes_page():
    """
    Exibe a interface de usuário para o cálculo de Frete Nacional (FN Transportes).
    """
    # --- Configuração da Imagem de Fundo para a página ---
    current_dir = os.path.dirname(os.path.abspath(__file__))
    app_root_dir = os.path.join(current_dir, '..', '..') # Ajustado para ir para a raiz do app
    background_image_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'assets', 'logo_navio_atracado.png')
    set_background_image(background_image_path)
    
    # Define a imagem de fundo com a opacidade padrão de 0.20 para o conteúdo principal
    set_background_image(background_image_path, opacity=0.20)


    st.subheader("Cálculo Frete Nacional (FN Transportes)")

    # Inicializa variáveis de estado para a página se elas não existirem
    # This is the primary place to initialize session state variables for widgets
    if 'fn_transportes_di_data' not in st.session_state:
        st.session_state.fn_transportes_di_data = None
    if 'fn_transportes_processo_ref' not in st.session_state:
        st.session_state.fn_transportes_processo_ref = "PCH-XXXX-XX"
    if 'fn_transportes_qtde_processos_input' not in st.session_state:
        st.session_state.fn_transportes_qtde_processos_input = "1"
    if 'fn_transportes_qtde_container_input' not in st.session_state:
        st.session_state.fn_transportes_qtde_container_input = "1"
    if 'fn_transportes_diferenca_input' not in st.session_state:
        st.session_state.fn_transportes_diferenca_input = _format_currency(0.00)
    if 'fn_transportes_baixa_vazio_option' not in st.session_state:
        st.session_state.fn_transportes_baixa_vazio_option = "Não"
    if 'fn_transportes_qtde_baixa_vazio_input' not in st.session_state:
        st.session_state.fn_transportes_qtde_baixa_vazio_input = "0"
    if 'show_fn_email_expander' not in st.session_state:
        st.session_state.show_fn_email_expander = False
    if 'fn_email_type_to_show' not in st.session_state:
        st.session_state.fn_email_type_to_show = None
    if 'fn_transportes_vmld_raw' not in st.session_state:
        st.session_state.fn_transportes_vmld_raw = 0.0
    if 'fn_transportes_peso_bruto_raw' not in st.session_state:
        st.session_state.fn_transportes_peso_bruto_raw = 0.0
    if 'fn_transportes_peso_liquido_raw' not in st.session_state:
        st.session_state.fn_transportes_peso_liquido_raw = 0.0
    if 'fn_transportes_frete_nacional_db_raw' not in st.session_state:
        st.session_state.fn_transportes_frete_nacional_db_raw = 0.0
    if 'fn_transportes_vmld_di_display' not in st.session_state:
        st.session_state.fn_transportes_vmld_di_display = _format_currency(0.00)
    if 'fn_transportes_base_calculo_display' not in st.session_state:
        st.session_state.fn_transportes_base_calculo_display = _format_currency(0.00)
    if 'fn_transportes_percentual_vmld_display' not in st.session_state:
        st.session_state.fn_transportes_percentual_vmld_display = _format_currency(0.00)
    if 'fn_transportes_total_parcial_display' not in st.session_state:
        st.session_state.fn_transportes_total_parcial_display = _format_currency(0.00)
    if 'fn_transportes_total_a_depositar_display' not in st.session_state:
        st.session_state.fn_transportes_total_a_depositar_display = _format_currency(0.00)


    # Carrega os dados da DI se um ID foi passado da página anterior
    if 'selected_di_id_fn_transportes' in st.session_state and st.session_state.selected_di_id_fn_transportes:
        load_fn_transportes_di_data(st.session_state.selected_di_id_fn_transportes)
        st.session_state.selected_di_id_fn_transportes = None # Limpa o ID após carregar

    st.markdown(f"#### Processo: **{st.session_state.fn_transportes_processo_ref}**")
    st.markdown("---")

    # --- Tabela de Cálculos ---
    st.markdown("##### Detalhes do Cálculo de Frete")
    # Usando st.container para agrupar e controlar o layout
    with st.container():
        # Primeira linha de colunas para Qtde de Processos e Qtde de Contêiner
        col1_qty_proc, col2_qty_cont, col3_vmld_base, col4_total_parcial = st.columns(4)

        with col1_qty_proc:
            st.markdown(f"**Qtde de Processos:**")
            st.text_input(
                "Qtde de Processos",
                value=st.session_state.fn_transportes_qtde_processos_input,
                key="fn_transportes_qtde_processos_input",
                on_change=perform_fn_transportes_calculations, # Recalcula ao alterar
                label_visibility="collapsed"
            )
            # Botões de ajuste de quantidade
            qty_processos_col1, qty_processos_col2 = st.columns(2)
            with qty_processos_col1:
                st.button(" ➕ ", key="fn_qtde_processos_plus", use_container_width=True, on_click=_increment_qtde_processos)
            with qty_processos_col2:
                st.button("➖", key="fn_qtde_processos_minus", use_container_width=True, on_click=_decrement_qtde_processos)

        with col2_qty_cont:
            st.markdown(f"**Qtde de Contêiner:**")
            st.text_input(
                "Qtde de Contêiner",
                value=st.session_state.fn_transportes_qtde_container_input,
                key="fn_transportes_qtde_container_input",
                on_change=perform_fn_transportes_calculations, # Recalcula ao alterar
                label_visibility="collapsed"
            )
            # Botões de ajuste de quantidade
            qty_container_col1, qty_container_col2 = st.columns(2)
            with qty_container_col1:
                st.button(" ➕ ", key="fn_qtde_container_plus", use_container_width=True, on_click=_increment_qtde_container)
            with qty_container_col2:
                st.button(" ➖ ", key="fn_qtde_container_minus", use_container_width=True, on_click=_decrement_qtde_container)

        with col3_vmld_base:
            st.markdown(f"**VMLD DI:** {st.session_state.fn_transportes_vmld_di_display}")
            st.markdown(f"**Base Cálculo:** {st.session_state.fn_transportes_base_calculo_display}")
            st.markdown(f"**% VMLD DI:** {st.session_state.fn_transportes_percentual_vmld_display}")
        
        with col4_total_parcial:
            st.markdown(f"**Total Parcial:** {st.session_state.fn_transportes_total_parcial_display}")
            st.markdown(f"**Total a Depositar:** {st.session_state.fn_transportes_total_a_depositar_display}")
            
            st.markdown(f"**Baixa de Vazio?**")
            baixa_vazio_option = st.radio(
                "Baixa de Vazio?",
                options=["Não", "Sim"],
                key="fn_transportes_baixa_vazio_option",
                horizontal=True,
                on_change=perform_fn_transportes_calculations, # Recalcula ao alterar
                label_visibility="collapsed"
            )
            
            if baixa_vazio_option == "Sim":
                st.markdown(f"**Qtde Baixa Vazio:**")
                st.text_input(
                    "Qtde Baixa Vazio",
                    value=st.session_state.fn_transportes_qtde_baixa_vazio_input,
                    key="fn_transportes_qtde_baixa_vazio_input",
                    on_change=perform_fn_transportes_calculations, # Recalcula ao alterar
                    label_visibility="collapsed"
                )
                qty_baixa_col1, qty_baixa_col2 = st.columns(2)
                with qty_baixa_col1:
                    st.button(" ➕ ", key="fn_qtde_baixa_vazio_plus", use_container_width=True, on_click=_increment_qtde_baixa_vazio)
                with qty_baixa_col2:
                    st.button(" ➖ ", key="fn_qtde_baixa_vazio_minus", use_container_width=True, on_click=_decrement_qtde_baixa_vazio)
            else:
                # Garante que o valor seja 0 se "Não" for selecionado
                st.session_state.fn_transportes_qtde_baixa_vazio_input = "0"


    st.markdown("---")

    # Campo Diferença
    st.markdown(f"**Diferença:**")
    st.markdown("""
    <style>
    /* Ajusta o tamanho máximo dos campos de texto na página principal */
    .main .block-container div[data-testid="stTextInput"] {
        max-width: 300px !important;
        margin-left: 0 !important;
        margin-right: auto !important;
    }

    /* Ajusta o input dentro do container na página principal */
    .main .block-container div[data-testid="stTextInput"] input {
        max-width: 300px !important;
        width: 100% !important;
        box-sizing: border-box !important;
        margin-left: 0 !important;
    }

    /* Ajusta o label do input na página principal */
    .main .block-container div[data-testid="stTextInput"] label {
        max-width: 300px !important;
        width: 100% !important;
        margin-bottom: 5px !important;
        text-align: left !important;
        margin-left: 0 !important;
    }

    /* Ajusta o container do input na página principal */
    .main .block-container div[data-testid="stTextInput"] > div {
        max-width: 300px !important;
        width: 100% !important;
        margin-left: 0 !important;
        margin-right: auto !important;
    }

    /* Ajusta os botões para alinhar à esquerda na página principal */
    .main .block-container .stButton > button {
        max-width: 300px !important;
        width: 150px !important;
        margin-left: 0 !important;
        margin-right: auto !important;
    }

    /* Container dos botões na página principal */
    .main .block-container div[data-testid="column"] {
        max-width: 300px !important;
        width: 150px !important;
        margin-left: 0 !important;
        margin-right: auto !important;
    }

    /* Remove espaço entre as colunas dos botões na página principal */
    .main .block-container [data-testid="column"] {
        padding: 0 !important;
        margin: 0 !important;
        gap: 0 !important;
    }

    /* Remove espaço entre os botões na página principal */
    .main .block-container .stButton {
        margin: 0 !important;
        padding: 0 !important;
    }

    /* Ajusta o container das colunas na página principal */
    .main .block-container [data-testid="columns"] {
        gap: 0 !important;
        margin: 0 !important;
        padding: 0 !important;
    }

    /* Container para os botões lado a lado */
    .main .block-container .btn-container {
        display: flex !important;
        max-width: 300px !important;
        gap: 0 !important;
    }
    </style>
    """, unsafe_allow_html=True)
    col_1  = st.columns(2)

    with col_1[0]:
        st.text_input(
            "Diferença",
            value=st.session_state.fn_transportes_diferenca_input,
            key="fn_transportes_diferenca_input",
            on_change=perform_fn_transportes_calculations, # Recalcula ao alterar
            label_visibility="collapsed"
        )
    
    col_2  = st.columns(8)
    with col_2[0]:
        st.write('<div class="btn-container">', unsafe_allow_html=True)
        col_2  = st.columns(2)
        with col_2[0]:
            st.button("+0.01", key="fn_diferenca_plus", on_click=_increment_diferenca)
        with col_2[1]:
            st.button("-0.01", key="fn_diferenca_minus", on_click=_decrement_diferenca)
            st.write('</div>', unsafe_allow_html=True)

    st.markdown("---")

    col_1 = st.columns(5)

    with col_1[0]:
        if st.button("Gerar E-mail FN Transportes", key="fn_generate_email_btn", use_container_width=True):
            st.session_state.show_fn_email_expander = True
            st.rerun()

    with col_1[1]:
        if st.button("Salvar Frete Nacional no DB", key="fn_save_frete_nacional_btn", use_container_width=True):
            _save_frete_nacional_to_db()
            # Opcional: Recarregar dados da DI para exibir o valor atualizado
            if st.session_state.fn_transportes_di_data:
                load_fn_transportes_di_data(st.session_state.fn_transportes_di_data[0])
                           

    

    # Expander para exibir o conteúdo do e-mail
    if st.session_state.get('show_fn_email_expander', False):
        email_subject, email_body_plaintext = generate_fn_email_content()

        with st.expander(f"Conteúdo do E-mail: FN Transportes", expanded=True):
            st.text_area("Assunto do E-mail", value=email_subject, height=68, disabled=False, key="fn_exp_email_subject")
            st.text_area("Corpo do E-mail", value=email_body_plaintext, height=300, disabled=False, key="fn_exp_email_body")

            st.info("Copie o conteúdo acima e cole no seu cliente de e-mail. Lembre-se de aplicar a formatação manualmente, se desejar.")
            
            if st.button("Fechar E-mail", key="fn_close_email_expander_btn"):
                st.session_state.show_fn_email_expander = False
                st.rerun()

    st.markdown("---")
    if st.button("Voltar para Detalhes da DI", key="fn_voltar_di"):
        st.session_state.current_page = "Pagamentos" # Assumindo que você voltaria para a página de Pagamentos
        st.rerun()
        
    st.markdown("---")