import streamlit as st
import pandas as pd
import logging
import os
from datetime import datetime
import urllib.parse # Importa para codificar URLs para o mailto

# Importa as funções reais do db_utils
# ATENÇÃO: Certifique-se de que 'db_utils.py' existe no mesmo diretório raiz do 'app_main.py'
# e que as funções 'get_declaracao_by_id' e 'update_declaracao_field'
# estão corretamente implementadas nele para interagir com seu banco de dados real.
from db_utils import get_declaracao_by_id, update_declaracao_field

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
        return f"{val:,.{decimals}f}\"".replace('.', '#').replace(',', '.').replace('#', ',')
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

def generate_armazenagem_email_content():
    """Gera o conteúdo do e-mail para Armazenagem Elo - Pac Log."""
    di_data = st.session_state.elo_di_data
    referencia_processo = st.session_state.elo_processo_ref
    valor_total_depositar = st.session_state.elo_total_a_depositar_display # Puxa do valor calculado e exibido

    current_hour = datetime.now().hour
    saudacao = "Bom dia" if 6 <= current_hour < 12 else "Boa tarde"
    usuario_programa = st.session_state.get('user_info', {}).get('username', 'usuário do sistema')
    data_hoje = datetime.now().strftime("%d/%m/%Y")

    email_body_plaintext = f"""{saudacao} Mayra,

Segue armazenagem.
Processo: {referencia_processo}
Valor total a Depositar: {valor_total_depositar}
Vencimento: {data_hoje}

PAGAMENTO VIA BOLETO (em vermelho)

Banco: ITAÚ
Empresa: ELO SOLUCOES LOGISTICAS INTEGRADAS LTDA
CNPJ nº 31.626.973/0001-64
Agência: 0292
Conta Corrente: 58339-0-7

Conforme instruções em anexo.
Obs.: Invoice e DI da importação em anexo.

Obrigado(a),
{usuario_programa}
"""
    email_subject = f"{referencia_processo} - Armazenagem Elo - Pac Log"
    
    return email_subject, email_body_plaintext

# --- Funções de Ação ---

def _save_armazenagem_to_db():
    """Salva o valor da armazenagem calculada no banco de dados."""
    if 'elo_di_data' not in st.session_state or not st.session_state.elo_di_data:
        st.error("Não há dados da DI carregados para salvar a armazenagem.")
        return

    di_id = st.session_state.elo_di_data[0] # O ID da DI é o primeiro elemento da tupla
    
    # O valor a ser salvo é o 'Total Armazenagem' calculado (VMLD * 0.40%)
    # Ele já está disponível em st.session_state.elo_total_a_depositar_display
    # Precisamos convertê-lo de volta para float.
    armazenagem_to_save_str = st.session_state.elo_total_a_depositar_display
    try:
        armazenagem_float = float(armazenagem_to_save_str.replace('R$', '').replace('.', '').replace(',', '.').strip())
    except ValueError:
        st.error("Valor de Armazenagem calculado inválido para salvar no banco de dados.")
        return

    if update_declaracao_field(di_id, 'armazenagem', armazenagem_float):
        st.success(f"Valor de armazenagem ({_format_currency(armazenagem_float)}) salvo com sucesso para a DI ID {di_id}!")
    else:
        st.error(f"Falha ao salvar o valor de armazenagem para a DI ID {di_id}.")

def load_elo_di_data(declaracao_id):
    """
    Carrega os dados da DI para a tela Armazenagem Elo e inicializa o estado da sessão.
    """
    if not declaracao_id:
        logger.warning("Nenhum ID de declaração fornecido para carregar dados (Armazenagem Elo).")
        clear_elo_di_data()
        return

    logger.info(f"Carregando dados para DI ID (Armazenagem Elo): {declaracao_id}")
    di_data_row = get_declaracao_by_id(declaracao_id)

    if di_data_row:
        # Converte sqlite3.Row para uma tupla ou lista para desempacotar
        di_data = tuple(di_data_row)
        st.session_state.elo_di_data = di_data
        
        # Desempacota os dados para acessar informacao_complementar e outros campos
        (id_db, numero_di, data_registro_db, valor_total_reais_xml,
         arquivo_origem, data_importacao, informacao_complementar,
         vmle, frete, seguro, vmld, ipi, pis_pasep, cofins, icms_sc,
         taxa_cambial_usd, taxa_siscomex, numero_invoice, peso_bruto, peso_liquido,
         cnpj_importador, importador_nome, recinto, embalagem, quantidade_volumes, acrescimo,
         imposto_importacao, armazenagem_db_value, frete_nacional_db) = di_data

        st.session_state.elo_processo_ref = informacao_complementar if informacao_complementar else "N/A"
        
        # O campo de entrada de armazenagem (que foi removido) não precisa mais ser inicializado aqui.
        # A armazenagem será sempre calculada.

        # DEBUG: Log dos valores brutos carregados do DB
        logger.info(f"DEBUG ELO: DI ID {declaracao_id} - VMLD: {vmld}, Peso Bruto: {peso_bruto}, Armazenagem DB: {armazenagem_db_value}")


        # Atualiza os valores brutos da DI no session_state para uso nos cálculos
        st.session_state.elo_vmld_raw = vmld
        st.session_state.elo_peso_bruto_raw = peso_bruto
        st.session_state.elo_peso_liquido_raw = peso_liquido
        st.session_state.elo_armazenagem_db_raw = armazenagem_db_value # Guarda o valor do DB para comparação

        # Força o recálculo para garantir que os valores iniciais do DB sejam usados
        perform_elo_calculations()

    else:
        st.warning(f"Nenhum dado encontrado para a DI ID: {declaracao_id} (Armazenagem Elo)")
        clear_elo_di_data()

def clear_elo_di_data():
    """Limpa todos os dados e estados da sessão para a tela Armazenagem Elo."""
    st.session_state.elo_di_data = None
    st.session_state.elo_processo_ref = "PCH-XXXX-XX"
    # st.session_state.elo_armazenagem_value = _format_currency(0.00) # Removido, não é mais um input
    st.session_state.show_elo_email_expander = False
    st.session_state.elo_email_type_to_show = None
    # Limpa os valores calculados e brutos
    st.session_state.elo_vmld_raw = 0.0
    st.session_state.elo_peso_bruto_raw = 0.0
    st.session_state.elo_peso_liquido_raw = 0.0
    st.session_state.elo_armazenagem_db_raw = 0.0
    st.session_state.elo_vmld_di_display = _format_currency(0.00)
    st.session_state.elo_periodo_display = "N/A"
    st.session_state.elo_peso_bruto_di_display = _format_weight_no_kg(0.00)
    st.session_state.elo_peso_liquido_di_display = _format_weight_no_kg(0.00)
    st.session_state.elo_total_armazenagem_display = _format_currency(0.00)
    st.session_state.elo_tabela_valor_display = _format_currency(0.00)
    st.session_state.elo_total_a_depositar_display = _format_currency(0.00)
    st.session_state.elo_taxas_extras_value = _format_currency(0.00)
    st.session_state.elo_diferenca_value = _format_currency(0.00)
    st.session_state.elo_pis_cofins_iss_display = _format_currency(0.00)
    st.session_state.elo_carregamento_display = _format_currency(0.00)


def perform_elo_calculations():
    """Realiza os cálculos de armazenagem, capatazia e impostos para a tela Elo."""
    if 'elo_di_data' not in st.session_state or not st.session_state.elo_di_data:
        logger.warning("Não há dados da DI para realizar cálculos (Elo).")
        return

    # Puxa os valores brutos da DI do session_state
    vmld = st.session_state.elo_vmld_raw
    peso_bruto = st.session_state.elo_peso_bruto_raw
    # peso_liquido = st.session_state.elo_peso_liquido_raw # Não usado diretamente nos cálculos abaixo
    # armazenagem_db_value = st.session_state.elo_armazenagem_db_raw # Não é mais usado para inicializar input

    # Obter valores de Taxas Extras e Diferença como floats dos session_states
    try:
        taxas_extras_atual_float = float(st.session_state.elo_taxas_extras_value.replace('R$', '').replace('.', '').replace(',', '.').strip())
    except ValueError:
        taxas_extras_atual_float = 0.00
        logger.warning("Valor de Taxas Extras (Elo) inválido, usando 0.00 para cálculo.")
    
    try:
        diferenca_atual_float = float(st.session_state.elo_diferenca_value.replace('R$', '').replace('.', '').replace(',', '.').strip())
    except ValueError:
        diferenca_atual_float = 0.00
        logger.warning("Valor de Diferença (Elo) inválido, usando 0.00 para cálculo.")

    # --- Cálculo de Armazenagem ---
    # O total de armazenagem é VMLD x período (0,40%)
    periodo_percent = 0.0040 # 0,40%
    total_armazenagem = vmld * periodo_percent
        
    # --- Cálculo de Capatazia ---
    capatazia_por_kg = 0.08
    capatazia_calculada = peso_bruto * capatazia_por_kg
    capatazia_minima = 17.95 # Valor fixo da tabela
    
    # Aplica a regra da capatazia mínima (se a calculada for menor que a mínima, usa a mínima)
    capatazia_final = max(capatazia_calculada, capatazia_minima)
    
    tabela_valor = capatazia_final 

    # --- Cálculo de Impostos (PIS/COFINS/ISS) - NOVA FÓRMULA ---
    # Impostos = (Armazenagem + Capatazia + Carregamento) / 0.8775 - (Armazenagem + Capatazia + Carregamento)
    CARREGAMENTO_FIXO = 350.00 # Definido como constante na classe CalculoPacLogEloView
    base_impostos_nova = total_armazenagem + capatazia_final + CARREGAMENTO_FIXO
    if base_impostos_nova != 0: # Evita divisão por zero
        impostos_calculados = (base_impostos_nova / 0.8775) - base_impostos_nova
    else:
        impostos_calculados = 0.0
    
    # --- Total a Depositar ---
    # Inclui o CARREGAMENTO_FIXO, Taxas Extras e o valor da DIFERENÇA
    total_a_depositar = total_armazenagem + capatazia_final + impostos_calculados + CARREGAMENTO_FIXO + diferenca_atual_float + taxas_extras_atual_float

    # DEBUG: Log dos valores intermediários e final do cálculo
    logger.info(f"DEBUG ELO Cálculos: Total Armazenagem (VMLD*0.40%): {total_armazenagem}, Capatazia Final: {capatazia_final}, Impostos Calculados: {impostos_calculados}, Carregamento Fixo: {CARREGAMENTO_FIXO}, Diferença: {diferenca_atual_float}, Taxas Extras: {taxas_extras_atual_float}")
    logger.info(f"DEBUG ELO Cálculos: TOTAL A DEPOSITAR FINAL: {total_a_depositar}")


    # Atualiza os valores no session_state para exibição
    st.session_state.elo_vmld_di_display = _format_currency(vmld)
    st.session_state.elo_periodo_display = "1" # Placeholder, você pode calcular o período real aqui
    st.session_state.elo_peso_bruto_di_display = _format_weight_no_kg(peso_bruto)
    st.session_state.elo_peso_liquido_di_display = _format_weight_no_kg(st.session_state.elo_peso_liquido_raw) # Puxa do raw para exibição
    st.session_state.elo_total_armazenagem_display = _format_currency(total_armazenagem)
    st.session_state.elo_tabela_valor_display = _format_currency(tabela_valor)
    st.session_state.elo_total_a_depositar_display = _format_currency(total_a_depositar)
    st.session_state.elo_pis_cofins_iss_display = _format_currency(impostos_calculados)
    st.session_state.elo_carregamento_display = _format_currency(CARREGAMENTO_FIXO)


def show_calculo_paclog_elo_page():
    """
    Exibe a interface de usuário para o cálculo de Armazenagem Elo - Pac Log.
    """
    # --- Configuração da Imagem de Fundo para a página ---
    current_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.join(current_dir, '..')
    app_root_dir = os.path.join(root_dir, '..')
    #background_image_path = os.path.join(app_root_dir, 'assets', 'logo_navio_atracado.png')
    
    background_image_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'assets', 'logo_navio_atracado.png')
    

    try:
        from app_logic.utils import set_background_image
        set_background_image(background_image_path)
    except ImportError:
        st.warning("Não foi possível carregar a função de imagem de fundo. Verifique o arquivo utils.py.")
    # --- Fim da Configuração da Imagem de Fundo ---

    st.subheader("Armazenagem Elo - Pac Log")

    # Inicializa variáveis de estado para a página
    if 'elo_di_data' not in st.session_state:
        st.session_state.elo_di_data = None
    if 'elo_processo_ref' not in st.session_state:
        st.session_state.elo_processo_ref = "PCH-XXXX-XX"
    # if 'elo_armazenagem_value' not in st.session_state: # Removido, não é mais um input
    #     st.session_state.elo_armazenagem_value = _format_currency(0.00)
    if 'show_elo_email_expander' not in st.session_state:
        st.session_state.show_elo_email_expander = False
    if 'elo_email_type_to_show' not in st.session_state:
        st.session_state.elo_email_type_to_show = None
    # Inicializa variáveis de estado para os valores calculados
    if 'elo_vmld_di_display' not in st.session_state:
        st.session_state.elo_vmld_di_display = _format_currency(0.00)
    if 'elo_periodo_display' not in st.session_state:
        st.session_state.elo_periodo_display = "N/A"
    if 'elo_peso_bruto_di_display' not in st.session_state:
        st.session_state.elo_peso_bruto_di_display = _format_weight_no_kg(0.00)
    if 'elo_peso_liquido_di_display' not in st.session_state:
        st.session_state.elo_peso_liquido_di_display = _format_weight_no_kg(0.00)
    if 'elo_total_armazenagem_display' not in st.session_state:
        st.session_state.elo_total_armazenagem_display = _format_currency(0.00)
    if 'elo_tabela_valor_display' not in st.session_state:
        st.session_state.elo_tabela_valor_display = _format_currency(0.00)
    if 'elo_total_a_depositar_display' not in st.session_state:
        st.session_state.elo_total_a_depositar_display = _format_currency(0.00)
    if 'elo_taxas_extras_value' not in st.session_state:
        st.session_state.elo_taxas_extras_value = _format_currency(0.00)
    if 'elo_diferenca_value' not in st.session_state:
        st.session_state.elo_diferenca_value = _format_currency(0.00)
    if 'elo_pis_cofins_iss_display' not in st.session_state:
        st.session_state.elo_pis_cofins_iss_display = _format_currency(0.00)
    if 'elo_carregamento_display' not in st.session_state:
        st.session_state.elo_carregamento_display = _format_currency(0.00)
    # Inicializa os valores brutos da DI no session_state
    if 'elo_vmld_raw' not in st.session_state:
        st.session_state.elo_vmld_raw = 0.0
    if 'elo_peso_bruto_raw' not in st.session_state:
        st.session_state.elo_peso_bruto_raw = 0.0
    if 'elo_peso_liquido_raw' not in st.session_state:
        st.session_state.elo_peso_liquido_raw = 0.0
    if 'elo_armazenagem_db_raw' not in st.session_state:
        st.session_state.elo_armazenagem_db_raw = 0.0


    # Carrega os dados da DI se um ID foi passado da página anterior
    if 'selected_di_id_paclog' in st.session_state and st.session_state.selected_di_id_paclog:
        load_elo_di_data(st.session_state.selected_di_id_paclog)
        st.session_state.selected_di_id_paclog = None # Limpa o ID após carregar

    st.markdown(f"#### Processo: **{st.session_state.elo_processo_ref}**")
    st.markdown("---")

    # O campo de entrada para o valor da armazenagem foi removido.
    # O valor da armazenagem agora é sempre calculado com base no VMLD.

    # --- Tabela de Cálculo da Armazenagem ---
    st.markdown("##### Detalhes do Cálculo de Armazenagem")
    col_vmld, col_periodo, col_peso_bruto, col_peso_liquido = st.columns(4)
    with col_vmld:
        st.markdown(f"**VMLD DI:** {st.session_state.elo_vmld_di_display}")
    with col_periodo:
        st.markdown(f"**Período:** {st.session_state.elo_periodo_display}")
    with col_peso_bruto:
        st.markdown(f"**Peso Bruto DI:** {st.session_state.elo_peso_bruto_di_display}")
    with col_peso_liquido:
        st.markdown(f"**Peso Líquido DI:** {st.session_state.elo_peso_liquido_di_display}")

    col_total_armazenagem, col_tabela_valor = st.columns(2)
    with col_total_armazenagem:
        st.markdown(f"**Total Armazenagem:** {st.session_state.elo_total_armazenagem_display}")
    with col_tabela_valor:
        st.markdown(f"**Tabela Valor (Capatazia):** {st.session_state.elo_tabela_valor_display}")
    
    st.markdown("---") # Separador antes do Total a Depositar

    # NOVO: Exibição do "Total a Depositar" acima de "Taxas Extras" e "Diferença"
    st.markdown(f"##### **Total a Depositar:** {st.session_state.elo_total_a_depositar_display}")
    
   

    # Taxas Extras e Diferença
    col_taxas_extras, col_diferenca,col1, col2,col3,col4 = st.columns(6)
    with col_taxas_extras:
        taxas_extras_input = st.text_input(
            "Taxas Extras (R$):",
            value=st.session_state.elo_taxas_extras_value,
            key="elo_taxas_extras_input",
            on_change=perform_elo_calculations # Recalcula ao alterar
        )
        st.session_state.elo_taxas_extras_value = taxas_extras_input
        col_taxas_btn1, col_taxas_btn2 = st.columns(2)
        with col_taxas_btn1:
            if st.button("+0.01", key="elo_taxas_plus", use_container_width=True):
                try:
                    current_value = float(st.session_state.elo_taxas_extras_value.replace('R$', '').replace('.', '').replace(',', '.').strip())
                    st.session_state.elo_taxas_extras_value = _format_currency(round(current_value + 0.01, 2))
                    perform_elo_calculations()
                    st.rerun()
                except ValueError:
                    st.error("Valor inválido para Taxas Extras.")
        with col_taxas_btn2:
            if st.button("-0.01", key="elo_taxas_minus", use_container_width=True):
                try:
                    current_value = float(st.session_state.elo_taxas_extras_value.replace('R$', '').replace('.', '').replace(',', '.').strip())
                    st.session_state.elo_taxas_extras_value = _format_currency(round(current_value - 0.01, 2))
                    perform_elo_calculations()
                    st.rerun()
                except ValueError:
                    st.error("Valor inválido para Taxas Extras.")

    with col_diferenca:
        diferenca_input = st.text_input(
            "Diferença (R$):",
            value=st.session_state.elo_diferenca_value,
            key="elo_diferenca_input",
            on_change=perform_elo_calculations # Recalcula ao alterar
        )
        st.session_state.elo_diferenca_value = diferenca_input
        col_diff_btn1, col_diff_btn2 = st.columns(2)
        with col_diff_btn1:
            if st.button("+0.01", key="elo_diferenca_plus", use_container_width=True):
                try:
                    current_value = float(st.session_state.elo_diferenca_value.replace('R$', '').replace('.', '').replace(',', '.').strip())
                    st.session_state.elo_diferenca_value = _format_currency(round(current_value + 0.01, 2))
                    perform_elo_calculations()
                    st.rerun()
                except ValueError:
                    st.error("Valor inválido para Diferença.")
        with col_diff_btn2:
            if st.button("-0.01", key="elo_diferenca_minus", use_container_width=True):
                try:
                    current_value = float(st.session_state.elo_diferenca_value.replace('R$', '').replace('.', '').replace(',', '.').strip())
                    st.session_state.elo_diferenca_value = _format_currency(round(current_value - 0.01, 2))
                    perform_elo_calculations()
                    st.rerun()
                except ValueError:
                    st.error("Valor inválido para Diferença.")

    st.markdown("---")

    # Tabela de Impostos e Carregamento Fixo
    st.markdown("##### Impostos e Carregamento")
    col_pis_cofins, col_carregamento = st.columns(2)
    with col_pis_cofins:
        st.markdown(f"**PIS/COFINS/ISS (12,25%):** {st.session_state.elo_pis_cofins_iss_display}")
    with col_carregamento:
        st.markdown(f"CARREGAMENTO: {st.session_state.elo_carregamento_display}")

    st.markdown("---")

    col_buttons_email, col_buttons_action, col1, col2, col3, col4 = st.columns(6)

    with col_buttons_email:
        if st.button("Gerar E-mail Armazenagem", key="elo_generate_email_btn", use_container_width=True):
            st.session_state.elo_email_type_to_show = "Armazenagem"
            st.session_state.show_elo_email_expander = True
            st.rerun()

    with col_buttons_action:
        if st.button("Salvar Armazenagem no DB", key="elo_save_armazenagem_btn", use_container_width=True):
            _save_armazenagem_to_db()
            # Opcional: Recarregar dados da DI para exibir o valor atualizado
            if st.session_state.elo_di_data:
                load_elo_di_data(st.session_state.elo_di_data[0])
            st.rerun()

    st.markdown("---")

    # Expander para exibir o conteúdo do e-mail
    if st.session_state.get('show_elo_email_expander', False):
        email_subject, email_body_plaintext = generate_armazenagem_email_content()

        with st.expander(f"Conteúdo do E-mail: Armazenagem", expanded=True):
            # Assunto do E-mail (copiável)
            st.text_area("Assunto do E-mail", value=email_subject, height=68, disabled=False, key="elo_exp_email_subject")
            # Corpo do E-mail como um único text_area copiável
            st.text_area("Corpo do E-mail", value=email_body_plaintext, height=300, disabled=False, key="elo_exp_email_body")

            st.info("Copie o conteúdo acima e cole no seu cliente de e-mail. Lembre-se de aplicar a cor vermelha em 'PAGAMENTO VIA BOLETO' manualmente, se desejar.")
            
            if st.button("Fechar E-mail", key="elo_close_email_expander_btn"):
                st.session_state.show_elo_email_expander = False
                st.session_state.elo_email_type_to_show = None
                st.rerun()

    st.markdown("---")
    if st.button("Voltar para Detalhes da DI", key="elo_voltar_di"):
        st.session_state.current_page = "Pagamentos" # Assumindo que você voltaria para a página de Pagamentos
        st.rerun()
