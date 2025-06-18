import os
import streamlit as st
import pandas as pd
from datetime import datetime
import logging
import urllib.parse # Para codificar URLs de e-mail

# Importa as funções de utilidade para o fundo
try:
    from app_logic.utils import set_background_image, set_sidebar_background_image
except ImportError:
    logging.warning("Módulo 'app_logic.utils' não encontrado. Funções de imagem de fundo podem não funcionar.")
    def set_background_image(image_path, opacity=None):
        pass # Função mock se utils não for encontrado
    def set_sidebar_background_image(image_path, opacity=None):
        pass # Função mock se utils não for encontrado

# Importa as funções reais do db_utils
# ADIÇÃO: Importa get_declaracao_by_id e update_declaracao
from db_utils import get_declaracao_by_id, update_declaracao_field, update_declaracao


logger = logging.getLogger(__name__)

# Constantes da Tabela Portonave (baseado na imagem fornecida para a lógica de armazenagem)
TABELA_PORTONAVE = {
    "1": {"percent": 0.0047, "dias_min_total": 1, "dias_max_total": 6, "minimo": 909.00}, # 0,47% sobre CIF, min 909.00 (até 6 dias)
    "2": {"percent": 0.0033, "dias_min_total": 7, "dias_max_total": 14, "minimo": 263.00}, # 0,33% ao dia, min 263.00 (para dias 7-14)
    "3": {"percent": 0.0040, "dias_min_total": 15, "dias_max_total": 29, "minimo": 386.00}, # 0,40% ao dia, min 386.00 (para dias 15-29)
    "4": {"percent": 0.0044, "dias_min_total": 30, "dias_max_total": float('inf'), "minimo": 487.00} # 0,44% ao dia, min 487.00 (para dias 30 em diante)
}
LEVANTE_FIXO = 419.00
PESAGEM_FIXA = 141.00

# --- Funções Auxiliares de Formatação ---
def _format_currency(value):
    """Formata um valor numérico para o formato de moeda R$ X.XXX,XX."""
    try:
        val = float(value)
        return f"R$ {val:,.2f}".replace('.', '#').replace(',', '.').replace('#', ',')
    except (ValueError, TypeError):
        return "R$ 0,00"

def _unformat_currency(text):
    """Converte uma string de moeda formatada para float."""
    try:
        return float(text.replace('R$', '').replace('.', '').replace(',', '.').strip())
    except (ValueError, TypeError):
        return 0.0

# --- Lógica de Cálculo (Adaptada do Tkinter) ---
def perform_calculations():
    """
    Realiza os cálculos para a tela Portonave e armazena os resultados no session_state.
    Esta função é chamada sempre que um valor de entrada muda.
    """
    if 'portonave_di_data' not in st.session_state or not st.session_state.portonave_di_data:
        st.session_state.portonave_calculated_data = {
            'vmld_di': 0.0,
            'armazenagem': 0.0,
            'levante': 0.0,
            'pesagem': 0.0,
            'total_a_depositar': 0.0
        }
        return

    di_data = st.session_state.portonave_di_data

    # Obter valores editáveis do session_state
    try:
        # Garantir que os valores são numéricos
        qtde_processos = int(st.session_state.portonave_qtde_processos)
        qtde_container = int(st.session_state.portonave_qtde_container)
        periodo_selecionado = int(st.session_state.portonave_periodo)
        dias_no_periodo = int(st.session_state.portonave_dias)
        diferenca = float(st.session_state.portonave_diferenca) # Já é float, mas garantir
        taxas_extras = float(st.session_state.portonave_taxas_extras) # Já é float, mas garantir
    except ValueError:
        logging.warning("Valores de entrada inválidos para Portonave, usando 0 para cálculo.")
        st.session_state.portonave_calculated_data = {
            'vmld_di': 0.0,
            'armazenagem': 0.0,
            'levante': 0.0,
            'pesagem': 0.0,
            'total_a_depositar': 0.0
        }
        return

    # Desempacota os dados da DI
    vmld_di_original = di_data['vmld'] if 'vmld' in di_data and di_data['vmld'] is not None else 0.0

    # --- Cálculo do Dia Total para a Armazenagem ---
    dia_total_para_calculo = 0
    if periodo_selecionado == 1:
        dia_total_para_calculo = dias_no_periodo
    elif periodo_selecionado == 2:
        dia_total_para_calculo = TABELA_PORTONAVE["1"]["dias_max_total"] + dias_no_periodo
    elif periodo_selecionado == 3:
        dia_total_para_calculo = TABELA_PORTONAVE["2"]["dias_max_total"] + dias_no_periodo
    elif periodo_selecionado == 4:
        dia_total_para_calculo = TABELA_PORTONAVE["3"]["dias_max_total"] + dias_no_periodo
    
    if str(periodo_selecionado) in TABELA_PORTONAVE and TABELA_PORTONAVE[str(periodo_selecionado)]["dias_max_total"] != float('inf'):
        dia_total_para_calculo = min(dia_total_para_calculo, TABELA_PORTONAVE[str(periodo_selecionado)]["dias_max_total"])

    # --- Cálculo de Armazenagem por Contêiner (Lógica Ajustada) ---
    total_armazenagem_todos_containers = 0.0
    
    vmld_por_container = vmld_di_original / qtde_container if qtde_container > 0 else 0.0

    for _ in range(qtde_container):
        armazenagem_container = 0.0
        
        if dia_total_para_calculo <= 0:
            armazenagem_container = 0.0
        else:
            current_total_days_processed = 0
            
            # Período 1 (dias 1 a 6)
            if dia_total_para_calculo >= TABELA_PORTONAVE["1"]["dias_min_total"]:
                val_periodo1_base_raw = vmld_por_container * TABELA_PORTONAVE["1"]["percent"]
                val_periodo1_base = val_periodo1_base_raw
                if qtde_processos <= 1 and val_periodo1_base < TABELA_PORTONAVE["1"]["minimo"]:
                    val_periodo1_base = TABELA_PORTONAVE["1"]["minimo"]
                
                if dia_total_para_calculo <= TABELA_PORTONAVE["1"]["dias_max_total"]:
                    armazenagem_container = val_periodo1_base
                else:
                    armazenagem_container = val_periodo1_base
                    current_total_days_processed = TABELA_PORTONAVE["1"]["dias_max_total"]

                    # Acumula para Período 2 (dias 7 a 14)
                    if dia_total_para_calculo > current_total_days_processed and dia_total_para_calculo >= TABELA_PORTONAVE["2"]["dias_min_total"]:
                        days_in_p2_segment = min(dia_total_para_calculo, TABELA_PORTONAVE["2"]["dias_max_total"]) - current_total_days_processed
                        if days_in_p2_segment > 0:
                            val_diario_p2_raw = vmld_por_container * TABELA_PORTONAVE["2"]["percent"]
                            val_diario_p2 = val_diario_p2_raw
                            if qtde_processos <= 1 and val_diario_p2 < TABELA_PORTONAVE["2"]["minimo"]:
                                val_diario_p2 = TABELA_PORTONAVE["2"]["minimo"]
                            armazenagem_container += val_diario_p2 * days_in_p2_segment
                        current_total_days_processed = TABELA_PORTONAVE["2"]["dias_max_total"]

                    # Acumula para Período 3 (dias 15 a 29)
                    if dia_total_para_calculo > current_total_days_processed and dia_total_para_calculo >= TABELA_PORTONAVE["3"]["dias_min_total"]:
                        days_in_p3_segment = min(dia_total_para_calculo, TABELA_PORTONAVE["3"]["dias_max_total"]) - current_total_days_processed
                        if days_in_p3_segment > 0:
                            val_diario_p3_raw = vmld_por_container * TABELA_PORTONAVE["3"]["percent"]
                            val_diario_p3 = val_diario_p3_raw
                            if qtde_processos <= 1 and val_diario_p3 < TABELA_PORTONAVE["3"]["minimo"]:
                                val_diario_p3 = TABELA_PORTONAVE["3"]["minimo"]
                            armazenagem_container += val_diario_p3 * days_in_p3_segment
                        current_total_days_processed = TABELA_PORTONAVE["3"]["dias_max_total"]

                    # Acumula para Período 4 (dias 30 em diante)
                    if dia_total_para_calculo > current_total_days_processed and dia_total_para_calculo >= TABELA_PORTONAVE["4"]["dias_min_total"]:
                        days_in_p4_segment = dia_total_para_calculo - current_total_days_processed
                        if days_in_p4_segment > 0:
                            val_diario_p4_raw = vmld_por_container * TABELA_PORTONAVE["4"]["percent"]
                            val_diario_p4 = val_diario_p4_raw
                            if qtde_processos <= 1 and val_diario_p4 < TABELA_PORTONAVE["4"]["minimo"]:
                                val_diario_p4 = TABELA_PORTONAVE["4"]["minimo"]
                            armazenagem_container += val_diario_p4 * days_in_p4_segment
        
        total_armazenagem_todos_containers += armazenagem_container

    # --- Cálculo de Levante e Pesagem ---
    base_levante = LEVANTE_FIXO
    base_pesagem = PESAGEM_FIXA

    levante_final = base_levante * qtde_container
    pesagem_final = base_pesagem * qtde_container

    if qtde_processos > 1:
        levante_final = levante_final / qtde_processos
        pesagem_final = pesagem_final / qtde_processos
    else:
        levante_final = max(levante_final, LEVANTE_FIXO)
        pesagem_final = max(pesagem_final, PESAGEM_FIXA)

    # --- Total a Depositar ---
    total_a_depositar = total_armazenagem_todos_containers + levante_final + pesagem_final + diferenca + taxas_extras

    # Armazena os resultados no session_state
    st.session_state.portonave_calculated_data = {
        'vmld_di': vmld_di_original,
        'armazenagem': total_armazenagem_todos_containers,
        'levante': levante_final,
        'pesagem': pesagem_final,
        'total_a_depositar': total_a_depositar
    }

# --- Funções de Ação ---
def load_di_data_for_portonave(declaracao_id):
    """
    Carrega os dados da DI selecionada do banco de dados e inicializa
    os campos de entrada e dados calculados no session_state.
    """
    # Usar a função importada get_declaracao_by_id
    di_data_raw = get_declaracao_by_id(declaracao_id)
    if di_data_raw:
        # Converte sqlite3.Row para dicionário para facilitar o acesso por chave
        di_data = dict(di_data_raw)
        st.session_state.portonave_di_data = di_data
        st.session_state.portonave_declaracao_id = declaracao_id

        # Inicializa campos editáveis com valores padrão ou da DI
        st.session_state.portonave_qtde_processos = 1 # Alterado para int
        st.session_state.portonave_qtde_container = 1 # Alterado para int
        st.session_state.portonave_periodo = 1 # Alterado para int
        st.session_state.portonave_dias = 1 # Alterado para int
        st.session_state.portonave_diferenca = 0.00 # Alterado para float
        st.session_state.portonave_taxas_extras = 0.00 # Alterado para float
        # Os valores de levante e pesagem serão calculados e exibidos, não são inputs diretos aqui
        # st.session_state.portonave_levante_display = _format_currency(LEVANTE_FIXO)
        # st.session_state.portonave_pesagem_display = _format_currency(PESAGEM_FIXA)


        perform_calculations() # Realiza o cálculo inicial
        logging.info(f"Dados da DI {declaracao_id} carregados para Portonave.")
    else:
        st.error(f"Nenhum dado encontrado para a DI ID: {declaracao_id} (Portonave)")
        clear_portonave_data()

def clear_portonave_data():
    """Limpa todos os dados e campos da tela Portonave no session_state."""
    st.session_state.portonave_di_data = None
    st.session_state.portonave_declaracao_id = None
    st.session_state.portonave_qtde_processos = 1 # Alterado para int
    st.session_state.portonave_qtde_container = 1 # Alterado para int
    st.session_state.portonave_periodo = 1 # Alterado para int
    st.session_state.portonave_dias = 1 # Alterado para int
    st.session_state.portonave_diferenca = 0.00 # Alterado para float
    st.session_state.portonave_taxas_extras = 0.00 # Alterado para float
    # st.session_state.portonave_levante_display = _format_currency(LEVANTE_FIXO) # Não são inputs
    # st.session_state.portonave_pesagem_display = _format_currency(PESAGEM_FIXA) # Não são inputs
    
    st.session_state.portonave_calculated_data = {
        'vmld_di': 0.0,
        'armazenagem': 0.0,
        'levante': 0.0,
        'pesagem': 0.0,
        'total_a_depositar': 0.0
    }
    logging.info("Dados da tela Portonave limpos.")

def send_email_action():
    """
    Prepara e exibe o conteúdo do e-mail para cópia/envio.
    """
    if 'portonave_di_data' not in st.session_state or not st.session_state.portonave_di_data:
        st.warning("Carregue os dados da DI antes de enviar o e-mail.")
        return

    di_data = st.session_state.portonave_di_data
    referencia_processo = di_data['informacao_complementar'] if 'informacao_complementar' in di_data and di_data['informacao_complementar'] else "N/A"
    
    # Pega os valores formatados para exibição
    valor_total_depositar = _format_currency(st.session_state.portonave_calculated_data['total_a_depositar'])
    periodo = st.session_state.portonave_periodo
    dias = st.session_state.portonave_dias
    qtde_container = st.session_state.portonave_qtde_container
    
    current_hour = datetime.now().hour
    saudacao = "Bom dia" if 6 <= current_hour < 12 else "Boa tarde"

    # Mock user_info do session_state (assumindo que app_main o define)
    usuario_programa = st.session_state.get('user_info', {}).get('username', 'Usuário do Programa')

    email_body_plaintext = f"""{saudacao} Mayra,

Segue armazenagem Portuária.
Referência dos Processos: {referencia_processo}
Valor total a Depositar: {valor_total_depositar}
Período: {periodo}
Dias: {dias}
Serviço: Armazenagem portuária, Levante, Pesagem Balança Gate de {qtde_container}*40HC

PAGAMENTO VIA BOLETO

Favorecido: PORTONAVE S/A
CNPJ: 01.335.341/0001-80
Banco: Santander
Agência: 2271
Conta Corrente: 13067114-3
Código Identificador: Não é necessário código identificador

Conforme instruções em anexo.
Obs.: Invoice e DI da importação em anexo.

Obrigado,
{usuario_programa}
"""
    email_subject = f"{referencia_processo} - Pagamento de Armazenagem Portonave"

    st.subheader("Conteúdo do E-mail")
    st.text_area("Assunto do E-mail", value=email_subject, height=50)
    st.text_area("Corpo do E-mail (Copie e Cole)", value=email_body_plaintext, height=300)

    col_copy, col_gmail = st.columns(2)
    with col_copy:
        st.button("Copiar Texto para Área de Transferência", key="copy_email_text", 
                  on_click=lambda: st.session_state.update(email_to_copy=email_body_plaintext, email_subject_to_copy=email_subject))
        # O Streamlit não tem acesso direto à área de transferência do sistema.
        # A cópia real para a área de transferência precisaria de um componente personalizado ou JS.
        # Aqui, estamos apenas "simulando" que o texto foi copiado.
        if st.session_state.get('email_to_copy'):
            st.info("Texto do e-mail (sem formatação) e assunto prontos para serem copiados manualmente.")
            # st.session_state.email_to_copy = None # Limpa após "cópia"
    with col_gmail:
        # Link mailto para abrir no Gmail (precisa de navegador)
        mailto_link = f"mailto:mayra@pichau.com.br?subject={urllib.parse.quote(email_subject)}&body={urllib.parse.quote(email_body_plaintext.replace('**', ''))}"
        st.markdown(f"[Abrir no Gmail]({mailto_link})")
        st.info("Clique para abrir um rascunho no Gmail. Anexe os documentos manualmente.")

def save_armazenagem_to_db():
    """Salva o valor do Total a Depositar calculado no banco de dados."""
    if 'portonave_di_data' not in st.session_state or st.session_state.portonave_declaracao_id is None:
        st.warning("Dados da DI não carregados para salvar o Total a Depositar.")
        return

    try:
        total_a_depositar_float = st.session_state.portonave_calculated_data['total_a_depositar']

        di_data = st.session_state.portonave_di_data
        declaracao_id = st.session_state.portonave_declaracao_id

        # Cria um dicionário com os dados da DI, atualizando apenas 'armazenagem'
        updated_di_data = dict(di_data) # Cria uma cópia mutável
        updated_di_data['armazenagem'] = total_a_depositar_float

        # Chamar a função de atualização do db_utils
        # Usar a função importada update_declaracao
        success = update_declaracao(declaracao_id, updated_di_data)

        if success:
            st.success("Valor do Total a Depositar salvo no banco de dados!")
            # Opcional: Recarregar os dados da DI para refletir a mudança, se necessário
            # load_di_data_for_portonave(declaracao_id)
        else:
            st.error("Falha ao salvar o Total a Depositar no banco de dados.")

    except Exception as e:
        st.error(f"Ocorreu um erro inesperado ao salvar o Total a Depositar: {e}")
        logging.exception("Erro inesperado ao salvar Total a Depositar no DB (Portonave).")


# --- Tela Principal do Streamlit para Portonave ---
def show_page():
    background_image_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'assets', 'logo_navio_atracado.png')
    set_background_image(background_image_path)
    
    st.subheader("Cálculo Armazenagem Portuária - Portonave")

    # Inicializa o estado da sessão para esta página
    if 'portonave_di_data' not in st.session_state:
        clear_portonave_data()
    
    # Verifica se há um ID de DI vindo da tela de Detalhes
    if 'portonave_selected_di_id' in st.session_state and st.session_state.portonave_selected_di_id is not None:
        if st.session_state.portonave_di_data is None or st.session_state.portonave_di_data.get('id') != st.session_state.portonave_selected_di_id:
            load_di_data_for_portonave(st.session_state.portonave_selected_di_id)
            # Limpa o ID após o carregamento para evitar recarregar em cada rerun
            st.session_state.portonave_selected_di_id = None


    # Seção para carregar DI
    st.markdown("---")
    st.markdown("#### Carregar Dados da DI")
    

    # Exibe a referência do processo
    if st.session_state.portonave_di_data:
        st.markdown(f"**Processo Referência:** {st.session_state.portonave_di_data.get('informacao_complementar', 'N/A')}")
        st.markdown(f"**VMLD da DI:** {_format_currency(st.session_state.portonave_di_data.get('vmld', 0.0))}")
    else:
        st.info("Nenhuma DI carregada. Por favor, carregue uma DI para iniciar os cálculos.")


    st.markdown("---")
    st.markdown("#### Parâmetros de Cálculo")

    # Layout dos campos de entrada em colunas
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.number_input("Qtde de Processos", min_value=1, format="%d", key="portonave_qtde_processos", on_change=perform_calculations)
        st.number_input("Qtde de Contêiner", min_value=1, format="%d", key="portonave_qtde_container", on_change=perform_calculations)
    
    with col2:
        st.number_input("Período", min_value=1, max_value=4, format="%d", key="portonave_periodo", on_change=perform_calculations)
        st.number_input("Dias no Período", min_value=1, format="%d", key="portonave_dias", on_change=perform_calculations)

    with col3:
        # Levante e Pesagem como inputs, permitindo edição
        # Usar os valores calculados do session_state, não as constantes fixas
        st.number_input("Levante (R$)", format="%.2f", key="portonave_levante_display", 
                        value=st.session_state.portonave_calculated_data['levante'], on_change=perform_calculations)
        st.number_input("Pesagem (R$)", format="%.2f", key="portonave_pesagem_display", 
                        value=st.session_state.portonave_calculated_data['pesagem'], on_change=perform_calculations)
    
    with col4:
        # Diferença e Taxas Extras
        st.number_input("DIFERENÇA (R$)", format="%.2f", key="portonave_diferenca", on_change=perform_calculations)
        st.number_input("Taxas Extras (R$)", format="%.2f", key="portonave_taxas_extras", on_change=perform_calculations)


    st.markdown("---")
    st.markdown("#### Resultados do Cálculo")

    # Exibição dos resultados em uma tabela ou colunas
    if st.session_state.portonave_calculated_data:
        calc_data = st.session_state.portonave_calculated_data
        
        col_res1, col_res2, col_res3, col_res4 = st.columns(4)
        with col_res1:
            st.metric("Armazenagem", _format_currency(calc_data['armazenagem']))
        with col_res2:
            st.metric("Levante", _format_currency(calc_data['levante']))
        with col_res3:
            st.metric("Pesagem", _format_currency(calc_data['pesagem']))
        with col_res4:
            st.metric("Total a Depositar", _format_currency(calc_data['total_a_depositar']))
    else:
        st.info("Aguardando dados para cálculo...")

    st.markdown("---")
    st.markdown("#### Tabela de Referência Portonave")
    # Exibir a tabela de referência (pode ser um DataFrame ou Markdown)
    tabela_data_df = pd.DataFrame([
        ("1º período", "0,47%", "até 6 dias", "R$ 909,00"),
        ("2º período", "0,33%", "7 a 14 dias", "R$ 263,00"),
        ("3º período", "0,40%", "15 a 29 dias", "R$ 386,00"),
        ("4º período", "0,44%", "30 em diante", "R$ 487,00"),
        ("LEVANTE", "", "", "R$ 419,00"),
        ("PESAGEM", "", "", "R$ 141,00")
    ], columns=["Período", "%", "Dias", "Mínimos"])
    st.dataframe(tabela_data_df, hide_index=True, use_container_width=True)

    st.markdown("* Observação: em casos de divergência de valores consultar tabela padrão no link abaixo")
    st.markdown("[www.portonave.com.br/site/wp-content/uploads/Tabela-de-Pre%C3%A7o-e-Servi%C3%B7os.pdf](https://www.portonave.com.br/site/wp-content/uploads/Tabela-de-Pre%C3%A7o-e-Servi%C3%B7os.pdf)")

    st.markdown("---")
    st.markdown("#### Enviar E-mail e Salvar")
    col_send_email, col_save_db = st.columns(2)
    with col_send_email:
        st.button("Gerar E-mail", on_click=send_email_action)
    with col_save_db:
        st.button("Salvar Armazenagem no Banco de Dados", on_click=save_armazenagem_to_db)

    st.markdown("---")
    # A área de texto e o botão de cópia/abrir Gmail serão exibidos após clicar em "Gerar E-mail"
    # dentro da função send_email_action().
    if st.button("Voltar para Detalhes da DI", key="elo_voltar_di"):
        st.session_state.current_page = "Pagamentos" # Assumindo que você voltaria para a página de Pagamentos
        st.rerun()

