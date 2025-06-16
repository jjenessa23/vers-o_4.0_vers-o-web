import streamlit as st
import pandas as pd
import logging
import os
from datetime import datetime
import urllib.parse # Importa para codificar URLs para o mailto

# Importa as funções reais do db_utils
try:
    from db_utils import get_declaracao_by_id
except ImportError:
    st.error("Erro: db_utils não encontrado. Certifique-se de que o arquivo está acessível.")
    get_declaracao_by_id = None

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

# --- Constantes de Cálculo ---
ASSESSORIA_LOGISTICA = 1000.00
TAXA_MERCANTE_FIXA = 20.00
AFRMM_PERCENTUAL_CALC = 0.08

def perform_futura_calculations():
    """
    Realiza os cálculos para a tela Futura e atualiza o estado da sessão.
    """
    if 'futura_di_data' not in st.session_state or not st.session_state.futura_di_data:
        logger.warning("Não há dados da DI para realizar cálculos (Futura).")
        return

    di_data = st.session_state.futura_di_data

    # Desempacota os dados (agora com 29 campos conforme db_utils.py)
    (id_db, numero_di, data_registro_db, valor_total_reais_xml,
     arquivo_origem, data_importacao, informacao_complementar,
     vmle, frete, seguro, vmld, ipi, pis_pasep, cofins, icms_sc,
     taxa_cambial_usd, taxa_siscomex, numero_invoice, peso_bruto, peso_liquido,
     cnpj_importador, importador_nome, recinto, embalagem, quantidade_volumes, acrescimo_xml,
     imposto_importacao_xml, armazenagem_db, frete_nacional_db) = di_data

    # Obter valores editáveis
    try:
        # Remove R$, . e substitui , por . para converter para float
        diferenca_atual_float = float(st.session_state.futura_diferenca_value.replace('R$', '').replace('.', '').replace(',', '.').strip())
        st.session_state.futura_diferenca_value = _format_currency(diferenca_atual_float) # Atualiza o valor formatado no session_state
    except ValueError:
        diferenca_atual_float = 0.00
        logger.warning("Valor de Diferença inválido, usando 0.00 para cálculo.")
        st.session_state.futura_diferenca_value = _format_currency(diferenca_atual_float) # Garante que o valor seja formatado mesmo com erro

    # Frete DI (Reais) e Acréscimo AFRMM agora são puxados diretamente do DB (di_data)
    frete_di_reais_float = float(frete)
    acrescimo_afrmm_float = float(acrescimo_xml)

    try:
        capatazias_afrmm_float = float(st.session_state.futura_capatazias_afrmm_value.replace('R$', '').replace('.', '').replace(',', '.').strip())
        st.session_state.futura_capatazias_afrmm_value = _format_currency(capatazias_afrmm_float) # Atualiza o valor formatado
    except ValueError:
        capatazias_afrmm_float = 0.0
        logger.warning("Valor de Capatazias AFRMM inválido, usando 0.0 para cálculo.")
        st.session_state.futura_capatazias_afrmm_value = _format_currency(capatazias_afrmm_float) # Garante que o valor seja formatado mesmo com erro

    try:
        tarifa_afrmm_float = float(st.session_state.futura_tarifa_afrmm_value.replace('R$', '').replace('.', '').replace(',', '.').strip())
        st.session_state.futura_tarifa_afrmm_value = _format_currency(tarifa_afrmm_float) # Atualiza o valor formatado
    except ValueError:
        tarifa_afrmm_float = 0.0
        logger.warning("Valor de Tarifa AFRMM inválido, usando 0.0 para cálculo.")
        st.session_state.futura_tarifa_afrmm_value = _format_currency(tarifa_afrmm_float) # Garante que o valor seja formatado mesmo com erro

    # --- Cálculos para VALORES ESTIMADOS PARA PAGAMENTO E/OU DÉBITO PELO IMPORTADOR ---
    imposto_importacao_calc = imposto_importacao_xml

    total_importador = imposto_importacao_calc + ipi + pis_pasep + cofins + taxa_siscomex
    # Verifica se icms_sc é um valor numérico antes de tentar somar
    if icms_sc and icms_sc.replace(',', '.').replace('R$', '').strip().replace('.', '', 1).isdigit():
        try:
            total_importador += float(icms_sc.replace('R$', '').replace('.', '').replace(',', '.').strip())
        except ValueError:
            pass # Não adiciona se não for um número válido

    st.session_state.futura_imposto_importacao_display = _format_currency(imposto_importacao_calc)
    st.session_state.futura_ipi_display = _format_currency(ipi)
    st.session_state.futura_pis_pasep_display = _format_currency(pis_pasep)
    st.session_state.futura_cofins_display = _format_currency(cofins)
    st.session_state.futura_taxa_siscomex_display = _format_currency(taxa_siscomex)
    st.session_state.futura_icms_sc_display = icms_sc if icms_sc else "N/A"
    st.session_state.futura_total_debito_importador = _format_currency(total_importador)

    # --- Cálculos para VALORES ESTIMADOS PARA DEPÓSITOS E PAGAMENTOS PELA COMISSÁRIA DE DESPACHOS ---
    total_afrmm_calc = 0.0
    if st.session_state.futura_tipo_transporte == "Marítimo":
        # Fórmula AFRMM: (Frete(BRL) + Acrescimo(BRL) + capatazia(BRL) ) x 0,08 + Tarifa + Taxa do Mercante
        total_afrmm_calc = (frete_di_reais_float + acrescimo_afrmm_float + capatazias_afrmm_float) * AFRMM_PERCENTUAL_CALC + tarifa_afrmm_float + TAXA_MERCANTE_FIXA
        st.session_state.futura_afrmm_comissaria_display = _format_currency(total_afrmm_calc)
        st.session_state.futura_total_afrmm_calc_display = _format_currency(total_afrmm_calc)
        st.session_state.futura_taxa_ptax_display = _format_float(taxa_cambial_usd, decimals=4) # Taxa PTAX é a Taxa Cambial (USD)
        st.session_state.futura_taxa_mercante_afrmm_display = _format_currency(TAXA_MERCANTE_FIXA)
    else:
        st.session_state.futura_afrmm_comissaria_display = _format_currency(0.00)
        st.session_state.futura_total_afrmm_calc_display = _format_currency(0.00)
        st.session_state.futura_taxa_ptax_display = "R$ 0,00"
        st.session_state.futura_taxa_mercante_afrmm_display = "R$ 0,00"

    st.session_state.futura_assessoria_logistica_display = _format_currency(ASSESSORIA_LOGISTICA)
    st.session_state.futura_remessa_documentos_display = _format_currency(0.00)

    total_comissaria = ASSESSORIA_LOGISTICA + total_afrmm_calc + 0.00 # Remessa de documentos (assumindo 0.00)
    st.session_state.futura_total_debito_comissaria = _format_currency(total_comissaria + diferenca_atual_float)

def load_futura_di_data(declaracao_id):
    """
    Carrega os dados da DI para a tela Futura e inicializa o estado da sessão.
    """
    if not declaracao_id:
        logger.warning("Nenhum ID de declaração fornecido para carregar dados (Futura).")
        clear_futura_di_data()
        return

    logger.info(f"Carregando dados para DI ID (Futura): {declaracao_id}")
    di_data_row = get_declaracao_by_id(declaracao_id)

    if di_data_row:
        # Converte sqlite3.Row para uma tupla ou lista para desempacotar
        di_data = tuple(di_data_row)
        st.session_state.futura_di_data = di_data
        
        # Desempacota os dados (agora com 29 campos)
        (id_db, numero_di, data_registro_db, valor_total_reais_xml,
         arquivo_origem, data_importacao, informacao_complementar,
         vmle, frete, seguro, vmld, ipi, pis_pasep, cofins, icms_sc,
         taxa_cambial_usd, taxa_siscomex, numero_invoice, peso_bruto, peso_liquido,
         cnpj_importador, importador_nome, recinto, embalagem, quantidade_volumes, acrescimo,
         imposto_importacao, armazenagem_db, frete_nacional_db) = di_data

        logger.info(f"DEBUG: Taxa Cambial (USD) carregada para DI {numero_di}: {taxa_cambial_usd}")


        st.session_state.futura_processo_ref = informacao_complementar if informacao_complementar else "N/A"
        
        # Inicializa os campos editáveis com valores da DI ou padrão
        st.session_state.futura_diferenca_value = _format_currency(0.00)
        # Frete DI (Reais) e Acréscimo AFRMM agora são exibidos, não editáveis
        st.session_state.futura_frete_di_reais_display = _format_currency(frete)
        st.session_state.futura_acrescimo_afrmm_display = _format_currency(acrescimo)

        st.session_state.futura_capatazias_afrmm_value = _format_currency(0.00)
        st.session_state.futura_tarifa_afrmm_value = _format_currency(0.00)
        st.session_state.futura_tipo_transporte = "Aéreo" # Valor inicial

        perform_futura_calculations() # Realiza os cálculos iniciais
    else:
        st.warning(f"Nenhum dado encontrado para a DI ID: {declaracao_id} (Futura)")
        clear_futura_di_data()

def clear_futura_di_data():
    """Limpa todos os dados e estados da sessão para a tela Futura."""
    st.session_state.futura_di_data = None
    st.session_state.futura_processo_ref = "PCH-XXXX-XX"

    st.session_state.futura_diferenca_value = _format_currency(0.00)
    # Limpar os valores de exibição
    st.session_state.futura_frete_di_reais_display = _format_currency(0.00)
    st.session_state.futura_acrescimo_afrmm_display = _format_currency(0.00)

    st.session_state.futura_capatazias_afrmm_value = _format_currency(0.00)
    st.session_state.futura_tarifa_afrmm_value = _format_currency(0.00)
    st.session_state.futura_tipo_transporte = "Aéreo"

    # Limpar os valores de exibição
    st.session_state.futura_imposto_importacao_display = "R$ 0,00"
    st.session_state.futura_ipi_display = "R$ 0,00"
    st.session_state.futura_pis_pasep_display = "R$ 0,00"
    st.session_state.futura_cofins_display = "R$ 0,00"
    st.session_state.futura_taxa_siscomex_display = "R$ 0,00"
    st.session_state.futura_icms_sc_display = "N/A"
    st.session_state.futura_total_debito_importador = "R$ 0,00"

    st.session_state.futura_assessoria_logistica_display = "R$ 0,00"
    st.session_state.futura_afrmm_comissaria_display = "R$ 0,00"
    st.session_state.futura_remessa_documentos_display = "R$ 0,00"
    st.session_state.futura_total_debito_comissaria = "R$ 0,00"

    st.session_state.futura_taxa_ptax_display = "R$ 0,00"
    st.session_state.futura_taxa_mercante_afrmm_display = "R$ 0,00"
    st.session_state.futura_total_afrmm_calc_display = "R$ 0,00"


def show_calculo_futura_page():
    """
    Exibe a interface de usuário para o cálculo Futura.
    """
    # --- Configuração da Imagem de Fundo para a página ---
    # O caminho da imagem deve ser relativo ao diretório do script principal (app_main.py)
    # Assumindo que 'app_logic' está dentro do diretório raiz do aplicativo.
    # O caminho para o arquivo de imagem será '../assets/logo_navio_atracado.png'
    # a partir de 'app_logic/calculo_futura_page.py'.
    current_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.join(current_dir, '..') # Volta para o diretório 'app_logic'
    app_root_dir = os.path.join(root_dir, '..') # Volta para o diretório raiz do aplicativo
    background_image_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'assets', 'logo_navio_atracado.png')

    # Importa a função set_background_image do módulo utils
    try:
        from app_logic.utils import set_background_image
        set_background_image(background_image_path)
    except ImportError:
        st.warning("Não foi possível carregar a função de imagem de fundo. Verifique o arquivo utils.py.")
    # --- Fim da Configuração da Imagem de Fundo ---

    # Inicializa o estado da sessão para esta página
    # Garante que todas as variáveis de sessão necessárias existam antes de serem usadas
    if 'futura_di_data' not in st.session_state:
        st.session_state.futura_di_data = None
    if 'futura_processo_ref' not in st.session_state:
        st.session_state.futura_processo_ref = "PCH-XXXX-XX"
    if 'futura_diferenca_value' not in st.session_state:
        st.session_state.futura_diferenca_value = _format_currency(0.00)
    if 'futura_frete_di_reais_display' not in st.session_state: # Inicialização adicionada
        st.session_state.futura_frete_di_reais_display = _format_currency(0.00)
    if 'futura_acrescimo_afrmm_display' not in st.session_state: # Inicialização adicionada
        st.session_state.futura_acrescimo_afrmm_display = _format_currency(0.00)
    if 'futura_capatazias_afrmm_value' not in st.session_state:
        st.session_state.futura_capatazias_afrmm_value = _format_currency(0.00)
    if 'futura_tarifa_afrmm_value' not in st.session_state:
        st.session_state.futura_tarifa_afrmm_value = _format_currency(0.00)
    if 'futura_tipo_transporte' not in st.session_state:
        st.session_state.futura_tipo_transporte = "Aéreo"

    if 'futura_imposto_importacao_display' not in st.session_state:
        st.session_state.futura_imposto_importacao_display = "R$ 0,00"
    if 'futura_ipi_display' not in st.session_state:
        st.session_state.futura_ipi_display = "R$ 0,00"
    if 'futura_pis_pasep_display' not in st.session_state:
        st.session_state.futura_pis_pasep_display = "R$ 0,00"
    if 'futura_cofins_display' not in st.session_state:
        st.session_state.futura_cofins_display = "R$ 0,00"
    if 'futura_taxa_siscomex_display' not in st.session_state:
        st.session_state.futura_taxa_siscomex_display = "R$ 0,00"
    if 'futura_icms_sc_display' not in st.session_state:
        st.session_state.futms_sc_display = "N/A"
    if 'futura_total_debito_importador' not in st.session_state:
        st.session_state.futura_total_debito_importador = "R$ 0,00"

    if 'futura_assessoria_logistica_display' not in st.session_state:
        st.session_state.futura_assessoria_logistica_display = "R$ 0,00"
    if 'futura_afrmm_comissaria_display' not in st.session_state:
        st.session_state.futura_afrmm_comissaria_display = "R$ 0,00"
    if 'futura_remessa_documentos_display' not in st.session_state:
        st.session_state.futura_remessa_documentos_display = "R$ 0,00"
    if 'futura_total_debito_comissaria' not in st.session_state:
        st.session_state.futura_total_debito_comissaria = "R$ 0,00"

    if 'futura_taxa_ptax_display' not in st.session_state:
        st.session_state.futura_taxa_ptax_display = "R$ 0,00"
    if 'futura_taxa_mercante_afrmm_display' not in st.session_state:
        st.session_state.futura_taxa_mercante_afrmm_display = "R$ 0,00"
    if 'futura_total_afrmm_calc_display' not in st.session_state:
        st.session_state.futura_total_afrmm_calc_display = "R$ 0,00"
    
    # Inicializa variáveis de estado para controlar o expander de e-mail
    if 'show_futura_email_expander' not in st.session_state:
        st.session_state.show_futura_email_expander = False
    if 'email_type_to_show' not in st.session_state:
        st.session_state.email_type_to_show = None


    # Carrega os dados da DI se um ID foi passado da página anterior
    # A condição 'st.session_state.futura_di_data is None' garante que a DI só é carregada
    # se ainda não estiver na sessão (ou se foi explicitamente limpa por navigate_to_calc_page)
    if 'selected_di_id_futura' in st.session_state and st.session_state.selected_di_id_futura and st.session_state.futura_di_data is None:
        load_futura_di_data(st.session_state.selected_di_id_futura)
        # Limpa o ID após carregar para evitar recarregar na próxima atualização
        st.session_state.selected_di_id_futura = None

    st.markdown(f"#### Processo: **{st.session_state.futura_processo_ref}**")
    st.markdown("---")

    # Seletor Aéreo/Marítimo
    # O valor retornado pelo selectbox é atribuído diretamente à session_state
    # e o perform_futura_calculations é chamado em seguida para reexecutar a lógica.
    st.session_state.futura_tipo_transporte = st.selectbox(
        "Tipo de Transporte:",
        ["Aéreo", "Marítimo"],
        key="futura_tipo_transporte_select",
        # on_change removido daqui, pois a atribuição direta e a chamada subsequente já geram o comportamento desejado.
    )
    # Chama a função de cálculo após a atualização do selectbox
    perform_futura_calculations()


    st.markdown("##### Cálculo Futura - Impostos e Taxas")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("###### VALORES ESTIMADOS PARA PAGAMENTO E/OU DÉBITO PELO IMPORTADOR")
        
        # Usando st.expander para a lista de impostos do importador para melhor organização
        with st.expander("Detalhes dos Impostos do Importador", expanded=True):
            # Usando st.columns para alinhar labels e valores
            c1_imp, c2_imp = st.columns([0.7, 0.3])
            with c1_imp: st.markdown("- Imposto de Importação (e-DARF - código 0086):")
            with c2_imp: st.markdown(f"**{st.session_state.futura_imposto_importacao_display}**")

            c1_imp, c2_imp = st.columns([0.7, 0.3])
            with c1_imp: st.markdown("- Imposto s/Produtos Industrializados (e-DARF - código 1038):")
            with c2_imp: st.markdown(f"**{st.session_state.futura_ipi_display}**")

            c1_imp, c2_imp = st.columns([0.7, 0.3])
            with c1_imp: st.markdown("- PIS/PASEP na Importação (e-DARF - código 5602):")
            with c2_imp: st.markdown(f"**{st.session_state.futura_pis_pasep_display}**")

            c1_imp, c2_imp = st.columns([0.7, 0.3])
            with c1_imp: st.markdown("- COFINS na Importação (e-DARF - código 5629):")
            with c2_imp: st.markdown(f"**{st.session_state.futura_cofins_display}**")

            c1_imp, c2_imp = st.columns([0.7, 0.3])
            with c1_imp: st.markdown("- Taxa de Utilização do SISCOMEX (e-DARF - código 7811):")
            with c2_imp: st.markdown(f"**{st.session_state.futura_taxa_siscomex_display}**")

            c1_imp, c2_imp = st.columns([0.7, 0.3])
            with c1_imp: st.markdown("- ICMS-SC :")
            with c2_imp: st.markdown(f"**{st.session_state.futura_icms_sc_display}**")

            st.markdown("---")
            c1_imp, c2_imp = st.columns([0.7, 0.3])
            with c1_imp: st.markdown("**TOTAL DO DÉBITO (BRL):**")
            with c2_imp: st.markdown(f"**{st.session_state.futura_total_debito_importador}**")


    with col2:
        st.markdown("###### VALORES ESTIMADOS PARA DEPÓSITOS E PAGAMENTOS PELA COMISSÁRIA DE DESPACHOS")
        
        with st.expander("Detalhes dos Pagamentos da Comissária", expanded=True):
            c1_com, c2_com = st.columns([0.7, 0.3])
            with c1_com: st.markdown("- 1 - Assessoria do Processo Logístico Internacional:")
            with c2_com: st.markdown(f"**{st.session_state.futura_assessoria_logistica_display}**")

            if st.session_state.futura_tipo_transporte == "Marítimo":
                c1_com, c2_com = st.columns([0.7, 0.3])
                with c1_com: st.markdown("- 2 - AFRMM - Adicional de Frete p/Renovação da Marinha Mercante:")
                with c2_com: st.markdown(f"**{st.session_state.futura_afrmm_comissaria_display}**")

                c1_com, c2_com = st.columns([0.7, 0.3])
                with c1_com: st.markdown("- 3 - Remessa de Documentos para Liberação B/L/HAWB/CRT:")
                with c2_com: st.markdown(f"**{st.session_state.futura_remessa_documentos_display}**")
            
            st.markdown("---")
            c1_com, c2_com = st.columns([0.7, 0.3])
            with c1_com: st.markdown("**TOTAL DO DÉBITO (BRL):**")
            with c2_com: st.markdown(f"**{st.session_state.futura_total_debito_comissaria}**")
        
        st.markdown("---")
        st.markdown("###### Diferença")
        
        # Input de texto para Diferença
        diferenca_input = st.text_input(
            "Diferença",
            value=st.session_state.futura_diferenca_value,
            key="futura_diferenca_input",
            on_change=perform_futura_calculations, # Recalcula ao alterar
            label_visibility="collapsed" # Oculta o label padrão para melhor alinhamento
        )
        # Atualiza o valor no session_state após o input
        st.session_state.futura_diferenca_value = diferenca_input

        # Botões +0.01 e -0.01 em uma nova linha, centralizados ou alinhados
        col_diff_btn1, col_diff_btn2 = st.columns(2) # Duas colunas para os botões

        with col_diff_btn1:
            # Botão +0.01
            if st.button("+0.01", key="futura_diferenca_plus", use_container_width=True):
                try:
                    current_value = float(st.session_state.futura_diferenca_value.replace('R$', '').replace('.', '').replace(',', '.').strip())
                    st.session_state.futura_diferenca_value = _format_currency(round(current_value + 0.01, 2))
                    perform_futura_calculations()
                    st.rerun() # Força a atualização da tela
                except ValueError:
                    st.error("Valor inválido para Diferença.")
        with col_diff_btn2:
            # Botão -0.01
            if st.button("-0.01", key="futura_diferenca_minus", use_container_width=True):
                try:
                    current_value = float(st.session_state.futura_diferenca_value.replace('R$', '').replace('.', '').replace(',', '.').strip())
                    st.session_state.futura_diferenca_value = _format_currency(round(current_value - 0.01, 2))
                    perform_futura_calculations()
                    st.rerun() # Força a atualização da tela
                except ValueError:
                    st.error("Valor inválido para Diferença.")


    if st.session_state.futura_tipo_transporte == "Marítimo":
        st.markdown("---")
        st.markdown("###### Cálculo AFRMM")
        col_afrmm_input, col_btn ,col_afrmm_display = st.columns(3) # Três colunas para inputs, botões e exibição
        with col_afrmm_input:
            # Campos editáveis que permanecem inputs
            capatazias_afrmm_input = st.text_input(
                "Capatazias",
                value=st.session_state.futura_capatazias_afrmm_value,
                key="futura_capatazias_afrmm_input",
                on_change=perform_futura_calculations # Recalcula ao alterar
            )
            

            st.session_state.futura_capatazias_afrmm_value = capatazias_afrmm_input

            tarifa_afrmm_input = st.text_input(
                "Tarifa",
                value=st.session_state.futura_tarifa_afrmm_value,
                key="futura_tarifa_afrmm_input",
                on_change=perform_futura_calculations # Recalcula ao alterar
            )
            st.session_state.futura_tarifa_afrmm_value = tarifa_afrmm_input
            
            if st.button("Recalcular AFRMM", key="futura_recalcular_afrmm_btn", on_click=st.rerun, use_container_width=True):
                try:
                    # Recalcula os valores com os inputs atualizados
                    perform_futura_calculations()
                    st.rerun()  # Força a atualização da tela
                except Exception as e:
                    st.error(f"Erro ao recalcular AFRMM: {e}")

        # Exibição dos resultados do cálculo AFRMM
        with col_afrmm_display:
            # Frete DI (Reais) - Agora apenas exibição
            c1_afrmm, c2_afrmm = st.columns([0.7, 0.3])
            with c1_afrmm: st.markdown("- Frete DI (Reais):")
            with c2_afrmm: st.markdown(f"**{st.session_state.futura_frete_di_reais_display}**")

            # Acréscimo - Agora apenas exibição
            c1_afrmm, c2_afrmm = st.columns([0.7, 0.3])
            with c1_afrmm: st.markdown("- Acréscimo:")
            with c2_afrmm: st.markdown(f"**{st.session_state.futura_acrescimo_afrmm_display}**")

            # Taxa PTAX - Já era exibição
            c1_afrmm, c2_afrmm = st.columns([0.7, 0.3])
            with c1_afrmm: st.markdown("- Taxa PTAX: (USD)")
            with c2_afrmm: st.markdown(f"**{st.session_state.futura_taxa_ptax_display}**")

            # Taxa do Mercante - Já era exibição
            c1_afrmm, c2_afrmm = st.columns([0.7, 0.3])
            with c1_afrmm: st.markdown("- Taxa do Mercante:")
            with c2_com: st.markdown(f"**{st.session_state.futura_taxa_mercante_afrmm_display}**")
            
            st.markdown("---")
            c1_afrmm, c2_afrmm = st.columns([0.7, 0.3])
            with c1_afrmm: st.markdown("**Total AFRMM:**")
            with c2_afrmm: st.markdown(f"**{st.session_state.futura_total_afrmm_calc_display}**")


    st.markdown("---")
    # Botões para enviar e-mails
    st.markdown("###### Enviar E-mail")
    col_email_buttons = st.columns(2) # Alterado para 2 colunas
    with col_email_buttons[0]:
        if st.button("Pagamento de Honorários", key="futura_enviar_email_honorarios", use_container_width=True):
            st.session_state.email_type_to_show = "Pagamento de Honorários"
            st.session_state.show_futura_email_expander = True # Ativa o expander
            st.rerun()
    with col_email_buttons[1]:
        if st.button("Débito em Conta", key="futura_enviar_email_debito", use_container_width=True):
            st.session_state.email_type_to_show = "Débito em Conta"
            st.session_state.show_futura_email_expander = True # Ativa o expander
            st.rerun()

    # Expander para exibir o conteúdo do e-mail
    if st.session_state.get('show_futura_email_expander', False):
        email_type = st.session_state.get('email_type_to_show', "Conferência Futura")
        
        # Gerar o conteúdo do e-mail
        if email_type == "Conferência Futura":
            email_subject, email_body_plaintext = generate_email_content_futura()
        elif email_type == "Pagamento de Honorários":
            email_subject, email_body_plaintext = generate_payment_email_content()
        elif email_type == "Débito em Conta":
            email_subject, email_body_plaintext = generate_debit_email_content()
        else:
            email_subject = "Assunto Padrão"
            email_body_plaintext = "Corpo do e-mail padrão."

        with st.expander(f"Conteúdo do E-mail: {email_type}", expanded=True):
            # Assunto do E-mail (agora copiável)
            st.text_area("Assunto do E-mail", value=email_subject, height=68, disabled=False, key="exp_email_subject")
            st.text_area("Corpo do E-mail", value=email_body_plaintext, height=300, key="exp_email_body")

            # O link "Abrir no Gmail" foi removido conforme sua solicitação
            # st.markdown(f"[Abrir no Gmail]({gmail_compose_link})")
            st.info("Copie o conteúdo acima e cole no seu cliente de e-mail.")
            
            if st.button("Fechar E-mail", key="close_email_expander_btn"):
                st.session_state.show_futura_email_expander = False
                st.session_state.email_type_to_show = None
                st.rerun()


    st.markdown("---")
    if st.button("Voltar para Detalhes da DI", key="futura_voltar_di"):
        st.session_state.current_page = "Pagamentos"
        st.rerun()

# --- Funções para o pop-up de e-mail (agora geram conteúdo para o expander) ---
def generate_email_content_futura():
    """Gera o conteúdo do e-mail para exibição na tela Futura (Conferência)."""
    di_data = st.session_state.futura_di_data
    referencia_processo = di_data[6] if di_data and di_data[6] else "N/A"
    
    # Valores do cálculo Futura
    total_debito_importador = st.session_state.futura_total_debito_importador
    total_debito_comissaria = st.session_state.futura_total_debito_comissaria
    diferenca = st.session_state.futura_diferenca_value

    current_hour = datetime.now().hour
    saudacao = "Bom dia" if 6 <= current_hour < 12 else "Boa tarde"
    usuario_programa = st.session_state.get('user_info', {}).get('username', 'usuário do sistema')
    data_atual_formatada = datetime.now().strftime("%d/%m/%Y")

    email_body_plaintext = f"""{saudacao},

Segue a conferência do processo: {referencia_processo}

VALORES ESTIMADOS PARA PAGAMENTO E/OU DÉBITO PELO IMPORTADOR
- Imposto de Importação (e-DARF - código 0086): {st.session_state.futura_imposto_importacao_display}
- Imposto s/Produtos Industrializados (e-DARF - código 1038): {st.session_state.futura_ipi_display}
- PIS/PASEP na Importação (e-DARF - código 5602): {st.session_state.futura_pis_pasep_display}
- COFINS na Importação (e-DARF - código 5629): {st.session_state.futura_cofins_display}
- Taxa de Utilização do SISCOMEX (e-DARF - código 7811): {st.session_state.futura_taxa_siscomex_display}
- ICMS-SC (se houver): {st.session_state.futura_icms_sc_display}
TOTAL DO DÉBITO (BRL): {total_debito_importador}

VALORES ESTIMADOS PARA DEPÓSITOS E PAGAMENTOS PELA COMISSÁRIA DE DESPACHOS
1 - Assessoria do Processo Logístico Internacional: {st.session_state.futura_assessoria_logistica_display}
"""
    if st.session_state.futura_tipo_transporte == "Marítimo":
        email_body_plaintext += f"""2 - AFRMM - Adicional de Frete p/Renovação da Marinha Mercante: {st.session_state.futura_afrmm_comissaria_display}
3 - Remessa de Documentos para Liberação B/L/HAWB/CRT: {st.session_state.futura_remessa_documentos_display}
"""
    email_body_plaintext += f"""TOTAL DO DÉBITO (BRL): {total_debito_comissaria}

Diferença: {diferenca}

Data da conferência: {data_atual_formatada}

Obrigado,
{usuario_programa}
"""
    email_subject = f"{referencia_processo} - Conferência Futura"
    
    return email_subject, email_body_plaintext

def generate_payment_email_content():
    """Gera o conteúdo do e-mail para Pagamento de Honorários."""
    di_data = st.session_state.futura_di_data
    referencia_processo = di_data[6] if di_data and di_data[6] else "N/A"
    valor_total = st.session_state.futura_total_debito_comissaria # Usando o total da comissaria para o valor total

    current_hour = datetime.now().hour
    saudacao = "Bom dia" if 6 <= current_hour < 12 else "Boa tarde"
    usuario_programa = st.session_state.get('user_info', {}).get('username', 'usuário do sistema')

    email_body_plaintext = f"""{saudacao} Mayra,

Gentileza realizar depósito para a Futura:
Processo: {referencia_processo}
Valor total: {valor_total}

Serviço: honorários de despacho aduaneiro de importação.

Chave PIX: +55 47 999720387
Favorecido: Futura Despachos Aduaneiros Ltda
Banco: ITAÚ UNIBANCO S/A - 341
Agência: 0154
Conta Corrente: 20907-6
CNPJ: 15.010.021/0001-65

Conforme instruções em anexo.
Obs.: Invoice e DI da importação em anexo.

Obrigado(a),
{usuario_programa}
"""
    email_subject = f"{referencia_processo} - Pagamento de honorários Futura"
    return email_subject, email_body_plaintext

def generate_debit_email_content():
    """Gera o conteúdo do e-mail para Débito em Conta Impostos."""
    di_data = st.session_state.futura_di_data
    referencia_processo = di_data[6] if di_data and di_data[6] else "N/A"
    total_debito_importador = st.session_state.futura_total_debito_importador

    current_hour = datetime.now().hour
    saudacao = "Bom dia" if 6 <= current_hour < 12 else "Boa tarde"
    usuario_programa = st.session_state.get('user_info', {}).get('username', 'usuário do sistema')

    email_body_plaintext = f"""{saudacao} Mayra,

Aviso de débito em conta de impostos de importação:
Valor total: {total_debito_importador}
Processo: {referencia_processo}

Conforme instruções em anexo.
Obs.: Invoice e DI da importação em anexo.

Obrigado(a),
{usuario_programa}
"""
    email_subject = f"{referencia_processo} - Débito em conta impostos"
    return email_subject, email_body_plaintext
