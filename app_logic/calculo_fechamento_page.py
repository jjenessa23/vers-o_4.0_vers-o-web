import streamlit as st
import pandas as pd
import logging
import os
from datetime import datetime

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

def _format_float(value, decimals=6):
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

def perform_fechamento_calculations():
    """Realiza os cálculos para a tela de Fechamento."""
    if 'fechamento_di_data' not in st.session_state or not st.session_state.fechamento_di_data:
        logger.warning("Não há dados da DI para realizar cálculos (Fechamento).")
        return

    di_data = st.session_state.fechamento_di_data

    # Desempacota os dados da DI (29 campos)
    (id_db, numero_di, data_registro_db, valor_total_reais_xml,
     arquivo_origem, data_importacao, informacao_complementar,
     vmle, frete_di, seguro_di, vmld, ipi, pis_pasep, cofins, icms_sc,
     taxa_cambial_usd, taxa_siscomex, numero_invoice, peso_bruto, peso_liquido,
     cnpj_importador, importador_nome, recinto, embalagem, quantidade_volumes, acrescimo,
     imposto_importacao, armazenagem_db, frete_nacional_db) = di_data

    # --- Obter valores dos campos editáveis e labels ---
    # Lendo diretamente da chave do widget no session_state
    try:
        valor_nfs_float = float(st.session_state.fechamento_valor_nfs_input.replace('R$', '').replace('.', '').replace(',', '.').strip())
    except ValueError:
        valor_nfs_float = 0.0
        logger.warning("Valor NFs inválido, usando 0.0 para cálculo.")
    
    try:
        # Lendo diretamente da chave do widget no session_state
        afrmm_float = float(st.session_state.fechamento_afrmm_input.replace('R$', '').replace('.', '').replace(',', '.').strip())
    except ValueError:
        afrmm_float = 0.0
        logger.warning("AFRMM inválido, usando 0.0 para cálculo.")

    armazenagem_float = armazenagem_db if armazenagem_db is not None else 0.0
    frete_nacional_float = frete_nacional_db if frete_nacional_db is not None else 0.0

    # Lendo diretamente da chave do widget no session_state
    try:
        frete_internacional_pago_float = float(st.session_state.fechamento_frete_internacional_pago_input.replace('R$', '').replace('.', '').replace(',', '.').strip())
    except ValueError:
        frete_internacional_pago_float = 0.0
        logger.warning("Frete Internacional Pago inválido, usando 0.0 para cálculo.")

    # --- Cálculos dos Impostos ---
    total_impostos = imposto_importacao + ipi + pis_pasep + cofins
    st.session_state.fechamento_total_impostos_display = _format_currency(total_impostos)

    # --- Cálculo de TAXAS DESTINO ---
    taxas_destino_calculado = frete_internacional_pago_float - frete_di
    st.session_state.fechamento_taxas_destino_display = _format_currency(taxas_destino_calculado)

    # --- Cálculo do TOTAL DESPESAS ---
    despachante_fixo = 1000.00
    siscomex_fixo = taxa_siscomex
    connecta_fixo = 0.00
    descarregamento_fixo = 0.00 # Assumindo 0 se não tiver valor
    icms_4_percent_fixo = 0.00
    envio_docs_fixo = 0.00 # Valor fixo da template

    total_despesas = (afrmm_float + armazenagem_float + envio_docs_fixo + frete_nacional_float +
                      despachante_fixo + siscomex_fixo + connecta_fixo +
                      descarregamento_fixo + taxas_destino_calculado + icms_4_percent_fixo)
    
    st.session_state.fechamento_total_despesas_display = _format_currency(total_despesas)

    # Atualiza os valores de exibição das despesas
    st.session_state.fechamento_afrmm_display = _format_currency(afrmm_float)
    st.session_state.fechamento_armazenagem_display = _format_currency(armazenagem_float)
    st.session_state.fechamento_frete_nacional_display = _format_currency(frete_nacional_float)
    st.session_state.fechamento_siscomex_display = _format_currency(siscomex_fixo)
    st.session_state.fechamento_envio_docs_display = _format_currency(envio_docs_fixo)
    st.session_state.fechamento_despachante_display = _format_currency(despachante_fixo)
    st.session_state.fechamento_connecta_display = _format_currency(connecta_fixo)
    st.session_state.fechamento_descarregamento_display = "R$ -" if descarregamento_fixo == 0 else _format_currency(descarregamento_fixo)
    st.session_state.fechamento_icms_4_percent_display = _format_currency(icms_4_percent_fixo)


    # --- Cálculos dos Totais Finais ---
    st.session_state.fechamento_total_mercadoria_display = _format_currency(vmle)

    total_adicionais_final = total_impostos + total_despesas + seguro_di + frete_di
    st.session_state.fechamento_total_adicionais_display = _format_currency(total_adicionais_final)

    total_nfs_calculado = vmle + total_adicionais_final
    st.session_state.fechamento_total_nfs_calculado_display = _format_currency(total_nfs_calculado)

    # MODIFICADO: A conta da diferença agora é (Valor NFs - TOTAL NFS)
    diferenca_calculada = valor_nfs_float - total_nfs_calculado
    st.session_state.fechamento_diferenca_final_value = _format_currency(diferenca_calculada)

    # Força a re-execução da página para atualizar os valores exibidos
    st.rerun()


def load_fechamento_di_data(declaracao_id):
    """
    Carrega os dados da DI para a tela de Fechamento e inicializa o estado da sessão.
    """
    if not declaracao_id:
        logger.warning("Nenhum ID de declaração fornecido para carregar dados (Fechamento).")
        clear_fechamento_di_data()
        return

    logger.info(f"Carregando dados para DI ID (Fechamento): {declaracao_id}")
    di_data_row = get_declaracao_by_id(declaracao_id)

    if di_data_row:
        di_data = tuple(di_data_row)
        st.session_state.fechamento_di_data = di_data
        
        # Desempacota os dados (29 campos)
        (id_db, numero_di, data_registro_db, valor_total_reais_xml,
         arquivo_origem, data_importacao, informacao_complementar,
         vmle, frete, seguro, vmld, ipi, pis_pasep, cofins, icms_sc,
         taxa_cambial_usd, taxa_siscomex, numero_invoice, peso_bruto, peso_liquido,
         cnpj_importador, importador_nome, recinto, embalagem, quantidade_volumes, acrescimo,
         imposto_importacao, armazenagem_db, frete_nacional_db) = di_data

        st.session_state.fechamento_processo_ref = f"Processo : {informacao_complementar if informacao_complementar else 'N/A'}"
        
        # Inicializa os campos editáveis (agora usando as chaves dos widgets diretamente para consistência)
        st.session_state.fechamento_valor_nfs_input = _format_currency(0.00) # Inicializa a chave do widget
        st.session_state.fechamento_afrmm_input = _format_currency(0.00) # Inicializa a chave do widget
        st.session_state.fechamento_frete_internacional_pago_input = _format_currency(frete) # Inicializa a chave do widget com o frete da DI

        # Atualiza os labels da seção "Base de Cálculo"
        st.session_state.fechamento_valor_mercadoria_display = _format_currency(valor_total_reais_xml)
        st.session_state.fechamento_fatura_comercial_display = _format_currency(valor_total_reais_xml)
        st.session_state.fechamento_acrescimo_display = _format_currency(acrescimo)
        st.session_state.fechamento_vmle_display = _format_currency(vmle)
        st.session_state.fechamento_frete_internacional_display = _format_currency(frete)
        st.session_state.fechamento_seguro_display = _format_currency(seguro)
        st.session_state.fechamento_cif_display = _format_currency(vmld)

        # Atualiza os labels da seção "IMPOSTOS"
        st.session_state.fechamento_ii_display = _format_currency(imposto_importacao)
        st.session_state.fechamento_ipi_display = _format_currency(ipi)
        st.session_state.fechamento_pis_display = _format_currency(pis_pasep)
        st.session_state.fechamento_cofins_display = _format_currency(cofins)

        # Armazenagem e Frete Nacional do DB
        st.session_state.fechamento_armazenagem_display = _format_currency(armazenagem_db)
        st.session_state.fechamento_frete_nacional_display = _format_currency(frete_nacional_db)

        perform_fechamento_calculations() # Realiza os cálculos iniciais
    else:
        st.warning(f"Nenhum dado encontrado para a DI ID: {declaracao_id} (Fechamento)")
        clear_fechamento_di_data()

def clear_fechamento_di_data():
    """Limpa todos os dados e estados da sessão para a tela de Fechamento."""
    st.session_state.fechamento_di_data = None
    st.session_state.fechamento_processo_ref = "PCH-XXXX-XX"

    st.session_state.fechamento_valor_nfs_input = _format_currency(0.00) # Limpa a chave do widget
    st.session_state.fechamento_afrmm_input = _format_currency(0.00) # Limpa a chave do widget
    st.session_state.fechamento_frete_internacional_pago_input = _format_currency(0.00) # Limpa a chave do widget

    # Limpar valores de exibição
    st.session_state.fechamento_valor_mercadoria_display = "R$ 0,00"
    st.session_state.fechamento_fatura_comercial_display = "R$ 0,00"
    st.session_state.fechamento_acrescimo_display = "R$ 0,00"
    st.session_state.fechamento_vmle_display = "R$ 0,00"
    st.session_state.fechamento_frete_internacional_display = "R$ 0,00"
    st.session_state.fechamento_seguro_display = "R$ 0,00"
    st.session_state.fechamento_cif_display = "R$ 0,00"

    st.session_state.fechamento_ii_display = "R$ 0,00"
    st.session_state.fechamento_ipi_display = "R$ 0,00"
    st.session_state.fechamento_pis_display = "R$ 0,00"
    st.session_state.fechamento_cofins_display = "R$ 0,00"
    st.session_state.fechamento_total_impostos_display = "R$ 0,00"

    st.session_state.fechamento_afrmm_display = "R$ 0,00"
    st.session_state.fechamento_armazenagem_display = "R$ 0,00"
    st.session_state.fechamento_envio_docs_display = "R$ 0,00"
    st.session_state.fechamento_frete_nacional_display = "R$ 0,00"
    st.session_state.fechamento_despachante_display = "R$ 0,00"
    st.session_state.fechamento_siscomex_display = "R$ 0,00"
    st.session_state.fechamento_connecta_display = "R$ 0,00"
    st.session_state.fechamento_descarregamento_display = "R$ -"
    st.session_state.fechamento_taxas_destino_display = "R$ 0,00"
    st.session_state.fechamento_icms_4_percent_display = "R$ 0,00"
    st.session_state.fechamento_total_despesas_display = "R$ 0,00"

    st.session_state.fechamento_total_mercadoria_display = "R$ 0,00"
    st.session_state.fechamento_total_adicionais_display = "R$ 0,00"
    st.session_state.fechamento_total_nfs_calculado_display = "R$ 0,00"
    st.session_state.fechamento_diferenca_final_value = "R$ 0,00"

# --- Funções de utilidade para o fundo (simulando app_logic.utils) ---
# Esta é a versão mais robusta de set_background_image para garantir que o fundo apareça
def set_background_image_local(image_path, opacity=None):
    """Define uma imagem de fundo para o aplicativo Streamlit com opacidade."""
    if not os.path.exists(image_path):
        logging.error(f"Erro: Imagem de fundo não encontrada no caminho: {image_path}")
        st.warning(f"A imagem de fundo não foi encontrada no caminho: {image_path}") # Adiciona um aviso visível no Streamlit
        return # Sai da função se a imagem não for encontrada

    opacity_style = f"opacity: {opacity};" if opacity is not None else ""
    st.markdown(
        f"""
        <style>
        .stApp {{
            background-image: url("data:image/png;base64,{_get_base64_image(image_path)}");
            background-size: cover;
            background-repeat: no-repeat;
            background-attachment: fixed;
            background-position: center;
            {opacity_style}
        }}
        </style>
        """,
        unsafe_allow_html=True
    )

import base64
def _get_base64_image(image_path):
    """Converte uma imagem para base64."""
    with open(image_path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode()

# Tenta importar as funções de utils, caso contrário, usa os fallbacks locais
try:
    from app_logic.utils import set_background_image, get_default_background_opacity
except ImportError:
    logging.warning("Módulo 'app_logic.utils' não encontrado. Usando funções de imagem de fundo locais.")
    set_background_image = set_background_image_local # Usa a função local como fallback
    def get_default_background_opacity():
        return 0.20 # Opacidade padrão mockada para o fundo


def show_calculo_fechamento_page():
    """
    Exibe a interface de usuário para o cálculo de Fechamento.
    """
    st.subheader("Pichau Conferência - Fechamento")

    # Configuração da Imagem de Fundo para a página
    # Caminho mais robusto para a raiz do projeto
    # Assumindo que 'Nova Estrutura' é o diretório raiz do seu projeto
    # e que calculo_fechamento_page.py está em Nova Estrutura/app_logic/
    script_dir = os.path.dirname(os.path.abspath(__file__)) # Obtém o diretório absoluto do script atual
    # Volta dois níveis para a raiz do projeto "Nova Estrutura"
    background_image_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'assets', 'logo_navio_atracado.png')
    set_background_image(background_image_path)
    # Define a imagem de fundo com a opacidade obtida de get_default_background_opacity()
    


    # Inicializa o estado da sessão para esta página
    if 'fechamento_di_data' not in st.session_state:
        clear_fechamento_di_data()

    # Carrega os dados da DI se um ID foi passado da página anterior
    if 'selected_di_id_fechamento' in st.session_state and st.session_state.selected_di_id_fechamento and st.session_state.fechamento_di_data is None:
        load_fechamento_di_data(st.session_state.selected_di_id_fechamento)
        st.session_state.selected_di_id_fechamento = None # Limpa o ID após carregar

    st.markdown(f"#### {st.session_state.fechamento_processo_ref}")
    st.markdown("---")

    # Reorganização das colunas para BASE DE CÁLCULO e IMPOSTOS
    # Ajustando a proporção para dar mais espaço, se necessário
    col1_base_calc, col2_despesas = st.columns([0.6, 0.3]) # Ajuste de largura das colunas.

    with col1_base_calc:
        st.markdown("###### BASE DE CÁLCULO")
        base_calc_data = {
            "Item": ["VALOR MERCADORIA", "AMOSTRA/PEÇAS", "FATURA COMERCIAL", "ACRESCIMO", "ADICIONAIS BL", "VMLE", "FRETE INTERNACIONAL", "SEGURO", "CIF"],
            "Valor": [
                str(st.session_state.fechamento_valor_mercadoria_display), # Convertido para string
                "R$ 0,00", # Valor fixo
                str(st.session_state.fechamento_fatura_comercial_display), # Convertido para string
                str(st.session_state.fechamento_acrescimo_display), # Convertido para string
                "R$ 0,00", # Valor fixo
                str(st.session_state.fechamento_vmle_display), # Convertido para string
                str(st.session_state.fechamento_frete_internacional_display), # Convertido para string
                str(st.session_state.fechamento_seguro_display), # Convertido para string
                str(st.session_state.fechamento_cif_display) # Convertido para string
            ],
            "Descrição": [
                "Valor da mercadoria sem amostras e peças",
                "Peças de Reposição",
                "Invoice",
                "",
                "THC, Amostras e Peças",
                "marítimos",
                "Valor pago a Ethima que compõe base de cálculo dos impostos",
                "Ação Seguros",
                "FOB+FRETE INTERNACIONAL+SEGURO"
            ]
        }
        df_base_calc = pd.DataFrame(base_calc_data)
        st.dataframe(df_base_calc, hide_index=True, use_container_width=True)

        # MOVENDO: IMPOSTOS para abaixo de BASE DE CÁLCULO
        st.markdown("###### IMPOSTOS")
        impostos_data = {
            "Item": ["II", "IPI", "PIS", "COFINS", "TOTAL"],
            "Valor": [
                str(st.session_state.fechamento_ii_display), # Convertido para string
                str(st.session_state.fechamento_ipi_display), # Convertido para string
                str(st.session_state.fechamento_pis_display), # Convertido para string
                str(st.session_state.fechamento_cofins_display), # Convertido para string
                str(st.session_state.fechamento_total_impostos_display) # Convertido para string
            ],
            "Descrição": [
                "Base de cálculo: CIF",
                "Base de cálculo: CIF+II",
                "Base de cálculo: CIF",
                "Base de cálculo: CIF",
                ""
            ]
        }
        df_impostos = pd.DataFrame(impostos_data)
        st.dataframe(df_impostos, hide_index=True, use_container_width=True)


    with col2_despesas: # Esta coluna agora conterá apenas DESPESAS
        st.markdown("###### DESPESAS")
        # Campos editáveis e labels
        afrmm_input = st.text_input(
            "AFRMM",
            value=st.session_state.fechamento_afrmm_input, # Lendo da chave do widget
            key="fechamento_afrmm_input",
            on_change=perform_fechamento_calculations
        )

        # Usando st.columns para organizar as despesas em pares de label e valor
        # Isso pode ajudar a evitar o "esmagamento"
        st.markdown("") # Espaço em branco para melhor alinhamento
        st.markdown(f"**ARMAZENAGEM**: {st.session_state.fechamento_armazenagem_display}")
        st.markdown(f"**ENVIO DE DOCS**: {st.session_state.fechamento_envio_docs_display}")
        st.markdown(f"**FRETE NACIONAL**: {st.session_state.fechamento_frete_nacional_display}")
        st.markdown(f"**DESPACHANTE**: {st.session_state.fechamento_despachante_display}")
        st.markdown(f"**SISCOMEX**: {st.session_state.fechamento_siscomex_display}")
        st.markdown(f"**CONNECTA**: {st.session_state.fechamento_connecta_display}")
        st.markdown(f"**DESCARREGAMENTO**: {st.session_state.fechamento_descarregamento_display}")
        st.markdown(f"**TAXAS DESTINO**: {st.session_state.fechamento_taxas_destino_display}")
        st.markdown(f"**ICMS 4%**: {st.session_state.fechamento_icms_4_percent_display}")
        st.markdown(f"**TOTAL DESPESAS**: {st.session_state.fechamento_total_despesas_display}")


    st.markdown("---")
    col1, con2 = st.columns(2)
    with col1:
        st.markdown("###### Totais Finais")
        # Usando uma única coluna para Totais Finais para evitar problemas de layout
        # que podem causar o erro removeChild
        with st.container(): # Usando um container para agrupar os elementos
            st.markdown(f"- **Frete Internacional Pago:**")
            st.text_input(
                "Frete Internacional Pago",
                value=st.session_state.fechamento_frete_internacional_pago_input,
                key="fechamento_frete_internacional_pago_input",
                on_change=perform_fechamento_calculations,
                label_visibility="collapsed"
            )

            st.markdown(f"- **TOTAL MERCADORIA:** {st.session_state.fechamento_total_mercadoria_display}")
            st.markdown(f"- **TOTAL ADICIONAIS FOB + FRETE + SEGURO + IMPOSTOS + DESPESAS:** {st.session_state.fechamento_total_adicionais_display}")
            
            st.markdown(f"- **TOTAL NFS:** {st.session_state.fechamento_total_nfs_calculado_display}")

            st.markdown(f"- **Valor NFs:**")
            st.text_input(
                "Valor NFs",
                value=st.session_state.fechamento_valor_nfs_input,
                key="fechamento_valor_nfs_input",
                on_change=perform_fechamento_calculations,
                label_visibility="collapsed"
            )

            st.markdown(f"- **Diferença:** {st.session_state.fechamento_diferenca_final_value}")
        

    st.markdown("---")
    if st.button("Voltar para Detalhes da DI", key="fechamento_voltar_di"):
        st.session_state.current_page = "Pagamentos"
        st.rerun()
