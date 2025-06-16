import streamlit as st
import pandas as pd
import logging
import os
import base64
import re
from typing import Optional, Any, Dict, List

import followup_db_manager as db_manager
# Assuming ncm_list_page is in the same app_logic directory
try:
    from app_logic import ncm_list_page
except ImportError:
    logging.warning("Módulo 'ncm_list_page' não encontrado. Funções NCM podem não estar disponíveis.")
    ncm_list_page = None

logger = logging.getLogger(__name__)

def set_background_image(image_path: str):
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

def show_produtos_page():
    """Exibe a tela de gerenciamento de Produtos."""
    background_image_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'assets', 'logo_navio_atracado.png')
    set_background_image(background_image_path)

    st.subheader("Gerenciamento de Produtos")

    # Inicialização dos estados de sessão para filtros
    if 'produtos_filter_codigo_interno' not in st.session_state:
        st.session_state.produtos_filter_codigo_interno = ""
    if 'produtos_filter_denominacao' not in st.session_state:
        st.session_state.produtos_filter_denominacao = ""
    if 'produtos_filter_sku' not in st.session_state:
        st.session_state.produtos_filter_sku = ""
    if 'produtos_filter_ncm' not in st.session_state:
        st.session_state.produtos_filter_ncm = ""

    # Botão de retorno à página de Follow-up Importação
    if st.button("Voltar para Follow-up Importação"):
        st.session_state.current_page = "Follow-up Importação"
        st.rerun()

    st.markdown("---")

    # Expander para filtros
    with st.expander("Filtros de Produtos"):
        col1, col2 = st.columns(2)
        with col1:
            st.session_state.produtos_filter_codigo_interno = st.text_input(
                "Filtrar por Código Interno:",
                value=st.session_state.produtos_filter_codigo_interno,
                key="filter_codigo_interno"
            )
            st.session_state.produtos_filter_sku = st.text_input(
                "Filtrar por SKU:",
                value=st.session_state.produtos_filter_sku,
                key="filter_sku"
            )
        with col2:
            st.session_state.produtos_filter_denominacao = st.text_input(
                "Filtrar por Denominação:",
                value=st.session_state.produtos_filter_denominacao,
                key="filter_denominacao"
            )
            st.session_state.produtos_filter_ncm = st.text_input(
                "Filtrar por NCM:",
                value=st.session_state.produtos_filter_ncm,
                key="filter_ncm"
            )
        
        # Botão para aplicar filtros (pode ser automático via st.text_input, mas um botão dá controle)
        if st.button("Aplicar Filtros"):
            st.rerun() # Dispara um rerun para aplicar os filtros

        if st.button("Limpar Filtros"):
            st.session_state.produtos_filter_codigo_interno = ""
            st.session_state.produtos_filter_denominacao = ""
            st.session_state.produtos_filter_sku = ""
            st.session_state.produtos_filter_ncm = ""
            st.rerun()

    st.markdown("---")

    # Carregar todos os itens de processo com a referência do processo
    all_process_items_raw = db_manager.get_all_process_items_with_process_ref()
    
    if not all_process_items_raw:
        st.info("Nenhum produto encontrado. Adicione itens aos processos para que apareçam aqui.")
        return

    df_items = pd.DataFrame(all_process_items_raw)

    # Adicionar a coluna 'Status_Geral' se não existir, com um valor padrão
    if 'Status_Geral' not in df_items.columns:
        df_items['Status_Geral'] = 'Status Desconhecido'

    # Limpar e formatar o NCM para filtragem
    if 'ncm' in df_items.columns:
        df_items['ncm_cleaned'] = df_items['ncm'].astype(str).apply(lambda x: re.sub(r'\D', '', x) if x else '')
    else:
        df_items['ncm_cleaned'] = '' # Add if column doesn't exist to avoid error

    # Aplicar filtros
    filtered_df_items = df_items.copy()

    if st.session_state.produtos_filter_codigo_interno:
        filtered_df_items = filtered_df_items[
            filtered_df_items['codigo_interno'].astype(str).str.contains(
                st.session_state.produtos_filter_codigo_interno, case=False, na=False
            )
        ]
    if st.session_state.produtos_filter_denominacao:
        filtered_df_items = filtered_df_items[
            filtered_df_items['denominacao_produto'].astype(str).str.contains(
                st.session_state.produtos_filter_denominacao, case=False, na=False
            )
        ]
    if st.session_state.produtos_filter_sku:
        filtered_df_items = filtered_df_items[
            filtered_df_items['sku'].astype(str).str.contains(
                st.session_state.produtos_filter_sku, case=False, na=False
            )
        ]
    if st.session_state.produtos_filter_ncm:
        # Filter on the cleaned NCM code
        filtered_df_items = filtered_df_items[
            filtered_df_items['ncm_cleaned'].astype(str).str.contains(
                re.sub(r'\D', '', st.session_state.produtos_filter_ncm), case=False, na=False
            )
        ]
    
    if filtered_df_items.empty:
        st.info("Nenhum produto encontrado com os filtros aplicados.")
        return

    # Preparar DataFrame para exibição
    # Formatação de NCM para exibição
    if 'ncm' in filtered_df_items.columns and ncm_list_page:
        filtered_df_items['NCM Formatado'] = filtered_df_items['ncm'].apply(lambda x: ncm_list_page.format_ncm_code(str(x)) if x else '')
    else:
        filtered_df_items['NCM Formatado'] = filtered_df_items['ncm'].astype(str) if 'ncm' in filtered_df_items.columns else ''


    # Renomear colunas para exibição amigável
    display_df = filtered_df_items[[
        "Processo_Novo", "Status_Geral", "codigo_interno", "denominacao_produto", "sku", "NCM Formatado",
        "quantidade", "valor_unitario", "valor_total_item", "peso_unitario"
    ]].rename(columns={
        "Processo_Novo": "Referência do Processo",
        "Status_Geral": "Status do Processo", # Adicionada a nova coluna
        "codigo_interno": "Código Interno",
        "denominacao_produto": "Denominação do Produto",
        "sku": "SKU",
        "NCM Formatado": "NCM",
        "quantidade": "Quantidade",
        "valor_unitario": "Valor Unitário (USD)",
        "valor_total_item": "Valor Total Item (USD)",
        "peso_unitario": "Peso Unitário (KG)"
    })

    # Formatar valores numéricos para exibição
    for col in ["Quantidade", "Valor Unitário (USD)", "Valor Total Item (USD)", "Peso Unitário (KG)"]:
        if col in display_df.columns:
            if "Quantidade" in col:
                display_df[col] = display_df[col].apply(lambda x: f"{x:.0f}" if pd.notna(x) else "")
            elif "Peso" in col: # Peso Unitário
                 display_df[col] = display_df[col].apply(lambda x: f"{x:,.4f}".replace('.', '#').replace(',', '.').replace('#', ',') if pd.notna(x) else "0,0000")
            else: # Valores monetários
                display_df[col] = display_df[col].apply(lambda x: f"{x:,.2f}".replace('.', '#').replace(',', '.').replace('#', ',') if pd.notna(x) else "0,00")

    st.markdown("#### Lista de Produtos")
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Referência do Processo": st.column_config.TextColumn("Referência do Processo", width="small"),
            "Status do Processo": st.column_config.TextColumn("Status do Processo", width="small"), # Configuração da nova coluna
            "Código Interno": st.column_config.TextColumn("Código Interno", width="small"),
            "Denominação do Produto": st.column_config.TextColumn("Denominação do Produto", width="medium"),
            "SKU": st.column_config.TextColumn("SKU", width="small"),
            "NCM": st.column_config.TextColumn("NCM", width="small"),
            "Quantidade": st.column_config.TextColumn("Quantidade", width="small"),
            "Valor Unitário (USD)": st.column_config.TextColumn("Valor Unitário (USD)", width="small"),
            "Valor Total Item (USD)": st.column_config.TextColumn("Valor Total Item (USD)", width="small"),
            "Peso Unitário (KG)": st.column_config.TextColumn("Peso Unitário (KG)", width="small"),
        }
    )

    st.markdown("---")
    st.write("Esta tela exibe todos os produtos registrados nos processos de importação, permitindo a filtragem por diversos atributos.")
