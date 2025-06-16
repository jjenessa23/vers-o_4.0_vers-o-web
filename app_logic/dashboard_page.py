import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, date # Importar 'date' explicitamente
import logging
import altair as alt # Importar Altair para gráficos mais avançados
import os
import base64 # Importar base64 para codificar imagens

# Assuming db_manager is accessible or can be imported similarly to followup_importacao_page
try:
    import followup_db_manager as db_manager
except ImportError:
    st.error("Erro: Módulo 'followup_db_manager' não encontrado. Certifique-se de que o arquivo está no caminho correto.")
    st.stop() # Stop execution if essential module is missing

logger = logging.getLogger(__name__)

# --- Função para definir imagem de fundo com opacidade (copiada de app_main.py) ---
def set_background_image(image_path):
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

def _load_processes_for_dashboard():
    """Carrega todos os processos do DB para a dashboard."""
    if not db_manager.get_followup_db_path():
        st.warning("Caminho do banco de dados de Follow-up não configurado para a dashboard.")
        return []

    conn = db_manager.conectar_followup_db()
    if conn:
        try:
            # Ensure the new columns exist before querying
            db_manager.criar_tabela_followup(conn)
            processes_raw = db_manager.obter_todos_processos()
            processes_dicts = [dict(row) for row in processes_raw]
            return processes_dicts
        except Exception as e:
            st.error(f"Erro ao carregar dados para a dashboard: {e}")
            return []
        finally:
            conn.close()
    else:
        st.error("Não foi possível conectar ao banco de dados para a dashboard.")
        return []

def show_dashboard_page():
    # --- Configuração da Imagem de Fundo para o Dashboard ---
    # Certifique-se de que o caminho para a imagem esteja correto
    background_image_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'assets', 'logo_navio_atracado.png')
    set_background_image(background_image_path)
    # --- Fim da Configuração da Imagem de Fundo ---

    st.subheader("Dashboard de Follow-up")

    # Ensure DB connection and table creation for dashboard
    conn_check = db_manager.conectar_followup_db()
    if conn_check:
        try:
            # Add new columns if they don't exist
            db_manager.adicionar_coluna_se_nao_existe(conn_check, 'ETA_Recinto', 'TEXT')
            db_manager.adicionar_coluna_se_nao_existe(conn_check, 'Data_Registro', 'TEXT')
            # st.success("Colunas 'ETA_Recinto' e 'Data_Registro' verificadas/adicionadas no DB.") # Removed debug message
        except Exception as e:
            st.error(f"Erro ao adicionar/verificar novas colunas no DB: {e}")
        finally:
            conn_check.close()
    else:
        st.error(f"Não foi possível conectar ao banco de dados de Follow-up para a dashboard.")

    processes_data = _load_processes_for_dashboard()
    df = pd.DataFrame(processes_data)

    # Convert 'Data_Registro' to datetime objects (datetime64[ns])
    if not df.empty and 'Data_Registro' in df.columns:
        df['Data_Registro_dt'] = pd.to_datetime(df['Data_Registro'], errors='coerce')
    else:
        df['Data_Registro_dt'] = pd.NaT # Add a NaT column if no data or column missing

    # --- Análise de Status e Previsões (Movido para o início) ---
    if not df.empty:
        st.markdown("#### Análise de Status e Previsões")
        
        col_pie, col_bar = st.columns(2)

        with col_pie:
            st.markdown("##### Quantidade de Processos por Status")
            if 'Status_Geral' in df.columns and not df['Status_Geral'].empty:
                status_counts = df['Status_Geral'].value_counts().reset_index()
                status_counts.columns = ['Status_Geral', 'Quantidade']
                
                # MODIFICADO: Gráfico de Barras em vez de Pizza
                chart = alt.Chart(status_counts).mark_bar().encode(
                    x=alt.X("Status_Geral", type="nominal", title="Status"),
                    y=alt.Y("Quantidade", type="quantitative", title="Quantidade"),
                    tooltip=["Status_Geral", "Quantidade"],
                    
                    # MODIFICADO: Legenda para exibir Status e Quantidade
                    color=alt.Color("Status_Geral", legend=alt.Legend(
                        title="Status",
                        labelExpr= 'datum.value'   # Exibe 'Status (Quantidade)'
                    ))
                ).properties(
                    title="Processos por Status"
                )

                # Adicionar rótulos de texto sobre as barras
                text = chart.mark_text(
                    align='center',
                    baseline='bottom',
                    dy=-5 # Ajusta a posição do texto acima da barra
                ).encode(
                    text=alt.Text("Quantidade"),
                    color=alt.value("white") # Cor do texto para melhor visibilidade
                )
                
                st.altair_chart(chart + text, use_container_width=True)
            else:
                st.info("Nenhum processo com 'Status_Geral' para exibir.")
                


        with col_bar:
            st.markdown("##### Quantidade de Processos por Previsão na Pichau")
            # Filter out empty/invalid dates for plotting
            df_valid_previsao = df[df['Previsao_Pichau'].notna() & (df['Previsao_Pichau'] != '')].copy()
            
            if not df_valid_previsao.empty:
                # Convert to datetime for proper sorting
                df_valid_previsao['Previsao_Pichau_dt'] = pd.to_datetime(df_valid_previsao['Previsao_Pichau'], errors='coerce')
                df_valid_previsao = df_valid_previsao.dropna(subset=['Previsao_Pichau_dt'])

                if not df_valid_previsao.empty:
                    # Group by date and count
                    previsao_counts = df_valid_previsao['Previsao_Pichau_dt'].dt.date.value_counts().reset_index()
                    previsao_counts.columns = ['Data', 'Quantidade']
                    previsao_counts = previsao_counts.sort_values('Data')

                    st.bar_chart(previsao_counts, x='Data', y='Quantidade', color="#5DADE2")

                    # Total por mês abaixo do gráfico de barras
                    st.markdown("---")
                    st.markdown("###### Total de Processos por Mês (Previsão na Pichau)")
                    df_valid_previsao['Mes_Ano'] = df_valid_previsao['Previsao_Pichau_dt'].dt.to_period('M')
                    monthly_counts = df_valid_previsao['Mes_Ano'].value_counts().sort_index().reset_index()
                    monthly_counts.columns = ['Mês/Ano', 'Quantidade']
                    monthly_counts['Mês/Ano'] = monthly_counts['Mês/Ano'].astype(str) # Converter para string para exibição
                    st.dataframe(monthly_counts, hide_index=True, use_container_width=True)

                else:
                    st.info("Nenhum processo com 'Previsão na Pichau' válida para exibir.")
            else:
                st.info("Nenhum processo com 'Previsao na Pichau' para exibir.")
    else:
        st.info("Nenhum dado de processo disponível para gerar a dashboard. Importe processos para visualizar.")

    st.markdown("---")


    # --- Cálculos para os valores superiores ---
    # Garante que 'current_today' é um objeto date puro
    current_today = date.today() 
    
    # Seletor para a quantidade de dias
    days_option = st.selectbox(
        "Mostrar dados para os próximos:",
        options=[5, 10, 15, 20, 25, 30],
        index=0, # Padrão para 5 dias
        key="dashboard_days_selector"
    )
    st.markdown("---")

    end_date = current_today + timedelta(days=days_option - 1) 
    
    total_frete_usd_selected_days = 0.0
    total_impostos_br_selected_days = 0.0
    total_processes_selected_days = 0

    if not df.empty and 'Data_Registro_dt' in df.columns:
        # Converter current_today e end_date para pd.Timestamp para comparação direta com datetime64[ns]
        # Isso evita o TypeError.
        current_today_ts = pd.to_datetime(current_today)
        end_date_ts = pd.to_datetime(end_date)

        filtered_df_for_summary = df[
            (df['Data_Registro_dt'].notna()) &
            (df['Data_Registro_dt'] >= current_today_ts) & # Compara diretamente com Timestamp
            (df['Data_Registro_dt'] <= end_date_ts) # Compara diretamente com Timestamp
        ].copy()

        filtered_df_for_summary['Estimativa_Frete_USD'] = pd.to_numeric(filtered_df_for_summary['Estimativa_Frete_USD'], errors='coerce').fillna(0)
        filtered_df_for_summary['Estimativa_Impostos_BR'] = pd.to_numeric(filtered_df_for_summary['Estimativa_Impostos_BR'], errors='coerce').fillna(0)

        total_frete_usd_selected_days = filtered_df_for_summary['Estimativa_Frete_USD'].sum()
        total_impostos_br_selected_days = filtered_df_for_summary['Estimativa_Impostos_BR'].sum()
        total_processes_selected_days = len(filtered_df_for_summary)
    
    # REMOVIDO: O texto "Status Geral Embarcado", "Resumo dos Processos" e "Quantidade"
    # st.markdown("#### Status Geral Embarcado")
    # st.markdown(f"**Resumo dos Processos**")
    # st.markdown(f"**Quantidade:** {total_processes_selected_days}")

    st.markdown("#### Resumo dos Processos") 
    col_frete, col_impostos, col_total_processos, col_placeholder1, col_placeholder2 = st.columns(5)

    with col_frete:
        st.metric(label=f"Frete (USD) Próximos {days_option} Dias", value=f"US$ {total_frete_usd_selected_days:,.2f}".replace('.', '#').replace(',', '.').replace('#', ','))
    with col_impostos:
        st.metric(label=f"Impostos (R$) Próximos {days_option} Dias", value=f"R$ {total_impostos_br_selected_days:,.2f}".replace('.', '#').replace(',', '.').replace('#', ','))
    with col_total_processos:
        st.metric(label=f"Total de Processos Próximos {days_option} Dias", value=total_processes_selected_days)
    with col_placeholder1:
        st.write("")
    with col_placeholder2:
        st.write("")
    st.markdown("---")


    # --- Detalhes por Data de Registro (Próximos X Dias) ---
    st.markdown(f"#### Detalhes por Data de Registro (Próximos {days_option} Dias)")
    if not df.empty and 'Data_Registro_dt' in df.columns:
        daily_summary = {}
        for i in range(days_option): # Ajustado para incluir hoje + (N-1) dias
            current_date_loop = current_today + timedelta(days=i) # Renomeado para evitar conflito
            daily_summary[current_date_loop] = {'frete': 0.0, 'impostos': 0.0}

        # Ensure 'Estimativa_Frete_USD' and 'Estimativa_Impostos_BR' are numeric
        df['Estimativa_Frete_USD'] = pd.to_numeric(df['Estimativa_Frete_USD'], errors='coerce').fillna(0)
        df['Estimativa_Impostos_BR'] = pd.to_numeric(df['Estimativa_Impostos_BR'], errors='coerce').fillna(0)

        for index, row in df.iterrows():
            # Use .dt.date to get Python date objects for direct comparison with `current_date_loop`
            data_registro_date = row['Data_Registro_dt'].date() if pd.notna(row['Data_Registro_dt']) else None
            
            if data_registro_date and data_registro_date in daily_summary:
                daily_summary[data_registro_date]['frete'] += row['Estimativa_Frete_USD']
                daily_summary[data_registro_date]['impostos'] += row['Estimativa_Impostos_BR']
                
        # Sort the dictionary by date
        sorted_daily_summary = sorted(daily_summary.items())

        # Display in columns, 5 per row
        cols_per_row = 5
        
        # Iterate and create columns for each row
        for i in range(0, len(sorted_daily_summary), cols_per_row):
            current_row_data = sorted_daily_summary[i : i + cols_per_row]
            cols = st.columns(cols_per_row)
            for j, (date_key, values) in enumerate(current_row_data): # Renomeado 'date' para 'date_key'
                with cols[j]:
                    st.markdown(f"**{date_key.strftime('%d/%m')}**") # Usando date_key
                    st.markdown(f"Frete (USD): US$ {values['frete']:,.2f}".replace('.', '#').replace(',', '.').replace('#', ','))
                    st.markdown(f"Impostos (BRL): R$ {values['impostos']:,.2f}".replace('.', '#').replace(',', '.').replace('#', ','))
            
            # Add a spacer after each full row of 5 days, but not after the last row
            if (i + cols_per_row) < len(sorted_daily_summary):
                st.markdown("---") # Visual separator

    else:
        st.info("Nenhum dado de 'Data_Registro' ou DataFrame vazio para exibir detalhes diários.")
    st.markdown("---")

    st.write("Esta dashboard oferece uma visão geral dos processos de importação.")
    
