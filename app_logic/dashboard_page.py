import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, date # Importar 'date' explicitamente
import logging
import altair as alt # Importar Altair para gráficos mais avançados
import os
import base64 # Importar base64 para codificar imagens

# Importar db_utils diretamente
try:
    from app_logic import db_utils
except ImportError:
    st.error("Erro: Módulo 'db_utils' não encontrado. Certifique-se de que o arquivo está no caminho correto.")
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

# Função para obter todos os processos de followup (volta a usar esta coleção)
def obter_todos_processos_followup_firestore():
    """Obtém todos os processos de followup do Firestore."""
    if not st.session_state.get('firebase_ready', False) or db_utils.db_firestore is None:
        logger.error("Firestore não está pronto para obter processos de followup.")
        return []
    
    followup_ref = db_utils.get_firestore_collection_ref("followup_processos")
    if not followup_ref:
        logger.error("Não foi possível obter a referência da coleção 'followup_processos' no Firestore.")
        return []
    
    try:
        docs = followup_ref.stream()
        processes = []
        for doc in docs:
            data = doc.to_dict()
            data['id'] = doc.id # Adiciona o ID do documento ao dicionário
            processes.append(data)
        logger.info(f"Obtidos {len(processes)} processos de followup do Firestore.")
        return processes
    except Exception as e:
        logger.error(f"Erro ao obter todos os processos de followup do Firestore: {e}")
        return []


def _load_processes_for_dashboard():
    """Carrega todos os processos do DB (follow-up) para a dashboard usando Firestore."""
    processes_dicts = obter_todos_processos_followup_firestore() # Usa a função que busca do follow-up
    return processes_dicts

def show_dashboard_page():
    # --- Configuração da Imagem de Fundo para o Dashboard ---
    background_image_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'assets', 'logo_navio_atracado.png')
    set_background_image(background_image_path)
    # --- Fim da Configuração da Imagem de Fundo ---

    st.subheader("Dashboard de Follow-up")

    if not st.session_state.get('firebase_ready', False):
        st.error("O Firebase não está conectado. Por favor, verifique a configuração e reinicie o aplicativo.")
        return # Impede a execução do restante da dashboard sem conexão com o banco

    # Dados para a seção de Status e Previsões (do Follow-up)
    processes_data = _load_processes_for_dashboard()
    df_followup = pd.DataFrame(processes_data) # Renomeado para df_followup para clareza

    # Convert 'Data_Registro' to datetime objects (datetime64[ns]) for followup data
    if not df_followup.empty and 'Data_Registro' in df_followup.columns:
        df_followup['Data_Registro_dt'] = pd.to_datetime(df_followup['Data_Registro'], errors='coerce')
    else:
        df_followup['Data_Registro_dt'] = pd.NaT 

    # --- Análise de Status e Previsões (DO FOLLOW-UP) ---
    if not df_followup.empty:
        st.markdown("#### Análise de Status e Previsões")
        
        col_pie, col_bar = st.columns(2)

        with col_pie:
            st.markdown("##### Quantidade de Processos por Status")
            if 'Status_Geral' in df_followup.columns and not df_followup['Status_Geral'].empty:
                status_counts = df_followup['Status_Geral'].value_counts().reset_index()
                status_counts.columns = ['Status_Geral', 'Quantidade']
                
                chart = alt.Chart(status_counts).mark_bar().encode(
                    x=alt.X("Status_Geral", type="nominal", title="Status"),
                    y=alt.Y("Quantidade", type="quantitative", title="Quantidade"),
                    tooltip=["Status_Geral", "Quantidade"],
                    color=alt.Color("Status_Geral", legend=alt.Legend(
                        title="Status",
                        labelExpr= 'datum.value'
                    ))
                ).properties(
                    title="Processos por Status"
                )

                text = chart.mark_text(
                    align='center',
                    baseline='bottom',
                    dy=-5
                ).encode(
                    text=alt.Text("Quantidade"),
                    color=alt.value("white")
                )
                
                st.altair_chart(chart + text, use_container_width=True)
            else:
                st.info("Nenhum processo com 'Status_Geral' para exibir.")
                
        with col_bar:
            st.markdown("##### Quantidade de Processos por Previsão na Pichau")
            df_valid_previsao = df_followup[df_followup['Previsao_Pichau'].notna() & (df_followup['Previsao_Pichau'] != '')].copy()
            
            if not df_valid_previsao.empty:
                df_valid_previsao['Previsao_Pichau_dt'] = pd.to_datetime(df_valid_previsao['Previsao_Pichau'], errors='coerce')
                df_valid_previsao = df_valid_previsao.dropna(subset=['Previsao_Pichau_dt'])

                if not df_valid_previsao.empty:
                    previsao_counts = df_valid_previsao['Previsao_Pichau_dt'].dt.date.value_counts().reset_index()
                    previsao_counts.columns = ['Data', 'Quantidade']
                    previsao_counts = previsao_counts.sort_values('Data')

                    st.bar_chart(previsao_counts, x='Data', y='Quantidade', color="#5DADE2")

                    st.markdown("---")
                    st.markdown("###### Total de Processos por Mês (Previsão na Pichau)")
                    df_valid_previsao['Mes_Ano'] = df_valid_previsao['Previsao_Pichau_dt'].dt.to_period('M')
                    monthly_counts = df_valid_previsao['Mes_Ano'].value_counts().sort_index().reset_index()
                    monthly_counts.columns = ['Mês/Ano', 'Quantidade']
                    monthly_counts['Mês/Ano'] = monthly_counts['Mês/Ano'].astype(str)
                    st.dataframe(monthly_counts, hide_index=True, use_container_width=True)

                else:
                    st.info("Nenhum processo com 'Previsão na Pichau' válida para exibir.")
            else:
                st.info("Nenhum processo com 'Previsao na Pichau' para exibir.")
    else:
        st.info("Nenhum dado de processo de follow-up disponível para gerar a dashboard. Importe processos para visualizar.")

    st.markdown("---")

    # --- Cálculos para os valores superiores (DO FOLLOW-UP) ---
    current_today = date.today() 
    
    days_option = st.selectbox(
        "Mostrar dados para os próximos (Follow-up):", # Ajustado para indicar que é do Follow-up
        options=[5, 10, 15, 20, 25, 30],
        index=0,
        key="dashboard_days_selector_followup" # Chave única
    )
    st.markdown("---")

    end_date = current_today + timedelta(days=days_option - 1) 
    
    total_frete_usd_selected_days_followup = 0.0
    total_impostos_br_selected_days_followup = 0.0
    total_processes_selected_days_followup = 0

    if not df_followup.empty and 'Data_Registro_dt' in df_followup.columns:
        current_today_ts = pd.to_datetime(current_today)
        end_date_ts = pd.to_datetime(end_date)

        filtered_df_for_summary_followup = df_followup[
            (df_followup['Data_Registro_dt'].notna()) &
            (df_followup['Data_Registro_dt'] >= current_today_ts) &
            (df_followup['Data_Registro_dt'] <= end_date_ts)
        ].copy()

        # Usar Estimativa_Frete_USD e Estimativa_Impostos_BR do df_followup
        filtered_df_for_summary_followup['Estimativa_Frete_USD'] = pd.to_numeric(filtered_df_for_summary_followup['Estimativa_Frete_USD'], errors='coerce').fillna(0)
        filtered_df_for_summary_followup['Estimativa_Impostos_BR'] = pd.to_numeric(filtered_df_for_summary_followup['Estimativa_Impostos_BR'], errors='coerce').fillna(0)

        total_frete_usd_selected_days_followup = filtered_df_for_summary_followup['Estimativa_Frete_USD'].sum()
        total_impostos_br_selected_days_followup = filtered_df_for_summary_followup['Estimativa_Impostos_BR'].sum()
        total_processes_selected_days_followup = len(filtered_df_for_summary_followup)
    
    st.markdown("#### Resumo dos Processos (Follow-up)") 
    col_frete_f, col_impostos_f, col_total_processos_f, _, _ = st.columns(5) # Usando _ para as colunas não usadas

    with col_frete_f:
        st.metric(label=f"Frete (USD) Próximos {days_option} Dias", value=f"US$ {total_frete_usd_selected_days_followup:,.2f}".replace('.', '#').replace(',', '.').replace('#', ','))
    with col_impostos_f:
        st.metric(label=f"Impostos (R$) Próximos {days_option} Dias", value=f"R$ {total_impostos_br_selected_days_followup:,.2f}".replace('.', '#').replace(',', '.').replace('#', ','))
    with col_total_processos_f:
        st.metric(label=f"Total de Processos Próximos {days_option} Dias", value=total_processes_selected_days_followup)
    st.markdown("---")


    # --- Detalhes por Data de Registro (Próximos X Dias) (DO FOLLOW-UP) ---
    st.markdown(f"#### Detalhes por Data de Registro (Próximos {days_option} Dias - Follow-up)")
    if not df_followup.empty and 'Data_Registro_dt' in df_followup.columns:
        daily_summary_followup = {}
        for i in range(days_option):
            current_date_loop = current_today + timedelta(days=i)
            daily_summary_followup[current_date_loop] = {'frete': 0.0, 'impostos': 0.0}

        df_followup['Estimativa_Frete_USD'] = pd.to_numeric(df_followup['Estimativa_Frete_USD'], errors='coerce').fillna(0)
        df_followup['Estimativa_Impostos_BR'] = pd.to_numeric(df_followup['Estimativa_Impostos_BR'], errors='coerce').fillna(0)

        for index, row in df_followup.iterrows():
            data_registro_date = row['Data_Registro_dt'].date() if pd.notna(row['Data_Registro_dt']) else None
            
            if data_registro_date and data_registro_date in daily_summary_followup:
                daily_summary_followup[data_registro_date]['frete'] += row['Estimativa_Frete_USD']
                daily_summary_followup[data_registro_date]['impostos'] += row['Estimativa_Impostos_BR']
                
        sorted_daily_summary_followup = sorted(daily_summary_followup.items())

        cols_per_row = 5
        
        for i in range(0, len(sorted_daily_summary_followup), cols_per_row):
            current_row_data = sorted_daily_summary_followup[i : i + cols_per_row]
            cols = st.columns(cols_per_row)
            for j, (date_key, values) in enumerate(current_row_data):
                with cols[j]:
                    st.markdown(f"**{date_key.strftime('%d/%m')}**")
                    st.markdown(f"Frete (USD): US$ {values['frete']:,.2f}".replace('.', '#').replace(',', '.').replace('#', ','))
                    st.markdown(f"Impostos (BRL): R$ {values['impostos']:,.2f}".replace('.', '#').replace(',', '.').replace('#', ','))
            
            if (i + cols_per_row) < len(sorted_daily_summary_followup):
                st.markdown("---")

    else:
        st.info("Nenhum dado de 'Data_Registro' ou DataFrame vazio para exibir detalhes diários do Follow-up.")
    st.markdown("---")


    # --- Novas Métricas de Custo: Armazenagem, Frete Internacional, Frete Nacional, Impostos (DAS DECLARAÇÕES XML) ---
    st.subheader("Análise de Custos Detalhados (Declarações XML)")

    # Carregar dados das declarações XML com custos unidos
    xml_declaracoes_with_costs = db_utils.get_all_xml_declaracoes_with_costs_from_firestore()
    df_xml_costs = pd.DataFrame(xml_declaracoes_with_costs)

    if not df_xml_costs.empty and 'data_registro' in df_xml_costs.columns:
        # Convert 'data_registro' from xml_declaracoes to datetime
        df_xml_costs['Data_Registro_dt'] = pd.to_datetime(df_xml_costs['data_registro'], errors='coerce')
        
        # Garante que os tipos são numéricos e preenche NaNs com 0 para as colunas de custo
        cost_columns = ['armazenagem', 'frete_internacional_valor', 'frete_nacional', 'ipi', 'pis_pasep', 'cofins', 'imposto_importacao']
        for col in cost_columns:
            df_xml_costs[col] = pd.to_numeric(df_xml_costs.get(col, 0), errors='coerce').fillna(0)
        
        # O total de impostos será a soma de IPI, PIS/PASEP, COFINS e Imposto de Importação
        df_xml_costs['total_impostos_calculados'] = df_xml_costs['ipi'] + df_xml_costs['pis_pasep'] + df_xml_costs['cofins'] + df_xml_costs['imposto_importacao']

        # --- Cálculo das Métricas Gerais ---
        total_armazenagem = df_xml_costs['armazenagem'].sum()
        total_frete_internacional = df_xml_costs['frete_internacional_valor'].sum()
        total_frete_nacional = df_xml_costs['frete_nacional'].sum()
        total_impostos = df_xml_costs['total_impostos_calculados'].sum()

        st.markdown("##### Totais Gerais de Custos")
        col_arm, col_fret_int, col_fret_nac, col_impostos = st.columns(4)
        with col_arm:
            st.metric(label="Total Armazenagem", value=f"R$ {total_armazenagem:,.2f}".replace('.', '#').replace(',', '.').replace('#', ','))
        with col_fret_int:
            st.metric(label="Total Frete Internacional", value=f"US$ {total_frete_internacional:,.2f}".replace('.', '#').replace(',', '.').replace('#', ','))
        with col_fret_nac:
            st.metric(label="Total Frete Nacional", value=f"R$ {total_frete_nacional:,.2f}".replace('.', '#').replace(',', '.').replace('#', ','))
        with col_impostos:
            st.metric(label="Total Impostos", value=f"R$ {total_impostos:,.2f}".replace('.', '#').replace(',', '.').replace('#', ','))
        st.markdown("---")

        # --- Cálculo e Visualização por Período ---
        periods = {
            "Semana": "W",
            "Mês": "M",
            "Ano": "Y"
        }

        selected_period = st.selectbox(
            "Visualizar Custos por (Declarações XML):", # Ajustado para indicar que é do XML
            options=list(periods.keys()),
            key="cost_period_selector_xml" # Chave única
        )
        st.markdown("---")

        if selected_period in periods:
            freq = periods[selected_period]
            
            df_xml_costs['Period'] = df_xml_costs['Data_Registro_dt'].dt.to_period(freq)
            
            df_grouped_costs = df_xml_costs.groupby('Period').agg(
                total_armazenagem=('armazenagem', 'sum'),
                total_frete_internacional=('frete_internacional_valor', 'sum'),
                total_frete_nacional=('frete_nacional', 'sum'),
                total_impostos=('total_impostos_calculados', 'sum')
            ).reset_index()
            
            df_grouped_costs['Period_str'] = df_grouped_costs['Period'].astype(str)
            df_grouped_costs = df_grouped_costs.sort_values('Period')

            st.markdown(f"##### Custos Totais por {selected_period} (Declarações XML)")
            
            # Gráfico de Linha para Armazenagem
            st.markdown(f"###### Armazenagem por {selected_period}")
            chart_armazenagem = alt.Chart(df_grouped_costs).mark_line(point=True).encode(
                x=alt.X('Period_str', title=selected_period),
                y=alt.Y('total_armazenagem', title='Valor (R$)'),
                tooltip=['Period_str', alt.Tooltip('total_armazenagem', format='.2f')]
            ).properties(
                title=f"Total de Armazenagem por {selected_period}"
            ).interactive()
            st.altair_chart(chart_armazenagem, use_container_width=True)

            # Gráfico de Linha para Frete Internacional
            st.markdown(f"###### Frete Internacional por {selected_period}")
            chart_frete_int = alt.Chart(df_grouped_costs).mark_line(point=True).encode(
                x=alt.X('Period_str', title=selected_period),
                y=alt.Y('total_frete_internacional', title='Valor (US$)'),
                tooltip=['Period_str', alt.Tooltip('total_frete_internacional', format='.2f')]
            ).properties(
                title=f"Total de Frete Internacional por {selected_period}"
            ).interactive()
            st.altair_chart(chart_frete_int, use_container_width=True)

            # Gráfico de Linha para Frete Nacional
            st.markdown(f"###### Frete Nacional por {selected_period}")
            chart_frete_nac = alt.Chart(df_grouped_costs).mark_line(point=True).encode(
                x=alt.X('Period_str', title=selected_period),
                y=alt.Y('total_frete_nacional', title='Valor (R$)'),
                tooltip=['Period_str', alt.Tooltip('total_frete_nacional', format='.2f')]
            ).properties(
                title=f"Total de Frete Nacional por {selected_period}"
            ).interactive()
            st.altair_chart(chart_frete_nac, use_container_width=True)

            # Gráfico de Linha para Impostos
            st.markdown(f"###### Impostos por {selected_period}")
            chart_impostos = alt.Chart(df_grouped_costs).mark_line(point=True).encode(
                x=alt.X('Period_str', title=selected_period),
                y=alt.Y('total_impostos', title='Valor (R$)'),
                tooltip=['Period_str', alt.Tooltip('total_impostos', format='.2f')]
            ).properties(
                title=f"Total de Impostos por {selected_period}"
            ).interactive()
            st.altair_chart(chart_impostos, use_container_width=True)

        else:
            st.info("Selecione um período para visualizar os custos detalhados das Declarações XML.")

    else:
        st.info("Nenhum dado de custo disponível para análise detalhada das Declarações XML. Certifique-se de que as declarações XML foram importadas e os custos associados foram salvos.")

    st.markdown("---")
    st.write("Esta dashboard oferece uma visão geral dos processos de importação.")
