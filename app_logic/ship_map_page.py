import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
import os
import time
import sqlite3
import requests # Necessário para fazer requisições HTTP a APIs

from app_logic.utils import set_background_image

# Configuração da API Maersk (Track & Trace Events)
MAERSK_CONSUMER_KEY = "GJhpMY1GH45LNZLKHj20uacD1vYgR5jd" # SUA CONSUMER KEY REAL DA MAERSK
MAERSK_BASE_URL = "https://api.maersk.com/track-and-trace-private" # URL de Produção

# Mock do db_utils (ajustado para a nova estrutura de dados da Maersk API)
class MockDbUtils:
    def __init__(self, db_path="data/comex_db.db"):
        self.db_path = db_path
        self._create_table_if_not_exists()

    def _create_table_if_not_exists(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        # Nova tabela para rastreamento de remessas/equipamentos
        cursor.execute("DROP TABLE IF EXISTS tracked_shipments") # Drop para garantir esquema atualizado
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tracked_shipments (
                tracking_ref TEXT PRIMARY KEY, -- Número do BL ou Contêiner (da Maersk)
                tracking_type TEXT,            -- 'BL' ou 'Container'
                imo_number TEXT,               -- Número IMO do navio (da Maersk API)
                vessel_name TEXT,              -- Nome do navio (da Maersk API)
                latitude REAL,
                longitude REAL,
                status TEXT,                   -- Status do evento (ACT, PLN, EST)
                last_updated TEXT,
                shipping_line TEXT             -- Companhia de navegação (entrada do usuário para contexto)
            )
        """)
        conn.commit()
        conn.close()

    def add_tracked_shipment(self, tracking_ref, tracking_type, shipping_line, imo_number="N/A", vessel_name="Desconhecido", latitude=0.0, longitude=0.0, status="Indefinido"):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO tracked_shipments (tracking_ref, tracking_type, imo_number, vessel_name, latitude, longitude, status, last_updated, shipping_line)
                VALUES (?, ?, ?, ?, ?, ?, ?, DATETIME('now'), ?)
            """, (tracking_ref, tracking_type, imo_number, vessel_name, latitude, longitude, status, shipping_line))
            conn.commit()
            st.success(f"Rastreamento para '{tracking_ref}' adicionado/atualizado no banco de dados local.")
            return True
        except sqlite3.Error as e:
            st.error(f"Erro ao adicionar/atualizar rastreamento no DB local: {e}")
            return False
        finally:
            conn.close()

    def get_all_tracked_shipments(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT tracking_ref, tracking_type, imo_number, vessel_name, latitude, longitude, status, last_updated, shipping_line FROM tracked_shipments")
        shipments = cursor.fetchall()
        conn.close()
        return [{"Tracking Ref": s[0], "Tipo": s[1], "IMO": s[2], "Nome do Navio": s[3], "Latitude": s[4], "Longitude": s[5], "Status": s[6], "Última Atualização": s[7], "Companhia": s[8]} for s in shipments]

    def update_shipment_position(self, tracking_ref, imo_number, vessel_name, latitude, longitude, status, last_updated):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("""
                UPDATE tracked_shipments
                SET imo_number = ?, vessel_name = ?, latitude = ?, longitude = ?, status = ?, last_updated = ?
                WHERE tracking_ref = ?
            """, (imo_number, vessel_name, latitude, longitude, status, last_updated, tracking_ref))
            conn.commit()
            return True
        except sqlite3.Error as e:
            st.error(f"Erro ao atualizar posição para {tracking_ref} no DB local: {e}")
            return False
        finally:
            conn.close()

# Inicializa o mock de DB
mock_db = MockDbUtils()

# Função para fazer a requisição GET para Maersk API e obter eventos
def maersk_get_events_data(consumer_key, transport_document_reference=None, equipment_reference=None, carrier_booking_reference=None):
    url = f"{MAERSK_BASE_URL}/events"
    headers = {
        'Consumer-Key': consumer_key,
        'API-Version': '1' # Conforme documentação, versão MAIOR '1'
    }
    params = {}
    
    # Adiciona apenas o parâmetro que foi fornecido (apenas um é necessário por requisição)
    if transport_document_reference:
        params['transportDocumentReference'] = transport_document_reference
    elif equipment_reference:
        params['equipmentReference'] = equipment_reference
    elif carrier_booking_reference:
        params['carrierBookingReference'] = carrier_booking_reference
    
    # Se nenhum parâmetro de rastreamento foi fornecido, retorna None
    if not params:
        st.error("É necessário fornecer pelo menos uma referência (BL, Contêiner ou Booking) para a API Maersk.")
        return None

    # Filtra por eventos de Transporte ou Equipamento, pois contêm dados de localização
    params['eventType'] = 'TRANSPORT,EQUIPMENT' 
    
    # Ordena por eventDateTime em ordem decrescente para obter o evento mais recente primeiro
    params['sort'] = 'eventDateTime:DESC'
    params['limit'] = 1 # Obtém apenas o evento mais recente

    st.info(f"DEBUG (Maersk API): URL: {url}")
    st.info(f"DEBUG (Maersk API): Headers: {{'Consumer-Key': '...', 'API-Version': '1'}}") # Oculta a chave real no debug
    st.info(f"DEBUG (Maersk API): Params: {params}")

    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status() # Lança HTTPError para respostas de erro (4xx ou 5xx)
        data = response.json()

        if data and 'events' in data and len(data['events']) > 0:
            # Itera sobre os eventos para encontrar o mais relevante com dados de localização/navio
            for event in data['events']:
                event_type = event.get('eventType')
                
                if event_type in ['TRANSPORT', 'EQUIPMENT']:
                    transport_call = event.get('transportCall')
                    if transport_call:
                        location = transport_call.get('location')
                        vessel = transport_call.get('vessel')

                        if location and vessel:
                            # Converte latitude e longitude para float, caso venham como string
                            lat = float(location.get('latitude', 0.0))
                            lon = float(location.get('longitude', 0.0))
                            imo = str(vessel.get('vesselIMONumber', 'N/A'))
                            vessel_name = vessel.get('vesselName', 'Desconhecido')
                            status = event.get('eventClassifierCode', 'Indefinido') # ACT, PLN, EST

                            st.success(f"Dados obtidos da API Maersk para a referência.")
                            return {
                                'latitude': lat,
                                'longitude': lon,
                                'imo_number': imo,
                                'vessel_name': vessel_name,
                                'status': status
                            }
            st.warning(f"Nenhum evento de transporte/equipamento com dados de localização/navio encontrado para a referência.")
            return None
        else:
            st.warning(f"Nenhum evento encontrado na API Maersk para a referência: {transport_document_reference or equipment_reference or carrier_booking_reference}")
            return None

    except requests.exceptions.RequestException as e:
        st.error(f"Erro de rede ao obter dados da API Maersk: {e}")
        return None
    except ValueError:
        st.error("Erro ao decodificar JSON da resposta da API Maersk.")
        return None
    except Exception as e:
        st.error(f"Erro inesperado ao obter dados da API Maersk: {e}")
        return None


def show_page():
    """
    Exibe a página do mapa de navios, com marcadores para latitudes e longitudes.
    Permite adicionar novos rastreamentos ao banco de dados e usar a API Maersk.
    """
    # Define a imagem de fundo para esta página
    background_image_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'assets', 'logo_navio_atracado.png')
    set_background_image(background_image_path)

    st.title("Mapa de Navios e Rastreamento Maersk")
    st.write("Visualize a localização dos navios no mapa e gerencie seus rastreamentos via API Maersk.")

    # --- Seção para Adicionar Novo Rastreamento Maersk ---
    st.subheader("Adicionar Novo Rastreamento Maersk")

    with st.form("add_maersk_tracking_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            bl_number_input = st.text_input("Número do BL (Transport Document Reference)", help="Ex: 260029935")
        with col2:
            container_number_input = st.text_input("Número do Contêiner (Equipment Reference)", help="Ex: APZU4812090")
        
        # Companhia de navegação como campo de texto para contexto, não para API Maersk
        shipping_line_input = st.text_input("Companhia de Navegação (para referência interna)", help="Ex: MAERSK")

        add_button = st.form_submit_button("Iniciar Rastreamento Maersk e Adicionar ao DB")

        if add_button:
            tracking_ref = None
            tracking_type = None

            if bl_number_input:
                tracking_ref = bl_number_input
                tracking_type = "BL"
            elif container_number_input:
                tracking_ref = container_number_input
                tracking_type = "Container"
            
            if not tracking_ref:
                st.warning("Por favor, preencha o 'Número do BL' OU o 'Número do Contêiner'.")
            elif MAERSK_CONSUMER_KEY == "SUA_CONSUMER_KEY_AQUI":
                st.error("Por favor, configure sua MAERSK_CONSUMER_KEY no código.")
            else:
                # 1. Tentar obter dados da API Maersk para validar e preencher informações iniciais
                api_data = maersk_get_events_data(
                    MAERSK_CONSUMER_KEY,
                    transport_document_reference=bl_number_input if bl_number_input else None,
                    equipment_reference=container_number_input if container_number_input else None
                    # carrier_booking_reference não está no formulário simplificado
                )
                
                if api_data:
                    # 2. Se a API retornou dados, adicionar/atualizar no DB local
                    success = mock_db.add_tracked_shipment(
                        tracking_ref=tracking_ref,
                        tracking_type=tracking_type,
                        shipping_line=shipping_line_input,
                        imo_number=api_data.get('imo_number', 'N/A'),
                        vessel_name=api_data.get('vessel_name', 'Desconhecido'),
                        latitude=api_data.get('latitude', 0.0),
                        longitude=api_data.get('longitude', 0.0),
                        status=api_data.get('status', 'Indefinido')
                    )
                    if success:
                        st.session_state['refresh_map'] = True # Sinaliza para recarregar o mapa
                    else:
                        st.error("Falha ao adicionar/atualizar rastreamento no banco de dados local.")
                else:
                    st.error("Não foi possível obter dados da API Maersk para a referência fornecida. Verifique a referência e sua Consumer Key.")
            

    st.markdown("---")


    # --- Exibição e Atualização de Dados de Rastreamento ---
    st.subheader("Rastreamentos Atuais")

    # Placeholder para o mapa e a tabela para permitir atualização
    map_placeholder = st.empty()
    table_placeholder = st.empty()
    status_placeholder = st.empty()

    # Botão para atualizar posições (agora usando a API Maersk)
    if st.button("Atualizar Posições dos Rastreamentos (via Maersk API)") or st.session_state.get('refresh_map', False):
        st.session_state['refresh_map'] = False # Reseta o sinal
        
        status_placeholder.info("Buscando e atualizando dados dos rastreamentos via Maersk API...")
        
        tracked_shipments = mock_db.get_all_tracked_shipments()
        updated_shipments_for_display = []

        if not tracked_shipments:
            status_placeholder.warning("Nenhum rastreamento no banco de dados para rastrear.")
        elif MAERSK_CONSUMER_KEY == "SUA_CONSUMER_KEY_AQUI":
            status_placeholder.error("Por favor, configure sua MAERSK_CONSUMER_KEY no código para atualizar os rastreamentos.")
        else:
            for shipment in tracked_shipments:
                tracking_ref = shipment.get('Tracking Ref')
                tracking_type = shipment.get('Tipo')
                
                api_data = None
                if tracking_type == "BL":
                    api_data = maersk_get_events_data(MAERSK_CONSUMER_KEY, transport_document_reference=tracking_ref)
                elif tracking_type == "Container":
                    api_data = maersk_get_events_data(MAERSK_CONSUMER_KEY, equipment_reference=tracking_ref)
                
                if api_data:
                    # Atualizar a posição no DB local
                    mock_db.update_shipment_position(
                        tracking_ref,
                        api_data.get('imo_number', 'N/A'),
                        api_data.get('vessel_name', 'Desconhecido'),
                        api_data.get('latitude', 0.0),
                        api_data.get('longitude', 0.0),
                        api_data.get('status', 'Indefinido'),
                        time.strftime("%Y-%m-%d %H:%M:%S")
                    )
                    # Adicionar os dados atualizados para exibição
                    updated_shipments_for_display.append({
                        **shipment, # Copia os dados existentes
                        'IMO': api_data.get('imo_number', 'N/A'),
                        'Nome do Navio': api_data.get('vessel_name', 'Desconhecido'),
                        'Latitude': api_data.get('latitude', 0.0),
                        'Longitude': api_data.get('longitude', 0.0),
                        'Status': api_data.get('status', 'Indefinido'),
                        'Última Atualização': time.strftime("%Y-%m-%d %H:%M:%S")
                    })
                else:
                    # Se a API falhou, manter os dados existentes do DB para exibição
                    updated_shipments_for_display.append(shipment)
                    st.warning(f"Não foi possível obter dados atualizados para {tracking_ref}. Usando dados locais.")

            df_shipments_display = pd.DataFrame(updated_shipments_for_display)

            # Exibir a tabela de rastreamentos
            with table_placeholder:
                st.dataframe(df_shipments_display, use_container_width=True)

            # Exibir o mapa interativo
            with map_placeholder:
                if not df_shipments_display.empty:
                    m = folium.Map(location=[-23.5505, -46.6333], zoom_start=5)

                    for idx, row in df_shipments_display.iterrows():
                        if row['Status'] == 'ACT': # Exemplo de status da Maersk API
                            icon_color = 'green'
                        elif row['Status'] == 'PLN':
                            icon_color = 'blue'
                        elif row['Status'] == 'EST':
                            icon_color = 'orange'
                        else:
                            icon_color = 'red'

                        folium.Marker(
                            location=[row['Latitude'], row['Longitude']],
                            popup=f"<b>{row['Nome do Navio']}</b><br>IMO: {row['IMO']}<br>Tipo: {row['Tipo']}<br>Ref: {row['Tracking Ref']}<br>Status: {row['Status']}<br>Última Atualização: {row['Última Atualização']}",
                            tooltip=row['Nome do Navio'],
                            icon=folium.Icon(color=icon_color, icon='ship', prefix='fa')
                        ).add_to(m)
                    
                    st_folium(m, width=1400, height=615)
                else:
                    st.info("Nenhum rastreamento disponível para exibição no mapa.")
            
            status_placeholder.empty()
            st.success("Dados dos rastreamentos atualizados!")

    else:
        # Exibe os dados do banco na inicialização ou se não clicou em atualizar
        tracked_shipments = mock_db.get_all_tracked_shipments()
        df_shipments_display = pd.DataFrame(tracked_shipments)
        
        with table_placeholder:
            st.dataframe(df_shipments_display, use_container_width=True)

        with map_placeholder:
            if not df_shipments_display.empty:
                m = folium.Map(location=[-23.5505, -46.6333], zoom_start=5)

                for idx, row in df_shipments_display.iterrows():
                    if row['Status'] == 'ACT':
                        icon_color = 'green'
                    elif row['Status'] == 'PLN':
                        icon_color = 'blue'
                    elif row['Status'] == 'EST':
                        icon_color = 'orange'
                    else:
                        icon_color = 'red'

                    folium.Marker(
                        location=[row['Latitude'], row['Longitude']],
                        popup=f"<b>{row['Nome do Navio']}</b><br>IMO: {row['IMO']}<br>Tipo: {row['Tipo']}<br>Ref: {row['Tracking Ref']}<br>Status: {row['Status']}<br>Última Atualização: {row['Última Atualização']}",
                        tooltip=row['Nome do Navio'],
                        icon=folium.Icon(color=icon_color, icon='ship', prefix='fa')
                    ).add_to(m)
                st_folium(m, width=1400, height=615)
            else:
                st.info("Nenhum rastreamento disponível para exibição no mapa.")


    st.markdown("---")
    st.write("Este mapa visualiza o rastreamento de cargas e navios via API Maersk (requer Consumer Key e acesso autorizado).")
    st.write("Use o botão 'Atualizar Posições dos Rastreamentos (via Maersk API)' para carregar os dados mais recentes.")
    st.write("Adicione novas referências de rastreamento (BL ou Contêiner) para persistir no banco de dados local.")
