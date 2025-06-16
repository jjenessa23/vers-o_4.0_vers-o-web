import streamlit as st
import os
import base64
from datetime import datetime # Adicionado
import requests # Adicionado
import logging # Adicionado

logger = logging.getLogger(__name__) # Adicionado

# --- Função para definir imagem de fundo com opacidade (para o corpo principal) ---
def set_background_image(image_path, opacity=0.5): # Adicionado 'opacity' como parâmetro com valor padrão
    """
    Define uma imagem de fundo para o corpo principal da aplicação Streamlit.
    A imagem é convertida para Base64 e injetada via CSS em um pseudo-elemento ::before,
    garantindo que o conteúdo da página não fique transparente.
    """
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
                opacity: {opacity}; /* Opacidade ajustada dinamicamente */
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

# --- Função para definir imagem de fundo para a Sidebar ---
def set_sidebar_background_image(image_path, opacity=0.6):
    """
    Define uma imagem de fundo para a barra lateral (sidebar) da aplicação Streamlit.
    A imagem é convertida para Base64 e injetada via CSS em um pseudo-elemento ::before,
    garantindo que o conteúdo da sidebar não fique transparente.
    """
    try:
        with open(image_path, "rb") as image_file:
            encoded_string = base64.b64encode(image_file.read()).decode()
        st.markdown(
            f"""
            <style>
            [data-testid="stSidebar"] {{
                background-color: transparent !important; /* Garante que o fundo da sidebar seja transparente */
                position: relative; /* Necessário para que o pseudo-elemento se posicione corretamente */
            }}
            [data-testid="stSidebar"]::before {{
                content: "";
                position: absolute;
                top: 0;
                left: 0;
                width: 100%;
                height: 100%;
                background-image: url("data:image/png;base64,{encoded_string}");
                background-size: cover;
                background-position: center;
                background-repeat: no-repeat;
                background-attachment: scroll; /* Use scroll if you want it to scroll with sidebar content */
                opacity: {opacity}; /* Opacidade da imagem de fundo da sidebar */
                z-index: -1; /* Garante que o pseudo-elemento fique atrás do conteúdo da sidebar */
            }}
            /* Garante que o conteúdo da sidebar (botões, texto) seja totalmente opaco */
            [data-testid="stSidebarContent"] > div {{
                opacity: 1 !important;
            }}
            </style>
            """,
            unsafe_allow_html=True
        )
    except FileNotFoundError:
        st.warning(f"A imagem de fundo da sidebar não foi encontrada no caminho: {image_path}")
    except Exception as e:
        st.error(f"Erro ao carregar a imagem de fundo da sidebar: {e}")

# --- Função para buscar a cotação do dólar (MOVIDA PARA CÁ) ---
@st.cache_data(ttl=3600) # Cache por 1 hora para evitar chamadas excessivas à API
def get_dolar_cotacao():
    """
    Busca a cotação do dólar (abertura e PTAX) da API do Banco Central.
    Retorna um dicionário com as cotações ou None em caso de erro.
    """
    today = datetime.now().strftime('%m-%d-%Y') # Formato MM-DD-AAAA exigido pela API
    
    # URL da API do Banco Central para boletins do dólar, usando o endpoint CotacaoMoedaPeriodo
    api_url = f"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaPeriodo(moeda=@moeda,dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@moeda='USD'&@dataInicial='{today}'&@dataFinalCotacao='{today}'&$top=100&$format=json&$select=cotacaoCompra,cotacaoVenda,dataHoraCotacao,tipoBoletim"
    
    try:
        response = requests.get(api_url)
        response.raise_for_status() # Levanta um HTTPError para respostas 4xx/5xx
        data = response.json()
        
        cotacoes = {
            "abertura_compra": "N/A",
            "abertura_venda": "N/A",
            "ptax_compra": "N/A",
            "ptax_venda": "N/A"
        }

        # A API retorna uma lista de dicionários dentro da chave 'value'
        for item in data.get('value', []):
            # Busca a cotação de Abertura
            if item.get('tipoBoletim') == 'Abertura':
                cotacoes['abertura_compra'] = f"{item.get('cotacaoCompra', 0):.4f}".replace('.', ',')
                cotacoes['abertura_venda'] = f"{item.get('cotacaoVenda', 0):.4f}".replace('.', ',')
            
            # PTAX é frequentemente associado ao "Fechamento Interbancário" ou "Fechamento".
            # Priorizamos "Fechamento Interbancário" se disponível, senão "Fechamento" como PTAX.
            if item.get('tipoBoletim') == 'Fechamento Interbancário':
                cotacoes['ptax_compra'] = f"{item.get('cotacaoCompra', 0):.4f}".replace('.', ',')
                cotacoes['ptax_venda'] = f"{item.get('cotacaoVenda', 0):.4f}".replace('.', ',')
            elif item.get('tipoBoletim') == 'Fechamento':
                # Se não houver Fechamento Interbancário, usamos Fechamento.
                # Garantimos que não sobrescrevemos se Fechamento Interbancário já foi encontrado.
                if cotacoes['ptax_compra'] == "N/A": 
                    cotacoes['ptax_compra'] = f"{item.get('cotacaoCompra', 0):.4f}".replace('.', ',')
                    cotacoes['ptax_venda'] = f"{item.get('cotacaoVenda', 0):.4f}".replace('.', ',')
        return cotacoes
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao buscar cotação do dólar da API: {e}")
        st.error(f"Erro ao buscar cotação do dólar. Por favor, tente novamente mais tarde. Detalhes: {e}")
        return None
    except Exception as e:
        logger.error(f"Erro inesperado ao processar cotação do dólar: {e}")
        st.error(f"Erro inesperado ao processar cotação do dólar. Detalhes: {e}")
        return None
