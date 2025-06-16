import streamlit as st
import requests
import json
import os
from app_logic.utils import set_background_image

# Configuração da API TTCE
# Use o ambiente de Produção ou Validação conforme sua necessidade
TTCE_BASE_URL = "https://portalunico.siscomex.gov.br/ttce" # Ambiente de Produção
# TTCE_BASE_URL = "https://val.portalunico.siscomex.gov.br/ttce" # Ambiente de Validação

# --- Configuração do Certificado Digital (Lendo de Variáveis de Ambiente) ---
# IMPORTANTE: NÃO FAÇA COMMIT DESTAS VARIÁVEIS DE AMBIENTE PARA O GITHUB!
# Elas devem ser definidas no ambiente onde o Streamlit é executado.

# Exemplo de como definir (no terminal, antes de rodar 'streamlit run app_main.py'):
# export TTCE_CERT_PATH="/caminho/completo/para/seu_certificado.crt"
# export TTCE_KEY_PATH="/caminho/completo/para/sua_chave_privada.key"
# export TTCE_CERT_PASSWORD="SUA_SENHA_DO_CERTIFICADO" (opcional, se não houver senha, defina como string vazia ou não defina)

# Ou, se os arquivos estiverem em 'app_logic/certs/' no deploy, você pode usar um caminho relativo.
# No entanto, para segurança, é melhor ter caminhos absolutos ou garantir que 'certs' não seja público.

# Tenta obter os caminhos e senha das variáveis de ambiente
CERT_PATH = os.getenv("TTCE_CERT_PATH")
KEY_PATH = os.getenv("TTCE_KEY_PATH")
CERT_PASSWORD = os.getenv("TTCE_CERT_PASSWORD", "") # Assume string vazia se a variável não for definida

# Listas de apoio (baseadas na documentação)
REGIMES_TRIBUTARIOS = {
    "1": "RECOLHIMENTO INTEGRAL",
    "2": "IMUNIDADE",
    "3": "ISENÇÃO",
    "4": "REDUÇÃO",
    "5": "SUSPENSÃO",
    "6": "NÃO INCIDÊNCIA",
    "10": "SUSPENSÃO COM PAGAMENTO PROPORCIONAL DE TRIBUTO"
}

TRIBUTOS = {
    "1": "IMPOSTO DE IMPORTAÇÃO",
    "2": "IPI",
    "3": "ANTIDUMPING",
    "4": "CIDE COMBUSTÍVEIS",
    "5": "MEDIDA COMPENSATORIA",
    "6": "PIS IMPORTAÇÃO",
    "7": "COFINS IMPORTAÇÃO",
    "8": "MULTAS REGULAMENTARES",
    "9": "SALVAGUARDA",
    "10": "TAXA SISCOMEX",
    "11": "IMPOSTO DE EXPORTAÇÃO"
}

# Função para chamar a API TTCE
def get_tratamentos_tributarios(ncm, codigo_pais, data_fato_gerador, tipo_operacao, fundamentos_opcionais=None):
    url = f"{TTCE_BASE_URL}/api/ext/tratamentos-tributarios/importacao/"
    
    payload = {
        "ncm": str(ncm),
        "codigoPais": int(codigo_pais),
        "dataFato_gerador": str(data_fato_gerador), # Corrigido para 'dataFato_gerador' conforme padrão de nomenclatura
        "tipoOperacao": str(tipo_operacao)
    }
    
    if fundamentos_opcionais:
        payload["fundamentosOpcionais"] = fundamentos_opcionais

    headers = {
        "Content-Type": "application/json"
    }

    # Configuração do certificado para a requisição
    cert_config = None
    if CERT_PATH and KEY_PATH and os.path.exists(CERT_PATH) and os.path.exists(KEY_PATH):
        if CERT_PASSWORD:
            cert_config = (CERT_PATH, KEY_PATH, CERT_PASSWORD)
        else:
            cert_config = (CERT_PATH, KEY_PATH)
        st.info(f"DEBUG (TTCE API): Usando certificado digital de: {CERT_PATH}")
    else:
        st.warning("DEBUG (TTCE API): Certificado digital não encontrado ou configurado via variáveis de ambiente. A requisição pode falhar por falta de autenticação.")
        st.warning("DEBUG (TTCE API): Verifique se TTCE_CERT_PATH e TTCE_KEY_PATH estão definidos e os arquivos existem.")


    st.info(f"DEBUG (TTCE API): URL: {url}")
    st.info(f"DEBUG (TTCE API): Payload: {payload}")

    try:
        # Inclui o parâmetro 'cert' na requisição POST
        response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30, cert=cert_config)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Erro de rede ao consultar a API TTCE: {e}. Verifique o certificado e as permissões.")
        return None
    except ValueError:
        st.error("Erro ao decodificar JSON da resposta da API TTCE.")
        return None
    except Exception as e:
        st.error(f"Erro inesperado ao consultar a API TTCE: {e}")
        return None

def show_page():
    """
    Exibe a página de Cálculo de Tributos TTCE, permitindo consultar a API.
    """
    background_image_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'assets', 'logo_navio_atracado.png')
    set_background_image(background_image_path)

    st.title("Cálculo de Tributos TTCE")
    st.write("Consulte os tratamentos tributários de importação/exportação via API Siscomex TTCE.")

    st.subheader("Parâmetros da Consulta")

    with st.form("ttce_form", clear_on_submit=False):
        col1, col2 = st.columns(2)
        with col1:
            ncm_input = st.text_input("NCM (8 dígitos)", help="Nomenclatura Comum do Mercosul (ex: 84149039)")
        with col2:
            codigo_pais_input = st.number_input("Código do País", min_value=1, help="Código numérico identificador do país (ex: 158 para Brasil)")
        
        col3, col4 = st.columns(2)
        with col3:
            data_fato_gerador_input = st.date_input("Data do Fato Gerador", help="Data no formato aaaa-mm-dd")
        with col4:
            tipo_operacao_input = st.selectbox("Tipo de Operação", ["I", "E", "F"], help="I: Importação, E: Exportação, F: Frete. Para DUIMP, fixar 'I'.")

        st.markdown("---")
        st.write("Fundamentos Opcionais (Opcional)")
        col_fo1, col_fo2, col_fo3 = st.columns(3)
        with col_fo1:
            fo_tributo_input = st.selectbox("Tributo Opcional", [""] + list(TRIBUTOS.keys()), format_func=lambda x: TRIBUTOS.get(x, "Selecione..."))
        with col_fo2:
            fo_regime_input = st.selectbox("Regime Opcional", [""] + list(REGIMES_TRIBUTARIOS.keys()), format_func=lambda x: REGIMES_TRIBUTARIOS.get(x, "Selecione..."))
        with col_fo3:
            fo_fundamento_legal_input = st.text_input("Código Fundamento Legal Opcional", help="Código identificador do Fundamento Legal (até 4 dígitos)")
        
        submit_button = st.form_submit_button("Consultar Tributos")

    if submit_button:
        if not ncm_input or not codigo_pais_input or not data_fato_gerador_input or not tipo_operacao_input:
            st.warning("Por favor, preencha os campos obrigatórios: NCM, Código do País, Data do Fato Gerador e Tipo de Operação.")
        else:
            fundamentos_opcionais_list = []
            if fo_tributo_input and fo_regime_input and fo_fundamento_legal_input:
                try:
                    fundamentos_opcionais_list.append({
                        "codigoTributo": int(fo_tributo_input),
                        "codigoRegime": int(fo_regime_input),
                        "codigoFundamentoLegal": int(fo_fundamento_legal_input)
                    })
                except ValueError:
                    st.error("Por favor, insira valores numéricos válidos para os Fundamentos Opcionais.")
                    fundamentos_opcionais_list = None

            if fundamentos_opcionais_list is not None:
                with st.spinner("Consultando API TTCE..."):
                    result = get_tratamentos_tributarios(
                        ncm_input,
                        codigo_pais_input,
                        data_fato_gerador_input.strftime("%Y-%m-%d"),
                        tipo_operacao_input,
                        fundamentos_opcionais=fundamentos_opcionais_list if fundamentos_opcionais_list else None
                    )

                if result:
                    st.subheader("Resultados da Consulta")
                    st.json(result)

                    if 'tratamentosTributarios' in result and result['tratamentosTributarios']:
                        st.write("#### Tratamentos Tributários Encontrados:")
                        for tt in result['tratamentosTributarios']:
                            st.write(f"- **Tributo**: {TRIBUTOS.get(str(tt.get('tributo', {}).get('codigo')), 'N/A')} ({tt.get('tributo', {}).get('codigo')})")
                            st.write(f"  **Regime**: {REGIMES_TRIBUTARIOS.get(str(tt.get('regime', {}).get('codigo')), 'N/A')} ({tt.get('regime', {}).get('codigo')})")
                            st.write(f"  **Fundamento Legal**: {tt.get('fundamentoLegal', {}).get('nome', 'N/A')} ({tt.get('fundamentoLegal', {}).get('codigo')})")
                            if 'mercadorias' in tt and tt['mercadorias']:
                                st.write("  **Mercadorias e Atributos:**")
                                for merc in tt['mercadorias']:
                                    st.write(f"    - NCM: {merc.get('ncm', 'N/A')}")
                                    if 'atributos' in merc and merc['atributos']:
                                        for attr in merc['atributos']:
                                            st.write(f"      - Atributo: {attr.get('descricaoCodigo', 'N/A')} ({attr.get('codigo', 'N/A')}) = {attr.get('valor', 'N/A')}")
                            st.markdown("---")
                    else:
                        st.info("Nenhum tratamento tributário encontrado para os parâmetros informados.")
                    
                    if 'fundamentosOpcionaisDisponiveis' in result and result['fundamentosOpcionaisDisponiveis']:
                        st.write("#### Fundamentos Opcionais Disponíveis:")
                        for fo in result['fundamentosOpcionaisDisponiveis']:
                            st.write(f"- Tributo: {TRIBUTOS.get(str(fo.get('tributo', {}).get('codigo')), 'N/A')} ({fo.get('tributo', {}).get('codigo')})")
                            st.write(f"  Regime: {REGIMES_TRIBUTARIOS.get(str(fo.get('regime', {}).get('codigo')), 'N/A')} ({fo.get('regime', {}).get('codigo')})")
                            st.write(f"  Fundamento Legal: {fo.get('fundamentoLegal', {}).get('nome', 'N/A')} ({fo.get('fundamentoLegal', {}).get('codigo')})")
                            st.markdown("---")

                else:
                    st.error("Não foi possível obter resultados da API TTCE. Verifique os parâmetros e sua conexão.")
