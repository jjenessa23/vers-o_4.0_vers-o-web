import os
from typing import Any, Dict, List
import streamlit as st
import pandas as pd
from datetime import datetime
import io # Para manipulação de arquivos em memória
import sqlite3 # Importar sqlite3 para verificar tipo de dado
import base64 # Para codificar a imagem de fundo em base64

# Importar funções do módulo de utilitários de banco de dados
# As importações agora incluem 'app_logic.' para garantir o caminho correto.
from app_logic.db_utils import (
    get_all_declaracoes,
    delete_declaracao,
    get_declaracao_by_id,
    update_declaracao, # Importa a função de atualização
    get_itens_by_declaracao_id,
    parse_xml_data_to_dict, # NOVO: Importa a função de parsear sem salvar
    save_parsed_di_data,    # NOVO: Importa a função de salvar dados parseados
    # REMOVIDO: connect_sqlite_db e get_sqlite_db_path não são mais necessários diretamente aqui
    # porque get_declaracao_by_id já faz essa abstração.
    # connect_sqlite_db,
    # get_sqlite_db_path
)

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




# --- Funções Auxiliares de Formatação ---
def _format_di_number(di_number):
    """Formata o número da DI para o padrão **/*******-*."""
    if di_number and isinstance(di_number, str) and len(di_number) == 10:
        return f"{di_number[0:2]}/{di_number[2:9]}-{di_number[9]}"
    return di_number

def _format_currency(value):
    """Formata um valor numérico para moeda BRL (R$)."""
    try:
        val = float(value)
        return f"R$ {val:,.2f}".replace('.', '#').replace(',', '.').replace('#', ',')
    except (ValueError, TypeError):
        return "R$ 0,00"

def _format_currency_usd(value):
    """Formata um valor numérico para moeda USD (US$)."""
    try:
        val = float(value)
        return f"US$ {val:,.2f}".replace('.', '#').replace(',', '.').replace('#', ',')
    except (ValueError, TypeError):
        return "US$ 0,00"

def _format_float(value, decimals=6):
    """Formata um valor numérico float com um número específico de casas decimais."""
    try:
        val = float(value)
        return f"{val:,.{decimals}f}".replace('.', '#').replace(',', '.').replace('#', ',')
    except (ValueError, TypeError):
        return "N/A"

def _format_percentage(value, decimals=2):
    """Formata um valor numérico como porcentagem (multiplicado por 100)."""
    try:
        val = float(value)
        return f"{val*100:,.{decimals}f}%".replace('.', '#').replace(',', '.').replace('#', ',')
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

def _format_ncm(ncm_value):
    if ncm_value and isinstance(ncm_value, str) and len(ncm_value) == 8:
        return f"{ncm_value[0:4]}.{ncm_value[4:6]}.{ncm_value[6:8]}"
    return ncm_value

# --- NOVO: Pop-up de Edição antes de Salvar ---
def _open_edit_popup_before_save(di_data: Dict[str, Any], itens_data: List[Dict[str, Any]]):
    """Abre um pop-up para editar os dados da DI e itens antes de salvar no DB."""
    st.session_state.temp_di_data = di_data
    st.session_state.temp_itens_data = itens_data
    st.session_state.show_edit_popup_before_save = True
    st.rerun() # Força o rerun para exibir o pop-up


def _display_edit_popup_before_save():
    """Exibe o pop-up de edição da DI e itens antes de salvar."""
    if 'show_edit_popup_before_save' not in st.session_state or not st.session_state.show_edit_popup_before_save:
        return

    di_data = st.session_state.temp_di_data
    itens_data = st.session_state.temp_itens_data

    if not di_data:
        st.session_state.show_edit_popup_before_save = False
        return

    # Usando st.container para controlar a largura do formulário
    with st.container():
        with st.form(key="edit_di_before_save_form"): # Removido use_container_width aqui
            st.markdown("### Revisar e Editar Dados da DI (Antes de Salvar)")
            st.info("Edite os campos abaixo. Clique em 'Salvar no Banco de Dados' para finalizar a importação.")

            # Usando st.tabs para criar abas
            tab_dados_di, tab_itens_di = st.tabs(["Dados da DI", "Itens da DI"])

            with tab_dados_di:
                # Reorganizando os campos em 3 colunas
                col1, col2, col3 = st.columns(3)
                edited_di_data = {}

                with col1:
                    edited_di_data['numero_di'] = st.text_input("Número DI", value=di_data.get('numero_di', ''))
                    edited_di_data['data_registro'] = st.date_input("Data Registro", value=datetime.strptime(di_data['data_registro'], "%Y-%m-%d") if di_data.get('data_registro') else datetime.now()).strftime("%Y-%m-%d")
                    edited_di_data['vmle'] = st.number_input("VMLE (R$)", value=float(di_data.get('vmle', 0.0)), format="%.2f")
                    edited_di_data['frete'] = st.number_input("Frete (R$)", value=float(di_data.get('frete', 0.0)), format="%.2f")
                    edited_di_data['seguro'] = st.number_input("Seguro (R$)", value=float(di_data.get('seguro', 0.0)), format="%.2f")
                    edited_di_data['vmld'] = st.number_input("VMLD (R$)", value=float(di_data.get('vmld', 0.0)), format="%.2f")
                    edited_di_data['imposto_importacao'] = st.number_input("Imposto de Importação (R$)", value=float(di_data.get('imposto_importacao', 0.0)), format="%.2f")
                    edited_di_data['armazenagem'] = st.number_input("Armazenagem (R$)", value=float(di_data.get('armazenagem', 0.0)), format="%.2f")
                    edited_di_data['frete_nacional'] = st.number_input("Frete Nacional (R$)", value=float(di_data.get('frete_nacional', 0.0)), format="%.2f")
                    edited_di_data['peso_bruto'] = st.number_input("Peso Bruto (KG)", value=float(di_data.get('peso_bruto', 0.0)), format="%.3f")

                with col2:
                    edited_di_data['informacao_complementar'] = st.text_input("Referência", value=di_data.get('informacao_complementar', '') or "")
                    edited_di_data['ipi'] = st.number_input("IPI (R$)", value=float(di_data.get('ipi', 0.0)), format="%.2f")
                    edited_di_data['pis_pasep'] = st.number_input("PIS/PASEP (R$)", value=float(di_data.get('pis_pasep', 0.0)), format="%.2f")
                    edited_di_data['cofins'] = st.number_input("COFINS (R$)", value=float(di_data.get('cofins', 0.0)), format="%.2f")
                    edited_di_data['icms_sc'] = st.text_input("ICMS-SC", value=di_data.get('icms_sc', '') or "")
                    edited_di_data['taxa_cambial_usd'] = st.number_input("Taxa Cambial (USD)", value=float(di_data.get('taxa_cambial_usd', 0.0)), format="%.6f")
                    edited_di_data['taxa_siscomex'] = st.number_input("Taxa SISCOMEX (R$)", value=float(di_data.get('taxa_siscomex', 0.0)), format="%.2f")
                    edited_di_data['numero_invoice'] = st.text_input("Nº Invoice", value=di_data.get('numero_invoice', '') or "")
                    edited_di_data['peso_liquido'] = st.number_input("Peso Líquido (KG)", value=float(di_data.get('peso_liquido', 0.0)), format="%.3f")


                with col3:
                    edited_di_data['cnpj_importador'] = st.text_input("CNPJ Importador", value=di_data.get('cnpj_importador', '') or "")
                    edited_di_data['importador_nome'] = st.text_input("Importador Nome", value=di_data.get('importador_nome', '') or "")
                    edited_di_data['recinto'] = st.text_input("Recinto", value=di_data.get('recinto', '') or "")
                    edited_di_data['embalagem'] = st.text_input("Embalagem", value=di_data.get('embalagem', '') or "")
                    edited_di_data['quantidade_volumes'] = st.number_input("Quantidade Volumes", value=int(di_data.get('quantidade_volumes', 0)), format="%d")
                    edited_di_data['acrescimo'] = st.number_input("Acréscimo (R$)", value=float(di_data.get('acrescimo', 0.0)), format="%.2f")
                    edited_di_data['arquivo_origem'] = st.text_input("Arquivo Origem", value=di_data.get('arquivo_origem', '') or "")
                    
                    # Data de importação não é editável, pega o valor original
                    edited_di_data['data_importacao'] = di_data.get('data_importacao', '')

            with tab_itens_di:
                st.markdown("### Itens da DI")
                # Exibe os itens em um DataFrame para revisão (não editável aqui, apenas visualização)
                if itens_data:
                    df_itens = pd.DataFrame(itens_data)
                    # Formatar NCM para exibição
                    if 'ncm_item' in df_itens.columns:
                        df_itens['ncm_item'] = df_itens['ncm_item'].apply(_format_ncm)
                    # Certifique-se de que a coluna 'id' não esteja sendo usada diretamente na exibição se não for um ID de exibição
                    # A coluna 'id' do parse XML é temporária e não deve ser exibida ao usuário
                    # O ID real dos itens no DB será tratado internamente
                    cols_to_drop_if_exists = ['id', 'declaracao_id']
                    display_df_itens = df_itens.drop(columns=[col for col in cols_to_drop_if_exists if col in df_itens.columns])
                    st.dataframe(display_df_itens, use_container_width=True, hide_index=True)
                else:
                    st.info("Nenhum item encontrado no XML.")

            col_save, col_cancel = st.columns(2)
            with col_save:
                if col_save.form_submit_button("Salvar no Banco de Dados"):
                    # Tenta salvar os dados editados
                    if save_parsed_di_data(edited_di_data, itens_data): # Usa a nova função de salvar
                        st.success(f"DI {edited_di_data['numero_di']} e itens salvos com sucesso!")
                        st.session_state.show_edit_popup_before_save = False
                        # O rerun será feito automaticamente pelo Streamlit após o submit do form
                        # st.rerun() # REMOVIDO: Este st.rerun() é um no-op dentro de um form_submit_button.
                    else:
                        st.error("Falha ao salvar no banco de dados. Verifique o log.")
            with col_cancel:
                if col_cancel.form_submit_button("Cancelar Importação"):
                    st.session_state.show_edit_popup_before_save = False
                    st.warning("Importação cancelada.")
                    # O rerun será feito automaticamente pelo Streamlit após o submit do form
                    # st.rerun() # REMOVIDO: Este st.rerun() é um no-op dentro de um form_submit_button.

# --- NOVO: Pop-up para exibir itens da DI ---
def _open_items_popup(declaracao_id: int):
    """Abre um pop-up para exibir os itens de uma DI selecionada."""
    st.session_state.items_popup_declaracao_id = declaracao_id
    st.session_state.show_items_popup = True
    st.rerun() # Força o rerun para exibir o pop-up


def _display_items_popup():
    """Exibe o pop-up com a tabela de itens da DI."""
    if 'show_items_popup' not in st.session_state or not st.session_state.show_items_popup:
        return

    declaracao_id = st.session_state.items_popup_declaracao_id
    if not declaracao_id:
        st.session_state.show_items_popup = False
        return

    di_data = get_declaracao_by_id(declaracao_id)
    itens_data_raw = get_itens_by_declaracao_id(declaracao_id)

    st.markdown(f"### Itens da DI: {_format_di_number(di_data.get('numero_di')) if di_data else 'N/A'}")
    st.markdown(f"Referência: **{di_data.get('informacao_complementar') if di_data else 'N/A'}**")

    if itens_data_raw:
        # Converte a lista de Row objects para lista de dicionários para DataFrame
        itens_data_dicts = [dict(row) for row in itens_data_raw]
        df_itens = pd.DataFrame(itens_data_dicts)

        # Formatar colunas para exibição
        if not df_itens.empty:
            if 'ncm_item' in df_itens.columns:
                df_itens['ncm_item'] = df_itens['ncm_item'].apply(_format_ncm)
            # Adicione outras formatações se desejar (moeda, percentual, etc.)
            if 'quantidade' in df_itens.columns:
                df_itens['quantidade'] = df_itens['quantidade'].apply(_format_int)
            if 'valor_unitario' in df_itens.columns:
                df_itens['valor_unitario'] = df_itens['valor_unitario'].apply(_format_currency_usd)
            if 'valor_item_calculado' in df_itens.columns:
                df_itens['valor_item_calculado'] = df_itens['valor_item_calculado'].apply(_format_currency)
            if 'peso_liquido_item' in df_itens.columns:
                df_itens['peso_liquido_item'] = df_itens['peso_liquido_item'].apply(_format_weight_no_kg)
            if 'ii_percent_item' in df_itens.columns:
                df_itens['ii_percent_item'] = df_itens['ii_percent_item'].apply(_format_percentage)
            if 'ipi_percent_item' in df_itens.columns:
                df_itens['ipi_percent_item'] = df_itens['ipi_percent_item'].apply(_format_percentage)
            if 'pis_percent_item' in df_itens.columns:
                df_itens['pis_percent_item'] = df_itens['pis_percent_item'].apply(_format_percentage)
            if 'cofins_percent_item' in df_itens.columns:
                df_itens['cofins_percent_item'] = df_itens['cofins_percent_item'].apply(_format_percentage)
            if 'icms_percent_item' in df_itens.columns:
                df_itens['icms_percent_item'] = df_itens['icms_percent_item'].apply(_format_percentage)
            if 'custo_unit_di_usd' in df_itens.columns:
                df_itens['custo_unit_di_usd'] = df_itens['custo_unit_di_usd'].apply(_format_currency_usd)


        # Colunas a exibir no pop-up de itens
        cols_to_display = [
            "numero_adicao", "numero_item_sequencial", "sku_item", "descricao_mercadoria",
            "quantidade", "unidade_medida", "ncm_item", "custo_unit_di_usd",
            "ii_percent_item", "ipi_percent_item", "pis_percent_item", "cofins_percent_item"
        ]
        
        # Filtra as colunas para exibição, removendo aquelas que não existem no DataFrame
        cols_to_display_filtered = [col for col in cols_to_display if col in df_itens.columns]
        st.dataframe(df_itens[cols_to_display_filtered], use_container_width=True, hide_index=True)
    else:
        st.info("Nenhum item encontrado para esta DI.")

    if st.button("Fechar", key="close_items_popup"):
        st.session_state.show_items_popup = False
        st.rerun() # Manter: Essencial para fechar o popup e re-renderizar a página principal


# Callback para manipular a seleção da tabela de declarações
def _handle_declarations_table_change():
    # Acessa o estado do st.data_editor
    current_edited_data = st.session_state.xml_declarations_table_editor

    selected_di_id = None
    
    # Itera sobre as linhas editadas para encontrar qual checkbox foi marcado
    # st.data_editor retorna um dicionário onde a chave é o índice da linha
    # e o valor é um dicionário com as colunas editadas.
    for idx, row_changes in current_edited_data['edited_rows'].items():
        if '_Selecionar_DI' in row_changes and row_changes['_Selecionar_DI'] is True:
            # Se a checkbox foi marcada, pegamos o ID original da linha
            # Acessamos o DataFrame original que foi usado para popular o data_editor
            # (st.session_state.xml_declaracoes_data é uma lista de dicionários)
            original_row = st.session_state.xml_declaracoes_data[idx]
            selected_di_id = original_row.get('ID') # Usar .get() aqui também
            break # Como queremos seleção única, paramos na primeira encontrada

    st.session_state.selected_di_id = selected_di_id

    # Atualiza a visualização para garantir que apenas um checkbox esteja marcado
    # Isso é feito modificando a lista de dicionários que alimenta o data_editor
    # e forçando um rerun.
    if st.session_state.xml_declaracoes_data:
        new_data_for_editor = []
        for row_dict in st.session_state.xml_declaracoes_data:
            # Cria uma cópia mutável do dicionário da linha
            temp_row = dict(row_dict) 
            if temp_row.get('ID') == selected_di_id: # Usar .get() aqui
                temp_row['_Selecionar_DI'] = True
            else:
                temp_row['_Selecionar_DI'] = False
            new_data_for_editor.append(temp_row)
        
        st.session_state.xml_declaracoes_data = new_data_for_editor
    
    # st.rerun() # REMOVIDO: A mudança no st.data_editor já dispara uma reexecução.


# NOVO: Função de callback para o st.file_uploader
def _handle_xml_upload():
    # Acessa o arquivo via key do session_state
    current_uploader_key = f"upload_xml_di_widget_{st.session_state.upload_xml_di_key}"
    uploaded_file_obj = st.session_state.get(current_uploader_key)

    if uploaded_file_obj is not None:
        xml_content = uploaded_file_obj.getvalue().decode("utf-8")
        di_data_parsed, itens_data_parsed_raw = parse_xml_data_to_dict(xml_content)
        itens_data_parsed = itens_data_parsed_raw if itens_data_parsed_raw is not None else []
        
        if di_data_parsed:
            numero_di_from_xml = di_data_parsed.get('numero_di')
            if not numero_di_from_xml:
                st.error("Não foi possível extrair o número da DI do arquivo XML. Verifique o formato do arquivo.")
                # Para limpar o uploader após um erro no parse
                st.session_state.upload_xml_di_key += 1
                return

            # MODIFICADO: Utiliza db_utils.get_declaracao_by_id para verificar a existência
            # Esta função já cuida de verificar no Firestore ou SQLite conforme a prioridade.
            existing_di = get_declaracao_by_id(numero_di_from_xml)

            if existing_di:
                st.error(f"A Declaração de Importação número {_format_di_number(numero_di_from_xml)} já existe no banco de dados.")
            else:
                # Se não existe, abre o pop-up para edição/confirmação antes de salvar
                _open_edit_popup_before_save(di_data_parsed, itens_data_parsed)
        
        # Para limpar o st.file_uploader, a forma mais robusta é redefinir a key do widget.
        # Isso força o Streamlit a tratar o widget como um novo.
        if 'upload_xml_di_key' not in st.session_state:
            st.session_state.upload_xml_di_key = 0
        st.session_state.upload_xml_di_key += 1 # Altera a key para forçar a re-renderização como vazio
        # st.rerun() # REMOVIDO: Chamada de st.rerun() dentro de um callback é um no-op e causa o aviso.


def show_page():
    st.subheader("Importar e Analisar XML DI")

    background_image_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'assets', 'logo_navio_atracado.png')
    set_background_image(background_image_path)
    
    _display_edit_popup_before_save()
    _display_items_popup()

    # Se um dos pop-ups estiver ativo, não renderiza o resto da página principal
    if st.session_state.get('show_edit_popup_before_save', False) or st.session_state.get('show_items_popup', False):
        return

    # Inicializa a key do uploader se não existir
    if 'upload_xml_di_key' not in st.session_state:
        st.session_state.upload_xml_di_key = 0

    # Botão de importação XML
    st.file_uploader(
        "Importar XML DI",
        type=["xml"],
        key=f"upload_xml_di_widget_{st.session_state.upload_xml_di_key}",
        on_change=_handle_xml_upload
    )

    st.markdown("---") # Separador após o uploader

    # Ações para a DI selecionada (botões de edição, delete e detalhes)
    if st.session_state.get('selected_di_id'):
        selected_di_id = st.session_state.selected_di_id
        col_edit, col_delete, col_details = st.columns(3)

        with col_edit:
            if st.button("Editar DI Selecionada", key="btn_edit_selected_di"):
                _open_edit_popup(selected_di_id)

        with col_delete:
            # Adiciona uma caixa de seleção para confirmação antes de deletar
            confirm_delete = st.checkbox("Confirmar exclusão desta DI?", key=f"confirm_delete_main_{selected_di_id}")
            delete_button_disabled = not confirm_delete # O botão é desabilitado até que a caixa seja marcada

            if st.button("Deletar DI do Banco de Dados", key=f"btn_delete_main_{selected_di_id}", disabled=delete_button_disabled):
                if delete_declaracao(selected_di_id):
                    st.success(f"DI deletada com sucesso.")
                    st.session_state.selected_di_id = None # Limpa a seleção
                    st.rerun() # MANTER: Essencial para atualizar a tabela após a exclusão e limpar a seleção
                else:
                    st.error(f"Falha ao deletar DI.")
        
        with col_details:
            if st.button(f"Ver Detalhes da DI ID {selected_di_id}", key="btn_view_details"):
                st.session_state.selected_di_id_detalhes = selected_di_id
                st.session_state.current_page = "Pagamentos"
                st.rerun() # MANTER: Essencial para navegar para outra página imediatamente
    else:
        st.info("Selecione uma DI na tabela abaixo para habilitar as opções de edição, exclusão e detalhes.")
    
    st.markdown("---") # Separador antes da tabela

    st.subheader("Declarações de Importação Salvas")

    # --- Início da Seção de Carregamento e Exibição da Tabela ---
    # Sempre recarrega os dados do DB para garantir que estejam atualizados
    raw_data = get_all_declaracoes()
    
    # Converte para dicionários (se ainda não for) e adiciona a coluna de seleção
    # Garante que 'xml_declaracoes_data' no session_state sempre reflita o estado atual do DB
    st.session_state.xml_declaracoes_data = []
    if raw_data:
        for row in raw_data:
            # Garante que 'row' é um dicionário. Se for sqlite3.Row, converte.
            row_dict = dict(row) if not isinstance(row, dict) else row
            
            # Verifica se a DI já estava selecionada para manter o estado do checkbox
            # O problema 'KeyError: 'id'' acontece aqui se row_dict não tiver 'id'.
            # A correção virá do db_utils.py garantindo que 'id' esteja sempre presente.
            is_selected = (st.session_state.get('selected_di_id') == row_dict.get('id'))
            mapped_row = {
                "ID": row_dict.get('id', None), # Usar .get() para robustez, default para None se faltar
                "Número DI": row_dict.get('numero_di'),
                "Data Registro": row_dict.get('data_registro'),
                "Referência": row_dict.get('informacao_complementar'),
                "Arquivo Origem": row_dict.get('arquivo_origem'),
                "Data Importação": row_dict.get('data_importacao'),
                "_Selecionar_DI": is_selected # Mantém o estado do checkbox
            }
            st.session_state.xml_declaracoes_data.append(mapped_row)
    
    # Cria o DataFrame para exibição no st.data_editor
    df_display = pd.DataFrame(st.session_state.xml_declaracoes_data)
    
    # Se o DataFrame estiver vazio, inicializa com as colunas esperadas para evitar KeyError
    if df_display.empty:
        df_display = pd.DataFrame(columns=["ID", "Número DI", "Data Registro", "Referência", "Arquivo Origem", "Data Importação", "_Selecionar_DI"])
    
    # Aplica as formatações APENAS SE AS COLUNAS EXISTIREM
    if "Número DI" in df_display.columns:
        df_display["Número DI"] = df_display["Número DI"].apply(_format_di_number)
    if "Data Registro" in df_display.columns:
        df_display["Data Registro"] = df_display["Data Registro"].apply(lambda x: datetime.strptime(str(x), "%Y-%m-%d").strftime("%d/%m/%Y") if x else "N/A")
    if "Data Importação" in df_display.columns:
        df_display["Data Importação"] = df_display["Data Importação"].apply(lambda x: datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y %H:%M:%S") if x else "N/A")


    if not df_display.empty:
        st.data_editor(
            df_display,
            key="xml_declarations_table_editor",
            hide_index=True,
            use_container_width=True,
            column_config={
                "_Selecionar_DI": st.column_config.CheckboxColumn(
                    "Selecionar",
                    help="Selecione uma DI para editar/excluir",
                    default=False,
                    width="small"
                ),
                "ID": st.column_config.NumberColumn("ID", format="%d", disabled=True, width="small"),
                "Número DI": st.column_config.TextColumn("Número DI", width="medium"),
                "Data Registro": st.column_config.TextColumn("Data Registro", width="small"),
                "Referência": st.column_config.TextColumn("Referência", width="medium"),
                "Arquivo Origem": st.column_config.TextColumn("Arquivo Origem", width="large"),
                "Data Importação": st.column_config.TextColumn("Data Importação", width="medium"),
            },
            on_change=_handle_declarations_table_change
        )
        
        # A lógica para atualizar st.session_state.selected_di_id foi movida para _handle_declarations_table_change
        # e agora é acionada automaticamente na mudança de seleção do checkbox.

    else:
        st.info("Nenhuma declaração de importação encontrada. Importe um XML para começar.")

    # --- Fim da Seção de Carregamento e Exibição da Tabela ---

    # Final general info
    st.markdown("---")
    st.write("Esta tela permite importar XMLs de Declarações de Importação, visualizá-los, editá-los e excluí-los.")


def _open_edit_popup(declaracao_id_db):
    """Abre um modal para editar os dados da DI selecionada."""
    declaracao_data = get_declaracao_by_id(declaracao_id_db)

    if not declaracao_data:
        st.error("Não foi possível carregar os dados da declaração para edição.")
        return

    # Converte para dicionário se ainda não for
    declaracao_dict = dict(declaracao_data) if not isinstance(declaracao_data, dict) else declaracao_data
    
    # Busca os itens da DI para a aba de itens
    itens_data_raw = get_itens_by_declaracao_id(declaracao_id_db)
    # Garante que itens_data_dicts é uma lista de dicionários
    itens_data_dicts = [dict(row) for row in itens_data_raw] if itens_data_raw else []

    # Usando st.container para envolver o formulário e controlar a largura
    with st.container():
        with st.form(key=f"edit_di_form_{declaracao_id_db}"):
            st.subheader(f"Editar DI: {_format_di_number(declaracao_dict.get('numero_di'))}")

            edited_data = {}

            # Usando st.tabs para criar abas
            tab_dados_di, tab_itens_di = st.tabs(["Dados da DI", "Itens da DI"])

            with tab_dados_di:
                # Reorganizando os campos em 3 colunas
                col1, col2, col3 = st.columns(3)

                with col1:
                    edited_data['numero_di'] = st.text_input("Número DI", value=declaracao_dict.get('numero_di', ''))
                    
                    data_registro_dt = None
                    if declaracao_dict.get('data_registro'):
                        try:
                            data_registro_dt = datetime.strptime(str(declaracao_dict['data_registro']), "%Y-%m-%d")
                        except ValueError:
                            data_registro_dt = datetime.now() # Fallback se a data for inválida
                    else:
                        data_registro_dt = datetime.now()
                    edited_data['data_registro'] = st.date_input("Data Registro", value=data_registro_dt).strftime("%Y-%m-%d")

                    edited_data['vmle'] = st.number_input("VMLE (R$)", value=float(declaracao_dict.get('vmle', 0.0) or 0.0), format="%.2f")
                    edited_data['frete'] = st.number_input("Frete (R$)", value=float(declaracao_dict.get('frete', 0.0) or 0.0), format="%.2f")
                    edited_data['seguro'] = st.number_input("Seguro (R$)", value=float(declaracao_dict.get('seguro', 0.0) or 0.0), format="%.2f")
                    edited_data['vmld'] = st.number_input("VMLD (R$)", value=float(declaracao_dict.get('vmld', 0.0) or 0.0), format="%.2f")
                    edited_data['imposto_importacao'] = st.number_input("II (R$)", value=float(declaracao_dict.get('imposto_importacao', 0.0) or 0.0), format="%.2f")
                    edited_data['armazenagem'] = st.number_input("Armazenagem (R$)", value=float(declaracao_dict.get('armazenagem', 0.0) or 0.0), format="%.2f")
                    edited_data['frete_nacional'] = st.number_input("Frete Nacional (R$)", value=float(declaracao_dict.get('frete_nacional', 0.0) or 0.0), format="%.2f")
                    edited_data['peso_bruto'] = st.number_input("Peso Bruto (KG)", value=float(declaracao_dict.get('peso_bruto', 0.0) or 0.0), format="%.3f")

                with col2:
                    edited_data['informacao_complementar'] = st.text_input("Referência", value=declaracao_dict.get('informacao_complementar', '') or "")
                    edited_data['ipi'] = st.number_input("IPI (R$)", value=float(declaracao_dict.get('ipi', 0.0) or 0.0), format="%.2f")
                    edited_data['pis_pasep'] = st.number_input("PIS/PASEP (R$)", value=float(declaracao_dict.get('pis_pasep', 0.0) or 0.0), format="%.2f")
                    edited_data['cofins'] = st.number_input("COFINS (R$)", value=float(declaracao_dict.get('cofins', 0.0) or 0.0), format="%.2f")
                    edited_data['icms_sc'] = st.text_input("ICMS-SC", value=declaracao_dict.get('icms_sc', '') or "")
                    edited_data['taxa_cambial_usd'] = st.number_input("Taxa Cambial (USD)", value=float(declaracao_dict.get('taxa_cambial_usd', 0.0) or 0.0), format="%.6f")
                    edited_data['taxa_siscomex'] = st.number_input("Taxa SISCOMEX (R$)", value=float(declaracao_dict.get('taxa_siscomex', 0.0) or 0.0), format="%.2f")
                    edited_data['numero_invoice'] = st.text_input("Nº Invoice", value=declaracao_dict.get('numero_invoice', '') or "")
                    edited_data['peso_liquido'] = st.number_input("Peso Líquido (KG)", value=float(declaracao_dict.get('peso_liquido', 0.0) or 0.0), format="%.3f")

                with col3:
                    edited_data['cnpj_importador'] = st.text_input("CNPJ Importador", value=declaracao_dict.get('cnpj_importador', '') or "")
                    edited_data['importador_nome'] = st.text_input("Importador Nome", value=declaracao_dict.get('importador_nome', '') or "")
                    edited_data['recinto'] = st.text_input("Recinto", value=declaracao_dict.get('recinto', '') or "")
                    edited_data['embalagem'] = st.text_input("Embalagem", value=declaracao_dict.get('embalagem', '') or "")
                    edited_data['quantidade_volumes'] = st.number_input("Quantidade Volumes", value=int(declaracao_dict.get('quantidade_volumes', 0) or 0), format="%d")
                    edited_data['acrescimo'] = st.number_input("Acréscimo (R$)", value=float(declaracao_dict.get('acrescimo', 0.0) or 0.0), format="%.2f")
                    edited_data['arquivo_origem'] = st.text_input("Arquivo Origem", value=declaracao_dict.get('arquivo_origem', '') or "")
                    
                    # Data de importação não é editável, pega o valor original
                    edited_data['data_importacao'] = declaracao_dict.get('data_importacao', '')

            with tab_itens_di:
                st.markdown("### Itens da DI")
                if itens_data_dicts:
                    df_itens = pd.DataFrame(itens_data_dicts)

                    # Formatar colunas para exibição
                    if not df_itens.empty:
                        if 'ncm_item' in df_itens.columns:
                            df_itens['ncm_item'] = df_itens['ncm_item'].apply(_format_ncm)
                        if 'quantidade' in df_itens.columns:
                            df_itens['quantidade'] = df_itens['quantidade'].apply(_format_int)
                        if 'valor_unitario' in df_itens.columns:
                            df_itens['valor_unitario'] = df_itens['valor_unitario'].apply(_format_currency_usd)
                        if 'valor_item_calculado' in df_itens.columns:
                            df_itens['valor_item_calculado'] = df_itens['valor_item_calculado'].apply(_format_currency)
                        if 'peso_liquido_item' in df_itens.columns:
                            df_itens['peso_liquido_item'] = df_itens['peso_liquido_item'].apply(_format_weight_no_kg)
                        if 'ii_percent_item' in df_itens.columns:
                            df_itens['ii_percent_item'] = df_itens['ii_percent_item'].apply(_format_percentage)
                        if 'ipi_percent_item' in df_itens.columns:
                            df_itens['ipi_percent_item'] = df_itens['ipi_percent_item'].apply(_format_percentage)
                        if 'pis_percent_item' in df_itens.columns:
                            df_itens['pis_percent_item'] = df_itens['pis_percent_item'].apply(_format_percentage)
                        if 'cofins_percent_item' in df_itens.columns:
                            df_itens['cofins_percent_item'] = df_itens['cofins_percent_item'].apply(_format_percentage)
                        if 'icms_percent_item' in df_itens.columns:
                            df_itens['icms_percent_item'] = df_itens['icms_percent_item'].apply(_format_percentage)
                        if 'custo_unit_di_usd' in df_itens.columns:
                            df_itens['custo_unit_di_usd'] = df_itens['custo_unit_di_usd'].apply(_format_currency_usd)

                    cols_to_display = [
                        "numero_adicao", "numero_item_sequencial", "sku_item", "descricao_mercadoria",
                        "quantidade", "unidade_medida", "ncm_item", "custo_unit_di_usd",
                        "ii_percent_item", "ipi_percent_item", "pis_percent_item", "cofins_percent_item"
                    ]
                    # Filtra as colunas para exibição
                    cols_to_display_filtered = [col for col in cols_to_display if col in df_itens.columns]
                    st.dataframe(df_itens[cols_to_display_filtered], use_container_width=True, hide_index=True)
                else:
                    st.info("Nenhum item encontrado para esta DI.")


            col_save, col_delete_popup, col_cancel_edit = st.columns(3) # 3 colunas para os botões

            with col_save:
                if col_save.form_submit_button("Salvar Alterações"):
                    if update_declaracao(declaracao_id_db, edited_data):
                        st.success("Declaração de Importação atualizada com sucesso!")
                        st.session_state.xml_declaracoes_data = get_all_declaracoes() # Recarrega a tabela
                        st.session_state.selected_di_id = None # Limpa seleção para fechar popup
                        st.rerun() # MANTER: Necessário para garantir que o pop-up feche e a tabela seja atualizada
                    else:
                        st.error(f"Falha ao atualizar a Declaração de Importação.")
            
            with col_delete_popup:
                # Ajuste para que checkbox e botão fiquem na mesma linha
                confirm_delete_popup = st.checkbox("Confirmar exclusão", key=f"confirm_delete_popup_{declaracao_id_db}")
                if st.form_submit_button("Excluir DI", help="Exclui a DI e todos os seus itens permanentemente."):
                    if confirm_delete_popup:
                        if delete_declaracao(declaracao_id_db):
                            st.success(f"DI {_format_di_number(declaracao_dict.get('numero_di'))} excluída com sucesso!")
                            st.session_state.xml_declaracoes_data = get_all_declaracoes()
                            st.session_state.selected_di_id = None # Limpa seleção para fechar popup
                            st.rerun() # MANTER: Essencial para atualizar a tabela após a exclusão e limpar a seleção
                        else:
                            st.error(f"Falha ao excluir a DI {_format_di_number(declaracao_dict.get('numero_di'))}.")
                    else:
                        st.warning("Marque a caixa de confirmação para excluir.")

            with col_cancel_edit:
                if col_cancel_edit.form_submit_button("Cancelar Edição"):
                    st.session_state.selected_di_id = None # Limpa seleção para fechar popup
                    st.rerun() # MANTER: Essencial para fechar o pop-up
# Função para ser importada por outras páginas para atualizar a DI
def update_declaracao_from_page(declaracao_id: int, di_data: Dict[str, Any]):
    """
    Função wrapper para update_declaracao do db_utils,
    para ser usada por outras páginas (ex: cálculos) que precisam atualizar a DI.
    """
    return update_declaracao(declaracao_id, di_data)
