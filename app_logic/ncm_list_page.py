import streamlit as st
import pandas as pd
import sys
import os
import logging
import re

# Adiciona o diretório raiz do projeto ao sys.path para importações
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Importar db_utils
try:
    import db_utils
except ImportError:
    st.error("ERRO: Não foi possível importar o módulo 'db_utils'.")
    st.stop()

logger = logging.getLogger(__name__)

def format_ncm_code(ncm_raw: str) -> str:
    """
    Formata o código NCM para o padrão '****.**.**' e garante 8 caracteres numéricos.
    Remove caracteres não numéricos e insere os pontos de formatação.
    """
    # Remove tudo que não é dígito
    ncm_digits = re.sub(r'\D', '', ncm_raw)

    # Trunca para no máximo 8 dígitos
    ncm_digits = ncm_digits[:8]

    # Aplica a formatação
    formatted_ncm = ""
    if len(ncm_digits) > 0:
        formatted_ncm += ncm_digits[0:4]
    if len(ncm_digits) > 4:
        formatted_ncm += "." + ncm_digits[4:6]
    if len(ncm_digits) > 6:
        formatted_ncm += "." + ncm_digits[6:8]
    
    return formatted_ncm

def show_ncm_list_page():
    """
    Exibe a página de Listagem NCM para adicionar, visualizar e gerenciar itens NCM.
    """
    st.title("Cadastro e Consulta de NCM e Impostos")

    # Formulário para adicionar/atualizar NCM
    with st.expander("Adicionar/Atualizar Item NCM", expanded=False):
        st.subheader("Dados do Item NCM")
        
        # Campo para Código NCM com formatação
        ncm_code_input = st.text_input(
            "Código NCM (formato: XXXX.XX.XX)",
            key="ncm_code_input_raw",
            value=st.session_state.get('current_ncm_code_display', '') # Mantém o valor formatado
        )
        
        # Aplica a formatação e atualiza o estado para exibição
        formatted_ncm_value = format_ncm_code(ncm_code_input)
        if formatted_ncm_value != st.session_state.get('current_ncm_code_display', ''):
            st.session_state['current_ncm_code_display'] = formatted_ncm_value
            # st.rerun() # Descomentar se precisar de atualização visual imediata ao digitar

        descricao_item = st.text_area("Descrição do Item", key="descricao_item_input").strip()
        
        col1, col2 = st.columns(2)
        with col1:
            ii_aliquota = st.number_input("Alíquota II (%)", min_value=0.0, max_value=100.0, value=0.0, step=0.01, key="ii_aliquota_input")
            ipi_aliquota = st.number_input("Alíquota IPI (%)", min_value=0.0, max_value=100.0, value=0.0, step=0.01, key="ipi_aliquota_input")
            pis_aliquota = st.number_input("Alíquota PIS (%)", min_value=0.0, max_value=100.0, value=0.0, step=0.01, key="pis_aliquota_input")
        with col2:
            cofins_aliquota = st.number_input("Alíquota COFINS (%)", min_value=0.0, max_value=100.0, value=0.0, step=0.01, key="cofins_aliquota_input")
            icms_aliquota = st.number_input("Alíquota ICMS (%)", min_value=0.0, max_value=100.0, value=0.0, step=0.01, key="icms_aliquota_input")

        if st.button("Salvar Item NCM"):
            # Valida o NCM formatado para ter exatamente 8 dígitos (desconsiderando pontos)
            ncm_numeric_only = re.sub(r'\D', '', formatted_ncm_value)
            if not ncm_numeric_only or len(ncm_numeric_only) != 8:
                st.warning("O Código NCM deve conter exatamente 8 dígitos numéricos.")
            elif not descricao_item:
                st.warning("Por favor, preencha a Descrição do Item.")
            else:
                if db_utils.adicionar_ou_atualizar_ncm_item(
                    ncm_numeric_only, # Salva no banco apenas os dígitos numéricos
                    descricao_item, ii_aliquota, ipi_aliquota,
                    pis_aliquota, cofins_aliquota, icms_aliquota
                ):
                    st.success(f"Item NCM '{formatted_ncm_value}' salvo com sucesso!")
                    st.session_state['current_ncm_code_display'] = '' # Limpa o campo após salvar
                    st.rerun() # Recarrega a página para atualizar a tabela
                else:
                    st.error("Erro ao salvar o item NCM. Verifique os logs para mais detalhes.")

    st.markdown("---")

    # Seção para importar Excel
    with st.expander("Importar NCMs de arquivo Excel/CSV", expanded=False):
        uploaded_file = st.file_uploader("Escolha um arquivo Excel ou CSV", type=["xlsx", "csv"])
        if uploaded_file:
            try:
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file)
                else: # Assumes .xlsx
                    df = pd.read_excel(uploaded_file)
                
                st.info(f"Arquivo '{uploaded_file.name}' carregado. Visualização das primeiras linhas:")
                st.dataframe(df.head())

                if st.button("Processar e Inserir/Atualizar NCMs do Arquivo"):
                    total_rows = len(df)
                    success_count = 0
                    fail_count = 0
                    
                    # ATUALIZADO: Mapeamento de colunas corrigido para corresponder ao seu Excel
                    column_mapping = {
                        'NCM': 'NCM', # Nome da coluna no seu arquivo Excel
                        'DESCRIÇÃO': 'DESCRIÇÃO', # Nome da coluna no seu arquivo Excel
                        'II (%)': 'II (%)', # Nome da coluna no seu arquivo Excel
                        'IPI (%)': 'IPI (%)', # Nome da coluna no seu arquivo Excel
                        'PIS (%)': 'PIS (%)', # Nome da coluna no seu arquivo Excel
                        'COFINS (%)': 'COFINS (%)', # Nome da coluna no seu arquivo Excel
                        'ICMS (%)': 'ICMS (%)', # Nome da coluna no seu arquivo Excel
                    }

                    # Verifica se todas as colunas esperadas estão presentes
                    missing_columns = [col for col in column_mapping.keys() if col not in df.columns]
                    if missing_columns:
                        st.error(f"Colunas ausentes no arquivo: {', '.join(missing_columns)}. Por favor, verifique o cabeçalho do arquivo.")
                    else:
                        progress_bar = st.progress(0)
                        status_text = st.empty()

                        for index, row in df.iterrows():
                            status_text.text(f"Processando linha {index + 1}/{total_rows}...")
                            progress_bar.progress((index + 1) / total_rows)

                            try:
                                # Tenta extrair os dados, convertendo para os tipos corretos
                                # Agora, acessamos diretamente os nomes das colunas como estão no DataFrame
                                ncm_code = str(row[column_mapping['NCM']]).strip()
                                # Formata o NCM antes de salvar (remove pontos e garante 8 dígitos)
                                ncm_code_clean = re.sub(r'\D', '', ncm_code)[:8]

                                descricao_item = str(row[column_mapping['DESCRIÇÃO']]).strip()
                                
                                # Converte para float, tratando valores vazios ou não numéricos
                                ii = float(str(row.get(column_mapping['II (%)'], 0.0)).replace('%', '').replace(',', '.') or 0.0) # Adicionado .replace(',', '.') para vírgulas em números
                                ipi = float(str(row.get(column_mapping['IPI (%)'], 0.0)).replace('%', '').replace(',', '.') or 0.0)
                                pis = float(str(row.get(column_mapping['PIS (%)'], 0.0)).replace('%', '').replace(',', '.') or 0.0)
                                cofins = float(str(row.get(column_mapping['COFINS (%)'], 0.0)).replace('%', '').replace(',', '.') or 0.0)
                                icms = float(str(row.get(column_mapping['ICMS (%)'], 0.0)).replace('%', '').replace(',', '.') or 0.0)


                                if db_utils.adicionar_ou_atualizar_ncm_item(
                                    ncm_code_clean, descricao_item, ii, ipi, pis, cofins, icms
                                ):
                                    success_count += 1
                                else:
                                    fail_count += 1
                                    logger.error(f"Falha ao processar a linha {index + 1} (NCM: {ncm_code}).")
                            except KeyError as ke:
                                st.error(f"Erro: Coluna '{ke}' não encontrada na linha {index + 1}. Verifique o mapeamento das colunas.")
                                fail_count += 1
                            except Exception as e:
                                fail_count += 1
                                st.error(f"Erro ao processar a linha {index + 1} (NCM: {ncm_code}): {e}")
                                logger.error(f"Erro ao processar linha {index + 1} do Excel: {e}")

                        status_text.empty()
                        progress_bar.empty()
                        st.success(f"Processamento concluído: {success_count} itens inseridos/atualizados, {fail_count} falhas.")
                        st.rerun() # Recarrega a página para exibir os dados atualizados
            except Exception as e:
                st.error(f"Erro ao ler o arquivo: {e}. Certifique-se de que é um arquivo Excel ou CSV válido e que as colunas estão corretas.")
                logger.error(f"Erro ao carregar/processar arquivo Excel/CSV: {e}")

    st.markdown("---")

    # Tabela para exibir os itens NCM existentes
    st.subheader("Itens NCM Cadastrados")
    itens_ncm = db_utils.selecionar_todos_ncm_itens()

    if itens_ncm:
        # Converte os resultados para um DataFrame do pandas
        # As colunas retornadas por sqlite3.Row são os nomes reais do banco de dados.
        # Criamos o DataFrame com esses nomes para facilitar o acesso.
        df_display_editable = pd.DataFrame(itens_ncm, columns=[
            "ID", "ncm_code", "descricao_item", "ii_aliquota", "ipi_aliquota", "pis_aliquota", "cofins_aliquota", "icms_aliquota"
        ])
        
        # Adiciona a coluna NCM formatada para exibição
        df_display_editable["Código NCM"] = df_display_editable["ncm_code"].apply(format_ncm_code)

        # Renomeia as colunas de alíquotas para os nomes de exibição (sem o RAW), mas os dados subjacentes são numéricos
        df_display_editable = df_display_editable.rename(columns={
            "descricao_item": "Descrição",
            "ii_aliquota": "II (%)",
            "ipi_aliquota": "IPI (%)",
            "pis_aliquota": "PIS (%)",
            "cofins_aliquota": "COFINS (%)",
            "icms_aliquota": "ICMS (%)"
        })

        # Exibe a tabela editável, passando o DataFrame com os tipos numéricos corretos
        edited_df = st.data_editor(
            df_display_editable[['ID', 'Código NCM', 'Descrição', 'II (%)', 'IPI (%)', 'PIS (%)', 'COFINS (%)', 'ICMS (%)']], # Seleciona as colunas a serem exibidas
            column_config={
                "ID": st.column_config.Column("ID", disabled=True),
                "Código NCM": st.column_config.TextColumn("Código NCM", disabled=True), # NCM não é editável na tabela
                "Descrição": st.column_config.TextColumn("Descrição do Item", help="Descrição detalhada do item NCM."),
                "II (%)": st.column_config.NumberColumn("II (%)", format="%.2f%%", help="Alíquota do Imposto de Importação.", min_value=0.0, max_value=100.0, step=0.01),
                "IPI (%)": st.column_config.NumberColumn("IPI (%)", format="%.2f%%", help="Alíquota do Imposto sobre Produtos Industrializados.", min_value=0.0, max_value=100.0, step=0.01),
                "PIS (%)": st.column_config.NumberColumn("PIS (%)", format="%.2f%%", help="Alíquota do Programa de Integração Social.", min_value=0.0, max_value=100.0, step=0.01),
                "COFINS (%)": st.column_config.NumberColumn("COFINS (%)", format="%.2f%%", help="Alíquota da Contribuição para o Financiamento da Seguridade Social.", min_value=0.0, max_value=100.0, step=0.01),
                "ICMS (%)": st.column_config.NumberColumn("ICMS (%)", format="%.2f%%", help="Alíquota do Imposto sobre Circulação de Mercadorias e Serviços.", min_value=0.0, max_value=100.0, step=0.01),
            },
            hide_index=True,
            num_rows="fixed", # Impede que o usuário adicione novas linhas diretamente na tabela
            key="ncm_data_editor"
        )

        if st.button("Atualizar Itens Selecionados da Tabela"):
            # Para cada linha editada, encontre a linha original e atualize
            for edited_row_dict in edited_df.to_dict('records'):
                ncm_code_clean = re.sub(r'\D', '', edited_row_dict['Código NCM'])
                
                # Para evitar acessar o DataFrame original novamente, use o NCM limpo
                # e recupere o item NCM original para comparação (se necessário)
                original_ncm_item_data = db_utils.get_ncm_item_by_ncm_code(ncm_code_clean)
                
                if original_ncm_item_data:
                    # Get the values from the edited row
                    new_descricao_item = edited_row_dict['Descrição']
                    new_ii_aliquota = edited_row_dict['II (%)']
                    new_ipi_aliquota = edited_row_dict['IPI (%)']
                    new_pis_aliquota = edited_row_dict['PIS (%)']
                    new_cofins_aliquota = edited_row_dict['COFINS (%)']
                    new_icms_aliquota = edited_row_dict['ICMS (%)']

                    # Compare with original values from the database (assuming 'original_ncm_item_data' is a dict or similar)
                    desc_changed = original_ncm_item_data['descricao_item'] != new_descricao_item
                    ii_changed = original_ncm_item_data['ii_aliquota'] != new_ii_aliquota
                    ipi_changed = original_ncm_item_data['ipi_aliquota'] != new_ipi_aliquota
                    pis_changed = original_ncm_item_data['pis_aliquota'] != new_pis_aliquota
                    cofins_changed = original_ncm_item_data['cofins_aliquota'] != new_cofins_aliquota
                    icms_changed = original_ncm_item_data['icms_aliquota'] != new_icms_aliquota
                    
                    if desc_changed or ii_changed or ipi_changed or pis_changed or cofins_changed or icms_changed:
                        if db_utils.adicionar_ou_atualizar_ncm_item(
                            ncm_code_clean,
                            new_descricao_item,
                            new_ii_aliquota,
                            new_ipi_aliquota,
                            new_pis_aliquota,
                            new_cofins_aliquota,
                            new_icms_aliquota
                        ):
                            st.success(f"Item NCM '{edited_row_dict['Código NCM']}' atualizado com sucesso!")
                        else:
                            st.error(f"Erro ao atualizar o item NCM '{edited_row_dict['Código NCM']}'.")
            st.rerun() # Recarrega a página para refletir as atualizações

        # Opção para deletar itens
        st.markdown("---")
        st.subheader("Deletar Item NCM")
        
        # Novo: Cria um dicionário para mapear ID para o NCM formatado
        # Usando 'ncm_code' ao invés de 'NCM_RAW'
        ncm_options_map = {item["ID"]: format_ncm_code(item["ncm_code"]) for item in itens_ncm}

        ncm_ids_to_delete = st.multiselect(
            "Selecione os IDs dos itens NCM para deletar:",
            options=list(ncm_options_map.keys()), # Passa apenas os IDs
            format_func=lambda x: f"ID: {x} - NCM: {ncm_options_map.get(x, 'N/A')}", # Usa o mapa para formatar
            key="ncm_delete_multiselect"
        )
        if st.button("Deletar Itens Selecionados"):
            if ncm_ids_to_delete:
                for ncm_id in ncm_ids_to_delete:
                    if db_utils.deletar_ncm_item(ncm_id):
                        st.success(f"Item NCM com ID '{ncm_id}' excluído com sucesso.")
                    else:
                        st.error(f"Erro ao deletar o item NCM com ID '{ncm_id}'.")
                st.rerun() # Recarrega a página para atualizar a tabela
            else:
                st.warning("Selecione pelo menos um item para deletar.")

    else:
        st.info("Nenhum item NCM cadastrado ainda. Use o formulário acima para adicionar um novo.")
