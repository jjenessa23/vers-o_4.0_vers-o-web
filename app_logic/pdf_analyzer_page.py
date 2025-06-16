import streamlit as st
import pdfplumber
import re
import pandas as pd
from io import BytesIO
import tempfile
import os

# Importar o db_utils para buscar descrições de produtos (assumindo que este arquivo existe e funciona)
import db_utils

def find_table_bbox_by_markers(pdf_page, area_start_marker_pattern, area_end_marker_pattern, table_header_pattern, table_footer_pattern):
    """
    Tenta encontrar a bounding box (bbox) para uma seção de tabela usando marcadores de início e fim da área,
    e padrões para o cabeçalho e rodapé da tabela dentro dessa área.
    Retorna uma tupla (x0, y0, x1, y1) ou None.
    """
    all_words = pdf_page.extract_words()
    
    # 1. Encontrar a área geral de busca usando area_start_marker_pattern e area_end_marker_pattern
    broad_search_start_y = 0
    start_section_pattern = re.compile(area_start_marker_pattern, re.IGNORECASE)
    # Reconstruct lines for better marker detection
    lines_by_y_group = {}
    for word_obj in all_words:
        line_y_group_key = int(word_obj['top'] // 3) * 3 
        if line_y_group_key not in lines_by_y_group:
            lines_by_y_group[line_y_group_key] = []
        lines_by_y_group[line_y_group_key].append(word_obj)

    sorted_line_keys = sorted(lines_by_y_group.keys())

    for line_key in sorted_line_keys:
        words_in_current_line = sorted(lines_by_y_group[line_key], key=lambda w: w['x0'])
        full_line_text = " ".join([w['text'] for w in words_in_current_line])
        
        if start_section_pattern.search(full_line_text):
            broad_search_start_y = max(broad_search_start_y, min(w['bottom'] for w in words_in_current_line)) # Get the bottom of the marker line
            break
    
    broad_search_end_y = pdf_page.height
    end_section_pattern = re.compile(area_end_marker_pattern, re.IGNORECASE)
    # Search for end marker after start marker has been found
    for line_key in sorted_line_keys:
        words_in_current_line = sorted(lines_by_y_group[line_key], key=lambda w: w['x0'])
        full_line_text = " ".join([w['text'] for w in words_in_current_line])
        current_line_top = min(w['top'] for w in words_in_current_line) # Top of current line
        
        if current_line_top > broad_search_start_y and end_section_pattern.search(full_line_text):
            broad_search_end_y = min(broad_search_end_y, max(w['top'] for w in words_in_current_line)) # Get the top of the marker line
            break

    # If section markers were not found, or area is invalid
    if broad_search_start_y == 0 and start_section_pattern.pattern != r"": 
         st.warning(f"Marcador de início de seção '{area_start_marker_pattern}' não encontrado.")
         return None
    if broad_search_end_y == pdf_page.height and end_section_pattern.pattern != r"": 
        st.warning(f"Marcador de fim de seção '{area_end_marker_pattern}' não encontrado.")
        # Attempt to proceed without end marker if it's the end of the page
        broad_search_end_y = pdf_page.height # Set to end of page as fallback
    
    # Ensure broad search area is valid and has minimum height
    if broad_search_end_y <= broad_search_start_y + 10: 
        st.warning(f"Área de busca ampla inválida para marcadores de seção: start_y={broad_search_start_y}, end_y={broad_search_end_y}. Marcadores muito próximos ou invertidos.")
        return None

    # Filter words to the broad search area
    words_in_broad_area = [w for w in all_words if w['top'] >= broad_search_start_y and w['bottom'] <= broad_search_end_y]
    if not words_in_broad_area: 
        st.warning(f"Nenhuma palavra encontrada na área de busca ampla após filtrar por seção.")
        return None

    # 2. Reconstruct lines and find the table header and footer within the broad area
    table_header_y = None 
    table_footer_y = None 

    header_pattern = re.compile(table_header_pattern, re.IGNORECASE)
    footer_pattern = re.compile(table_footer_pattern, re.IGNORECASE)

    lines_by_y_group_filtered = {}
    for word_obj in words_in_broad_area:
        line_y_group_key = int(word_obj['top'] // 3) * 3 
        if line_y_group_key not in lines_by_y_group_filtered:
            lines_by_y_group_filtered[line_y_group_key] = []
        lines_by_y_group_filtered[line_y_group_key].append(word_obj)

    sorted_line_keys_filtered = sorted(lines_by_y_group_filtered.keys())

    for line_key in sorted_line_keys_filtered:
        words_in_current_line = sorted(lines_by_y_group_filtered[line_key], key=lambda w: w['x0'])
        full_line_text = " ".join([w['text'] for w in words_in_current_line])
        
        current_line_top = min(w['top'] for w in words_in_current_line)
        current_line_bottom = max(w['bottom'] for w in words_in_current_line)

        # Search for table header
        if header_pattern.search(full_line_text):
            table_header_y = current_line_top
        
        # Search for table footer *after* header found
        if table_header_y is not None and current_line_top > table_header_y:
            if footer_pattern.search(full_line_text):
                table_footer_y = current_line_bottom
                break 

    # 3. Define the final bbox
    if table_header_y is None:
        st.warning(f"Cabeçalho da tabela '{table_header_pattern}' não encontrado dentro da área de busca ampla. Não é possível determinar a bbox da tabela.")
        return None
    
    # Fallback if footer is not found, use the end of the broad search area
    if table_footer_y is None:
        st.warning(f"Rodapé da tabela '{table_footer_pattern}' não encontrado. Usando o limite inferior da área de busca ampla como rodapé da tabela.")
        table_footer_y = broad_search_end_y 

    x0 = pdf_page.bbox[0] 
    x1 = pdf_page.bbox[2]
    
    # Add buffers to capture content accurately.
    # We want to start slightly above the header and end slightly below the footer.
    buffer_top = 15  # Increased buffer for header
    buffer_bottom = 15 # Increased buffer for footer

    y0_final = max(0, table_header_y - buffer_top)
    y1_final = min(pdf_page.height, table_footer_y + buffer_bottom)

    # Ensure final calculated area is valid and has a minimum height
    if y1_final <= y0_final + 5: 
        st.warning(f"Área da tabela calculada muito pequena ou inválida para padrões '{table_header_pattern}' e '{table_footer_pattern}': y0={y0_final}, y1={y1_final}. Tentando usar uma área mais geral para a tabela.")
        # Fallback to a more general area if precise area is invalid, but still within broad search
        y0_final = broad_search_start_y + 10 # Start slightly after broad section marker
        y1_final = broad_search_end_y - 10   # End slightly before broad section end marker
        if y1_final <= y0_final + 5: 
            st.error("Falha ao determinar uma área de tabela válida mesmo com fallback geral.")
            return None 

    return (x0, y0_final, x1, y1_final)


def extract_invoice_data(pdf_page):
    """
    Extrai informações gerais da fatura da página PDF.
    Assume que essas informações estão em posições relativamente fixas ou seguem padrões de texto.
    """
    text = pdf_page.extract_text()
    data = {}

    # Extrair Invoice No.
    invoice_no_match = re.search(r"Invoice No\.:\s*([A-Za-z0-9-]+)", text)
    if invoice_no_match: 
        data['Invoice N#'] = invoice_no_match.group(1).strip()
    else:
        invoice_no_match = re.search(r"Invoice No\.:\s*\"\s*([A-Za-z0-9-]+)\s*\"", text)
        if invoice_no_match:
            data['Invoice N#'] = invoice_no_match.group(1).strip()
        else:
            data['Invoice N#'] = "N/A"

    # Extrair Fornecedor (da primeira linha, assumindo que é o nome da empresa)
    lines = text.split('\n')
    if lines:
        data['Fornecedor'] = lines[0].strip()
        # Adjusted logic to capture manufacturer from "Manufacturer: "
        manufacturer_match = re.search(r"Manufacturer:\s*(.*)", text, re.IGNORECASE)
        if manufacturer_match:
            data['Fornecedor'] = manufacturer_match.group(1).strip()
        elif "LTD" not in data['Fornecedor'].upper() and len(lines) > 1:
            if "LTD" in lines[1].upper():
                data['Fornecedor'] = lines[1].strip()
    return data

def extract_products_table_from_pdfplumber_tables(pdfplumber_tables, section_keyword="PAID PRODUCTS", invoice_fornecedor="N/A"):
    """
    Processa uma lista de tabelas extraídas por pdfplumber.extract_tables() para o formato desejado.
    """
    product_data = []
    
    # Define as colunas esperadas na saída final e seus mapeamentos de sinônimos/variantes
    # Isso ajuda a flexibilizar a detecção de cabeçalhos
    expected_headers_mapping = {
        "EXP ou Fabricante": ["EXP ou Fabricante", "EXP", "Fabricante", "Exporter", "Manufacturer"], 
        "Código Interno": ["Código Interno", "COD ERP", "ERP Code", "Internal Code"],
        "Fornecedor": ["Fornecedor", "Supplier"],
        "Invoice N#": ["Invoice N#", "Invoice No.", "Invoice Number"],
        "NCM": ["NCM", "HS Code"],
        "Cobertura": ["Cobertura", "Coverage"],
        "Denominação do produto": ["Denominação do produto", "DESCRIPTION", "Product Name"],
        "SKU": ["SKU", "Part No."],
        "Detalhamento complementar do produto": ["Detalhamento complementar do produto", "MODEL", "Description Model", "Detailed Description"],
        "Qtde": ["Qtde", "QTY", "Quantity", "QTY. (PCS)"],
        "Peso Unitário": ["Peso Unitário", "Unit Weight", "GW/NW (KGS)", "Net Weight (KGS)"], 
        "Valor Unitário": ["Valor Unitário", "UNIT PRICE (USD)", "Unit Price"],
        "Valor total do item": ["Valor total do item", "AMOUNT (USD)", "Amount"] 
    }

    # Inverte o mapeamento para encontrar a chave interna a partir de um cabeçalho extraído do PDF
    reverse_header_mapping = {}
    for internal_header, pdf_variants in expected_headers_mapping.items():
        for variant in pdf_variants:
            reverse_header_mapping[variant.lower()] = internal_header 

    for table in pdfplumber_tables:
        if not table or len(table) < 2:
            continue

        header_row_index = -1
        actual_headers = [] 
        
        best_header_row_index = -1
        max_matched_headers = 0
        best_actual_headers = []

        for i, row in enumerate(table):
            if not row:
                continue
            
            row_elements = [str(c).strip() for c in row if c is not None and str(c).strip() != '']
            
            current_matched_headers = []
            temp_headers = []
            for cell_value in row_elements:
                found_match = False
                for expected_internal_header, variants in expected_headers_mapping.items():
                    if cell_value.lower() in [v.lower() for v in variants]:
                        current_matched_headers.append(expected_internal_header) 
                        temp_headers.append(cell_value) 
                        found_match = True
                        break
                if not found_match and cell_value:
                    temp_headers.append(cell_value)

            if len(current_matched_headers) > max_matched_headers:
                max_matched_headers = len(current_matched_headers)
                best_header_row_index = i
                best_actual_headers = temp_headers 

        if best_header_row_index != -1 and max_matched_headers >= 3: 
            header_row_index = best_header_row_index
            actual_headers = best_actual_headers
        else:
            st.warning(f"Não foi possível identificar uma linha de cabeçalho confiável (menos de 3 cabeçalhos conhecidos). Ignorando esta tabela.")
            continue

        st.info(f"Cabeçalhos detectados no PDF: {actual_headers}")

        for row_index in range(header_row_index + 1, len(table)):
            row = table[row_index]
            if not row:
                continue
            
            row_text_upper = " ".join([str(c).strip() for c in row if c is not None]).upper()
            if "TOTAL QUANTITY" in row_text_upper or "TOTAL AMOUNT" in row_text_upper or "SAY TOTAL" in row_text_upper or "SUBTOTAL" in row_text_upper:
                st.info(f"Linha de total detectada e ignorada: {row_text_upper}")
                continue

            item = {}
            for col_idx, header_pdf_raw in enumerate(actual_headers):
                if col_idx >= len(row) or row[col_idx] is None:
                    continue 

                header_pdf_lower = header_pdf_raw.lower()
                mapped_header = reverse_header_mapping.get(header_pdf_lower)
                
                # Only include columns that are explicitly mapped
                if mapped_header:
                    item[mapped_header] = str(row[col_idx]).strip()
            
            if item:
                final_item = process_product_item(item, section_keyword, invoice_fornecedor) 
                if final_item:
                    product_data.append(final_item)
                else:
                    st.warning(f"Item processado resultou em None: {item}")
            else:
                st.warning(f"Linha de dados vazia ou não processável: {row}")

    return product_data

def process_product_item(item, section_keyword, invoice_fornecedor): 
    """
    Processa um item individual da tabela e retorna um dicionário formatado
    """
    final_item = {}
    
    final_item["Código Interno"] = item.get("Código Interno", "N/A")
    final_item["Denominação do produto"] = item.get("Denominação do produto", "N/A")
    final_item["SKU"] = item.get("SKU", "N/A")
    final_item["Detalhamento complementar do produto"] = item.get("Detalhamento complementar do produto", "N/A")
    final_item["Fornecedor"] = item.get("Fornecedor", "N/A") 
    final_item["Invoice N#"] = item.get("Invoice N#", "N/A")
    final_item["NCM"] = item.get("NCM", "N/A") 

    qty_str = str(item.get("Qtde", "0")).replace(',', '').strip() 
    try:
        final_item["Qtde"] = str(int(float(qty_str)))
    except ValueError:
        final_item["Qtde"] = "N/A"

    unit_price_str = str(item.get("Valor Unitário", "0")).replace('$', '').replace(',', '').strip() 
    try:
        final_item["Valor Unitário"] = float(unit_price_str)
    except ValueError:
        final_item["Valor Unitário"] = 0.0

    peso_unitario_str = str(item.get("Peso Unitário", "0")).replace(',', '.').strip() 
    try:
        final_item["Peso Unitário"] = float(peso_unitario_str)
    except ValueError:
        final_item["Peso Unitário"] = 0.0

    final_item["Cobertura"] = "SIM" if "PAID PRODUCTS" in section_keyword.upper() else "NÃO"
    
    exp_ou_fabricante_extracted = item.get("EXP ou Fabricante")
    if exp_ou_fabricante_extracted in ["N/A", "", None] or str(exp_ou_fabricante_extracted).strip().lower() == "manufacturer": 
        final_item["EXP ou Fabricante"] = invoice_fornecedor 
    else:
        final_item["EXP ou Fabricante"] = exp_ou_fabricante_extracted
    
    total_item_value_str = str(item.get("Valor total do item", "0")).replace('$', '').replace(',', '').strip() 
    try:
        final_item["Valor total do item"] = float(total_item_value_str)
    except ValueError:
        final_item["Valor total do item"] = 0.0

    return final_item


def show_pdf_analyzer_page():
    st.subheader("Análise de Faturas/Packing List PDF")
    st.info("Faça o upload de um arquivo PDF (Fatura Comercial ou Packing List) para extrair e analisar as informações.")

    uploaded_file = st.file_uploader("Escolha um arquivo PDF", type=["pdf"])

    if uploaded_file is not None:
        try:
            with pdfplumber.open(uploaded_file) as pdf:
                first_page = pdf.pages[0]
                second_page = pdf.pages[1] if len(pdf.pages) > 1 else None

                invoice_info = extract_invoice_data(first_page)
                invoice_fornecedor = invoice_info.get('Fornecedor', 'N/A')
                invoice_n_fat = invoice_info.get('Invoice N#', 'N/A')

                # --- Extração de Produtos Pagos usando pdfplumber com bboxes refinadas ---
                paid_bbox = find_table_bbox_by_markers(
                    first_page,
                    area_start_marker_pattern=r"PAID\s+PRODUCTS|PRODUTOS\s+PAGOS", 
                    area_end_marker_pattern=r"PRODUTOS\s+NÃO\s+PAGOS|FREE\s+OF\s+CHARGE\s+PRODUCTS|TOTAIS\s+GERAIS|VALOR\s+TOTAL|Say\s+Total\s+Amount|TOTAL\s+QUANTITY|TOTAL\s+AMOUNT|\n\s*\d+\s*$", 
                    table_header_pattern=r"EXP\s+ou\s+Fabricante|COD\s+ERP|DESCRIPTION|MODEL|SKU|QTY|UNIT\s+PRICE|AMOUNT|Código\s+Interno|Denominação\s+do\s+produto", 
                    table_footer_pattern=r".*(total\s+quantity|TOTAL\s+AMOUNT|Say\s+Total\s+Amount|SUBTOTAL|VALOR\s+TOTAL|\n\s*\d+\s*$).*" 
                )
                
                paid_products_raw = []
                if paid_bbox:
                    st.write(f"Tentando extrair PRODUTOS PAGOS com pdfplumber na área: {paid_bbox}")
                    paid_page_region = first_page.crop(paid_bbox)
                    table_settings_paid = {
                        "vertical_strategy": "lines", 
                        "horizontal_strategy": "lines", 
                        "snap_tolerance": 3,
                        "text_tolerance": 1,
                        "intersection_tolerance": 3
                    }
                    pdfplumber_paid_tables = paid_page_region.extract_tables(table_settings_paid)
                    if pdfplumber_paid_tables:
                        st.info(f"pdfplumber encontrou {len(pdfplumber_paid_tables)} tabela(s) para PRODUTOS PAGOS.")
                        paid_products_raw = extract_products_table_from_pdfplumber_tables(pdfplumber_paid_tables, section_keyword="PAID PRODUCTS", invoice_fornecedor=invoice_fornecedor)
                    else:
                        st.warning("pdfplumber não encontrou tabelas na área especificada para PRODUTOS PAGOS.")
                else:
                    st.warning("Área de PRODUTOS PAGOS não pôde ser determinada por marcadores de texto para pdfplumber.")

                # --- Extração de Produtos Não Pagos (GRATUITOS / PEÇAS DE REPOSIÇÃO) ---
                free_bbox = find_table_bbox_by_markers(
                    first_page,
                    area_start_marker_pattern=r"FREE\s+OF\s+CHARGE\s+PRODUCTS|PRODUTOS\s+NÃO\s+PAGOS", 
                    area_end_marker_pattern=r"TOTAIS\s+GERAIS|VALOR\s+TOTAL|Say\s+Total\s+Amount.*SIXTY\s+ONLY|Say\s+Total\s+Amount|TOTAL\s+QUANTITY|TOTAL\s+AMOUNT|\n\s*\d+\s*$", 
                    table_header_pattern=r"EXP\s+ou\s+Fabricante|Código\s+Interno|Fornecedor|Invoice\s+N#|NCM|Cobertura|Denominação\s+do\s+produto|SKU|Detalhamento\s+complementar\s+do\s+produto", 
                    table_footer_pattern=r".*(total\s+quantity|TOTAL\s+AMOUNT|Say\s+Total\s+Amount|SUBTOTAL|VALOR\s+TOTAL|\n\s*\d+\s*$).*" 
                )

                free_products_raw = []
                if free_bbox:
                    st.write(f"Tentando extrair PRODUTOS NÃO PAGOS com pdfplumber na área: {free_bbox}")
                    free_page_region = first_page.crop(free_bbox)
                    table_settings_free = { 
                        "vertical_strategy": "lines",
                        "horizontal_strategy": "lines",
                        "snap_tolerance": 3,
                        "text_tolerance": 1,
                        "intersection_tolerance": 3
                    }
                    pdfplumber_free_tables = free_page_region.extract_tables(table_settings_free)
                    if pdfplumber_free_tables:
                        st.info(f"pdfplumber encontrou {len(pdfplumber_free_tables)} tabela(s) para PRODUTOS NÃO PAGOS.")
                        free_products_raw = extract_products_table_from_pdfplumber_tables(pdfplumber_free_tables, section_keyword="FREE OF CHARGE PRODUCTS", invoice_fornecedor=invoice_fornecedor)
                    else:
                        st.warning("pdfplumber não encontrou tabelas na área especificada para PRODUTOS NÃO PAGOS.")
                else:
                    st.warning("Área de PRODUTOS NÃO PAGOS não pôde ser determinada por marcadores de texto para pdfplumber.")

                # Tenta extrair NCM da invoice info (se não encontrado na tabela de produtos)
                ncm_invoice_match = re.search(r"NCM/HS Code Principal:\s*(\d+)", first_page.extract_text())
                ncm_from_invoice = ncm_invoice_match.group(1) if ncm_invoice_match else "N/A"

                # Preencher NCM, Fornecedor e Invoice N# para itens se não foi extraído da tabela ou se é "N/A"
                for item_list in [paid_products_raw, free_products_raw]:
                    for item in item_list:
                        if item.get('NCM', 'N/A') == "N/A" and ncm_from_invoice != "N/A":
                            item['NCM'] = ncm_from_invoice
                        if item.get('Fornecedor', 'N/A') == "N/A" and invoice_fornecedor != "N/A":
                            item['Fornecedor'] = invoice_fornecedor
                        if item.get('Invoice N#', 'N/A') == "N/A" and invoice_n_fat != "N/A":
                            item['Invoice N#'] = invoice_n_fat


                # --- Lógica de Enriquecimento e Processamento para Produtos Pagos ---
                final_paid_products_data = []
                paid_product_codes = [item.get('Código Interno') for item in paid_products_raw if item.get('Código Interno') != 'N/A']
                db_products_info_paid = {}
                if paid_product_codes:
                    db_path_produtos = db_utils.get_db_path("produtos")
                    if db_path_produtos:
                        fetched_products = db_utils.selecionar_produtos_por_ids(db_path_produtos, paid_product_codes)
                        db_products_info_paid = {p['id_key_erp']: dict(p) for p in fetched_products}
                    else:
                        st.warning("Caminho do banco de dados de produtos não encontrado para produtos pagos. Descrições do DB não serão carregadas.")

                for item in paid_products_raw:
                    code = item.get('Código Interno')
                    db_info = db_products_info_paid.get(code, {})
                    item['Denominação do produto'] = db_info.get('nome_part', item.get('Denominação do produto', item.get('Denominação do produto', 'N/A')))
                    item['Detalhamento complementar do produto'] = db_info.get('descricao', item.get('Detalhamento complementar do produto', 'N/A'))
                    item['NCM'] = db_info.get('ncm', item.get('NCM', ncm_from_invoice if ncm_from_invoice != "N/A" else 'N/A'))
                    processed_item = process_product_item(item, section_keyword="PAID PRODUCTS", invoice_fornecedor=invoice_fornecedor)
                    final_paid_products_data.append(processed_item)

                df_paid_final = pd.DataFrame(final_paid_products_data)

                # --- Lógica de Enriquecimento e Processamento para Produtos Não Pagos ---
                final_free_products_data = []
                free_product_codes = [item.get('Código Interno') for item in free_products_raw if item.get('Código Interno') != 'N/A']
                db_products_info_free = {}
                if free_product_codes:
                    db_path_produtos = db_utils.get_db_path("produtos")
                    if db_path_produtos:
                        fetched_products = db_utils.selecionar_produtos_por_ids(db_path_produtos, free_product_codes)
                        db_products_info_free = {p['id_key_erp']: dict(p) for p in fetched_products}
                    else:
                        st.warning("Caminho do banco de dados de produtos não encontrado para produtos não pagos. Descrições do DB não serão carregadas.")

                for item in free_products_raw:
                    code = item.get('Código Interno')
                    db_info = db_products_info_free.get(code, {})
                    item['Denominação do produto'] = db_info.get('nome_part', item.get('Denominação do produto', item.get('Denominação do produto', 'N/A')))
                    item['Detalhamento complementar do produto'] = db_info.get('descricao', item.get('Detalhamento complementar do produto', 'N/A'))
                    item['NCM'] = db_info.get('ncm', item.get('NCM', ncm_from_invoice if ncm_from_invoice != "N/A" else 'N/A'))
                    processed_item = process_product_item(item, section_keyword="FREE OF CHARGE PRODUCTS", invoice_fornecedor=invoice_fornecedor)
                    final_free_products_data.append(processed_item)

                df_free_final = pd.DataFrame(final_free_products_data)

                # --- Definir e reordenar as colunas para exibição ---
                cols_order_display = [
                    "EXP ou Fabricante", "Código Interno", "Fornecedor", "Invoice N#", "NCM", "Cobertura",
                    "Denominação do produto", "SKU", "Detalhamento complementar do produto",
                    "Qtde", "Peso Unitário", "Valor Unitário"
                ]

                # Garantir que todas as colunas existem antes de reordenar e filtrar
                for col in cols_order_display:
                    if col not in df_paid_final.columns:
                        df_paid_final[col] = pd.NA
                    if col not in df_free_final.columns:
                        df_free_final[col] = pd.NA

                df_paid_final = df_paid_final[cols_order_display]
                df_free_final = df_free_final[cols_order_display]

                # Formatação numérica para exibição nas tabelas
                for df in [df_paid_final, df_free_final]:
                    df['Qtde'] = df['Qtde'].apply(lambda x: str(x) if pd.notna(x) else 'N/A')
                    df['Peso Unitário'] = df['Peso Unitário'].apply(lambda x: f"{x:,.4f}".replace('.', '#').replace(',', '.').replace('#', ',') if pd.notna(x) else 'N/A')
                    df['Valor Unitário'] = df['Valor Unitário'].apply(lambda x: f"{x:,.2f}".replace('.', '#').replace(',', '.').replace('#', ',') if pd.notna(x) else 'N/A')

                st.markdown("---")
                st.subheader("Informações Gerais da Invoice")
                st.write(f"**Fornecedor:** {invoice_fornecedor}")
                st.write(f"**Invoice N#:** {invoice_n_fat}")
                st.write(f"**NCM/HS Code Principal:** {ncm_from_invoice}")
                st.markdown("---")

                st.subheader("PRODUTOS PAGOS")
                if not df_paid_final.empty:
                    st.dataframe(df_paid_final, use_container_width=True, hide_index=True)
                else:
                    st.info("Nenhum produto pago encontrado.")

                st.markdown("---")

                st.subheader("PRODUTOS NÃO PAGOS (GRATUITOS / PEÇAS DE REPOSIÇÃO)")
                if not df_free_final.empty:
                    st.dataframe(df_free_final, use_container_width=True, hide_index=True)
                else:
                    st.info("Nenhum produto não pago encontrado.")

                st.markdown("---")

                # Calcular subtotais e totais a partir dos dataframes finais para exibição
                total_from_raw_paid = sum([item.get('Valor total do item', 0.0) for item in paid_products_raw])
                total_from_raw_free = sum([item.get('Valor total do item', 0.0) for item in free_products_raw])

                total_invoice_amount = total_from_raw_paid + total_from_raw_free

                if total_invoice_amount == 0 and (not df_paid_final.empty or not df_free_final.empty):
                    calculated_total_paid = df_paid_final.apply(
                        lambda row: (float(str(row['Valor Unitário']).replace('.', '').replace(',', '.') if str(row['Valor Unitário']) != 'N/A' else '0')) * \
                                    (float(str(row['Qtde']).replace(',', '.') if str(row['Qtde']) != 'N/A' else '0')), axis=1
                    ).sum()
                    calculated_total_free = df_free_final.apply(
                        lambda row: (float(str(row['Valor Unitário']).replace('.', '').replace(',', '.') if str(row['Valor Unitário']) != 'N/A' else '0')) * \
                                    (float(str(row['Qtde']).replace(',', '.') if str(row['Qtde']) != 'N/A' else '0')), axis=1
                    ).sum()
                    total_invoice_amount = calculated_total_paid + calculated_total_free


                # Extrair totais de Qtde e Peso da segunda página, se disponível, ou somar do DataFrame
                total_qty_text = "N/A"
                total_peso_liquido_text = "N/A"

                if second_page:
                    second_page_text = second_page.extract_text()

                    # Extrair tabelas da segunda página
                    pdfplumber_second_page_tables = second_page.extract_tables({
                        "vertical_strategy": "lines",
                        "horizontal_strategy": "lines"
                    })

                    # Inicializar variável para armazenar candidatos
                    total_row_candidate_list = []

                    # Verificar se existem tabelas
                    if pdfplumber_second_page_tables and len(pdfplumber_second_page_tables) > 0:
                        # Pegar a última tabela
                        last_table = pdfplumber_second_page_tables[-1]

                        # Verificar se a tabela tem conteúdo
                        if last_table and len(last_table) > 1:
                            # Iterar pelas linhas da tabela de baixo para cima
                            for row in reversed(last_table):
                                if not row:  # Pula linhas vazias
                                    continue
                                
                                # Limpa e verifica células numéricas
                                numeric_cells = []
                                for cell in row:
                                    if cell is not None:
                                        cell_str = str(cell).strip()
                                        cell_clean = cell_str.replace(',', '.').replace(' ', '')
                                        try:
                                            float(cell_clean)
                                            numeric_cells.append(cell_clean)
                                        except ValueError:
                                            continue
                                
                                if len(numeric_cells) >= 3:  # Se encontrou pelo menos 3 números na linha
                                    total_row_candidate_list = numeric_cells
                                    break

                        if total_row_candidate_list:
                            try:
                                # Tenta converter os valores encontrados
                                total_qty = float(total_row_candidate_list[0])
                                total_peso = float(total_row_candidate_list[2])
                                
                                total_qty_text = str(int(total_qty))
                                total_peso_liquido_text = f"{total_peso:,.2f}".replace('.', '#').replace(',', '.').replace('#', ',')
                            except (IndexError, ValueError) as e:
                                st.warning(f"Erro ao processar valores totais: {str(e)}")
                                total_qty_text = "N/A"
                                total_peso_liquido_text = "N/A"

                # Fallback se a extração da Packing List falhar ou se não houver segunda página
                if total_qty_text == "N/A" or total_qty_text == "0":
                    try:
                        total_qty_text = str(int(df_paid_final['Qtde'].apply(lambda x: float(str(x).replace(',', '.') if str(x) != 'N/A' else '0')).sum() +
                                                 df_free_final['Qtde'].apply(lambda x: float(str(x).replace(',', '.') if str(x) != 'N/A' else '0')).sum()))
                    except ValueError:
                        total_qty_text = "N/A"

                if total_peso_liquido_text == "N/A" or total_peso_liquido_text == "0,00":
                    try:
                        total_peso_from_df = df_paid_final.apply(
                            lambda row: (float(str(row['Peso Unitário']).replace('.', '').replace(',', '.') if str(row['Peso Unitário']) != 'N/A' else '0')) * \
                                        (float(str(row['Qtde']).replace(',', '.') if str(row['Qtde']) != 'N/A' else '0')), axis=1
                        ).sum() + df_free_final.apply(
                            lambda row: (float(str(row['Peso Unitário']).replace('.', '').replace(',', '.') if str(row['Peso Unitário']) != 'N/A' else '0')) * \
                                        (float(str(row['Qtde']).replace(',', '.') if str(row['Qtde']) != 'N/A' else '0')), axis=1
                        ).sum()

                        total_peso_liquido_text = f"{total_peso_from_df:,.2f}".replace('.', '#').replace(',', '.').replace('#', ',')
                    except ValueError:
                        total_peso_liquido_text = "N/A"


                st.subheader("TOTAIS GERAIS:")
                st.write(f"**SUBTOTAL PRODUTOS PAGOS (USD):** {total_from_raw_paid:,.2f}".replace('.', '#').replace(',', '.').replace('#', ','))
                st.write(f"**SUBTOTAL PRODUTOS NÃO PAGOS (USD):** {total_from_raw_free:,.2f}".replace('.', '#').replace(',', '.').replace('#', ','))
                st.write(f"**Qtde Total (todas as peças):** {total_qty_text}")
                st.write(f"**Peso total (Líquido - KG):** {total_peso_liquido_text}")
                st.write(f"**VALOR TOTAL DA INVOICE (USD): {total_invoice_amount:,.2f}**".replace('.', '#').replace(',', '.').replace('#', ','))

                # Para o botão de download, cria um DataFrame combinado para exportação
                df_paid_export = pd.DataFrame(final_paid_products_data)
                df_paid_export['Tipo de Produto'] = 'Pago'
                df_free_export = pd.DataFrame(final_free_products_data)
                df_free_export['Tipo de Produto'] = 'Não Pago'

                df_combined_for_export = pd.concat([df_paid_export, df_free_export], ignore_index=True)

                # Colunas para exportação no Excel
                cols_order_export = [
                    "Tipo de Produto", "EXP ou Fabricante", "Código Interno", "Fornecedor", "Invoice N#", "NCM", "Cobertura",
                    "Denominação do produto", "SKU", "Detalhamento complementar do produto",
                    "Qtde", "Peso Unitário", "Valor Unitário", "Peso Líq", "Valor total do item"
                ]

                # Garantir que todas as colunas existem para exportação
                for col in cols_order_export:
                    if col not in df_combined_for_export.columns:
                        df_combined_for_export[col] = pd.NA

                df_combined_for_export = df_combined_for_export[cols_order_export]

                st.session_state['df_final_for_export'] = df_combined_for_export
                st.session_state['invoice_n_fat_for_export'] = invoice_n_fat

        except Exception as e:
            st.error(f"Ocorreu um erro ao processar o PDF: {e}")
            st.warning("Certifique-se de que o PDF é um arquivo de texto selecionável (não uma imagem escaneada) e que o layout corresponde aos padrões esperados. A extração de dados tabulares de PDFs pode ser sensível a variações de formatação.")

    if 'df_final_for_export' in st.session_state and 'invoice_n_fat_for_export' in st.session_state:
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            st.session_state['df_final_for_export'].to_excel(writer, sheet_name='Análise de Faturas', index=False)
        excel_data = output.getvalue()

        st.download_button(
            label="Exportar para Excel",
            data=excel_data,
            file_name=f"analise_fatura_{st.session_state['invoice_n_fat_for_export']}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
