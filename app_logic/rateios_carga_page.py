import streamlit as st
import pandas as pd
from typing import List, Dict, Any
import logging

# Importar funções de utilitários de banco de dados
from db_utils import get_declaracao_by_referencia, _clean_reference_string
from app_logic.utils import set_background_image # Para a imagem de fundo

logger = logging.getLogger(__name__)

# --- Constantes ---
MAX_VALOR_ADUANEIRO_PER_TRIP = 2_500_000.00 # R$ 2.500.000,00

# --- Funções Auxiliares de Formatação ---
def _format_currency(value):
    """Formata um valor numérico como moeda BRL, com arredondamento explícito."""
    try:
        val = float(value)
        # Arredonda para 2 casas decimais explicitamente antes de formatar
        val = round(val, 2)
        # Formata com milhar como ponto e decimal como vírgula
        # Primeiro, formata como float com 2 casas decimais e separador de milhar padrão (vírgula nos EUA)
        # Depois, substitui a vírgula por um caractere temporário, o ponto por vírgula, e o temporário por ponto.
        return f"R$ {val:,.2f}".replace(',', '#').replace('.', ',').replace('#', '.')
    except (ValueError, TypeError):
        return "R$ 0,00"

def _format_value_without_currency(value):
    """Formata um valor numérico como X.XXX.XXX,XX sem o símbolo da moeda, com 2 casas decimais."""
    try:
        val = float(value)
        val = round(val, 2) # Garante o arredondamento para duas casas decimais
        # Formata como float com 2 casas decimais, usando o separador de milhar e decimal do locale padrão (geralmente EUA).
        # Em seguida, substitui os separadores para o padrão brasileiro.
        formatted_val = f"{val:,.2f}" # Ex: 1,234,567.89 (virgula para milhar, ponto para decimal)
        # Troca a vírgula de milhar por um caractere temporário
        formatted_val = formatted_val.replace(',', '#') # Ex: 1#234#567.89
        # Troca o ponto decimal por vírgula
        formatted_val = formatted_val.replace('.', ',') # Ex: 1#234#567,89
        # Troca o caractere temporário por ponto
        formatted_val = formatted_val.replace('#', '.') # Ex: 1.234.567,89
        return formatted_val
    except (ValueError, TypeError):
        return "0,00" # Retorna "0,00" para valores inválidos

def _format_float_for_display(value, decimals=2):
    """Formata um valor numérico como float com número específico de casas decimais."""
    try:
        val = float(value)
        return f"{val:,.{decimals}f}".replace('.', '#').replace(',', '.').replace('#', ',')
    except (ValueError, TypeError):
        return "0,00"

def _format_int_no_float(value):
    """Formata um valor numérico que pode ser float para inteiro."""
    try:
        return str(int(float(value)))
    except (ValueError, TypeError):
        return "0"

def _format_di_number(di_number):
    """Formata o número da DI para o padrão **/*******-*."""
    if di_number and isinstance(di_number, str) and len(di_number) == 10:
        return f"{di_number[0:2]}/{di_number[2:9]}-{di_number[9]}"
    return di_number

# --- Função Principal da Página ---
def show_rateios_carga_page():
    # Configuração da imagem de fundo
    background_image_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'assets', 'logo_navio_atracado.png')
    set_background_image(background_image_path)

    st.subheader("Rateios de Carga")
    st.markdown(f"Insira as Referências dos Processos (uma por linha) para gerar a tabela de rateio. O limite por viagem é de {_format_currency(MAX_VALOR_ADUANEIRO_PER_TRIP)}.")

    # Área de texto para inserir as referências
    input_references_raw = st.text_area(
        "Referências de Processo",
        value="",
        height=150,
        help="Cole aqui as referências dos processos, uma por linha (ex: PCH-XXXX-YY)."
    )

    # Botão para carregar os dados
    if st.button("Carregar Dados de Rateio"):
        if input_references_raw:
            # Limpa e normaliza as referências
            references_list = [
                _clean_reference_string(ref) for ref in input_references_raw.split('\n') if ref.strip()
            ]
            
            # Remove duplicatas e mantém a ordem de inserção (aproximada)
            unique_references = []
            seen = set()
            for ref in references_list:
                if ref not in seen:
                    unique_references.append(ref)
                    seen.add(ref)

            st.session_state.rateio_data_loaded = True
            st.session_state.rateio_process_list = unique_references
            st.rerun() # Re-executa para exibir os resultados

    # Lógica para exibir a tabela após carregar os dados
    if st.session_state.get('rateio_data_loaded', False) and st.session_state.get('rateio_process_list'):
        
        all_processed_data = [] # Para armazenar todas as DIs processadas, para o total geral
        
        st.markdown("---")
        st.markdown("##### Tabela de Rateio de Carga")

        for ref in st.session_state.rateio_process_list:
            declaracao = get_declaracao_by_referencia(ref)
            if declaracao:
                pallets = 0.0
                cx_pap = 0.0
                
                quantidade_volumes_float = float(declaracao.get('quantidade_volumes', 0))

                embalagem = declaracao.get('embalagem', '').lower()
                if 'outros' in embalagem or 'pallett' in embalagem or 'pallet' in embalagem:
                    pallets = quantidade_volumes_float
                elif 'caixa' in embalagem and ('papelao' in embalagem or 'papelão' in embalagem):
                    cx_pap = quantidade_volumes_float
                else:
                    logger.warning(f"Embalagem '{embalagem}' para referência '{ref}' não categorizada para PALLETTS ou CX. PAP. Volume: {quantidade_volumes_float}")

                # Cálculo do VALOR ADUANEIRO
                vmld_declaracao = declaracao.get('vmld', 0.0)
                imposto_importacao = declaracao.get('imposto_importacao', 0.0)
                pis_pasep = declaracao.get('pis_pasep', 0.0)
                ipi = declaracao.get('ipi', 0.0)
                cofins = declaracao.get('cofins', 0.0)
                
                valor_aduaneiro = float(vmld_declaracao) + float(imposto_importacao) + float(pis_pasep) + float(ipi) + float(cofins)

                all_processed_data.append({
                    "REFERENCIA": ref,
                    "PALLETTS": pallets,
                    "CX. PAP": cx_pap,
                    "PESO": declaracao.get('peso_bruto', 0.0),
                    "VALOR ADUANEIRO": valor_aduaneiro, # Adiciona o valor numérico bruto
                    "NÚMERO DI": _format_di_number(declaracao.get('numero_di', 'N/A')),
                    "RAW_VALOR_ADUANEIRO": valor_aduaneiro # Guarda o valor bruto para cálculos de agrupamento
                })

            else:
                st.warning(f"Referência '{ref}' não encontrada no banco de dados de DIs.")
        
        if all_processed_data:
            # Sort data by value for better bin packing (greedy approach)
            all_processed_data_sorted = sorted(all_processed_data, key=lambda x: x['RAW_VALOR_ADUANEIRO'], reverse=True)

            trips = []
            current_trip_num = 1

            for item in all_processed_data_sorted:
                placed = False
                # Try to fit into an existing trip
                for trip in trips:
                    if trip["current_value"] + item["RAW_VALOR_ADUANEIRO"] <= MAX_VALOR_ADUANEIRO_PER_TRIP:
                        trip["items"].append(item)
                        trip["current_value"] += item["RAW_VALOR_ADUANEIRO"]
                        placed = True
                        break
                
                # If not placed, create a new trip
                if not placed:
                    trips.append({
                        "trip_num": current_trip_num,
                        "items": [item],
                        "current_value": item["RAW_VALOR_ADUANEIRO"]
                    })
                    current_trip_num += 1
            
            # --- Display each trip ---
            overall_total_pallets = 0.0
            overall_total_cx_pap = 0.0
            overall_total_peso_bruto = 0.0
            overall_total_valor_aduaneiro = 0.0

            for trip in trips:
                st.markdown(f"**VIAGEM {trip['trip_num']} - Limite: {_format_currency(MAX_VALOR_ADUANEIRO_PER_TRIP)}**")
                
                trip_data = []
                trip_total_pallets = 0.0
                trip_total_cx_pap = 0.0
                trip_total_peso_bruto = 0.0
                trip_total_valor_aduaneiro = 0.0

                for item in trip["items"]:
                    trip_data.append({
                        "REFERENCIA": item["REFERENCIA"],
                        "PALLETTS": item["PALLETTS"],
                        "CX. PAP": item["CX. PAP"],
                        "PESO": item["PESO"],
                        "VALOR ADUANEIRO": _format_value_without_currency(item["VALOR ADUANEIRO"]), # Formata aqui
                        "NÚMERO DI": item["NÚMERO DI"]
                    })
                    trip_total_pallets += item["PALLETTS"]
                    trip_total_cx_pap += item["CX. PAP"]
                    trip_total_peso_bruto += item["PESO"]
                    trip_total_valor_aduaneiro += item["RAW_VALOR_ADUANEIRO"] # Soma o valor bruto para o total da viagem

                df_trip = pd.DataFrame(trip_data)

                # Add trip total row
                trip_total_row = {
                    "REFERENCIA": "TOTAL VIAGEM",
                    "PALLETTS": trip_total_pallets,
                    "CX. PAP": trip_total_cx_pap,
                    "PESO": trip_total_peso_bruto,
                    "VALOR ADUANEIRO": trip_total_valor_aduaneiro, # Valor bruto para o total da viagem
                    "NÚMERO DI": ""
                }
                # Garante que total_row é adicionado como um novo DataFrame para concat
                df_trip = pd.concat([df_trip, pd.DataFrame([trip_total_row])], ignore_index=True)

                # Formatar o VALOR ADUANEIRO da linha TOTAL VIAGEM APÓS O CONCAT
                df_trip.loc[df_trip.index[-1], 'VALOR ADUANEIRO'] = _format_value_without_currency(df_trip.loc[df_trip.index[-1], 'VALOR ADUANEIRO'])

                column_config = {
                    "REFERENCIA": st.column_config.TextColumn("REFERENCIA", width="medium"),
                    "PALLETTS": st.column_config.NumberColumn("PALLETTS", format="%d", width="small"),
                    "CX. PAP": st.column_config.NumberColumn("CX. PAP", format="%d", width="small"),
                    "PESO": st.column_config.NumberColumn("PESO", format="%.2f KG", width="small"),
                    "VALOR ADUANEIRO": st.column_config.TextColumn("VALOR ADUANEIRO", width="large"),
                    "NÚMERO DI": st.column_config.TextColumn("NÚMERO DI", width="medium"),
                }
                column_order = ["REFERENCIA", "PALLETTS", "CX. PAP", "PESO", "VALOR ADUANEIRO", "NÚMERO DI"]

                st.dataframe(
                    df_trip,
                    hide_index=True,
                    use_container_width=True,
                    column_config=column_config,
                    column_order=column_order
                )
                st.markdown("---") # Separador entre viagens

                overall_total_pallets += trip_total_pallets
                overall_total_cx_pap += trip_total_cx_pap
                overall_total_peso_bruto += trip_total_peso_bruto
                overall_total_valor_aduaneiro += trip_total_valor_aduaneiro

            # --- Display overall total ---
            st.markdown("##### TOTAL GERAL DE CARGAS")
            overall_total_row = {
                "REFERENCIA": "TOTAL GERAL",
                "PALLETTS": overall_total_pallets,
                "CX. PAP": overall_total_cx_pap,
                "PESO": overall_total_peso_bruto,
                "VALOR ADUANEIRO": overall_total_valor_aduaneiro,
                "NÚMERO DI": ""
            }
            df_overall_total = pd.DataFrame([overall_total_row])
            df_overall_total['VALOR ADUANEIRO'] = df_overall_total['VALOR ADUANEIRO'].astype(float).apply(_format_value_without_currency)

            st.dataframe(
                df_overall_total,
                hide_index=True,
                use_container_width=True,
                column_config=column_config,
                column_order=column_order
            )


        else:
            st.info("Nenhum dado de DI válido foi encontrado para as referências fornecidas.")
    elif st.session_state.get('rateio_data_loaded', False): # Se o botão foi clicado mas a lista ficou vazia
        st.info("Nenhuma referência válida inserida ou encontrada para gerar a tabela de rateio.")

    st.markdown("---")
    st.markdown("Esta tela permite ratear custos de carga entre múltiplos processos de importação.")
import os