# -*- coding: utf-8 -*-
import sqlite3
import pandas as pd
import os
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
import json # Importar json para lidar com target_users
import streamlit as st # Adicionado para usar o cache do Streamlit

# Configuração de logging para o módulo de banco de dados (MOVIDO PARA O TOPO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) # Define o nível de logging para INFO
# Configurar handler para logger se ainda não estiver configurado
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Importar o SDK do Google Cloud Firestore - MOVIDO PARA O TOPO
# Isso garante que o módulo seja importado uma única vez no início
try:
    import google.cloud.firestore
except ImportError:
    # Este erro deve ser tratado no db_utils, mas é bom ter uma falha explícita aqui também.
    # Se você estiver rodando localmente, certifique-se de que 'google-cloud-firestore' está instalado:
    # pip install google-cloud-firestore
    logger.critical("ERRO CRÍTICO: O módulo 'google.cloud.firestore' não foi encontrado. "
                    "Certifique-se de que a biblioteca esteja instalada no seu ambiente Python.")
    raise RuntimeError("Dependência 'google.cloud.firestore' não encontrada.")


# Importar db_utils para obter a lista de usuários e configurações globais do DB
# Este módulo (followup_db_manager) deve depender de db_utils para acessar
# as configurações globais do banco de dados (Firestore e SQLite).
try:
    import db_utils
except ImportError:
    logger.critical("ERRO CRÍTICO: O módulo 'db_utils' não foi encontrado. "
                    "followup_db_manager.py depende de db_utils.py. "
                    "Verifique se db_utils.py está no diretório 'app_logic' "
                    "e se as dependências estão instaladas. O aplicativo pode não funcionar corretamente.")
    # Se db_utils não for encontrado, o aplicativo não pode prosseguir.
    # Em um ambiente de produção, st.stop() seria apropriado aqui.
    # Para fins de desenvolvimento ou para evitar que o Streamlit quebre,
    # podemos definir variáveis para falha graciosamente ou levantar um erro.
    raise RuntimeError("Módulo 'db_utils' é essencial e não foi encontrado.")

# Agora, acesse as variáveis e funções globais diretamente de db_utils
# NÃO DUPLIQUE A INICIALIZAÇÃO DO CLIENTE FIRESTORE AQUI. db_utils já faz isso.
# Apenas referencie-as.
_USE_FIRESTORE_AS_PRIMARY = db_utils._USE_FIRESTORE_AS_PRIMARY
_SQLITE_ENABLED = db_utils._SQLITE_ENABLED


# Variável global para armazenar o caminho do banco de dados de follow-up (SQLite)
_followup_sqlite_db_path: Optional[str] = None

# Removida a importação circular de process_form_page.
# from app_logic import process_form_page # Esta linha causava a importação circular.

# Lista fixa de opções de status (mantida aqui, mas usada na UI para consistência)
STATUS_OPTIONS = ["", "Processo Criado","Verificando","Em produção","Pré Embarque","Embarcado","Chegada Recinto","Registrado","Liberado","Agendado","Chegada Pichau","Encerrado", "Limbo Saldo", "Limbo Consolidado"]

# --- Lógica de inicialização do caminho do DB ao carregar o módulo ---
# Isso garante que o caminho padrão seja definido e o diretório 'data' criado
# assim que o followup_db_manager for importado.
# Use db_utils.get_sqlite_db_path para obter o caminho do SQLite
_followup_sqlite_db_path = db_utils.get_sqlite_db_path("followup")
if _followup_sqlite_db_path:
    _data_dir = os.path.dirname(_followup_sqlite_db_path)
    if not os.path.exists(_data_dir):
        os.makedirs(_data_dir)
        logger.info(f"Diretório de dados '{_data_dir}' criado para Follow-up DB (SQLite).")
    logger.info(f"[followup_db_manager] Caminho padrão do DB SQLite definido na inicialização do módulo: {_followup_sqlite_db_path}")
else:
    logger.error("Caminho do DB de Follow-up (SQLite) não pôde ser determinado via db_utils.")


# REMOVIDO: @st.cache_resource. Conexões SQLite não devem ser cacheadas entre threads.
def conectar_followup_db():
    """
    Estabelece uma conexão com o banco de dados SQLite de follow-up.
    Retorna o objeto de conexão ou None em caso de erro.
    """
    global _followup_sqlite_db_path
    if not _SQLITE_ENABLED:
        logger.warning("SQLite está desabilitado. Não é possível conectar ao DB de Follow-up (SQLite).")
        return None

    logger.debug(f"[conectar_followup_db] Tentando conectar a: {_followup_sqlite_db_path}")
    
    if not _followup_sqlite_db_path:
        logger.error("Caminho do DB de Follow-up (SQLite) não definido. Não é possível conectar.")
        return None
    try:
        conn = sqlite3.connect(_followup_sqlite_db_path)
        conn.row_factory = sqlite3.Row # Retorna linhas como dicionários-like
        conn.execute("PRAGMA foreign_keys = ON;") # Garante a integridade referencial
        logger.info(f"[conectar_followup_db] Conectado com sucesso a: {_followup_sqlite_db_path}")
        return conn
    except Exception as e:
        logger.exception(f"Erro ao conectar ao DB de Follow-up em {_followup_sqlite_db_path}")
        return None

# MODIFICADO: criar_tabela_followup agora abre e fecha sua própria conexão
def criar_tabela_followup() -> bool: # Removido 'conn' como parâmetro, adicionado retorno bool
    """
    Cria as tabelas 'processos', 'historico_processos', 'process_items' e 'notifications' no SQLite
    se não existirem, com todas as colunas definitivas.
    Retorna True se as tabelas foram verificadas/criadas com sucesso, False caso contrário.
    """
    conn = conectar_followup_db() # Abre a conexão internamente
    if conn is None:
        logger.error("Falha ao conectar ao DB para criar tabelas de Follow-up (SQLite).")
        return False # Indica falha
    try:
        cursor = conn.cursor()
        # Tabela principal 'processos'
        cursor.execute('''CREATE TABLE IF NOT EXISTS processos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Processo_Novo TEXT UNIQUE,
            Observacao TEXT,
            Tipos_de_item TEXT,
            Data_Embarque TEXT,
            Previsao_Pichau TEXT,
            Documentos_Revisados TEXT,
            Conhecimento_Embarque TEXT,
            Descricao_Feita TEXT,
            Descricao_Enviada TEXT,
            Fornecedor TEXT,
            N_Invoice TEXT,
            Quantidade INTEGER,
            Valor_USD REAL,
            Pago TEXT,
            N_Ordem_Compra TEXT,
            Data_Compra TEXT,
            Estimativa_Impostos_BR REAL, 
            Estimativa_Frete_USD REAL,
            Agente_de_Carga_Novo TEXT,
            Status_Geral TEXT,
            Modal TEXT,
            Navio TEXT,
            Origem TEXT,
            Destino TEXT,
            INCOTERM TEXT,
            Comprador TEXT,
            Status_Arquivado TEXT DEFAULT 'Não Arquivado',
            Caminho_da_pasta TEXT,
            Estimativa_Dolar_BRL REAL,
            Estimativa_Seguro_BRL REAL,
            Estimativa_II_BR REAL,
            Estimativa_IPI_BR REAL,
            Estimativa_PIS_BR REAL,
            Estimativa_COFINS_BR REAL,
            Estimativa_ICMS_BR REAL,
            Nota_feita TEXT,
            Conferido TEXT,
            Ultima_Alteracao_Por TEXT,
            Ultima_Alteracao_Em TEXT,
            Estimativa_Impostos_Total REAL,
            Quantidade_Containers INTEGER,
            ETA_Recinto TEXT,
            Data_Registro TEXT,
            DI_ID_Vinculada INTEGER
        )''')
        conn.commit()
        logger.info("Tabela 'processos' (SQLite) verificada/criada com sucesso com todas as colunas definitivas.")

        # Tabela 'historico_processos' para rastrear alterações
        cursor.execute('''CREATE TABLE IF NOT EXISTS historico_processos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            processo_id INTEGER,
            campo_alterado TEXT,
            valor_antigo TEXT,
            valor_novo TEXT,
            timestamp TEXT,
            usuario TEXT,
            FOREIGN KEY(processo_id) REFERENCES processos(id) ON DELETE CASCADE
        )''')
        conn.commit()
        logger.info("Tabela 'historico_processos' (SQLite) verificada/criada com sucesso.")

        # Tabela: process_items para armazenar os itens de cada processo
        cursor.execute('''CREATE TABLE IF NOT EXISTS process_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            processo_id INTEGER NOT NULL,
            codigo_interno TEXT,
            ncm TEXT,
            cobertura TEXT,
            sku TEXT,
            quantidade REAL,
            peso_unitario REAL,
            valor_unitario REAL,
            valor_total_item REAL,
            estimativa_ii_br REAL,
            estimativa_ipi_br REAL,
            estimativa_pis_br REAL,
            estimativa_cofins_br REAL,
            estimativa_icms_br REAL,
            frete_rateado_usd REAL,
            seguro_rateado_brl REAL,
            vlmd_item REAL,
            denominacao_produto TEXT,
            detalhamento_complementar_produto TEXT,
            FOREIGN KEY(processo_id) REFERENCES processos(id) ON DELETE CASCADE
        )''')
        conn.commit()
        logger.info("Tabela 'process_items' (SQLite) verificada/criada com sucesso.")

        # Novas tabelas para Notificações
        cursor.execute('''CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message TEXT NOT NULL,
            target_users TEXT,
            created_at TEXT NOT NULL,
            created_by TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active'
        )''')
        conn.commit()
        logger.info("Tabela 'notifications' (SQLite) verificada/criada com sucesso.")

        cursor.execute('''CREATE TABLE IF NOT EXISTS notification_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            notification_id INTEGER,
            action TEXT NOT NULL,
            action_by TEXT NOT NULL,
            action_at TEXT NOT NULL,
            original_message TEXT,
            FOREIGN KEY(notification_id) REFERENCES notifications(id) ON DELETE CASCADE
        )''')
        conn.commit()
        logger.info("Tabela 'notification_history' (SQLite) verificada/criada com sucesso.")
        return True # Indica sucesso

    except Exception as e:
        logger.exception("Erro ao criar ou atualizar as tabelas do Follow-up e Notificações (SQLite)")
        conn.rollback()
        return False # Indica falha
    finally:
        if conn:
            conn.close() # Fecha a conexão internamente

# --- Funções para manipulação de ITENS DE PROCESSO ---

# Aplicando @st.cache_data para evitar recargas desnecessárias de dados do DB.
# ttl=3600 significa que o cache dura 1 hora. Ajuste conforme a necessidade de atualização dos dados.
@st.cache_data(ttl=3600)
def obter_ultimo_processo_id() -> Optional[int]:
    """Obtém o ID do último processo inserido na tabela 'processos' (SQLite)."""
    if not _SQLITE_ENABLED:
        logger.warning("SQLite está desabilitado. Não é possível obter o último ID de processo do SQLite.")
        return None
    conn = conectar_followup_db()
    if conn is None:
        return None
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(id) FROM processos")
        last_id = cursor.fetchone()[0]
        logger.debug(f"Último ID de processo obtido (SQLite): {last_id}")
        return last_id
    except Exception as e:
        logger.exception("Erro ao obter o último ID de processo (SQLite).")
        return None
    finally:
        if conn:
            conn.close()

# Deletar itens do processo não deve ser cacheado, pois modifica o estado do DB.
def deletar_itens_processo(processo_id: Any) -> bool: # processo_id pode ser int (SQLite) ou string (Firestore)
    """Deleta todos os itens associados a um processo específico em ambos os DBs."""
    success_firestore = True
    success_sqlite = True

    if _USE_FIRESTORE_AS_PRIMARY and db_utils.db_firestore: # Use db_utils.db_firestore
        process_items_ref = db_utils.get_firestore_collection_ref("followup_process_items") # Use db_utils.get_firestore_collection_ref
        if process_items_ref:
            try:
                # Firestore não tem ON DELETE CASCADE, então deletamos itens manualmente
                batch = db_utils.db_firestore.batch() # Use db_utils.db_firestore.batch()
                docs_to_delete = process_items_ref.where("processo_id", "==", str(processo_id)).stream()
                for doc in docs:
                    batch.delete(doc.reference)
                batch.commit()
                logger.info(f"Itens do processo ID {processo_id} deletados com sucesso no Firestore.")
            except Exception as e:
                logger.error(f"Erro ao deletar itens do processo ID {processo_id} no Firestore: {e}")
                success_firestore = False
        else:
            success_firestore = False

    if _SQLITE_ENABLED:
        conn_sqlite = conectar_followup_db()
        if conn_sqlite:
            try:
                cursor_sqlite = conn_sqlite.cursor()
                cursor_sqlite.execute("DELETE FROM process_items WHERE processo_id = ?", (processo_id,))
                conn_sqlite.commit()
                logger.info(f"Itens do processo ID {processo_id} deletados com sucesso no SQLite.")
            except Exception as e:
                logger.exception(f"Erro ao deletar itens do processo ID {processo_id} no SQLite.")
                conn_sqlite.rollback()
                success_sqlite = False
            finally:
                if conn_sqlite:
                    conn_sqlite.close()
        else:
            success_sqlite = False
    
    return success_firestore and success_sqlite

# Inserir item do processo não deve ser cacheado, pois modifica o estado do DB.
def inserir_item_processo(
    processo_id: Any, # pode ser int (SQLite) ou string (Firestore)
    codigo_interno: Optional[str],
    ncm: Optional[str],
    cobertura: Optional[str],
    sku: Optional[str],
    quantidade: Optional[float],
    peso_unitario: Optional[float],
    valor_unitario: Optional[float],
    valor_total_item: Optional[float],
    estimativa_ii_br: Optional[float],
    estimativa_ipi_br: Optional[float],
    estimativa_pis_br: Optional[float],
    estimativa_cofins_br: Optional[float],
    estimativa_icms_br: Optional[float],
    frete_rateado_usd: Optional[float],
    seguro_rateado_brl: Optional[float],
    vlmd_item: Optional[float],
    denominacao_produto: Optional[str],
    detalhamento_complementar_produto: Optional[str]
) -> bool:
    """Insere um novo item associado a um processo em ambos os DBs."""
    success_firestore = True
    success_sqlite = True

    item_data = {
        "processo_id": str(processo_id), # Firestore sempre usa string para ID de referência
        "codigo_interno": codigo_interno,
        "ncm": ncm,
        "cobertura": cobertura,
        "sku": sku,
        "quantidade": quantidade,
        "peso_unitario": peso_unitario,
        "valor_unitario": valor_unitario,
        "valor_total_item": valor_total_item,
        "estimativa_ii_br": estimativa_ii_br,
        "estimativa_ipi_br": estimativa_ipi_br,
        "estimativa_pis_br": estimativa_pis_br,
        "estimativa_cofins_br": estimativa_cofins_br,
        "estimativa_icms_br": estimativa_icms_br,
        "frete_rateado_usd": frete_rateado_usd,
        "seguro_rateado_brl": seguro_rateado_brl,
        "vlmd_item": vlmd_item,
        "denominacao_produto": denominacao_produto,
        "detalhamento_complementar_produto": detalhamento_complementar_produto
    }

    if _USE_FIRESTORE_AS_PRIMARY and db_utils.db_firestore: # Use db_utils.db_firestore
        process_items_ref = db_utils.get_firestore_collection_ref("followup_process_items") # Use db_utils.get_firestore_collection_ref
        if process_items_ref:
            try:
                process_items_ref.add(item_data) # Firestore gera ID automaticamente
                logger.debug(f"Item inserido para o processo ID {processo_id} no Firestore.")
            except Exception as e:
                logger.error(f"Erro ao inserir item para o processo ID {processo_id} no Firestore: {e}")
                success_firestore = False
        else:
            success_firestore = False

    if _SQLITE_ENABLED:
        conn_sqlite = conectar_followup_db()
        if conn_sqlite:
            try:
                cursor_sqlite = conn_sqlite.cursor()
                cursor_sqlite.execute('''INSERT INTO process_items (
                                    processo_id, codigo_interno, ncm, cobertura, sku,
                                    quantidade, peso_unitario, valor_unitario, valor_total_item,
                                    estimativa_ii_br, estimativa_ipi_br, estimativa_pis_br,
                                    estimativa_cofins_br, estimativa_icms_br,
                                    frete_rateado_usd, seguro_rateado_brl, vlmd_item,
                                    denominacao_produto, detalhamento_complementar_produto
                                  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                               (
                                   processo_id, codigo_interno, ncm, cobertura, sku,
                                   quantidade, peso_unitario, valor_unitario, valor_total_item,
                                   estimativa_ii_br, estimativa_ipi_br, estimativa_pis_br,
                                   estimativa_cofins_br, estimativa_icms_br,
                                   frete_rateado_usd, seguro_rateado_brl, vlmd_item,
                                   denominacao_produto, detalhamento_complementar_produto
                               ))
                conn_sqlite.commit()
                logger.debug(f"Item inserido para o processo ID {processo_id} no SQLite.")
            except Exception as e:
                logger.exception(f"Erro ao inserir item para o processo ID {processo_id} no SQLite.")
                conn_sqlite.rollback()
                success_sqlite = False
            finally:
                if conn_sqlite:
                    conn_sqlite.close()
        else:
            success_sqlite = False
    
    return success_firestore and success_sqlite


@st.cache_data(ttl=3600)
def obter_itens_processo(processo_id: Any) -> List[Dict[str, Any]]: # processo_id pode ser int (SQLite) ou string (Firestore)
    """Obtém todos os itens associados a um processo específico. Prefere Firestore."""
    if _USE_FIRESTORE_AS_PRIMARY and db_utils.db_firestore: # Use db_utils.db_firestore
        process_items_ref = db_utils.get_firestore_collection_ref("followup_process_items") # Use db_utils.get_firestore_collection_ref
        if not process_items_ref: return []
        try:
            items = []
            # Assume que processo_id no Firestore é a string Processo_Novo
            docs = process_items_ref.where("processo_id", "==", str(processo_id)).stream()
            for doc in docs:
                items.append(doc.to_dict())
            return items
        except Exception as e:
            logger.error(f"Erro ao obter itens do processo ID {processo_id} do Firestore: {e}")
            return []
    elif _SQLITE_ENABLED:
        conn = conectar_followup_db()
        if conn is None: return []
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM process_items WHERE processo_id = ?", (processo_id,))
            itens = cursor.fetchall()
            return [dict(item) for item in itens]
        except Exception as e:
            logger.exception(f"Erro ao obter itens do processo ID {processo_id} do SQLite.")
            return []
        finally:
            if conn:
                conn.close()
    return []

@st.cache_data(ttl=3600)
def get_all_process_items_with_process_ref() -> List[Dict[str, Any]]:
    """
    Obtém todos os itens de processo juntamente com a referência do processo (Processo_Novo)
    e o Status_Geral ao qual pertencem. Prefere Firestore.
    """
    if _USE_FIRESTORE_AS_PRIMARY and db_utils.db_firestore: # Use db_utils.db_firestore
        process_items_ref = db_utils.get_firestore_collection_ref("followup_process_items") # Use db_utils.get_firestore_collection_ref
        processos_ref = db_utils.get_firestore_collection_ref("followup_processos") # Use db_utils.get_firestore_collection_ref
        if not process_items_ref or not processos_ref: return []
        try:
            items_with_process_info = []
            all_items_docs = process_items_ref.stream()
            
            # Para cada item, buscar o processo pai. Isso pode ser ineficiente para muitos itens.
            # Uma alternativa mais escalável seria armazenar Processo_Novo e Status_Geral diretamente no item.
            # Ou usar uma consulta de junção (que o Firestore não faz nativamente).
            # Para fins deste exemplo, faremos leituras individuais para cada item.
            for item_doc in all_items_docs:
                item_data = item_doc.to_dict()
                processo_novo_id = item_data.get('processo_id') # Este é o Processo_Novo do pai
                if processo_novo_id:
                    processo_doc = processos_ref.document(processo_novo_id).get()
                    if processo_doc.exists:
                        processo_data = processo_doc.to_dict()
                        item_data['Processo_Novo'] = processo_data.get('Processo_Novo')
                        item_data['Status_Geral'] = processo_data.get('Status_Geral')
                    else:
                        item_data['Processo_Novo'] = None
                        item_data['Status_Geral'] = None
                items_with_process_info.append(item_data)
            
            return items_with_process_info
        except Exception as e:
            logger.error(f"Erro ao obter todos os itens de processo com referência e status do processo do Firestore: {e}")
            return []
    elif _SQLITE_ENABLED:
        conn = conectar_followup_db()
        if conn is None: return []
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT
                    pi.*,
                    p.Processo_Novo,
                    p.Status_Geral
                FROM
                    process_items pi
                JOIN
                    processos p ON pi.processo_id = p.id
                ORDER BY
                    p.Processo_Novo, pi.id
            ''')
            items = cursor.fetchall()
            return [dict(item) for item in items]
        except Exception as e:
            logger.exception("Erro ao obter todos os itens de processo com referência e status do processo do SQLite.")
            return []
        finally:
            if conn:
                conn.close()
    return []

@st.cache_data(ttl=3600)
def obter_processos_filtrados(status_filtro="Todos", termos_pesquisa=None):
    """Busca processos do banco de dados aplicando filtros de status e termos de pesquisa. Prefere Firestore."""
    if _USE_FIRESTORE_AS_PRIMARY and db_utils.db_firestore: # Use db_utils.db_firestore
        processos_ref = db_utils.get_firestore_collection_ref("followup_processos") # Use db_utils.get_firestore_collection_ref
        if not processos_ref: return []
        try:
            import google.cloud.firestore # Importado aqui para garantir que esteja acessível
            query_firestore = processos_ref.limit(1000) # Limite para evitar buscar todos os documentos
            
            if status_filtro == "Arquivados":
                query_firestore = query_firestore.where("Status_Arquivado", "==", "Arquivado")
            elif status_filtro != "Todos":
                query_firestore = query_firestore.where("Status_Geral", "==", status_filtro)
                # No Firestore, para Status_Arquivado, você precisaria de um índice e uma query separada
                # ou garantir que o campo não exista para "Não Arquivado".
                # Para simplificar aqui, assumimos que 'Status_Arquivado' só existe se for 'Arquivado'
                # ou que 'Não Arquivado' é o valor padrão e não será filtrado ativamente para "Todos".
                # Se "Não Arquivado" for um valor explícito, a query seria mais complexa:
                # .where("Status_Arquivado", "in", ["Não Arquivado", None]) ou uma combinação de ORs.
                # Para o escopo, vamos focar no filtro principal.
            
            # Para termos_pesquisa, o Firestore não suporta queries LIKE nativamente.
            # Você precisaria de busca de texto completo (ex: Algoria, ElasticSearch) ou
            # fazer o filtro na aplicação após a busca, o que pode ser ineficiente para muitos dados.
            # Para termos_pesquisa, vamos simular uma busca por prefixo ou exata no Firestore.
            # Para mais complexidade, você precisaria de Cloud Functions ou soluções de busca de texto.
            
            if termos_pesquisa:
                # Firestore queries são limitadas: não é possível usar múltiplos operadores de "range"
                # (como `startswith` ou `like` em várias colunas).
                # Para simular `LIKE`, faremos uma query por prefixo na primeira chave de pesquisa
                # e filtraremos o restante no código Python. Isso é limitado.
                for col, termo in termos_pesquisa.items():
                    if termo:
                        # Assumindo que termos_pesquisa só tem uma chave para simplificar,
                        # ou que o usuário só busca em uma coluna por vez para uma query eficiente.
                        # Exemplo: Apenas para Processo_Novo ou N_Invoice
                        if col == "Processo_Novo":
                            query_firestore = query_firestore.where("Processo_Novo", ">=", termo).where("Processo_Novo", "<=", termo + '\uf8ff')
                        elif col == "N_Invoice":
                            query_firestore = query_firestore.where("N_Invoice", ">=", termo).where("N_Invoice", "<=", termo + '\uf8ff')
                        # Se houver outros campos para buscar, o filtro precisa ser feito na aplicação.
            
            query_firestore = query_firestore.order_by("Status_Geral").order_by("Modal")
            processos = [doc.to_dict() for doc in query_firestore.stream()]
            
            # Filtragem adicional para termos de pesquisa que o Firestore não suporta diretamente
            if termos_pesquisa:
                filtered_processes = []
                for proc in processos:
                    match = True
                    for col, termo in termos_pesquisa.items():
                        if termo and col in proc and termo.lower() not in str(proc[col]).lower():
                            match = False
                            break
                    if match:
                        filtered_processes.append(proc)
                processos = filtered_processes

            logger.debug(f"Obtidos {len(processos)} processos do Firestore com filtros/pesquisa.")
            return processos
        except Exception as e:
            logger.error(f"Erro ao obter processos filtrados do Firestore: {e}")
            return []
    elif _SQLITE_ENABLED:
        conn = conectar_followup_db()
        if conn is None: return []
        try:
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(processos);")
            colunas_info = cursor.fetchall()
            col_names = [info[1] for info in colunas_info]

            query = "SELECT * FROM processos WHERE 1"
            params = []

            if status_filtro == "Arquivados":
                query += ' AND "Status_Arquivado" = ?'
                params.append("Arquivado")
            elif status_filtro != "Todos":
                query += ' AND "Status_Geral" = ? AND ("Status_Arquivado" IS NULL OR "Status_Arquivado" = "Não Arquivado")'
                params.append(status_filtro)
            else:
                pass
                
            if termos_pesquisa:
                for col, termo in termos_pesquisa.items():
                    if termo and col in col_names:
                        query += f' AND "{col}" LIKE ?'
                        params.append(f'%{termo}%')

            query += ' ORDER BY "Status_Geral" ASC, "Modal" ASC'

            logger.debug(f"Query de busca (SQLite): {query}")
            logger.debug(f"Parâmetros da query (SQLite): {params}")

            cursor.execute(query, tuple(params))
            processos = cursor.fetchall()
            logger.debug(f"Obtidos {len(processos)} processos do DB (SQLite) com filtros/pesquisa.")
            return processos

        except Exception as e:
            logger.exception("Erro ao obter processos filtrados do DB (SQLite)")
            return []
        finally:
            if conn: conn.close()
    return []


@st.cache_data(ttl=3600)
def obter_todos_processos():
    """Busca todos os processos do banco de dados. Prefere Firestore."""
    if _USE_FIRESTORE_AS_PRIMARY and db_utils.db_firestore: # Use db_utils.db_firestore
        processos_ref = db_utils.get_firestore_collection_ref("followup_processos") # Use db_utils.get_firestore_collection_ref
        if not processos_ref: return []
        try:
            docs = processos_ref.stream()
            return [doc.to_dict() for doc in docs]
        except Exception as e:
            logger.error(f"Erro ao obter todos os processos do Firestore: {e}")
            return []
    elif _SQLITE_ENABLED:
        conn = conectar_followup_db()
        if conn is None: return []
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM processos")
            processos = cursor.fetchall()
            logger.debug(f"Obtidos {len(processos)} processos do DB (SQLite).")
            return processos
        except Exception as e:
            logger.exception("Erro ao obter todos os processos do DB (SQLite)")
            return []
        finally:
            if conn: conn.close()
    return []

@st.cache_data(ttl=3600)
def obter_processo_por_id(processo_id: Any): # pode ser int (SQLite) ou string (Firestore - Processo_Novo)
    """Busca um processo específico pelo ID. Prefere Firestore."""
    if _USE_FIRESTORE_AS_PRIMARY and db_utils.db_firestore: # Use db_utils.db_firestore
        processos_ref = db_utils.get_firestore_collection_ref("followup_processos") # Use db_utils.get_firestore_collection_ref
        if not processos_ref: return None
        try:
            # Assumimos que o processo_id no Firestore é o "Processo_Novo" (string)
            doc = processos_ref.document(str(processo_id)).get()
            if doc.exists:
                return doc.to_dict()
            return None
        except Exception as e:
            logger.error(f"Erro ao obter processo com ID {processo_id} do Firestore: {e}")
            return None
    elif _SQLITE_ENABLED:
        conn = conectar_followup_db()
        if conn is None: return None
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM processos WHERE id = ?", (processo_id,))
            processo = cursor.fetchone()
            logger.debug(f"Obtido processo com ID {processo_id} (SQLite): {processo is not None}")
            return processo
        except Exception as e:
            logger.exception(f"Erro ao obter processo com ID {processo_id} do SQLite")
            return None
        finally:
            if conn: conn.close()
    return None

@st.cache_data(ttl=3600)
def obter_processo_by_processo_novo(processo_novo: str):
    """Busca um processo específico pela sua referência (Processo_Novo). Prefere Firestore."""
    if _USE_FIRESTORE_AS_PRIMARY and db_utils.db_firestore: # Use db_utils.db_firestore
        processos_ref = db_utils.get_firestore_collection_ref("followup_processos") # Use db_utils.get_firestore_collection_ref
        if not processos_ref: return None
        try:
            doc = processos_ref.document(processo_novo).get() # Processo_Novo é o ID do documento
            if doc.exists:
                return doc.to_dict()
            return None
        except Exception as e:
            logger.error(f"Erro ao obter processo com Processo_Novo '{processo_novo}' do Firestore: {e}")
            return None
    elif _SQLITE_ENABLED:
        conn = conectar_followup_db()
        if conn is None: return None
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM processos WHERE "Processo_Novo" = ?', (processo_novo,))
            processo = cursor.fetchone()
            logger.debug(f"Obtido processo com Processo_Novo '{processo_novo}' (SQLite): {processo is not None}")
            return processo
        except Exception as e:
            logger.exception(f"Erro ao obter processo com Processo_Novo '{processo_novo}' do SQLite")
            return None
        finally:
            if conn: conn.close()
    return None

# Inserir processo não deve ser cacheado, pois modifica o estado do DB.
def inserir_processo(dados: Dict[str, Any]): # Agora recebe um dicionário
    """Insere um novo processo em ambos os bancos de dados."""
    success_firestore = True
    success_sqlite = True

    # --- Inserir no Firestore ---
    if _USE_FIRESTORE_AS_PRIMARY and db_utils.db_firestore: # Use db_utils.db_firestore
        processos_ref_firestore = db_utils.get_firestore_collection_ref("followup_processos") # Use db_utils.get_firestore_collection_ref
        if processos_ref_firestore:
            processo_novo_id = dados.get("Processo_Novo")
            if not processo_novo_id:
                logger.error("Campo 'Processo_Novo' é obrigatório para inserir no Firestore.")
                success_firestore = False
            else:
                try:
                    # Verifica se o Processo_Novo já existe no Firestore
                    existing_doc = processos_ref_firestore.document(processo_novo_id).get()
                    if existing_doc.exists:
                        logger.error(f"Processo '{processo_novo_id}' já existe no Firestore. Abortando inserção no Firestore.")
                        success_firestore = False
                    else:
                        processos_ref_firestore.document(processo_novo_id).set(dados)
                        logger.info(f"Novo processo '{processo_novo_id}' inserido com sucesso no Firestore.")
                except Exception as e:
                    logger.exception(f"Erro ao inserir novo processo no Firestore: {e}")
                    success_firestore = False
        else:
            success_firestore = False

    # --- Inserir no SQLite (Backup Físico) ---
    if _SQLITE_ENABLED:
        conn_sqlite = conectar_followup_db()
        if conn_sqlite:
            try:
                cursor_sqlite = conn_sqlite.cursor()
                cursor_sqlite.execute("PRAGMA table_info(processos);")
                colunas_info_sqlite = cursor_sqlite.fetchall()
                colunas_sqlite = [info[1] for info in colunas_info_sqlite if info[1] != 'id']

                # Mapear dados para a ordem das colunas do SQLite
                dados_sqlite_tuple = tuple(dados.get(col, None) for col in colunas_sqlite)

                if len(dados_sqlite_tuple) != len(colunas_sqlite):
                    logger.error(f"ERRO DE INSERÇÃO (SQLite): Número de dados ({len(dados_sqlite_tuple)}) não corresponde ao número de colunas no DB ({len(colunas_sqlite)}).")
                    success_sqlite = False
                else:
                    cols_str = ', '.join([f'"{c}"' for c in colunas_sqlite])
                    placeholders = ', '.join(['?'] * len(colunas_sqlite))
                    query_sqlite = f"INSERT INTO processos ({cols_str}) VALUES ({placeholders})"

                    cursor_sqlite.execute(query_sqlite, dados_sqlite_tuple)
                    conn_sqlite.commit()
                    logger.info("Novo processo inserido com sucesso no SQLite.")
            except sqlite3.IntegrityError:
                logger.warning(f"Processo '{dados.get('Processo_Novo')}' já existe no SQLite. Ignorando inserção duplicada.")
                conn_sqlite.rollback() # Reverte a tentativa de inserção
                success_sqlite = True # Não é um erro fatal para o backup, só uma duplicata
            except Exception as e:
                logger.exception("Erro ao inserir novo processo no SQLite.")
                conn_sqlite.rollback()
                success_sqlite = False
            finally:
                if conn_sqlite: conn_sqlite.close()
        else:
            success_sqlite = False

    # Invalida o cache das funções que obtêm processos, forçando uma nova leitura.
    # Isso é crucial para que a UI reflita os dados mais recentes após uma inserção.
    obter_processos_filtrados.clear()
    obter_todos_processos.clear()
    obter_processo_por_id.clear() # Se o ID do novo processo é conhecido, este é mais específico.
    obter_processo_by_processo_novo.clear() # Se o Processo_Novo é a chave, também deve ser limpo.
    obter_status_gerais_distintos.clear() # Se o novo processo pode introduzir um novo status.

    return success_firestore and success_sqlite


# Atualizar processo não deve ser cacheado, pois modifica o estado do DB.
def atualizar_processo(processo_id: Any, dados: Dict[str, Any]): # processo_id pode ser int (SQLite) ou string (Firestore)
    """Atualiza um processo existente em ambos os bancos de dados."""
    success_firestore = True
    success_sqlite = True

    # --- Atualizar no Firestore ---
    if _USE_FIRESTORE_AS_PRIMARY and db_utils.db_firestore: # Use db_utils.db_firestore
        processos_ref_firestore = db_utils.get_firestore_collection_ref("followup_processos") # Use db_utils.get_firestore_collection_ref
        if processos_ref_firestore:
            try:
                # O ID do documento no Firestore é Processo_Novo (string)
                doc_ref = processos_ref_firestore.document(str(processo_id))
                doc_ref.update(dados)
                logger.info(f"Processo com ID {processo_id} atualizado com sucesso no Firestore.")
            except Exception as e:
                logger.error(f"Erro ao atualizar processo com ID {processo_id} no Firestore: {e}")
                success_firestore = False
        else:
            success_firestore = False

    # --- Atualizar no SQLite (Backup Físico) ---
    if _SQLITE_ENABLED:
        conn_sqlite = conectar_followup_db()
        if conn_sqlite:
            try:
                cursor_sqlite = conn_sqlite.cursor()
                cursor_sqlite.execute("PRAGMA table_info(processos);")
                colunas_info_sqlite = cursor_sqlite.fetchall()
                colunas_sqlite = [info[1] for info in colunas_info_sqlite if info[1] != 'id']

                set_clause_sqlite = ', '.join([f'"{c}" = ?' for c in colunas_sqlite])
                query_sqlite = f"UPDATE processos SET {set_clause_sqlite} WHERE id = ?"
                
                # Mapear dados para a ordem das colunas do SQLite, adicionando o ID para a cláusula WHERE
                dados_sqlite_tuple = tuple(dados.get(col, None) for col in colunas_sqlite) + (processo_id,)

                cursor_sqlite.execute(query_sqlite, dados_sqlite_tuple)
                conn_sqlite.commit()
                logger.info(f"Processo com ID {processo_id} atualizado com sucesso no SQLite.")
            except Exception as e:
                logger.exception(f"Erro ao atualizar processo com ID {processo_id} no SQLite")
                conn_sqlite.rollback()
                success_sqlite = False
            finally:
                if conn_sqlite: conn_sqlite.close()
        else:
            success_sqlite = False
    
    # Invalida o cache das funções que obtêm processos, forçando uma nova leitura.
    obter_processos_filtrados.clear()
    obter_todos_processos.clear()
    obter_processo_por_id.clear()
    obter_processo_by_processo_novo.clear()
    obter_status_gerais_distintos.clear() # Caso a atualização mude o status e ele seja distinto.

    return success_firestore and success_sqlite

# Excluir processo não deve ser cacheado, pois modifica o estado do DB.
def excluir_processo(processo_id: Any): # pode ser int (SQLite) ou string (Firestore)
    """Exclui um processo em ambos os bancos de dados."""
    success_firestore = True
    success_sqlite = True

    # --- Excluir no Firestore ---
    if _USE_FIRESTORE_AS_PRIMARY and db_utils.db_firestore: # Use db_utils.db_firestore
        processos_ref_firestore = db_utils.get_firestore_collection_ref("followup_processos") # Use db_utils.get_firestore_collection_ref
        if processos_ref_firestore:
            try:
                # Firestore não tem ON DELETE CASCADE nativo para subcoleções (se houver)
                # ou coleções relacionadas. Você precisa deletar itens relacionados manualmente.
                # Assumimos que Processo_Novo é o ID do documento.
                processo_doc_ref = processos_ref_firestore.document(str(processo_id))
                processo_doc_ref.delete()
                logger.info(f"Processo com ID {processo_id} excluído com sucesso do Firestore.")

                # Deletar itens relacionados: historico_processos, process_items, notifications
                # (isso pode ser feito de forma mais otimizada com um batch ou Cloud Function)
                batch = db_utils.db_firestore.batch() # Use db_utils.db_firestore.batch()
                
                # Deletar historico_processos
                history_ref_firestore = db_utils.get_firestore_collection_ref("followup_historico_processos") # Use db_utils.get_firestore_collection_ref
                history_docs = history_ref_firestore.where("processo_id", "==", str(processo_id)).stream()
                for doc in history_docs:
                    batch.delete(doc.reference)
                
                # Deletar process_items
                items_ref_firestore = db_utils.get_firestore_collection_ref("followup_process_items") # Use db_utils.get_firestore_collection_ref
                items_docs = items_ref_firestore.where("processo_id", "==", str(processo_id)).stream()
                for doc in items_docs:
                    batch.delete(doc.reference)
                
                # Deletar notifications (se houver ligação direta ao processo_id)
                # Isso depende de como as notificações são ligadas aos processos.
                # Se notifications não têm `processo_id`, esta parte pode não ser necessária ou precisar de outra lógica.
                
                batch.commit()
                logger.info(f"Dados relacionados ao processo ID {processo_id} excluídos do Firestore.")

            except Exception as e:
                logger.error(f"Erro ao excluir processo com ID {processo_id} do Firestore: {e}")
                success_firestore = False
        else:
            success_firestore = False

    if _SQLITE_ENABLED:
        conn_sqlite = conectar_followup_db()
        if conn_sqlite is None:
            success_sqlite = False
        else:
            try:
                cursor_sqlite = conn_sqlite.cursor()
                cursor_sqlite.execute("DELETE FROM processos WHERE id = ?", (processo_id,))
                conn_sqlite.commit()
                logger.info(f"Processo com ID {processo_id} excluído com sucesso do SQLite.")
            except Exception as e:
                logger.exception(f"Erro ao excluir processo com ID {processo_id} do SQLite")
                conn_sqlite.rollback()
                success_sqlite = False
            finally:
                if conn_sqlite: conn_sqlite.close()
    else:
                    success_sqlite = False
    
    # Invalida o cache das funções que obtêm processos, forçando uma nova leitura.
    obter_processos_filtrados.clear()
    obter_todos_processos.clear()
    obter_processo_por_id.clear()
    obter_processo_by_processo_novo.clear()
    obter_status_gerais_distintos.clear() # Uma exclusão pode remover um status.

    return success_firestore and success_sqlite


# Arquivar processo não deve ser cacheado, pois modifica o estado do DB.
def arquivar_processo(processo_id: Any): # pode ser int (SQLite) ou string (Firestore)
    """Marca um processo como arquivado em ambos os bancos de dados."""
    success_firestore = True
    success_sqlite = True

    # --- Arquivar no Firestore ---
    if _USE_FIRESTORE_AS_PRIMARY and db_utils.db_firestore: # Use db_utils.db_firestore
        processos_ref_firestore = db_utils.get_firestore_collection_ref("followup_processos") # Use db_utils.get_firestore_collection_ref
        if processos_ref_firestore:
            try:
                import google.cloud.firestore # Importado aqui para garantir que esteja acessível
                doc_ref = processos_ref_firestore.document(str(processo_id))
                doc_ref.update({"Status_Arquivado": "Arquivado"})
                logger.info(f"Processo com ID {processo_id} arquivado com sucesso no Firestore.")
            except Exception as e:
                logger.error(f"Erro ao arquivar processo com ID {processo_id} no Firestore: {e}")
                success_firestore = False
        else:
            success_firestore = False

    # --- Arquivar no SQLite (Backup Físico) ---
    if _SQLITE_ENABLED:
        conn_sqlite = conectar_followup_db()
        if conn_sqlite is None:
            success_sqlite = False
        else:
            try:
                cursor_sqlite = conn_sqlite.cursor()
                cursor_sqlite.execute('UPDATE processos SET "Status_Arquivado" = ? WHERE id = ?', ('Arquivado', processo_id))
                conn_sqlite.commit()
                logger.info(f"Processo com ID {processo_id} arquivado com sucesso no SQLite.")
            except Exception as e:
                logger.exception(f"Erro ao arquivar processo com ID {processo_id} no SQLite")
                conn_sqlite.rollback()
                success_sqlite = False
            finally:
                if conn_sqlite: conn_sqlite.close()
    else:
            success_sqlite = False
    
    # Invalida o cache das funções que obtêm processos, forçando uma nova leitura.
    obter_processos_filtrados.clear()
    obter_todos_processos.clear() # Um processo arquivado ainda estará em "todos".
    obter_processo_por_id.clear()
    obter_processo_by_processo_novo.clear()

    return success_firestore and success_sqlite


# Desarquivar processo não deve ser cacheado, pois modifica o estado do DB.
def desarquivar_processo(processo_id: Any): # pode ser int (SQLite) ou string (Firestore)
    """Marca um processo como não arquivado em ambos os bancos de dados."""
    success_firestore = True
    success_sqlite = True

    # --- Desarquivar no Firestore ---
    if _USE_FIRESTORE_AS_PRIMARY and db_utils.db_firestore: # Use db_utils.db_firestore
        processos_ref_firestore = db_utils.get_firestore_collection_ref("followup_processos") # Use db_utils.get_firestore_collection_ref
        if processos_ref_firestore:
            try:
                import google.cloud.firestore # Importado aqui para garantir que esteja acessível
                doc_ref = processos_ref_firestore.document(str(processo_id))
                doc_ref.update({"Status_Arquivado": google.cloud.firestore.DELETE_FIELD}) # Remove o campo
                logger.info(f"Processo com ID {processo_id} desarquivado com sucesso no Firestore.")
            except Exception as e:
                logger.error(f"Erro ao desarquivar processo com ID {processo_id} no Firestore: {e}")
                success_firestore = False
        else:
            success_firestore = False

    # --- Desarquivar no SQLite (Backup Físico) ---
    if _SQLITE_ENABLED:
        conn_sqlite = conectar_followup_db()
        if conn_sqlite is None:
            success_sqlite = False
        else:
            try:
                cursor_sqlite = conn_sqlite.cursor()
                # Define como NULL ou "Não Arquivado" dependendo da sua convenção
                cursor_sqlite.execute('UPDATE processos SET "Status_Arquivado" = ? WHERE id = ?', (None, processo_id))
                conn_sqlite.commit()
                logger.info(f"Processo com ID {processo_id} desarquivado com sucesso no SQLite.")
            except Exception as e:
                logger.exception(f"Erro ao desarquivar processo com ID {processo_id} no SQLite")
                conn_sqlite.rollback()
                success_sqlite = False
            finally:
                if conn_sqlite: conn_sqlite.close()
    else:
            success_sqlite = False
    
    # Invalida o cache das funções que obtêm processos, forçando uma nova leitura.
    obter_processos_filtrados.clear()
    obter_todos_processos.clear()
    obter_processo_por_id.clear()
    obter_processo_by_processo_novo.clear()

    return success_firestore and success_sqlite


# Atualizar status do processo não deve ser cacheado, pois modifica o estado do DB.
def atualizar_status_processo(processo_id: Any, novo_status: Optional[str], username: Optional[str] = "Desconhecido"):
    """Atualiza o Status_Geral de um processo específico em ambos os bancos de dados."""
    original_process_data_firestore = None
    original_process_data_sqlite = None

    # Obter status original do Firestore
    if _USE_FIRESTORE_AS_PRIMARY and db_utils.db_firestore: # Use db_utils.db_firestore
        processos_ref_firestore = db_utils.get_firestore_collection_ref("followup_processos") # Use db_utils.get_firestore_collection_ref
        if processos_ref_firestore:
            try:
                original_process_doc_firestore = processos_ref_firestore.document(str(processo_id)).get()
                if original_process_doc_firestore.exists:
                    original_process_data_firestore = original_process_doc_ref.to_dict() # Corrected line
            except Exception as e:
                logger.error(f"Erro ao obter dados originais do processo ID {processo_id} do Firestore: {e}")

    # Obter status original do SQLite
    if _SQLITE_ENABLED:
        conn_sqlite = conectar_followup_db()
        if conn_sqlite:
            try:
                cursor_sqlite = conn_sqlite.cursor()
                cursor_sqlite.execute("SELECT * FROM processos WHERE id = ?", (processo_id,))
                original_process_data_sqlite = cursor_sqlite.fetchone()
            except Exception as e:
                logger.error(f"Erro ao obter dados originais do processo ID {processo_id} do SQLite: {e}")
            finally:
                if conn_sqlite: conn_sqlite.close()

    original_status_firestore = original_process_data_firestore.get('Status_Geral') if original_process_data_firestore else None
    original_status_sqlite = original_process_data_sqlite['Status_Geral'] if original_process_data_sqlite else None

    success_firestore = True
    success_sqlite = True

    # --- Atualizar no Firestore ---
    if _USE_FIRESTORE_AS_PRIMARY and db_utils.db_firestore: # Use db_utils.db_firestore
        processos_ref_firestore = db_utils.get_firestore_collection_ref("followup_processos") # Use db_utils.get_firestore_collection_ref
        if processos_ref_firestore:
            try:
                doc_ref = processos_ref_firestore.document(str(processo_id))
                doc_ref.update({"Status_Geral": novo_status})
                logger.info(f"Status do processo ID {processo_id} atualizado para '{novo_status}' no Firestore.")
                inserir_historico_processo(str(processo_id), "Status_Geral", original_status_firestore, novo_status, username, db_type="Firestore")
            except Exception as e:
                logger.error(f"Erro ao atualizar status do processo ID {processo_id} no Firestore: {e}")
                success_firestore = False
        else:
            success_firestore = False

    # --- Atualizar no SQLite (Backup Físico) ---
    if _SQLITE_ENABLED:
        conn_sqlite = conectar_followup_db()
        if conn_sqlite is None:
            success_sqlite = False
        else:
            try:
                cursor_sqlite = conn_sqlite.cursor()
                cursor_sqlite.execute('UPDATE processos SET "Status_Geral" = ? WHERE id = ?', (novo_status, processo_id))
                conn_sqlite.commit()
                logger.info(f"Status do processo ID {processo_id} atualizado para '{novo_status}' no SQLite.")
                inserir_historico_processo(processo_id, "Status_Geral", original_status_sqlite, novo_status, username, db_type="SQLite")
            except Exception as e:
                logger.exception(f"Erro ao atualizar status do processo ID {processo_id} no SQLite")
                conn_sqlite.rollback()
                success_sqlite = False
            finally:
                if conn_sqlite: conn_sqlite.close()
    else:
            success_sqlite = False
    
    # Invalida o cache das funções que obtêm processos, forçando uma nova leitura.
    obter_processos_filtrados.clear()
    obter_todos_processos.clear()
    obter_processo_por_id.clear()
    obter_processo_by_processo_novo.clear()
    obter_status_gerais_distintos.clear() # Caso o status seja novo ou altere a contagem de um status existente.

    return success_firestore and success_sqlite


# Inserir histórico não deve ser cacheado, pois modifica o estado do DB.
def inserir_historico_processo(processo_id: Any, field_name: str, old_value: Optional[str], new_value: Optional[str], username: Optional[str], db_type: str):
    """Insere um registro na tabela historico_processos em um DB específico."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    history_data = {
        "processo_id": str(processo_id), # Sempre string para o Firestore
        "campo_alterado": field_name,
        "valor_antigo": str(old_value) if old_value is not None else "Vazio",
        "valor_novo": str(new_value) if new_value is not None else "Vazio",
        "timestamp": timestamp,
        "usuario": username if username is not None else "Desconhecido"
    }

    if db_type == "Firestore" and db_utils.db_firestore: # Use db_utils.db_firestore
        history_ref = db_utils.get_firestore_collection_ref("followup_historico_processos") # Use db_utils.get_firestore_collection_ref
        if history_ref:
            try:
                history_ref.add(history_data) # Firestore gera ID automaticamente
                logger.debug(f"Histórico registrado para processo {processo_id}, campo '{field_name}' por '{username}' no Firestore.")
                return True
            except Exception as e:
                logger.exception(f"Erro ao inserir histórico para processo {processo_id}, campo '{field_name}' por '{username}' no Firestore.")
                return False
        return False
    elif db_type == "SQLite" and _SQLITE_ENABLED:
        conn = conectar_followup_db()
        if conn is None: return False
        try:
            cursor = conn.cursor()
            cursor.execute('''INSERT INTO historico_processos (processo_id, campo_alterado, valor_antigo, valor_novo, timestamp, usuario)
                              VALUES (?, ?, ?, ?, ?, ?)''',
                           (processo_id, field_name, str(old_value) if old_value is not None else "Vazio", str(new_value) if new_value is not None else "Vazio", timestamp, username if username is not None else "Desconhecido"))
            conn.commit()
            logger.debug(f"Histórico registrado para processo {processo_id}, campo '{field_name}' por '{username}' no SQLite.")
            return True
        except Exception as e:
            logger.exception(f"Erro ao inserir histórico para processo {processo_id}, campo '{field_name}' por '{username}' no SQLite.")
            conn.rollback()
            return False
        finally:
            if conn:
                conn.close()
    return False

@st.cache_data(ttl=3600)
def obter_historico_processo(processo_id: Any): # pode ser int (SQLite) ou string (Firestore)
    """Busca o histórico de alterações para um processo específico. Prefere Firestore."""
    if _USE_FIRESTORE_AS_PRIMARY and db_utils.db_firestore: # Use db_utils.db_firestore
        history_ref = db_utils.get_firestore_collection_ref("followup_historico_processos") # Use db_utils.get_firestore_collection_ref
        if not history_ref: return []
        try:
            history = []
            # Assume que processo_id no Firestore é a string Processo_Novo
            docs = history_ref.where("processo_id", "==", str(processo_id)).order_by("timestamp").stream()
            for doc in docs:
                data = doc.to_dict()
                history.append({
                    "campo_alterado": data.get("campo_alterado"),
                    "valor_antigo": data.get("valor_antigo"),
                    "valor_novo": data.get("valor_novo"),
                    "timestamp": data.get("timestamp"),
                    "usuario": data.get("usuario")
                })
            return history
        except Exception as e:
            logger.error(f"Erro ao obter histórico para processo {processo_id} do Firestore: {e}")
            return []
    elif _SQLITE_ENABLED:
        conn = conectar_followup_db()
        if conn is None: return []
        try:
            cursor = conn.cursor()
            cursor.execute('''SELECT campo_alterado, valor_antigo, valor_novo, timestamp, usuario
                              FROM historico_processos
                              WHERE processo_id = ?
                              ORDER BY timestamp ASC''', (processo_id,))
            historico = cursor.fetchall()
            logger.debug(f"Obtido {len(historico)} registros de histórico para processo {processo_id} do SQLite.")
            return historico
        except Exception as e:
            logger.exception(f"Erro ao obter histórico para processo {processo_id} do SQLite.")
            return []
        finally:
            if conn:
                conn.close()
    return []

@st.cache_data(ttl=3600)
def obter_status_gerais_distintos():
    """Busca todos os valores distintos da coluna Status_Geral. Prefere Firestore."""
    if _USE_FIRESTORE_AS_PRIMARY and db_utils.db_firestore: # Use db_utils.db_firestore
        processos_ref = db_utils.get_firestore_collection_ref("followup_processos") # Use db_utils.get_firestore_collection_ref
        if not processos_ref: return []
        try:
            import google.cloud.firestore # Importado aqui para garantir que esteja acessível
            # Firestore não tem DISTINCT nativo, então buscamos todos os valores e fazemos o set
            status_list = [doc.get("Status_Geral") for doc in processos_ref.stream() if doc.get("Status_Geral") is not None and doc.get("Status_Geral") != ""].copy() # Added .copy() to allow modification
            status_distinct = sorted(list(set(status_list)))
            logger.debug(f"Obtidos {len(status_distinct)} status gerais distintos do Firestore.")
            return status_distinct
        except Exception as e:
            logger.error(f"Erro ao obter status gerais distintos do Firestore: {e}")
            return []
    elif _SQLITE_ENABLED:
        conn = conectar_followup_db()
        if conn is None: return []
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT DISTINCT "Status_Geral" FROM processos WHERE "Status_Geral" IS NOT NULL AND "Status_Geral" != "" ORDER BY "Status_Geral"')
            status_do_db = [row[0] for row in cursor.fetchall()]
            logger.debug(f"Obtidos {len(status_do_db)} status gerais distintos do DB (SQLite).")
            return status_do_db
        except Exception as e:
            logger.exception("Erro ao obter status gerais distintos do DB (SQLite).")
            return []
        finally:
            if conn:
                conn.close()
    return []

@st.cache_data(ttl=3600)
def obter_nomes_colunas_db():
    """Retorna uma lista com os nomes das colunas da tabela processos (do SQLite)."""
    # Esta função é mais relevante para a estrutura SQL. Para o Firestore, os nomes dos campos são dinâmicos.
    # Se precisar de um "schema" para o Firestore, você precisaria defini-lo manualmente ou inferir.
    if _SQLITE_ENABLED:
        conn = conectar_followup_db()
        if conn is None:
            logger.error("Falha na conexão ao DB em obter_nomes_colunas_db (SQLite). Retornando lista vazia de colunas.")
            return []
        try:
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(processos);")
            colunas_info = cursor.fetchall()
            col_names = [info[1] for info in colunas_info]
            if not col_names:
                logger.error("PRAGMA table_info(processos) retornou lista de colunas vazia, tabela 'processos' (SQLite) pode não existir ou estar corrompida!")
            logger.info(f"obter_nomes_colunas_db: Colunas do DB obtidas (SQLite): {col_names} (total: {len(col_names)}).")
            return col_names
        except Exception as e:
            logger.exception("Erro inesperado ao obter nomes de colunas do DB (SQLite).")
            return []
        finally:
            if conn:
                conn.close()
    else:
        logger.warning("SQLite está desabilitado. Não é possível obter nomes de colunas via PRAGMA table_info.")
        # Retorna um mock de colunas se o SQLite estiver desabilitado e a função for chamada
        return [
            'Processo_Novo', 'Observacao', 'Tipos_de_item', 'Data_Embarque', 'Previsao_Pichau',
            'Documentos_Revisados', 'Conhecimento_Embarque', 'Descricao_Feita', 'Descricao_Enviada',
            'Fornecedor', 'N_Invoice', 'Quantidade', 'Valor_USD', 'Pago', 'N_Ordem_Compra',
            'Data_Compra', 'Estimativa_Impostos_BR', 'Estimativa_Frete_USD', 'Agente_de_Carga_Novo',
            'Status_Geral', 'Modal', 'Navio', 'Origem', 'Destino', 'INCOTERM', 'Comprador',
            'Status_Arquivado', 'Caminho_da_pasta', 'Estimativa_Dolar_BRL', 'Estimativa_Seguro_BRL',
            'Estimativa_II_BR', 'Estimativa_IPI_BR', 'Estimativa_PIS_BR', 'Estimativa_COFINS_BR',
            'Estimativa_ICMS_BR', 'Nota_feita', 'Conferido', 'Ultima_Alteracao_Por',
            'Ultima_Alteracao_Em', 'Estimativa_Impostos_Total', 'Quantidade_Containers',
            'ETA_Recinto', 'Data_Registro', 'DI_ID_Vinculada'
        ]
    return []


# --- Funções de gerenciamento de Notificações ---

# Adicionar notificação não deve ser cacheado, pois modifica o estado do DB.
def add_notification(message: str, target_user: str, created_by: str, status: str = 'active'):
    """Adiciona uma nova notificação em ambos os bancos de dados."""
    success_firestore = True
    success_sqlite = True

    notification_data = {
        "message": message,
        "target_users": target_user,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "created_by": created_by,
        "status": status
    }

    # --- Adicionar no Firestore ---
    if _USE_FIRESTORE_AS_PRIMARY and db_utils.db_firestore: # Use db_utils.db_firestore
        notifications_ref = db_utils.get_firestore_collection_ref("followup_notifications") # Use db_utils.get_firestore_collection_ref
        if notifications_ref:
            try:
                notifications_ref.add(notification_data) # Firestore gera ID
                logger.info(f"Notificação adicionada por {created_by} para {target_user} no Firestore.")
            except Exception as e:
                logger.exception("Erro ao adicionar notificação no Firestore.")
                success_firestore = False
        else:
            success_firestore = False

    # --- Adicionar no SQLite (Backup Físico) ---
    if _SQLITE_ENABLED:
        conn_sqlite = conectar_followup_db()
        if conn_sqlite:
            try:
                cursor_sqlite = conn_sqlite.cursor()
                cursor_sqlite.execute('''INSERT INTO notifications (message, target_users, created_at, created_by, status)
                                  VALUES (?, ?, ?, ?, ?)''',
                               (notification_data["message"], notification_data["target_users"],
                                notification_data["created_at"], notification_data["created_by"],
                                notification_data["status"]))
                conn_sqlite.commit()
                logger.info(f"Notificação adicionada por {created_by} para {target_user} no SQLite.")
            except Exception as e:
                logger.exception("Erro ao adicionar notificação no SQLite.")
                conn_sqlite.rollback()
                success_sqlite = False
            finally:
                if conn_sqlite: conn_sqlite.close()
        else:
            success_sqlite = False
    
    # Invalida o cache das funções que obtêm notificações.
    get_active_notifications.clear()
    get_deleted_notifications.clear()

    return success_firestore and success_sqlite


@st.cache_data(ttl=3600)
def get_active_notifications(username: Optional[str] = None):
    """
    Busca notificações ativas. Prefere Firestore.
    """
    if _USE_FIRESTORE_AS_PRIMARY and db_utils.db_firestore: # Use db_utils.db_firestore
        notifications_ref = db_utils.get_firestore_collection_ref("followup_notifications") # Use db_utils.get_firestore_collection_ref
        if not notifications_ref: return []
        try:
            import google.cloud.firestore # Importado aqui para garantir que esteja acessível
            filtered_notifications = []
            # Buscar todas as notificações ativas
            query_firestore = notifications_ref.where("status", "==", "active").order_by("created_at", direction=google.cloud.firestore.Query.DESCENDING)
            all_active_notifications_docs = query_firestore.stream()

            for notif_doc in all_active_notifications_docs:
                notif = notif_doc.to_dict()
                target_user_str = notif.get('target_users')
                
                if username is None: # Admin view: mostra todas as ativas
                    filtered_notifications.append(notif)
                elif target_user_str == "ALL": # Notificação para todos
                    filtered_notifications.append(notif)
                elif target_user_str == username: # Notificação para o usuário específico
                    filtered_notifications.append(notif)
            
            return filtered_notifications
        except Exception as e:
            logger.error(f"Erro ao obter notificações ativas do Firestore: {e}")
            return []
    elif _SQLITE_ENABLED:
        conn = conectar_followup_db()
        if conn is None: return []
        try:
            cursor = conn.cursor()
            query = "SELECT * FROM notifications WHERE status = 'active' ORDER BY created_at DESC"
            cursor.execute(query)
            all_active_notifications = cursor.fetchall()

            filtered_notifications = []
            for notif in all_active_notifications:
                target_user_str = notif['target_users']
                
                if username is None:
                    filtered_notifications.append(dict(notif))
                elif target_user_str == "ALL":
                    filtered_notifications.append(dict(notif))
                elif target_user_str == username:
                    filtered_notifications.append(dict(notif))
            
            return filtered_notifications
        except Exception as e:
            logger.exception("Erro ao obter notificações ativas do SQLite.")
            return []
        finally:
            if conn:
                conn.close()
    return []

# Marcar notificação como excluída não deve ser cacheado, pois modifica o estado do DB.
def mark_notification_as_deleted(notification_id: Any, deleted_by: str): # ID pode ser int (SQLite) ou string (Firestore)
    """Marca uma notificação como excluída e registra no histórico em ambos os DBs."""
    success_firestore = True
    success_sqlite = True
    original_message_text = "Mensagem original não encontrada."

    # --- Obter mensagem original para histórico (Firestore) ---
    if _USE_FIRESTORE_AS_PRIMARY and db_utils.db_firestore: # Use db_utils.db_firestore
        notifications_ref_firestore = db_utils.get_firestore_collection_ref("followup_notifications") # Use db_utils.get_firestore_collection_ref
        if notifications_ref_firestore:
            try:
                original_notif_doc = notifications_ref_firestore.document(str(notification_id)).get()
                if original_notif_doc.exists:
                    original_message_text = original_notif_doc.to_dict().get('message', original_message_text)
            except Exception as e:
                logger.error(f"Erro ao obter mensagem original da notificação ID {notification_id} do Firestore: {e}")

    # --- Marcar como excluída e registrar histórico no Firestore ---
    if _USE_FIRESTORE_AS_PRIMARY and db_utils.db_firestore: # Use db_utils.db_firestore
        notifications_ref_firestore = db_utils.get_firestore_collection_ref("followup_notifications") # Use db_utils.get_firestore_collection_ref
        notification_history_ref_firestore = db_utils.get_firestore_collection_ref("followup_notification_history") # Use db_utils.get_firestore_collection_ref
        if notifications_ref_firestore and notification_history_ref_firestore:
            try:
                import google.cloud.firestore # Importado aqui para garantir que esteja acessível
                notifications_ref_firestore.document(str(notification_id)).update({"status": "deleted"})
                action_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                history_data_firestore = {
                    "notification_id": str(notification_id),
                    "action": "deleted",
                    "action_by": deleted_by,
                    "action_at": action_at,
                    "original_message": original_message_text
                }
                notification_history_ref_firestore.add(history_data_firestore)
                logger.info(f"Notificação ID {notification_id} marcada como excluída por {deleted_by} no Firestore.")
            except Exception as e:
                logger.exception(f"Erro ao marcar notificação ID {notification_id} como excluída no Firestore.")
                success_firestore = False
        else:
            success_firestore = False

    if _SQLITE_ENABLED:
        conn_sqlite = conectar_followup_db()
        if conn_sqlite:
            try:
                cursor_sqlite = conn_sqlite.cursor()
                
                # Obter a mensagem original da notificação antes de marcar como excluída
                cursor_sqlite.execute("SELECT message FROM notifications WHERE id = ?", (notification_id,))
                original_message_sqlite = cursor_sqlite.fetchone()
                if original_message_sqlite:
                    original_message_text_sqlite = original_message_sqlite['message']
                else:
                    original_message_text_sqlite = "Mensagem original não encontrada."

                cursor_sqlite.execute("UPDATE notifications SET status = 'deleted' WHERE id = ?", (notification_id,))
                
                action_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                cursor_sqlite.execute('''INSERT INTO notification_history (notification_id, action, action_by, action_at, original_message)
                                  VALUES (?, ?, ?, ?, ?)''',
                               (notification_id, 'deleted', deleted_by, action_at, original_message_text_sqlite))
                conn_sqlite.commit()
                logger.info(f"Notificação ID {notification_id} marcada como excluída por {deleted_by} no SQLite.")
            except Exception as e:
                logger.exception(f"Erro ao marcar notificação ID {notification_id} como excluída no SQLite.")
                conn_sqlite.rollback()
                success_sqlite = False
            finally:
                if conn_sqlite:
                    conn_sqlite.close()
        else:
            success_sqlite = False
    
    # Invalida o cache das funções que obtêm notificações.
    get_active_notifications.clear()
    get_deleted_notifications.clear()

    return success_firestore and success_sqlite

@st.cache_data(ttl=3600)
def get_deleted_notifications():
    """Busca notificações excluídas do histórico. Prefere Firestore."""
    if _USE_FIRESTORE_AS_PRIMARY and db_utils.db_firestore: # Use db_utils.db_firestore
        notification_history_ref = db_utils.get_firestore_collection_ref("followup_notification_history") # Use db_utils.get_firestore_collection_ref
        if not notification_history_ref: return []
        try:
            import google.cloud.firestore # Importado aqui para garantir que esteja acessível
            deleted_notifications = []
            docs = notification_history_ref.where("action", "==", "deleted").order_by("action_at", direction=google.cloud.firestore.Query.DESCENDING).stream()
            for doc in docs:
                data = doc.to_dict()
                deleted_notifications.append({
                    "history_entry_id": doc.id, # O ID do documento no histórico do Firestore
                    "original_notification_id": data.get("notification_id"),
                    "original_message": data.get("original_message"),
                    "action_at": data.get("action_at"),
                    "action_by": data.get("action_by")
                })
            return deleted_notifications
        except Exception as e:
            logger.error(f"Erro ao obter notificações excluídas do Firestore: {e}")
            return []
    elif _SQLITE_ENABLED:
        conn = conectar_followup_db()
        if conn is None: return []
        try:
            cursor = conn.cursor()
            query = "SELECT nh.id as history_entry_id, nh.notification_id as original_notification_id, nh.original_message, nh.action_at, nh.action_by FROM notification_history nh WHERE nh.action = 'deleted' ORDER BY nh.action_at DESC"
            cursor.execute(query)
            notifications = cursor.fetchall()
            return [dict(n) for n in notifications]
        except Exception as e:
            logger.exception("Erro ao obter notificações excluídas do SQLite.")
            return []
        finally:
            if conn:
                conn.close()
    return []

# Restaurar notificação não deve ser cacheado, pois modifica o estado do DB.
def restore_notification(notification_id: Any, restored_by: str): # ID pode ser int (SQLite) ou string (Firestore)
    """Restaura uma notificação excluída e registra no histórico em ambos os DBs."""
    success_firestore = True
    success_sqlite = True
    original_message_text = "Mensagem original não encontrada."

    # --- Obter mensagem original para histórico (Firestore) ---
    if _USE_FIRESTORE_AS_PRIMARY and db_utils.db_firestore: # Use db_utils.db_firestore
        notifications_ref_firestore = db_utils.get_firestore_collection_ref("followup_notifications") # Use db_utils.get_firestore_collection_ref
        if notifications_ref_firestore:
            try:
                original_notif_doc = notifications_ref_firestore.document(str(notification_id)).get()
                if original_notif_doc.exists:
                    original_message_text = original_notif_doc.to_dict().get('message', original_message_text)
            except Exception as e:
                logger.error(f"Erro ao obter mensagem original da notificação ID {notification_id} do Firestore: {e}")

    # --- Restaurar e registrar histórico no Firestore ---
    if _USE_FIRESTORE_AS_PRIMARY and db_utils.db_firestore: # Use db_utils.db_firestore
        notifications_ref_firestore = db_utils.get_firestore_collection_ref("followup_notifications") # Use db_utils.get_firestore_collection_ref
        notification_history_ref_firestore = db_utils.get_firestore_collection_ref("followup_notification_history") # Use db_utils.get_firestore_collection_ref
        if notifications_ref_firestore and notification_history_ref_firestore:
            try:
                import google.cloud.firestore # Importado aqui para garantir que esteja acessível
                notifications_ref_firestore.document(str(notification_id)).update({"status": "active"})
                action_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                history_data_firestore = {
                    "notification_id": str(notification_id),
                    "action": "restored",
                    "action_by": restored_by,
                    "action_at": action_at,
                    "original_message": original_message_text
                }
                notification_history_ref_firestore.add(history_data_firestore)
                logger.info(f"Notificação ID {notification_id} restaurada por {restored_by} no Firestore.")
            except Exception as e:
                logger.exception(f"Erro ao restaurar notificação ID {notification_id} no Firestore.")
                success_firestore = False
        else:
            success_firestore = False

    if _SQLITE_ENABLED:
        conn_sqlite = conectar_followup_db()
        if conn_sqlite:
            try:
                cursor_sqlite = conn_sqlite.cursor()
                
                # Obter a mensagem original da notificação antes de restaurar
                cursor_sqlite.execute("SELECT message FROM notifications WHERE id = ?", (notification_id,))
                original_message_sqlite = cursor_sqlite.fetchone()
                if original_message_sqlite:
                    original_message_text_sqlite = original_message_sqlite['message']
                else:
                    original_message_text_sqlite = "Mensagem original não encontrada."

                cursor_sqlite.execute("UPDATE notifications SET status = 'active' WHERE id = ?", (notification_id,))
                
                action_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                cursor_sqlite.execute('''INSERT INTO notification_history (notification_id, action, action_by, action_at, original_message)
                                  VALUES (?, ?, ?, ?, ?)''',
                               (notification_id, 'restored', restored_by, action_at, original_message_text_sqlite))
                conn_sqlite.commit()
                logger.info(f"Notificação ID {notification_id} restaurada por {restored_by} no SQLite.")
            except Exception as e:
                logger.exception(f"Erro ao restaurar notificação ID {notification_id} no SQLite.")
                conn_sqlite.rollback()
                success_sqlite = False
            finally:
                if conn_sqlite:
                    conn_sqlite.close()
        else:
            success_sqlite = False
    
    # Invalida o cache das funções que obtêm notificações.
    get_active_notifications.clear()
    get_deleted_notifications.clear()

    return success_firestore and success_sqlite


# Excluir entrada do histórico permanentemente não deve ser cacheado, pois modifica o estado do DB.
def delete_history_entry_permanently(history_entry_id: Any, deleted_by: str): # ID pode ser int (SQLite) ou string (Firestore)
    """
    Exclui permanentemente uma entrada do histórico de notificações em ambos os DBs.
    """
    success_firestore = True
    success_sqlite = True

    # --- Deletar no Firestore ---
    if _USE_FIRESTORE_AS_PRIMARY and db_utils.db_firestore: # Use db_utils.db_firestore
        notification_history_ref = db_utils.get_firestore_collection_ref("followup_notification_history") # Use db_utils.get_firestore_collection_ref
        if notification_history_ref:
            try:
                doc_ref = notification_history_ref.document(str(history_entry_id))
                doc = doc_ref.get()
                if doc.exists:
                    doc_ref.delete()
                    logger.info(f"Entrada do histórico ID {history_entry_id} excluída permanentemente por {deleted_by} do Firestore.")
                else:
                    logger.warning(f"Entrada do histórico ID {history_entry_id} não encontrada no Firestore para exclusão.")
            except Exception as e:
                logger.exception(f"Erro ao excluir permanentemente a entrada do histórico ID {history_entry_id} do Firestore.")
                success_firestore = False
        else:
            success_firestore = False

    # --- Deletar no SQLite (Backup Físico) ---
    if _SQLITE_ENABLED:
        conn_sqlite = conectar_followup_db()
        if conn_sqlite:
            try:
                cursor_sqlite = conn_sqlite.cursor()
                cursor_sqlite.execute("DELETE FROM notification_history WHERE id = ?", (history_entry_id,))
                conn_sqlite.commit()
                logger.info(f"Entrada do histórico ID {history_entry_id} excluída permanentemente por {deleted_by} do SQLite.")
            except Exception as e:
                logger.exception(f"Erro ao excluir permanentemente a entrada do histórico ID {history_entry_id} do SQLite.")
                conn_sqlite.rollback()
                success_sqlite = False
            finally:
                if conn_sqlite:
                    conn_sqlite.close()
        else:
            success_sqlite = False
    
    # Invalida o cache das funções que obtêm notificações excluídas.
    get_deleted_notifications.clear()

    return success_firestore and success_sqlite


@st.cache_data(ttl=3600)
def get_all_users_from_db():
    """
    Obtém todos os usuários do banco de dados principal através de db_utils.
    """
    try:
        # Chama a função get_all_users do módulo db_utils
        users = db_utils.get_all_users()
        logger.info(f"Obtidos {len(users)} usuários via db_utils.get_all_users().")
        return users
    except AttributeError:
        logger.error("db_utils.get_all_users() não encontrada. Verifique o módulo db_utils.")
        # Fallback para usuários simulados se a função não existir em db_utils
        return [
            {'id': 1, 'username': 'admin'},
            {'id': 2, 'username': 'usuario_mock' if not os.getenv('IS_STREAMLIT_CLOUD') else 'usuario_streamlit'},
        ]
    except Exception as e:
        logger.exception("Erro inesperado ao obter usuários via db_utils.")
        return [
            {'id': 1, 'username': 'admin'},
            {'id': 2, 'username': 'usuario_mock' if not os.getenv('IS_STREAMLIT_CLOUD') else 'usuario_streamlit'},
        ]

