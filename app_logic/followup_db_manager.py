# -*- coding: utf-8 -*-
import os
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
import json
import streamlit as st # Adicionado para usar o cache do Streamlit

# Configuração de logging para o módulo de banco de dados
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING) # Define o nível de logging para WARNING
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Importar o SDK do Google Cloud Firestore
try:
    import google.cloud.firestore
except ImportError:
    logger.critical("ERRO CRÍTICO: O módulo 'google.cloud.firestore' não foi encontrado. "
                    "Certifique-se de que a biblioteca esteja instalada no seu ambiente Python.")
    raise RuntimeError("Dependência 'google.cloud.firestore' não encontrada.")

# Importar db_utils para obter a lista de usuários e configurações globais do DB (Firestore)
try:
    import db_utils
    if not hasattr(db_utils, 'get_firestore_collection_ref') or \
       not hasattr(db_utils, 'db_firestore') or \
       not hasattr(db_utils, '_USE_FIRESTORE_AS_PRIMARY') or \
       not hasattr(db_utils, 'get_all_users'): # Removido _SQLITE_ENABLED e get_sqlite_db_path
        logger.warning("Módulo 'db_utils' real não contém todas as funções/variáveis esperadas para Firestore. Usando MockDbUtils.")
        raise ImportError # Força o uso do MockDbUtils
except ImportError:
    logger.critical("ERRO CRÍTICO: O módulo 'db_utils' não foi encontrado ou está incompleto. "
                    "followup_db_manager.py depende de db_utils.py. "
                    "Verifique se db_utils.py está no diretório 'app_logic' "
                    "e se as dependências estão instaladas. O aplicativo pode não funcionar corretamente.")
    class MockDbUtils:
        _USE_FIRESTORE_AS_PRIMARY = True # Assume Firestore como primário no mock
        db_firestore = None # No Firestore client in mock
        
        def get_firestore_collection_ref(self, collection_name: str):
            logger.warning(f"MockDbUtils: Tentativa de obter referência Firestore para '{collection_name}'. Firestore desabilitado (Mock).")
            return None
        
        def get_all_users(self):
            return [
                {'id': 1, 'username': 'admin'},
                {'id': 2, 'username': 'usuario_mock'},
            ]

    db_utils = MockDbUtils() # Atribui a classe mock
except Exception as e:
    logger.error(f"Erro inesperado ao importar ou inicializar 'db_utils': {e}. Usando MockDbUtils.")
    class MockDbUtils:
        _USE_FIRESTORE_AS_PRIMARY = True
        db_firestore = None
        
        def get_firestore_collection_ref(self, collection_name: str):
            logger.warning(f"MockDbUtils: Tentativa de obter referência Firestore para '{collection_name}'. Firestore desabilitado (Mock).")
            return None
        
        def get_all_users(self):
            return [
                {'id': 1, 'username': 'admin'},
                {'id': 2, 'username': 'usuario_mock'},
            ]
    db_utils = MockDbUtils()

# Agora, acesse as variáveis e funções globais diretamente de db_utils
_USE_FIRESTORE_AS_PRIMARY = db_utils._USE_FIRESTORE_AS_PRIMARY

# Diagnóstico inicial do status do Firestore
logger.info(f"[DB_MANAGER_INIT] _USE_FIRESTORE_AS_PRIMARY: {_USE_FIRESTORE_AS_PRIMARY}")
logger.info(f"[DB_MANAGER_INIT] db_utils.db_firestore is None: {db_utils.db_firestore is None}")

# Lista fixa de opções de status
STATUS_OPTIONS = ["", "Processo Criado","Verificando","Em produção","Pré Embarque","Embarcado","Chegada Recinto","Registrado","Liberado","Agendado","Chegada Pichau","Encerrado", "Limbo Saldo", "Limbo Consolidado"]


def _criar_colecoes_firestore():
    """
    Tenta criar as coleções iniciais no Firestore para o Follow-up, se não existirem.
    Coleções são criadas automaticamente no primeiro write, esta função apenas verifica acessibilidade.
    """
    if db_utils.db_firestore is None:
        logger.error("_criar_colecoes_firestore: Firestore client não inicializado em db_utils. Não é possível criar coleções Firestore.")
        return False

    success = True
    collections_to_check = [
        "followup_processos",
        "followup_historico_processos",
        "followup_process_items",
        "followup_notifications",
        "followup_notification_history"
    ]

    for col_name in collections_to_check:
        try:
            col_ref = db_utils.get_firestore_collection_ref(col_name)
            if col_ref:
                logger.debug(f"_criar_colecoes_firestore: Referência para coleção Firestore '{col_name}' obtida com sucesso.")
            else:
                logger.error(f"_criar_colecoes_firestore: Falha ao obter referência para a coleção Firestore '{col_name}'.")
                success = False
        except Exception as e:
            logger.exception(f"_criar_colecoes_firestore: Erro ao verificar/criar coleção Firestore '{col_name}': {e}")
            success = False
    return success


def criar_tabela_followup() -> bool:
    """
    Gerencia a criação de coleções iniciais no Firestore para o Follow-up.
    Retorna True se a inicialização do DB for bem-sucedida, False caso contrário.
    """
    logger.info("[criar_tabela_followup] Iniciando verificação/criação de coleções para Follow-up (Firestore).")
    
    if db_utils.db_firestore is None:
        logger.error("criar_tabela_followup: Firestore cliente não inicializado. Não é possível criar coleções.")
        return False
        
    logger.info("[criar_tabela_followup] Firestore é o banco de dados primário. Tentando criar coleções Firestore.")
    if _criar_colecoes_firestore():
        logger.info("[criar_tabela_followup] Coleções Firestore de Follow-up verificadas/criadas com sucesso.")
        return True
    else:
        logger.error("[criar_tabela_followup] Falha ao verificar/criar coleções Firestore de Follow-up.")
        return False


# --- Funções para manipulação de ITENS DE PROCESSO ---

@st.cache_data(ttl=3600)
def obter_ultimo_processo_id() -> Optional[str]:
    """
    Obtém o ID do último processo inserido no Firestore.
    No Firestore, IDs não são sequenciais numéricos. Esta função pode retornar
    o ID do documento mais recente com base em um campo de timestamp,
    ou None se o conceito de "último ID" sequencial não for aplicável.
    Para IDs que são o "Processo_Novo", você pode apenas usar o próprio Processo_Novo.
    Aqui, retorna None como não há um "último ID" numérico sequencial.
    """
    logger.warning("obter_ultimo_processo_id: Não é diretamente suportado para IDs sequenciais no Firestore. Retornando None.")
    return None

def deletar_itens_processo(processo_id: str) -> bool: # processo_id é string (Firestore Document ID)
    """Deleta todos os itens associados a um processo específico no Firestore."""
    logger.info(f"deletar_itens_processo: Chamado para o processo ID: '{processo_id}' no Firestore.")

    process_items_ref = db_utils.get_firestore_collection_ref("followup_process_items")
    if not process_items_ref:
        logger.warning("deletar_itens_processo: Referência da coleção 'followup_process_items' Firestore não disponível.")
        return False

    try:
        batch = db_utils.db_firestore.batch()
        # Busca itens onde 'processo_id' (campo dentro do documento) é igual ao ID do processo pai
        docs_to_delete = process_items_ref.where("processo_id", "==", processo_id).stream()
        deleted_count = 0
        for doc in docs_to_delete:
            batch.delete(doc.reference)
            deleted_count += 1
        batch.commit()
        logger.info(f"deletar_itens_processo: {deleted_count} itens do processo ID '{processo_id}' deletados com sucesso no Firestore.")
        return True
    except Exception as e:
        logger.error(f"deletar_itens_processo: Erro ao deletar itens do processo ID '{processo_id}' no Firestore: {e}")
        return False

def inserir_item_processo(
    processo_id: str, # processo_id é string (Firestore Document ID)
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
    """Insere um novo item associado a um processo no Firestore."""
    logger.info(f"inserir_item_processo: Chamado para o processo ID: '{processo_id}' no Firestore.")

    item_data = {
        "processo_id": processo_id, # Chave estrangeira para o documento de processo
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
    logger.debug(f"inserir_item_processo: Dados do item a serem inseridos: {item_data}")

    process_items_ref = db_utils.get_firestore_collection_ref("followup_process_items")
    if not process_items_ref:
        logger.warning("inserir_item_processo: Referência da coleção 'followup_process_items' Firestore não disponível.")
        return False
    
    try:
        doc_ref = process_items_ref.add(item_data) # Firestore gera ID automaticamente
        logger.info(f"inserir_item_processo: Item inserido com sucesso para o processo ID '{processo_id}' no Firestore. Doc ID: {doc_ref[1].id}")
        return True
    except Exception as e:
        logger.error(f"inserir_item_processo: Erro ao inserir item para o processo ID '{processo_id}' no Firestore: {e}")
        return False


@st.cache_data(ttl=3600)
def obter_itens_processo(processo_id: str) -> List[Dict[str, Any]]: # processo_id é string (Firestore Document ID)
    """Obtém todos os itens associados a um processo específico do Firestore."""
    logger.info(f"obter_itens_processo: Chamado para o processo ID: '{processo_id}' no Firestore.")
    
    process_items_ref = db_utils.get_firestore_collection_ref("followup_process_items")
    if not process_items_ref: 
        logger.warning("obter_itens_processo: Referência da coleção 'followup_process_items' Firestore não disponível.")
        return []
    
    try:
        items = []
        # Consulta itens onde 'processo_id' (campo dentro do documento) é igual ao ID do documento pai
        docs = process_items_ref.where("processo_id", "==", processo_id).stream()
        fetched_count = 0
        for doc in docs:
            item_data = doc.to_dict()
            item_data['id'] = doc.id # Adiciona o ID do documento Firestore
            items.append(item_data)
            fetched_count += 1
        logger.info(f"obter_itens_processo: Obtidos {fetched_count} itens do Firestore para o processo ID '{processo_id}'.")
        return items
    except Exception as e:
        logger.error(f"obter_itens_processo: Erro ao obter itens do processo ID '{processo_id}' do Firestore: {e}")
        return []


@st.cache_data(ttl=3600)
def get_all_process_items_with_process_ref() -> List[Dict[str, Any]]:
    """
    Obtém todos os itens de processo juntamente com a referência do processo (Processo_Novo)
    e o Status_Geral ao qual pertencem do Firestore.
    """
    logger.info("get_all_process_items_with_process_ref: Chamado (Firestore).")
    process_items_ref = db_utils.get_firestore_collection_ref("followup_process_items")
    processos_ref = db_utils.get_firestore_collection_ref("followup_processos")
    if not process_items_ref or not processos_ref: 
        logger.warning("get_all_process_items_with_process_ref: Referências de coleções Firestore não disponíveis. Retornando lista vazia.")
        return []
    
    try:
        items_with_process_info = []
        all_items_docs = process_items_ref.stream() # Obtém todos os itens
        
        for item_doc in all_items_docs:
            item_data = item_doc.to_dict()
            item_data['id'] = item_doc.id # Garante que o ID do documento Firestore seja incluído
            processo_novo_id = item_data.get('processo_id') # Este é o Processo_Novo do pai

            if processo_novo_id:
                processo_doc = processos_ref.document(processo_novo_id).get() # Busca o processo pai
                if processo_doc.exists:
                    processo_data = processo_doc.to_dict()
                    item_data['Processo_Novo'] = processo_data.get('Processo_Novo')
                    item_data['Status_Geral'] = processo_data.get('Status_Geral')
                    logger.debug(f"get_all_process_items_with_process_ref: Item {item_doc.id} vinculado ao Processo_Novo: {processo_novo_id}, Status_Geral: {processo_data.get('Status_Geral')}")
                else:
                    logger.warning(f"get_all_process_items_with_process_ref: Processo pai '{processo_novo_id}' não encontrado para o item ID {item_doc.id}. Verifique a consistência dos dados.")
                    item_data['Processo_Novo'] = None 
                    item_data['Status_Geral'] = None 
            else:
                logger.warning(f"get_all_process_items_with_process_ref: Item ID {item_doc.id} não possui 'processo_id'.")
                item_data['Processo_Novo'] = None
                item_data['Status_Geral'] = None
            items_with_process_info.append(item_data)
        
        logger.info(f"get_all_process_items_with_process_ref: Obtidos {len(items_with_process_info)} itens com informações de processo do Firestore.")
        return items_with_process_info
    except Exception as e:
        logger.error(f"get_all_process_items_with_process_ref: Erro ao obter todos os itens de processo com referência e status do processo do Firestore: {e}")
        return []


@st.cache_data(ttl=3600)
def obter_processos_filtrados(status_filtro="Todos", termos_pesquisa=None):
    """Busca processos do Firestore aplicando filtros de status e termos de pesquisa."""
    logger.info(f"obter_processos_filtrados: status_filtro={status_filtro}, termos_pesquisa={termos_pesquisa} (Firestore).")
    
    processos_ref = db_utils.get_firestore_collection_ref("followup_processos")
    if not processos_ref:
        logger.error("obter_processos_filtrados: Coleção 'followup_processos' Firestore não disponível.")
        return []
    
    try:
        import google.cloud.firestore
        query_firestore = processos_ref
        
        # Aplica filtro de status no Firestore
        if status_filtro == "Arquivados":
            query_firestore = query_firestore.where("Status_Arquivado", "==", "Arquivado")
        elif status_filtro != "Todos":
            query_firestore = query_firestore.where("Status_Geral", "==", status_filtro)
            query_firestore = query_firestore.where("Status_Arquivado", "in", [None, "Não Arquivado"]) # Exclui explicitamente arquivados
        else: # "Todos", exclui arquivados por padrão
            query_firestore = query_firestore.where("Status_Arquivado", "in", [None, "Não Arquivado"])

        # Aplica filtros de data no Firestore
        if termos_pesquisa:
            if "ETA_Recinto_Start" in termos_pesquisa and termos_pesquisa["ETA_Recinto_Start"]:
                query_firestore = query_firestore.where("ETA_Recinto", ">=", termos_pesquisa["ETA_Recinto_Start"])
            if "ETA_Recinto_End" in termos_pesquisa and termos_pesquisa["ETA_Recinto_End"]:
                query_firestore = query_firestore.where("ETA_Recinto", "<=", termos_pesquisa["ETA_Recinto_End"])
            
            if "Data_Registro_Start" in termos_pesquisa and termos_pesquisa["Data_Registro_Start"]:
                query_firestore = query_firestore.where("Data_Registro", ">=", termos_pesquisa["Data_Registro_Start"])
            if "Data_Registro_End" in termos_pesquisa and termos_pesquisa["Data_Registro_End"]:
                query_firestore = query_firestore.where("Data_Registro", "<=", termos_pesquisa["Data_Registro_End"])
        
        # Ordena para consistência e requisitos de índice do Firestore
        query_firestore = query_firestore.order_by("Status_Geral").order_by("Modal")

        docs = query_firestore.stream() # Obtém documentos com base nos filtros iniciais do Firestore

        processos = []
        for doc in docs:
            data = doc.to_dict()
            data['id'] = doc.id # Adiciona o ID do documento Firestore como 'id'
            processos.append(data)
        
        logger.debug(f"obter_processos_filtrados (Firestore): Fetched {len(processos)} documents from Firestore before in-memory filtering.")

        # Filtragem em memória para termos de texto (Firestore não suporta LIKE)
        if termos_pesquisa:
            filtered_processes_in_memory = []
            for proc in processos:
                match = True
                for col, termo in termos_pesquisa.items():
                    # Ignora filtros de data já aplicados na query do Firestore
                    if col in ["ETA_Recinto_Start", "ETA_Recinto_End", "Data_Registro_Start", "Data_Registro_End"]:
                        continue
                    
                    # Aplica pesquisa de texto (case-insensitive)
                    if termo and col in proc and str(proc[col]).lower().find(str(termo).lower()) == -1:
                        match = False
                        break
                if match:
                    filtered_processes_in_memory.append(proc)
            processos = filtered_processes_in_memory
            logger.debug(f"obter_processos_filtrados (Firestore): {len(processos)} documents remaining after in-memory text filtering.")

        logger.info(f"obter_processos_filtrados: Obtidos {len(processos)} processos do Firestore com filtros/pesquisa (resultado final).")
        return processos
    except Exception as e:
        logger.exception(f"obter_processos_filtrados: Erro ao obter processos filtrados do Firestore: {e}")
        return []


@st.cache_data(ttl=3600)
def obter_todos_processos():
    """Busca todos os processos do Firestore."""
    logger.info("obter_todos_processos: Chamado (Firestore).")
    processos_ref = db_utils.get_firestore_collection_ref("followup_processos")
    if not processos_ref: 
        logger.warning("obter_todos_processos: Coleção 'followup_processos' Firestore não disponível.")
        return []
    
    try:
        docs = processos_ref.stream()
        processes = []
        for doc in docs:
            data = doc.to_dict()
            data['id'] = doc.id # Adiciona o ID do documento Firestore como 'id'
            processes.append(data)
        logger.info(f"obter_todos_processos: Obtidos {len(processes)} processos do Firestore.")
        return processes
    except Exception as e:
        logger.error(f"obter_todos_processos: Erro ao obter todos os processos do Firestore: {e}")
        return []


@st.cache_data(ttl=3600)
def obter_processo_por_id(processo_id: str):
    """Busca um processo específico pelo ID (Processo_Novo no Firestore)."""
    logger.info(f"obter_processo_por_id: Chamado para o processo ID: '{processo_id}' (Firestore).")
    processos_ref = db_utils.get_firestore_collection_ref("followup_processos")
    if not processos_ref: 
        logger.warning("obter_processo_por_id: Coleção 'followup_processos' Firestore não disponível.")
        return None
    
    try:
        # process_id aqui é o Processo_Novo (string) que é o ID do documento no Firestore
        doc = processos_ref.document(str(processo_id)).get() 
        if doc.exists:
            data = doc.to_dict()
            data['id'] = doc.id # Adiciona o ID do documento Firestore (que é Processo_Novo)
            logger.info(f"obter_processo_por_id: Processo com ID '{processo_id}' encontrado no Firestore.")
            return data
        logger.info(f"obter_processo_por_id: Processo com ID '{processo_id}' NÃO encontrado no Firestore.")
        return None
    except Exception as e:
        logger.error(f"obter_processo_por_id: Erro ao obter processo com ID '{processo_id}' do Firestore: {e}")
        return None


@st.cache_data(ttl=3600)
def obter_processo_by_processo_novo(processo_novo: str):
    """Busca um processo específico pela sua referência (Processo_Novo no Firestore)."""
    logger.info(f"obter_processo_by_processo_novo: Chamado para Processo_Novo: '{processo_novo}' (Firestore).")
    processos_ref = db_utils.get_firestore_collection_ref("followup_processos")
    if not processos_ref: 
        logger.warning("obter_processo_by_processo_novo: Coleção 'followup_processos' Firestore não disponível.")
        return None
    
    try:
        # No Firestore, o Processo_Novo é o próprio ID do documento
        doc = processos_ref.document(processo_novo).get()
        if doc.exists:
            data = doc.to_dict()
            data['id'] = doc.id # Adiciona o ID do documento Firestore (que é Processo_Novo)
            logger.info(f"obter_processo_by_processo_novo: Processo com Processo_Novo '{processo_novo}' encontrado no Firestore.")
            return data
        logger.info(f"obter_processo_by_processo_novo: Processo com Processo_Novo '{processo_novo}' NÃO encontrado no Firestore.")
        return None
    except Exception as e:
        logger.error(f"obter_processo_by_processo_novo: Erro ao obter processo com Processo_Novo '{processo_novo}' do Firestore: {e}")
        return None


def inserir_processo(dados: Dict[str, Any]) -> bool:
    """Insere um novo processo no Firestore."""
    logger.info(f"inserir_processo: Chamado para dados: {dados.get('Processo_Novo')} (Firestore).")

    processos_ref_firestore = db_utils.get_firestore_collection_ref("followup_processos")
    if not processos_ref_firestore:
        logger.warning("inserir_processo: Referência da coleção 'followup_processos' Firestore não disponível.")
        return False

    processo_novo_id = dados.get("Processo_Novo")
    if not processo_novo_id:
        logger.error("inserir_processo: Campo 'Processo_Novo' é obrigatório para inserir no Firestore.")
        return False
    
    try:
        # Verifica se o documento já existe para evitar sobrescrever acidentalmente
        doc_ref = processos_ref_firestore.document(processo_novo_id)
        if doc_ref.get().exists:
            logger.warning(f"inserir_processo: Processo '{processo_novo_id}' já existe no Firestore. Use 'atualizar_processo' ou 'upsert_processo'.")
            return False

        # O ID do documento Firestore é 'Processo_Novo'
        doc_ref.set(dados)
        logger.info(f"inserir_processo: Novo processo '{processo_novo_id}' inserido com sucesso no Firestore.")
        return True
    except Exception as e:
        logger.exception(f"inserir_processo: Erro ao inserir novo processo no Firestore: {e}")
        return False
    finally:
        obter_processos_filtrados.clear()
        obter_todos_processos.clear()
        obter_processo_por_id.clear()
        obter_processo_by_processo_novo.clear()
        obter_status_gerais_distintos.clear()
        obter_nomes_colunas_db.clear()


def atualizar_processo(processo_id: str, dados: Dict[str, Any]) -> bool:
    """Atualiza um processo existente no Firestore."""
    logger.info(f"atualizar_processo: Chamado para o processo ID: '{processo_id}' (Firestore).")
    processos_ref_firestore = db_utils.get_firestore_collection_ref("followup_processos")
    if not processos_ref_firestore:
        logger.warning("atualizar_processo: Referência da coleção 'followup_processos' Firestore não disponível.")
        return False
    
    try:
        doc_ref = processos_ref_firestore.document(processo_id)
        doc_ref.update(dados)
        logger.info(f"atualizar_processo: Processo com ID '{processo_id}' atualizado com sucesso no Firestore.")
        return True
    except Exception as e:
        logger.exception(f"atualizar_processo: Erro ao atualizar processo com ID '{processo_id}' no Firestore: {e}")
        return False
    finally:
        obter_processos_filtrados.clear()
        obter_todos_processos.clear()
        obter_processo_por_id.clear()
        obter_processo_by_processo_novo.clear()
        obter_status_gerais_distintos.clear()
        obter_nomes_colunas_db.clear()


def upsert_processo(dados: Dict[str, Any]) -> bool:
    """
    Insere ou atualiza um processo no Firestore.
    Usa 'Processo_Novo' como chave de identificação para upsert (ID do documento).
    """
    processo_novo = dados.get("Processo_Novo")
    if not processo_novo:
        logger.error("upsert_processo: Campo 'Processo_Novo' é obrigatório para upsert de processo (Firestore).")
        return False

    logger.info(f"upsert_processo: Chamado para Processo_Novo: '{processo_novo}' (Firestore).")
    processos_ref_firestore = db_utils.get_firestore_collection_ref("followup_processos")
    if not processos_ref_firestore:
        logger.warning("upsert_processo: Referência da coleção 'followup_processos' Firestore não disponível.")
        return False
    
    try:
        # Usa set com merge=True para atualizar ou criar um documento com o ID especificado
        processos_ref_firestore.document(processo_novo).set(dados, merge=True)
        logger.info(f"upsert_processo: Processo '{processo_novo}' upserted (inserido/atualizado) com sucesso no Firestore.")
        return True
    except Exception as e:
        logger.exception(f"upsert_processo: Erro ao fazer upsert do processo '{processo_novo}' no Firestore: {e}")
        return False
    finally:
        obter_processos_filtrados.clear()
        obter_todos_processos.clear()
        obter_processo_por_id.clear()
        obter_processo_by_processo_novo.clear()
        obter_status_gerais_distintos.clear()
        obter_nomes_colunas_db.clear()


def excluir_processo(processo_id: str) -> bool:
    """Exclui um processo e seus dados relacionados (histórico, itens) do Firestore."""
    logger.info(f"excluir_processo: Chamado para o processo ID: '{processo_id}' (Firestore).")

    processos_ref_firestore = db_utils.get_firestore_collection_ref("followup_processos")
    history_ref_firestore = db_utils.get_firestore_collection_ref("followup_historico_processos")
    items_ref_firestore = db_utils.get_firestore_collection_ref("followup_process_items")

    if not processos_ref_firestore or not history_ref_firestore or not items_ref_firestore:
        logger.warning("excluir_processo: Uma ou mais referências de coleção Firestore não disponíveis.")
        return False

    try:
        batch = db_utils.db_firestore.batch()
        
        # 1. Excluir o documento do processo principal
        processo_doc_ref = processos_ref_firestore.document(processo_id)
        batch.delete(processo_doc_ref)
        
        # 2. Excluir documentos de histórico relacionados (usando 'processo_id' como campo)
        history_docs = history_ref_firestore.where("processo_id", "==", processo_id).stream()
        for doc in history_docs:
            batch.delete(doc.reference)
        
        # 3. Excluir documentos de itens relacionados (usando 'processo_id' como campo)
        items_docs = items_ref_firestore.where("processo_id", "==", processo_id).stream()
        for doc in items_docs:
            batch.delete(doc.reference)
        
        batch.commit()
        logger.info(f"excluir_processo: Processo ID '{processo_id}' e dados relacionados excluídos com sucesso do Firestore.")
        return True
    except Exception as e:
        logger.exception(f"excluir_processo: Erro ao excluir processo ID '{processo_id}' e dados relacionados do Firestore: {e}")
        return False
    finally:
        obter_processos_filtrados.clear()
        obter_todos_processos.clear()
        obter_processo_por_id.clear()
        obter_processo_by_processo_novo.clear()
        obter_status_gerais_distintos.clear()


def arquivar_processo(processo_id: str) -> bool:
    """Marca um processo como arquivado no Firestore."""
    logger.info(f"arquivar_processo: Chamado para o processo ID: '{processo_id}' (Firestore).")
    processos_ref_firestore = db_utils.get_firestore_collection_ref("followup_processos")
    if not processos_ref_firestore:
        logger.warning("arquivar_processo: Referência da coleção 'followup_processos' Firestore não disponível.")
        return False
    try:
        processos_ref_firestore.document(processo_id).update({"Status_Arquivado": "Arquivado"})
        logger.info(f"arquivar_processo: Processo com ID '{processo_id}' arquivado com sucesso no Firestore.")
        return True
    except Exception as e:
        logger.error(f"arquivar_processo: Erro ao arquivar processo com ID '{processo_id}' no Firestore: {e}")
        return False
    finally:
        obter_processos_filtrados.clear()
        obter_todos_processos.clear()
        obter_processo_por_id.clear()
        obter_processo_by_processo_novo.clear()


def desarquivar_processo(processo_id: str) -> bool:
    """Desmarca um processo como arquivado no Firestore."""
    logger.info(f"desarquivar_processo: Chamado para o processo ID: '{processo_id}' (Firestore).")
    processos_ref_firestore = db_utils.get_firestore_collection_ref("followup_processos")
    if not processos_ref_firestore:
        logger.warning("desarquivar_processo: Referência da coleção 'followup_processos' Firestore não disponível.")
        return False
    try:
        # google.cloud.firestore.DELETE_FIELD remove o campo do documento
        processos_ref_firestore.document(processo_id).update({"Status_Arquivado": google.cloud.firestore.DELETE_FIELD})
        logger.info(f"desarquivar_processo: Processo com ID '{processo_id}' desarquivado com sucesso no Firestore.")
        return True
    except Exception as e:
        logger.error(f"desarquivar_processo: Erro ao desarquivar processo com ID '{processo_id}' no Firestore: {e}")
        return False
    finally:
        obter_processos_filtrados.clear()
        obter_todos_processos.clear()
        obter_processo_por_id.clear()
        obter_processo_by_processo_novo.clear()


def atualizar_status_processo(processo_id: str, novo_status: Optional[str], username: Optional[str] = "Desconhecido") -> bool:
    """Atualiza o Status_Geral de um processo específico no Firestore."""
    logger.info(f"atualizar_status_processo: Chamado para o processo ID: '{processo_id}', novo status: '{novo_status}' (Firestore).")
    
    processos_ref_firestore = db_utils.get_firestore_collection_ref("followup_processos")
    if not processos_ref_firestore:
        logger.warning("atualizar_status_processo: Referência da coleção 'followup_processos' Firestore não disponível.")
        return False

    original_status = None
    try:
        original_process_doc = processos_ref_firestore.document(processo_id).get()
        if original_process_doc.exists:
            original_status = original_process_doc.to_dict().get('Status_Geral')

        processos_ref_firestore.document(processo_id).update({"Status_Geral": novo_status})
        logger.info(f"atualizar_status_processo: Status do processo ID '{processo_id}' atualizado para '{novo_status}' no Firestore.")
        
        # Inserir histórico no Firestore
        inserir_historico_processo(processo_id, "Status_Geral", original_status, novo_status, username, db_type="Firestore")
        return True
    except Exception as e:
        logger.exception(f"atualizar_status_processo: Erro ao atualizar status do processo ID '{processo_id}' no Firestore: {e}")
        return False
    finally:
        obter_processos_filtrados.clear()
        obter_todos_processos.clear()
        obter_processo_por_id.clear()
        obter_processo_by_processo_novo.clear()
        obter_status_gerais_distintos.clear()


def inserir_historico_processo(processo_id: str, field_name: str, old_value: Optional[str], new_value: Optional[str], username: Optional[str], db_type: str):
    """Insere um registro na tabela historico_processos no Firestore."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    history_data = {
        "processo_id": processo_id, # ID do processo Firestore
        "campo_alterado": field_name,
        "valor_antigo": str(old_value) if old_value is not None else "Vazio",
        "valor_novo": str(new_value) if new_value is not None else "Vazio",
        "timestamp": timestamp,
        "usuario": username if username is not None else "Desconhecido"
    }
    logger.info(f"inserir_historico_processo: Chamado para processo ID: '{processo_id}', campo: '{field_name}' (Firestore).")

    history_ref = db_utils.get_firestore_collection_ref("followup_historico_processos")
    if not history_ref:
        logger.warning("inserir_historico_processo: Referência da coleção 'followup_historico_processos' Firestore não disponível.")
        return False
    
    try:
        history_ref.add(history_data)
        logger.debug(f"inserir_historico_processo: Histórico registrado para processo '{processo_id}', campo '{field_name}' por '{username}' no Firestore.")
        return True
    except Exception as e:
        logger.exception(f"inserir_historico_processo: Erro ao inserir histórico para processo '{processo_id}', campo '{field_name}' por '{username}' no Firestore.")
        return False


@st.cache_data(ttl=3600)
def obter_historico_processo(processo_id: str):
    """Busca o histórico de alterações para um processo específico do Firestore."""
    logger.info(f"obter_historico_processo: Chamado para o processo ID: '{processo_id}' (Firestore).")
    
    history_ref = db_utils.get_firestore_collection_ref("followup_historico_processos")
    if not history_ref: 
        logger.warning("obter_historico_processo: Coleção 'followup_historico_processos' Firestore não disponível.")
        return []
    
    try:
        history = []
        # Filtra por 'processo_id' (campo no documento de histórico)
        docs = history_ref.where("processo_id", "==", processo_id).order_by("timestamp").stream()
        fetched_count = 0
        for doc in docs:
            data = doc.to_dict()
            history.append({
                "campo_alterado": data.get("campo_alterado"),
                "valor_antigo": data.get("valor_antigo"),
                "valor_novo": data.get("valor_novo"),
                "timestamp": data.get("timestamp"),
                "usuario": data.get("usuario")
            })
            fetched_count += 1
        logger.info(f"obter_historico_processo: Obtidos {fetched_count} registros de histórico do Firestore para processo ID '{processo_id}'.")
        return history
    except Exception as e:
        logger.error(f"obter_historico_processo: Erro ao obter histórico para processo ID '{processo_id}' do Firestore: {e}")
        return []


@st.cache_data(ttl=3600)
def obter_status_gerais_distintos() -> List[str]:
    """Busca todos os valores distintos da coluna Status_Geral do Firestore."""
    logger.info(f"obter_status_gerais_distintos: Chamado (Firestore).")
    processos_ref = db_utils.get_firestore_collection_ref("followup_processos")
    if not processos_ref: 
        logger.warning("obter_status_gerais_distintos: Coleção 'followup_processos' Firestore não disponível.")
        return []
    
    try:
        status_list = []
        # Consulta todos os documentos e extrai o campo 'Status_Geral'
        for doc in processos_ref.stream():
            status = doc.get("Status_Geral")
            if status is not None and status != "":
                status_list.append(status)
        
        status_distinct = sorted(list(set(status_list)))
        logger.info(f"obter_status_gerais_distintos: Obtidos {len(status_distinct)} status gerais distintos do Firestore.")
        return status_distinct
    except Exception as e:
        logger.error(f"obter_status_gerais_distintos: Erro ao obter status gerais distintos do Firestore: {e}")
        return []

@st.cache_data(ttl=3600)
def obter_nomes_colunas_db() -> List[str]:
    """Retorna uma lista com os nomes das colunas (campos) esperados para um processo no Firestore."""
    logger.info("obter_nomes_colunas_db: Chamado (Firestore).")
    # Para Firestore, não há um "pragma table_info". Definimos as colunas esperadas.
    # Esta lista deve corresponder aos campos que você está salvando nos documentos 'followup_processos'.
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
        'ETA_Recinto', 'Data_Registro', 'DI_ID_Vinculada',
        # NOVOS CAMPOS DE ARQUIVO
        'Nome_do_arquivo',
        'Tipo_do_arquivo',
        'Conteudo_do_arquivo'
    ]


# --- Funções de gerenciamento de Notificações ---

def add_notification(message: str, target_user: str, created_by: str, status: str = 'active') -> bool:
    """Adiciona uma nova notificação no Firestore."""
    logger.info(f"add_notification: Chamado. Mensagem: '{message}', Para: '{target_user}' (Firestore).")

    notifications_ref = db_utils.get_firestore_collection_ref("followup_notifications")
    if not notifications_ref:
        logger.warning("add_notification: Referência da coleção 'followup_notifications' Firestore não disponível.")
        return False

    notification_data = {
        "message": message,
        "target_users": target_user,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "created_by": created_by,
        "status": status
    }

    try:
        notifications_ref.add(notification_data)
        logger.info(f"add_notification: Notificação adicionada por '{created_by}' para '{target_user}' no Firestore.")
        return True
    except Exception as e:
        logger.exception("add_notification: Erro ao adicionar notificação no Firestore.")
        return False
    finally:
        get_active_notifications.clear()
        get_deleted_notifications.clear()


@st.cache_data(ttl=3600)
def get_active_notifications(username: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Busca notificações ativas no Firestore.
    """
    logger.info(f"get_active_notifications: Chamado para usuário: {username} (Firestore).")
    notifications_ref = db_utils.get_firestore_collection_ref("followup_notifications")
    if not notifications_ref: 
        logger.warning("get_active_notifications: Coleção 'followup_notifications' Firestore não disponível.")
        return []
    
    try:
        import google.cloud.firestore
        filtered_notifications = []
        query_firestore = notifications_ref.where("status", "==", "active").order_by("created_at", direction=google.cloud.firestore.Query.DESCENDING)
        all_active_notifications_docs = query_firestore.stream()

        for notif_doc in all_active_notifications_docs:
            notif = notif_doc.to_dict()
            target_user_str = notif.get('target_users')
            
            if username is None: # Se não há usuário especificado, retorna todas as ativas
                filtered_notifications.append(notif)
            elif target_user_str == "ALL": # Notificações para todos
                filtered_notifications.append(notif)
            elif target_user_str == username: # Notificações específicas para o usuário
                filtered_notifications.append(notif)
        
        logger.info(f"get_active_notifications: Obtidas {len(filtered_notifications)} notificações ativas do Firestore para usuário '{username}'.")
        return filtered_notifications
    except Exception as e:
        logger.error(f"get_active_notifications: Erro ao obter notificações ativas do Firestore: {e}")
        return []


def mark_notification_as_deleted(notification_id: str, deleted_by: str) -> bool:
    """Marca uma notificação como excluída e registra no histórico no Firestore."""
    original_message_text = "Mensagem original não encontrada."
    logger.info(f"mark_notification_as_deleted: Chamado para notificação ID: '{notification_id}', por: '{deleted_by}' (Firestore).")

    notifications_ref_firestore = db_utils.get_firestore_collection_ref("followup_notifications")
    notification_history_ref_firestore = db_utils.get_firestore_collection_ref("followup_notification_history")
    
    if not notifications_ref_firestore or not notification_history_ref_firestore:
        logger.warning("mark_notification_as_deleted: Referência de coleção Firestore não disponível.")
        return False

    try:
        # Tenta obter a mensagem original antes de atualizar
        original_notif_doc = notifications_ref_firestore.document(notification_id).get()
        if original_notif_doc.exists:
            original_message_text = original_notif_doc.to_dict().get('message', original_message_text)
        
        notifications_ref_firestore.document(notification_id).update({"status": "deleted"})
        
        action_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        history_data_firestore = {
            "notification_id": notification_id,
            "action": "deleted",
            "action_by": deleted_by,
            "action_at": action_at,
            "original_message": original_message_text
        }
        notification_history_ref_firestore.add(history_data_firestore)
        
        logger.info(f"mark_notification_as_deleted: Notificação ID '{notification_id}' marcada como excluída por '{deleted_by}' no Firestore.")
        return True
    except Exception as e:
        logger.exception(f"mark_notification_as_deleted: Erro ao marcar notificação ID '{notification_id}' como excluída no Firestore.")
        return False
    finally:
        get_active_notifications.clear()
        get_deleted_notifications.clear()

@st.cache_data(ttl=3600)
def get_deleted_notifications() -> List[Dict[str, Any]]:
    """Busca notificações excluídas do histórico do Firestore."""
    logger.info(f"get_deleted_notifications: Chamado (Firestore).")
    notification_history_ref = db_utils.get_firestore_collection_ref("followup_notification_history")
    if not notification_history_ref: 
        logger.warning("get_deleted_notifications: Coleção 'followup_notification_history' Firestore não disponível.")
        return []
    
    try:
        import google.cloud.firestore
        deleted_notifications = []
        docs = notification_history_ref.where("action", "==", "deleted").order_by("action_at", direction=google.cloud.firestore.Query.DESCENDING).stream()
        for doc in docs:
            data = doc.to_dict()
            deleted_notifications.append({
                "history_entry_id": doc.id, # O ID do documento de histórico em si
                "original_notification_id": data.get("notification_id"),
                "original_message": data.get("original_message"),
                "action_at": data.get("action_at"),
                "action_by": data.get("action_by")
            })
        logger.info(f"get_deleted_notifications: Obtidas {len(deleted_notifications)} notificações excluídas do Firestore.")
        return deleted_notifications
    except Exception as e:
        logger.error(f"get_deleted_notifications: Erro ao obter notificações excluídas do Firestore: {e}")
        return []


def restore_notification(notification_id: str, restored_by: str) -> bool:
    """Restaura uma notificação excluída e registra no histórico no Firestore."""
    original_message_text = "Mensagem original não encontrada."
    logger.info(f"restore_notification: Chamado para notificação ID: '{notification_id}', por: '{restored_by}' (Firestore).")

    notifications_ref_firestore = db_utils.get_firestore_collection_ref("followup_notifications")
    notification_history_ref_firestore = db_utils.get_firestore_collection_ref("followup_notification_history")
    
    if not notifications_ref_firestore or not notification_history_ref_firestore:
        logger.warning("restore_notification: Referência de coleção Firestore não disponível.")
        return False

    try:
        # Tenta obter a mensagem original antes de atualizar
        original_notif_doc = notifications_ref_firestore.document(notification_id).get()
        if original_notif_doc.exists:
            original_message_text = original_notif_doc.to_dict().get('message', original_message_text)

        notifications_ref_firestore.document(notification_id).update({"status": "active"})
        
        action_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        history_data_firestore = {
            "notification_id": notification_id,
            "action": "restored",
            "action_by": restored_by,
            "action_at": action_at,
            "original_message": original_message_text
        }
        notification_history_ref_firestore.add(history_data_firestore)
        
        logger.info(f"restore_notification: Notificação ID '{notification_id}' restaurada por '{restored_by}' no Firestore.")
        return True
    except Exception as e:
        logger.exception(f"restore_notification: Erro ao restaurar notificação ID '{notification_id}' no Firestore.")
        return False
    finally:
        get_active_notifications.clear()
        get_deleted_notifications.clear()


def delete_history_entry_permanently(history_entry_id: str, deleted_by: str) -> bool:
    """
    Exclui permanentemente uma entrada do histórico de notificações no Firestore.
    """
    logger.info(f"delete_history_entry_permanently: Chamado para histórico ID: '{history_entry_id}', por: '{deleted_by}' (Firestore).")

    notification_history_ref = db_utils.get_firestore_collection_ref("followup_notification_history")
    if not notification_history_ref:
        logger.warning("delete_history_entry_permanently: Referência da coleção 'followup_notification_history' Firestore não disponível.")
        return False
    
    try:
        doc_ref = notification_history_ref.document(history_entry_id)
        doc = doc_ref.get()
        if doc.exists:
            doc_ref.delete()
            logger.info(f"delete_history_entry_permanently: Entrada do histórico ID '{history_entry_id}' excluída permanentemente por '{deleted_by}' do Firestore.")
            return True
        else:
            logger.warning(f"delete_history_entry_permanently: Entrada do histórico ID '{history_entry_id}' não encontrada no Firestore para exclusão.")
            return False
    except Exception as e:
        logger.exception(f"delete_history_entry_permanently: Erro ao excluir permanentemente a entrada do histórico ID '{history_entry_id}' do Firestore.")
        return False
    finally:
        get_deleted_notifications.clear()


@st.cache_data(ttl=3600)
def get_all_users_from_db() -> List[Dict[str, Any]]:
    """
    Obtém todos os usuários do banco de dados principal através de db_utils.
    """
    logger.info("get_all_users_from_db: Chamado (Firestore).")
    try:
        users = db_utils.get_all_users()
        logger.info(f"get_all_users_from_db: Obtidos {len(users)} usuários via db_utils.get_all_users().")
        return users
    except AttributeError:
        logger.error("get_all_users_from_db: db_utils.get_all_users() não encontrada. Verifique o módulo db_utils. Retornando usuários mock.")
        return [
            {'id': 1, 'username': 'admin'},
            {'id': 2, 'username': 'usuario_mock' if not os.getenv('IS_STREAMLIT_CLOUD') else 'usuario_streamlit'},
        ]
    except Exception as e:
        logger.exception("get_all_users_from_db: Erro inesperado ao obter usuários via db_utils. Retornando usuários mock.")
        return [
            {'id': 1, 'username': 'admin'},
            {'id': 2, 'username': 'usuario_mock' if not os.getenv('IS_STREAMLIT_CLOUD') else 'usuario_streamlit'},
        ]
