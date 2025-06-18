import logging
import os
import hashlib
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime
import json
import re
import pandas as pd
import xml.etree.ElementTree as ET
from google.cloud import firestore
from google.oauth2 import service_account
import streamlit as st

# Configuração do logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

logger.info("db_utils.py: Módulo inicializado.")

_USE_FIRESTORE_AS_PRIMARY = True

logger.info(f"db_utils.py: _USE_FIRESTORE_AS_PRIMARY = {_USE_FIRESTORE_AS_PRIMARY}")

db_firestore: Optional[firestore.Client] = None
try:
    logger.info("db_utils.py: Tentando importar streamlit para credenciais...")
    import streamlit as st
    logger.info("db_utils.py: Streamlit importado com sucesso. Tentando carregar credenciais de st.secrets.")

    if "firestore_service_account" not in st.secrets:
        logger.critical("db_utils.py: Erro CRÍTICO: Chave 'firestore_service_account' NÃO encontrada em st.secrets. Verifique secrets.toml.")
        db_firestore = None
        raise ValueError("Chave 'firestore_service_account' ausente em st.secrets.")

    firestore_secrets = st.secrets["firestore_service_account"]
    logger.debug(f"db_utils.py: Bloco 'firestore_service_account' encontrado em st.secrets. Conteúdo parcial: {list(firestore_secrets.keys())}")

    if "credentials_json" not in firestore_secrets:
        logger.critical("db_utils.py: Erro CRÍTICO: Chave 'credentials_json' NÃO encontrada dentro de 'firestore_service_account' em st.secrets. Verifique secrets.toml.")
        db_firestore = None
        raise ValueError("Chave 'credentials_json' ausente em st.secrets['firestore_service_account'].")

    _firestore_credentials_json = firestore_secrets["credentials_json"]
    logger.debug(f"db_utils.py: Comprimento da string credentials_json lida: {len(_firestore_credentials_json)} caracteres.")

    try:
        credentials_info = json.loads(_firestore_credentials_json)
        logger.debug("db_utils.py: JSON de credenciais PARSEADO com sucesso. Verificando estrutura...")

        if 'project_id' in credentials_info:
            logger.debug(f"db_utils.py: Project ID nas credenciais: {credentials_info['project_id']}")
        if 'client_email' in credentials_info:
            logger.debug(f"db_utils.py: Client Email nas credenciais: {credentials_info['client_email']}")

    except json.JSONDecodeError as jde:
        logger.critical(f"db_utils.py: Erro CRÍTICO de DECODIFICAÇÃO JSON nas credenciais do Firestore: {jde}. Verifique a formatação em secrets.toml, especialmente as quebras de linha e aspas.")
        db_firestore = None
        raise

    _firestore_credentials = service_account.Credentials.from_service_account_info(
        credentials_info
    )
    logger.debug("db_utils.py: Objeto service_account.Credentials criado com sucesso.")

    db_firestore = firestore.Client(credentials=_firestore_credentials, project=_firestore_credentials.project_id)
    logger.info(f"db_utils.py: Firestore client inicializado com sucesso via st.secrets para o projeto: {_firestore_credentials.project_id}.")
except ImportError:
    logger.warning("db_utils.py: Streamlit não encontrado. Tentando inicializar Firestore via variável de ambiente GOOGLE_APPLICATION_CREDENTIALS.")
    try:
        db_firestore = firestore.Client()
        logger.info("db_utils.py: Firestore client inicializado com sucesso via credenciais padrão do ambiente (GOOGLE_APPLICATION_CREDENTIALS).")
    except Exception as e:
        logger.error(f"db_utils.py: Erro CRÍTICO ao inicializar Firestore client sem Streamlit: {e}. Certifique-se de que GOOGLE_APPLICATION_CREDENTIALS esteja configurado corretamente.")
        db_firestore = None
except Exception as e:
    logger.exception(f"db_utils.py: Erro INESPERADO ao inicializar Firestore client com st.secrets (verifique secrets.toml, formatação JSON e permissões): {e}")
    db_firestore = None

if db_firestore is None:
    logger.critical("db_utils.py: Firestore client NÃO PÔDE ser inicializado. As operações de banco de dados no Firestore falharão.")
else:
    logger.info("db_utils.py: Firestore client parece estar pronto para uso.")


_DEFAULT_DB_FOLDER = "data"

_base_path = os.path.dirname(os.path.abspath(__file__))
_app_root_path = os.path.dirname(_base_path) if os.path.basename(_base_path) == 'app_logic' else _base_path

COLLECTIONS_FIRESTORE = {
    "users": "users",
    "xml_declaracoes": "xml_declaracoes",
    "xml_itens": "xml_itens",
    "processo_dados_custo": "processo_dados_custo",
    "processo_contratos_cambio": "processo_contratos_cambio",
    "produtos": "produtos",
    "ncm_items": "ncm_items",
    "pagamentos_container": "pagamentos_container",
    "ncm_impostos_items": "ncm_impostos_items",
    "followup_processos": "followup_processos", # Coleção de processos
    "followup_historico_processos": "followup_historico_processos",
    "followup_process_items": "followup_process_items",
    "followup_notifications": "followup_notifications",
    "followup_notification_history": "followup_notification_history",
    "frete_internacional": "frete_internacional",
    "cotacoes_dolar": "cotacoes_dolar",
}
logger.info(f"db_utils.py: Coleções Firestore definidas: {list(COLLECTIONS_FIRESTORE.keys())}")


def get_firestore_collection_ref(collection_name: str):
    """Retorna a referência da coleção do Firestore."""
    if db_firestore is None:
        logger.error(f"db_utils.py: Firestore client não inicializado ao tentar obter a coleção '{collection_name}'. Não é possível obter a coleção.")
        return None
    if collection_name not in COLLECTIONS_FIRESTORE:
        logger.error(f"db_utils.py: Coleção '{collection_name}' não encontrada em COLLECTIONS_FIRESTORE.")
        return None

    collection_path = COLLECTIONS_FIRESTORE[collection_name]
    logger.debug(f"db_utils.py: Obtendo referência da coleção Firestore para '{collection_name}' (path: {collection_path}).")
    return db_firestore.collection(collection_path)

def hash_password(password: str, username: str) -> str:
    """Cria um hash SHA-256 da senha usando o nome de usuário como sal."""
    password_salted = password + username
    hashed = hashlib.sha256(password_salted.encode('utf-8')).hexdigest()
    logger.debug(f"db_utils.py: Senha hashed para '{username}'.")
    return hashed

def create_initial_firestore_data_if_not_exists():
    """
    Cria o usuário admin padrão no Firestore se a coleção 'users' estiver vazia.
    Cria uma entrada NCM padrão se a coleção 'ncm_impostos_items' estiver vazia.
    """
    logger.info("db_utils.py: Iniciando verificação/criação de dados iniciais no Firestore.")
    if db_firestore is None:
        logger.error("db_utils.py: Firestore client não inicializado. Não é possível criar dados iniciais no Firestore.")
        return False

    users_ref = get_firestore_collection_ref("users")
    if users_ref:
        try:
            logger.info("db_utils.py: Verificando se a coleção 'users' (Firestore) contém dados.")
            users_docs = users_ref.limit(1).get()
            if not list(users_docs):
                admin_username = "admin"
                admin_password_hash = hash_password("admin", admin_username)
                all_screens_default = [
                    "Home", "Dashboard", "Descrições", "Listagem NCM", "Follow-up Importação",
                    "Importar XML DI", "Pagamentos", "Custo do Processo",
                    "Cálculo Portonave", "Análise de Documentos", "Pagamentos Container",
                    "Cálculo de Tributos TTCE", "Gerenciamento de Usuários",
                    "Cálculo Frete Internacional", "Análise de Faturas/PL (PDF)",
                    "Cálculo Futura", "Cálculo Pac Log - Elo", "Cálculo Fechamento",
                    "Cálculo FN Transportes", "Produtos", "Formulário Processo",
                    "Clonagem de Processo", "Consulta de Processo"
                ]
                user_data = {
                    "username": admin_username,
                    "password_hash": admin_password_hash,
                    "is_admin": True,
                    "allowed_screens": all_screens_default
                }
                users_ref.document(admin_username).set(user_data)
                logger.info("db_utils.py: Usuário admin padrão criado no Firestore.")
            else:
                logger.info("db_utils.py: Coleção 'users' (Firestore) já contém dados. Usuário admin padrão não criado.")
        except Exception as e:
            logger.exception("db_utils.py: Erro ao verificar/criar usuário admin padrão no Firestore.")
            return False

    ncm_impostos_ref = get_firestore_collection_ref("ncm_impostos_items")
    if ncm_impostos_ref:
        try:
            logger.info("db_utils.py: Verificando se a coleção 'ncm_impostos_items' (Firestore) contém dados.")
            ncm_docs = ncm_impostos_ref.limit(1).get()
            if not list(ncm_docs):
                default_ncm = {
                    "ncm_code": "85171231",
                    "descricao_item": "Telefones celulares",
                    "ii_aliquota": 16.0,
                    "ipi_aliquota": 5.0,
                    "pis_aliquota": 1.65,
                    "cofins_aliquota": 7.6,
                    "icms_aliquota": 18.0
                }
                ncm_impostos_ref.document(default_ncm["ncm_code"]).set(default_ncm)
                logger.info("db_utils.py: Entrada NCM padrão criada no Firestore.")
            else:
                logger.info("db_utils.py: Coleção 'ncm_impostos_items' (Firestore) já contém dados. Entrada padrão não criada.")
        except Exception as e:
            logger.exception("db_utils.py: Erro ao verificar/criar entrada NCM padrão no Firestore.")
            return False

    logger.info("db_utils.py: Verificação/criação de dados iniciais no Firestore concluída.")
    return True

def create_tables():
    """
    Gerencia a criação de diretórios e inicializa dados para o Firestore.
    Esta função deve ser chamada uma vez no início da aplicação Streamlit.
    """
    logger.info("db_utils.py: Iniciando create_tables.")
    success = True

    data_dir = os.path.join(_app_root_path, _DEFAULT_DB_FOLDER)
    if not os.path.exists(data_dir):
        try:
            os.makedirs(data_dir)
            logger.info(f"db_utils.py: Diretório de dados '{data_dir}' criado.")
        except OSError as e:
            logger.error(f"db_utils.py: Erro ao criar o diretório de dados '{data_dir}': {e}")
    else:
        logger.info(f"db_utils.py: Diretório de dados '{data_dir}' já existe.")

    if db_firestore and _USE_FIRESTORE_AS_PRIMARY:
        logger.info("db_utils.py: Firestore está HABILITADO como primário. Iniciando criação de dados iniciais no Firestore.")
        if not create_initial_firestore_data_if_not_exists():
            logger.error("db_utils.py: Falha na criação de dados iniciais no Firestore.")
            success = False
        else:
            logger.info("db_utils.py: Criação de dados iniciais no Firestore concluída com sucesso.")
    else:
        logger.info("db_utils.py: Firestore está DESABILITADO como primário ou cliente não inicializado. Ignorando criação de dados iniciais no Firestore.")

    logger.info(f"db_utils.py: create_tables finalizado. Sucesso geral: {success}")
    return success

def initialize_db_connections():
    """
    Função para ser chamada uma vez no início da aplicação Streamlit
    para garantir que os dados iniciais existam e as tabelas estejam prontas.
    """
    logger.info("db_utils.py: Iniciando initialize_db_connections.")
    create_tables()
    logger.info("db_utils.py: initialize_db_connections finalizado.")

def save_dolar_cotacao(cotacao_data):
    """
    Salva a cotação do dólar no Firestore.
    :param cotacao_data: Dicionário contendo os dados da cotação (abertura_compra, ptax_compra, etc.).
    """
    try:
        db = st.session_state.db_firestore
        cotacoes_ref = db.collection("cotacoes_dolar")

        # Adiciona o timestamp para saber quando a cotação foi salva
        cotacao_data["timestamp"] = datetime.now()

        # Cria um documento com um ID baseado na data para facilitar a consulta
        # e evitar duplicatas para o mesmo dia.
        # Formato do ID: "YYYY-MM-DD"
        doc_id = datetime.now().strftime("%Y-%m-%d")

        cotacoes_ref.document(doc_id).set(cotacao_data)
        st.success(f"Cotação do dólar salva com sucesso para {doc_id}!")
        return True
    except Exception as e:
        st.error(f"Erro ao salvar cotação do dólar no Firestore: {e}")
        return False

def get_latest_dolar_cotacao():
    """
    Busca a última cotação do dólar salva no Firestore.
    Retorna None se não houver cotações ou em caso de erro.
    """
    try:
        db = st.session_state.db_firestore
        cotacoes_ref = db.collection("cotacoes_dolar")

        # Busca o documento mais recente (ordenado por timestamp decrescente e limita a 1)
        query = cotacoes_ref.order_by("timestamp", direction="DESCENDING").limit(1)
        docs = query.stream()

        latest_cotacao = None
        for doc in docs:
            latest_cotacao = doc.to_dict()
            # Remove o timestamp do objeto retornado se não for necessário para exibição
            if "timestamp" in latest_cotacao:
                del latest_cotacao["timestamp"]
            break # Pega apenas o primeiro (mais recente)

        return latest_cotacao
    except Exception as e:
        st.error(f"Erro ao buscar a última cotação do dólar no Firestore: {e}")
        return None

def verify_credentials(username: str, password: str) -> Optional[Dict[str, Any]]:
    """Verifica as credenciais do usuário. SOMENTE Firestore."""
    logger.info(f"db_utils.py: Verificando credenciais para o usuário: {username}")
    if db_firestore:
        logger.info("db_utils.py: Usando Firestore para verificar credenciais.")
        users_ref = get_firestore_collection_ref("users")
        if not users_ref:
            logger.error("db_utils.py: Falha ao acessar coleção 'users' no Firestore para verificar credenciais.")
            return None
        try:
            user_doc = users_ref.document(username).get()
            if user_doc.exists:
                user_data = user_doc.to_dict()
                stored_password_hash = user_data.get('password_hash')
                is_admin = user_data.get('is_admin', False)
                allowed_screens = user_data.get('allowed_screens', [])
                provided_password_hash = hash_password(password, username)
                if provided_password_hash == stored_password_hash:
                    logger.info(f"db_utils.py: Login bem-sucedido para o usuário: {username} (Firestore)")
                    return {'username': username, 'is_admin': bool(is_admin), 'allowed_screens': allowed_screens}
                else:
                    logger.warning(f"db_utils.py: Tentativa de login falhou para o usuário {username}: Senha incorreta (Firestore).")
                    return False
            else:
                logger.warning(f"db_utils.py: Tentativa de login falhou: Usuário '{username}' não encontrado (Firestore).")
                return False
        except Exception as e:
            logger.exception(f"db_utils.py: Erro ao verificar credenciais para o usuário {username} no Firestore: {e}")
            return None
    else:
        logger.warning("db_utils.py: Firestore client não inicializado ou desabilitado. Não é possível verificar credenciais.")
    return None

def get_all_users() -> List[Dict[str, Any]]:
    """Obtém todos os usuários. SOMENTE Firestore."""
    logger.info("db_utils.py: Obtendo todos os usuários.")
    if db_firestore:
        logger.info("db_utils.py: Usando Firestore para obter todos os usuários.")
        users_ref = get_firestore_collection_ref("users")
        if not users_ref:
            logger.error("db_utils.py: Falha ao acessar coleção 'users' no Firestore para obter todos os usuários.")
            return []
        try:
            users = []
            for doc in users_ref.order_by("username").stream():
                data = doc.to_dict()
                users.append({
                    'id': doc.id,
                    'username': data.get('username'),
                    'is_admin': data.get('is_admin', False),
                    'allowed_screens': data.get('allowed_screens', [])
                })
            logger.info(f"db_utils.py: Obtidos {len(users)} usuários do Firestore.")
            return users
        except Exception as e:
            logger.error(f"db_utils.py: Erro ao obter todos os usuários do Firestore: {e}")
            return []
    else:
        logger.warning("db_utils.py: Firestore client não inicializado ou desabilitado. Não é possível obter todos os usuários.")
    return []

def get_user_by_id_or_username(identifier: Any) -> Optional[Dict[str, Any]]:
    """
    Obtém um único usuário pelo seu ID (Firestore usa username como ID do documento).
    Retorna um dicionário com os dados do usuário, ou None se não encontrado. SOMENTE Firestore.
    """
    logger.info(f"db_utils.py: Buscando usuário por identificador: {identifier}")
    if db_firestore:
        logger.info("db_utils.py: Usando Firestore para buscar usuário por username.")
        users_ref = get_firestore_collection_ref("users")
        if not users_ref:
            logger.error(f"db_utils.py: Falha ao acessar coleção 'users' no Firestore para buscar usuário.")
            return None
        try:
            user_doc = users_ref.document(str(identifier)).get()
            if user_doc.exists:
                user_data = user_doc.to_dict()
                logger.info(f"db_utils.py: Usuário '{identifier}' encontrado no Firestore.")
                return {
                    'id': user_doc.id,
                    'username': user_data.get('username'),
                    'is_admin': user_data.get('is_admin', False),
                    'allowed_screens': user_data.get('allowed_screens', [])
                }
            else:
                logger.warning(f"db_utils.py: Usuário com identificador '{identifier}' não encontrado no Firestore.")
                return None
        except Exception as e:
            logger.error(f"db_utils.py: Erro ao buscar usuário com identificador '{identifier}' no Firestore: {e}")
            return None
    else:
        logger.warning(f"db_utils.py: Firestore client não inicializado ou desabilitado. Não é possível buscar usuário.")
    return None

def adicionar_ou_atualizar_usuario(user_id: Optional[int], username: str, password_hash: str, is_admin: bool, allowed_screens: List[str]) -> bool:
    """
    Adiciona um novo usuário ou atualiza um existente. SOMENTE Firestore.
    No Firestore, o username é usado como ID do documento.
    """
    logger.info(f"db_utils.py: Adicionando/Atualizando usuário: {username}")
    success_firestore = True

    user_data = {
        "username": username,
        "password_hash": password_hash,
        "is_admin": is_admin,
        "allowed_screens": allowed_screens
    }

    if db_firestore:
        logger.info(f"db_utils.py: Usando Firestore para adicionar/atualizar usuário: {username}")
        users_ref = get_firestore_collection_ref("users")
        if users_ref:
            try:
                doc_ref = users_ref.document(username)

                # Check if it's the last admin and trying to remove admin status
                if not is_admin:
                    all_users = get_all_users()
                    admin_users = [u for u in all_users if u.get('is_admin')]
                    if len(admin_users) == 1 and admin_users[0].get('username') == username:
                        st.error("Não é possível remover o status de administrador do último usuário administrador.")
                        return False # Fail if trying to remove last admin status

                doc_ref.set(user_data, merge=True)
                logger.info(f"db_utils.py: Usuário '{username}' inserido/atualizado com sucesso no Firestore.")
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao inserir/atualizar usuário '{username}' no Firestore: {e}")
                success_firestore = False
        else:
            logger.error(f"db_utils.py: Falha ao obter referência da coleção 'users' no Firestore.")
            success_firestore = False
    else:
        logger.warning("db_utils.py: Firestore client não inicializado ou desabilitado. Não é possível adicionar/atualizar usuário.")
        success_firestore = False # Set to false if Firestore not ready

    return success_firestore


def atualizar_senha_usuario(user_id: Any, new_password: str, username: str) -> bool:
    """Atualiza a senha de um usuário específico. SOMENTE Firestore."""
    logger.info(f"db_utils.py: Atualizando senha para usuário: {username}")
    success_firestore = True

    new_password_hash = hash_password(new_password, username)

    if db_firestore:
        logger.info(f"db_utils.py: Usando Firestore para atualizar senha: {username}")
        users_ref = get_firestore_collection_ref("users")
        if users_ref:
            try:
                doc_ref = users_ref.document(username) # Firestore uses username as doc ID
                doc_ref.update({"password_hash": new_password_hash})
                logger.info(f"db_utils.py: Senha do usuário '{username}' atualizada com sucesso no Firestore.")
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao atualizar senha do usuário '{username}' no Firestore: {e}")
                success_firestore = False
        else:
            logger.error(f"db_utils.py: Falha ao obter referência da coleção 'users' no Firestore para atualizar senha.")
            success_firestore = False
    else:
        logger.warning("db_utils.py: Firestore client não inicializado ou desabilitado. Não é possível atualizar senha.")
        success_firestore = False # Set to false if Firestore not ready

    return success_firestore


def deletar_usuario(user_identifier: Any) -> bool:
    """
    Deleta um usuário do banco de dados. Pode receber user_id (int, será convertido para str) ou username (str).
    Considera a lógica de ser o último admin. SOMENTE Firestore.
    """
    logger.info(f"db_utils.py: Deletando usuário: {user_identifier}")
    success_firestore = True

    user_to_delete = get_user_by_id_or_username(user_identifier)
    if not user_to_delete:
        logger.warning(f"db_utils.py: Usuário '{user_identifier}' não encontrado para exclusão.")
        return False

    all_users = get_all_users()
    admin_users = [u for u in all_users if u.get('is_admin')]

    if user_to_delete.get('is_admin') and len(admin_users) <= 1:
        logger.error(f"db_utils.py: Não é possível excluir o último usuário administrador: {user_to_delete.get('username')}.")
        return False

    if db_firestore:
        logger.info(f"db_utils.py: Usando Firestore para deletar usuário: {user_to_delete.get('username')}")
        users_ref = get_firestore_collection_ref("users")
        if users_ref:
            try:
                doc_ref = users_ref.document(user_to_delete.get('username'))
                doc = doc_ref.get()
                if doc.exists:
                    doc_ref.delete()
                    logger.info(f"db_utils.py: Usuário '{user_to_delete.get('username')}' excluído com sucesso do Firestore.")
                else:
                    logger.warning(f"db_utils.py: Usuário '{user_to_delete.get('username')}' não encontrado no Firestore para exclusão.")
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao excluir usuário '{user_to_delete.get('username')}' do Firestore: {e}")
                success_firestore = False
        else:
            logger.error(f"db_utils.py: Falha ao obter referência da coleção 'users' no Firestore para deletar.")
            success_firestore = False
    else:
        logger.warning("db_utils.py: Firestore client não inicializado ou desabilitado. Não é possível deletar usuário.")
        success_firestore = False
    return success_firestore


def adicionar_ou_atualizar_ncm_item(ncm_code: str, descricao_item: str, ii_aliquota: float, ipi_aliquota: float, pis_aliquota: float, cofins_aliquota: float, icms_aliquota: float):
    """
    Adiciona/atualiza item NCM. SOMENTE Firestore.
    """
    logger.info(f"db_utils.py: Adicionando/Atualizando item NCM: {ncm_code}")
    success_firestore = True

    data = {
        "ncm_code": ncm_code,
        "descricao_item": descricao_item,
        "ii_aliquota": ii_aliquota,
        "ipi_aliquota": ipi_aliquota,
        "pis_aliquota": pis_aliquota,
        "cofins_aliquota": cofins_aliquota,
        "icms_aliquota": icms_aliquota
    }

    if db_firestore:
        logger.info(f"db_utils.py: Usando Firestore para adicionar/atualizar NCM: {ncm_code}")
        ncm_impostos_ref = get_firestore_collection_ref("ncm_impostos_items")
        if ncm_impostos_ref:
            try:
                doc_ref = ncm_impostos_ref.document(ncm_code)
                doc_ref.set(data)
                logger.info(f"db_utils.py: Item NCM '{ncm_code}' inserido/atualizado com sucesso no Firestore.")
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao inserir/atualizar item NCM '{ncm_code}' no Firestore: {e}")
                success_firestore = False
        else:
            logger.error(f"db_utils.py: Não foi possível obter referência da coleção 'ncm_impostos_items' no Firestore.")
            success_firestore = False
    else:
        logger.warning("db_utils.py: Firestore client não inicializado ou desabilitado. Não é possível adicionar/atualizar NCM.")
        success_firestore = False
    return success_firestore


def seleccionar_todos_ncm_itens():
    """
    Seleciona todos os itens NCM. SOMENTE Firestore.
    """
    logger.info("db_utils.py: Selecionando todos os itens NCM.")
    if db_firestore:
        logger.info("db_utils.py: Usando Firestore para selecionar todos os itens NCM.")
        ncm_impostos_ref = get_firestore_collection_ref("ncm_impostos_items")
        if not ncm_impostos_ref:
            logger.error(f"db_utils.py: Falha ao acessar coleção 'ncm_impostos_items' no Firestore para obter todos os itens.")
            return []
        try:
            itens = []
            for doc in ncm_impostos_ref.order_by("ncm_code").stream():
                data = doc.to_dict()
                itens.append({
                    "id": doc.id,
                    "ncm_code": data.get('ncm_code'),
                    "descricao_item": data.get('descricao_item'),
                    "ii_aliquota": data.get('ii_aliquota'),
                    "ipi_aliquota": data.get('ipi_aliquota'),
                    "pis_aliquota": data.get('pis_aliquota'),
                    "cofins_aliquota": data.get('cofins_aliquota'),
                    "icms_aliquota": data.get('icms_aliquota')
                })
            logger.info(f"db_utils.py: Obtidos {len(itens)} itens NCM do Firestore.")
            return itens
        except Exception as e:
            logger.error(f"db_utils.py: Erro ao buscar todos os itens NCM do Firestore: {e}")
            return []
    else:
        logger.warning(f"db_utils.py: Firestore client não inicializado ou desabilitado. Não é possível selecionar todos os itens NCM.")
    return []

def deletar_ncm_item(ncm_id: str):
    """
    Deleta um item NCM. SOMENTE Firestore.
    """
    logger.info(f"db_utils.py: Deletando item NCM: {ncm_id}")
    success_firestore = True

    if db_firestore:
        logger.info(f"db_utils.py: Usando Firestore para deletar NCM: {ncm_id}")
        ncm_impostos_ref = get_firestore_collection_ref("ncm_impostos_items")
        if ncm_impostos_ref:
            try:
                doc_ref = ncm_impostos_ref.document(ncm_id)
                doc = doc_ref.get()
                if doc.exists:
                    doc_ref.delete()
                    logger.info(f"db_utils.py: Item NCM com código '{ncm_id}' excluído com sucesso do Firestore.")
                else:
                    logger.warning(f"db_utils.py: Item NCM com código '{ncm_id}' não encontrado no Firestore para exclusão.")
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao excluir item NCM com código '{ncm_id}' do Firestore: {e}")
                success_firestore = False
        else:
            logger.error(f"db_utils.py: Não foi possível obter referência da coleção 'ncm_impostos_items' no Firestore para deletar.")
            success_firestore = False
    else:
        logger.warning(f"db_utils.py: Firestore client não inicializado ou desabilitado. Não é possível deletar NCM.")
        success_firestore = False
    return success_firestore


def get_ncm_item_by_ncm_code(ncm_code: str):
    """
    Busca um item NCM pelo seu código NCM. SOMENTE Firestore.
    """
    logger.info(f"db_utils.py: Buscando item NCM pelo código: {ncm_code}")
    if db_firestore:
        logger.info("db_utils.py: Usando Firestore para buscar item NCM.")
        ncm_impostos_ref = get_firestore_collection_ref("ncm_impostos_items")
        if not ncm_impostos_ref:
            logger.error(f"db_utils.py: Falha ao acessar coleção 'ncm_impostos_items' no Firestore para buscar item.")
            return None
        try:
            doc_ref = ncm_impostos_ref.document(ncm_code)
            doc = doc_ref.get()
            if doc.exists:
                data = doc.to_dict()
                logger.info(f"db_utils.py: Item NCM '{ncm_code}' encontrado no Firestore.")
                return {
                    "id": doc.id,
                    "ncm_code": data.get("ncm_code"),
                    "descricao_item": data.get("descricao_item"),
                    "ii_aliquota": data.get("ii_aliquota"),
                    "ipi_aliquota": data.get("ipi_aliquota"),
                    "pis_aliquota": data.get("pis_aliquota"),
                    "cofins_aliquota": data.get("cofins_aliquota"),
                    "icms_aliquota": data.get("icms_aliquota")
                }
            else:
                logger.warning(f"db_utils.py: Item NCM com código '{ncm_code}' não encontrado no Firestore.")
                return None
        except Exception as e:
            logger.error(f"db_utils.py: Erro ao buscar item NCM com código '{ncm_code}' no Firestore: {e}")
            return None
    else:
        logger.warning(f"db_utils.py: Firestore client não inicializado ou desabilitado. Não é possível buscar item NCM.")
    return None

def get_all_declaracoes():
    """Carrega e retorna todos os dados das declarações XML. SOMENTE Firestore."""
    logger.info("db_utils.py: Obtendo todas as declarações XML.")
    if db_firestore:
        logger.info("db_utils.py: Usando Firestore para obter todas as declarações XML.")
        declaracoes_ref = get_firestore_collection_ref("xml_declaracoes")
        if not declaracoes_ref:
            logger.error(f"db_utils.py: Falha ao acessar coleção 'xml_declaracoes' no Firestore para obter declarações.")
            return []
        try:
            docs = declaracoes_ref.order_by("data_importacao", direction=firestore.Query.DESCENDING).order_by("numero_di", direction=firestore.Query.DESCENDING).stream()
            declaracoes = []
            for doc in docs:
                data = doc.to_dict()
                data['id'] = doc.id
                declaracoes.append(data)
            logger.info(f"db_utils.py: Obtidas {len(declaracoes)} declarações XML do Firestore.")
            return declaracoes
        except Exception as e:
            logger.error(f"db_utils.py: Erro Firestore ao carregar todas as declarações XML DI: {e}")
        return []
    else:
        logger.warning(f"db_utils.py: Firestore client não inicializado ou desabilitado. Não é possível obter declarações XML.")
    return []

def get_declaracao_by_id(declaracao_id: Any):
    """Busca uma declaração pelo ID. SOMENTE Firestore."""
    # Para Firestore, o ID é o numero_di.
    logger.info(f"db_utils.py: Buscando declaração por ID: {declaracao_id}")
    if db_firestore:
        logger.info("db_utils.py: Usando Firestore para buscar declaração por ID (numero_di).")
        declaracoes_ref = get_firestore_collection_ref("xml_declaracoes")
        if not declaracoes_ref:
            logger.error(f"db_utils.py: Falha ao acessar coleção 'xml_declaracoes' no Firestore para buscar declaração por ID.")
            return None
        try:
            # Assumimos que declaracao_id é o numero_di que é o ID do documento
            doc = declaracoes_ref.document(str(declaracao_id)).get()
            if doc.exists:
                data = doc.to_dict()
                data['id'] = doc.id # Garante que o 'id' retornado é o ID do documento (numero_di)
                logger.info(f"db_utils.py: Declaração ID {declaracao_id} encontrada no Firestore.")
                return data
            logger.warning(f"db_utils.py: Declaração ID {declaracao_id} não encontrada no Firestore.")
            return None
        except Exception as e:
            logger.error(f"db_utils.py: Erro Firestore ao buscar declaração ID {declaracao_id}: {e}")
        return None
    else:
        logger.warning(f"db_utils.py: Firestore client não inicializado ou desabilitado. Não é possível buscar declaração por ID.")
    return None

def _clean_reference_string(s: str) -> str:
    """
    Cleans a reference string by removing leading/trailing whitespace (including unicode)
    and common invisible/non-breaking characters like zero-width spaces.
    Ensures the string is uppercase.
    """
    if not isinstance(s, str):
        return str(s) if s is not None else ""

    # Remove leading/trailing whitespace (standard and unicode)
    cleaned = s.strip()

    # Remove common invisible characters (e.g., zero-width space \u200b, byte order mark \uFEFF)
    # This regex targets specific invisible control characters and unicode whitespace
    # that might not be caught by .strip()
    cleaned = re.sub(r'[\u0000-\u001F\u007F-\u009F\u00AD\u034F\u061C\u180E\u2000-\u200F\u2028-\u202F\u205F\u2060-\u206F\u2070-\u20FF\uFEFF\s]+', '', cleaned)

    return cleaned.upper() # Ensure it's upper case for consistent comparison


def get_declaracao_by_referencia(referencia: str) -> Optional[Dict[str, Any]]:
    """
    Busca uma declaração de importação pela referência (informacao_complementar). SOMENTE Firestore.
    """
    logger.info(f"db_utils.py: Buscando declaração por referência: {referencia}")
    if db_firestore:
        logger.info("db_utils.py: Usando Firestore para buscar declaração por referência.")
        declaracoes_ref = get_firestore_collection_ref("xml_declaracoes")
        if not declaracoes_ref:
            logger.error(f"db_utils.py: Falha ao acessar coleção 'xml_declaracoes' no Firestore para buscar declaração por referência.")
            return None
        try:
            # Limpa a string de referência antes de usar na query
            query_val = _clean_reference_string(referencia)

            docs = declaracoes_ref.where("informacao_complementar", "==", query_val).limit(1).get()
            for doc in docs:
                data = doc.to_dict()
                data['id'] = doc.id
                logger.info(f"db_utils.py: Declaração com referência '{referencia}' encontrada no Firestore.")
                return data
            logger.warning(f"db_utils.py: Declaração com referência '{referencia}' não encontrada no Firestore.")
            return None
        except Exception as e:
            logger.error(f"db_utils.py: Erro Firestore ao buscar declaração por referência '{referencia}': {e}")
        return None
    else:
        logger.warning(f"db_utils.py: Firestore client não inicializado ou desabilitado. Não é possível buscar declaração por referência.")
    return None

def get_itens_by_declaracao_id(declaracao_id: Any):
    """Obtém itens de declaração. SOMENTE Firestore."""
    logger.info(f"db_utils.py: Obtendo itens para declaração ID: {declaracao_id} (Tipo: {type(declaracao_id)})")
    if db_firestore:
        logger.info("db_utils.py: Usando Firestore para obter itens da declaração.")
        itens_ref = get_firestore_collection_ref("xml_itens")
        if not itens_ref:
            logger.error(f"db_utils.py: Falha ao acessar coleção 'xml_itens' no Firestore para obter itens.")
            return []
        try:
            # Query Firestore for items linked to this declaracao_id
            query_declaracao_id = str(declaracao_id)
            logger.info(f"db_utils.py: Executando query Firestore para xml_itens com declaracao_id == '{query_declaracao_id}'")

            docs = itens_ref.where("declaracao_id", "==", query_declaracao_id).order_by("numero_adicao").order_by("numero_item_sequencial").stream()

            itens = []
            found_docs_count = 0
            for doc in docs:
                found_docs_count += 1 # Correction here, was 0
                data = doc.to_dict()
                data['id'] = doc.id # The ID of the item document
                itens.append(data)
            logger.info(f"db_utils.py: Query Firestore para xml_itens retornou {found_docs_count} documentos. Obtidos {len(itens)} itens para declaração ID {declaracao_id} do Firestore.")
            return itens
        except Exception as e:
            logger.error(f"db_utils.py: Erro Firestore ao buscar itens para declaração ID {declaracao_id}: {e}")
            logger.exception("Detalhes do erro ao buscar itens do Firestore:")
        return []
    else:
        logger.warning(f"db_utils.py: Firestore client não inicializado ou desabilitado. Não é possível obter itens da declaração.")
    return []

def update_xml_item_erp_code(item_id: Any, new_erp_code: str):
    """Atualiza código ERP de um item. SOMENTE Firestore."""
    logger.info(f"db_utils.py: Atualizando código ERP para item ID {item_id} para '{new_erp_code}'.")
    success_firestore = True

    if db_firestore:
        logger.info("db_utils.py: Usando Firestore para atualizar código ERP.")
        itens_ref = get_firestore_collection_ref("xml_itens")
        if itens_ref:
            try:
                doc_ref = itens_ref.document(str(item_id)) # item_id é o ID do documento do item
                doc_ref.update({"codigo_erp_item": new_erp_code})
                logger.info(f"db_utils.py: Item ID {item_id} atualizado com Código ERP '{new_erp_code}' no Firestore.")
            except Exception as e:
                logger.error(f"db_utils.py: Erro Firestore ao atualizar Código ERP para item ID {item_id}: {e}")
                success_firestore = False
        else:
            logger.error(f"db_utils.py: Não foi possível obter referência da coleção 'xml_itens' no Firestore para atualizar código ERP.")
            success_firestore = False
    else:
        logger.warning(f"db_utils.py: Firestore client não inicializado ou desabilitado. Não é possível atualizar código ERP.")
        success_firestore = False
    return success_firestore

def save_process_cost_data(declaracao_id: Any, afrmm: float, siscoserv: float, descarregamento: float, taxas_destino: float, multa: float, contracts_df: pd.DataFrame):
    """Salva dados de custo do processo. SOMENTE Firestore."""
    logger.info(f"db_utils.py: Salvando dados de custo para declaração ID: {declaracao_id}")
    success_firestore = True

    cost_data = {
        "afrmm": afrmm,
        "siscoserv": siscoserv,
        "descarregamento": descarregamento,
        "taxas_destino": taxas_destino,
        "multa": multa
    }

    if db_firestore:
        logger.info("db_utils.py: Usando Firestore para salvar dados de custo do processo.")
        processo_dados_custo_ref = get_firestore_collection_ref("processo_dados_custo")
        processo_contratos_cambio_ref = get_firestore_collection_ref("processo_contratos_cambio")
        if processo_dados_custo_ref and processo_contratos_cambio_ref:
            try:
                batch = db_firestore.batch()

                cost_doc_ref = processo_dados_custo_ref.document(str(declaracao_id)) # ID do documento de custo é o numero_di
                batch.set(cost_doc_ref, cost_data)

                old_contracts = processo_contratos_cambio_ref.where("declaracao_id", "==", str(declaracao_id)).stream()
                for doc in old_contracts:
                    batch.delete(doc.reference)
                logger.debug(f"db_utils.py: Deletados contratos antigos para DI ID {declaracao_id} no Firestore.")

                for index, row in contracts_df.iterrows():
                    num_contrato = row['Nº Contrato']
                    dolar_cambio = row['Dólar']
                    valor_contrato_usd = row['Valor (US$)']

                    if dolar_cambio > 0 and valor_contrato_usd > 0 and num_contrato:
                        contract_data = {
                            "declaracao_id": str(declaracao_id), # Linka ao numero_di
                            "numero_contrato": num_contrato,
                            "dolar_cambio": dolar_cambio,
                            "valor_usd": valor_contrato_usd
                        }
                        batch.set(processo_contratos_cambio_ref.document(), contract_data) # Firestore gera um ID automático

                batch.commit()
                logger.info(f"db_utils.py: Despesas/contratos salvos para DI ID {declaracao_id} no Firestore.")
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao salvar despesas/contratos para DI ID {declaracao_id} no Firestore: {e}")
                success_firestore = False
        else:
            logger.error(f"db_utils.py: Não foi possível obter referência das coleções de custo/contrato no Firestore.")
            success_firestore = False
    else:
        logger.warning("db_utils.py: Firestore client não inicializado ou desabilitado. Não é possível salvar dados de custo.")
        success_firestore = False
    return success_firestore


def get_process_cost_data(declaracao_id: Any):
    """Obtém dados de custo do processo. SOMENTE Firestore."""
    logger.info(f"db_utils.py: Obtendo dados de custo para declaração ID: {declaracao_id}")
    if db_firestore:
        logger.info("db_utils.py: Usando Firestore para obter dados de custo do processo.")
        processo_dados_custo_ref = get_firestore_collection_ref("processo_dados_custo")
        processo_contratos_cambio_ref = get_firestore_collection_ref("processo_contratos_cambio")
        if not processo_dados_custo_ref or not processo_contratos_cambio_ref:
            logger.error(f"db_utils.py: Falha ao acessar coleções de custo/contrato no Firestore para obter dados.")
            return None, []
        try:
            expenses_doc = processo_dados_custo_ref.document(str(declaracao_id)).get()
            expenses_data = expenses_doc.to_dict() if expenses_doc.exists else None

            contracts_data = []
            contract_docs = processo_contratos_cambio_ref.where("declaracao_id", "==", str(declaracao_id)).stream()
            for doc in contract_docs:
                contracts_data.append(doc.to_dict())

            logger.info(f"db_utils.py: Obtidos dados de custo para DI ID {declaracao_id} do Firestore.")
            return expenses_data, contracts_data
        except Exception as e:
            logger.error(f"Erro ao carregar dados de custo para DI ID {declaracao_id} no Firestore: {e}")
        return None, []
    else:
        logger.warning(f"db_utils.py: Firestore client não inicializado ou desabilitado. Não é possível obter dados de custo do processo.")
    return None, []


def parse_xml_data_to_dict(xml_file_content: str) -> Tuple[Optional[Dict[str, Any]], Optional[List[Dict[str, Any]]]]:
    logger.info("db_utils.py: Iniciando parse do conteúdo XML.")
    try:
        root = ET.fromstring(xml_file_content)
        numero_di_elem = root.find('.//declaracaoImportacao/numeroDI')
        numero_di = numero_di_elem.text.strip() if numero_di_elem is not None and numero_di_elem.text else None
        if not numero_di:
            logger.error(f"db_utils.py: Não foi possível encontrar o número da DI no XML.")
            return None, None
        data_registro_elem = root.find('.//declaracaoImportacao/dataRegistro')
        data_registro_str = data_registro_elem.text.strip() if data_registro_elem is not None and data_registro_elem.text else None
        data_registro_db = None
        if data_registro_str and len(data_registro_str) == 8:
            try:
                data_registro_obj = datetime.strptime(data_registro_str, "%Y%m%d")
                data_registro_db = data_registro_obj.strftime("%Y-%m-%d")
            except ValueError:
                logger.warning(f"db_utils.py: Erro de formato de data de registro no XML: {data_registro_str}")
                pass
        informacao_complementar_elem = root.find('.//declaracaoImportacao/informacaoComplementar')
        informacao_completa_str = informacao_complementar_elem.text.strip() if informacao_complementar_elem is not None and informacao_complementar_elem.text else ""
        referencia_extraida = "N/A"
        match_referencia = re.search(r'REFERENCIA:\s*([A-Z0-9-/]+)', informacao_completa_str)
        if match_referencia:
            referencia_extraida = match_referencia.group(1)
            # Limpa a referência extraída antes de armazenar
            referencia_extraida = _clean_reference_string(referencia_extraida)
            logger.debug(f"db_utils.py: Referência extraída e limpa do XML: {referencia_extraida}")

        vmle = float(root.find('.//declaracaoImportacao/localEmbarqueTotalReais').text.strip()) / 100 if root.find('.//declaracaoImportacao/localEmbarqueTotalReais') is not None and root.find('.//declaracaoImportacao/localEmbarqueTotalReais').text else 0.0
        frete = float(root.find('.//declaracaoImportacao/freteTotalReais').text.strip()) / 100 if root.find('.//declaracaoImportacao/freteTotalReais') is not None and root.find('.//declaracaoImportacao/freteTotalReais').text else 0.0
        seguro = float(root.find('.//declaracaoImportacao/seguroTotalReais').text.strip()) / 100 if root.find('.//declaracaoImportacao/seguroTotalReais') is not None and root.find('.//declaracaoImportacao/seguroTotalReais').text else 0.0
        vmld = float(root.find('.//declaracaoImportacao/localDescargaTotalReais').text.strip()) / 100 if root.find('.//declaracaoImportacao/localDescargaTotalReais') is not None and root.find('.//declaracaoImportacao/localDescargaTotalReais').text else 0.0
        ipi = float(root.find(".//pagamento[codigoReceita='1038']/valorReceita").text.strip()) / 100 if root.find(".//pagamento[codigoReceita='1038']/valorReceita") is not None and root.find(".//pagamento[codigoReceita='1038']/valorReceita").text else 0.0
        pis_pasep = float(root.find(".//pagamento[codigoReceita='5602']/valorReceita").text.strip()) / 100 if root.find(".//pagamento[codigoReceita='5602']/valorReceita") is not None and root.find(".//pagamento[codigoReceita='5602']/valorReceita").text else 0.0
        cofins = float(root.find(".//pagamento[codigoReceita='5629']/valorReceita").text.strip()) / 100 if root.find(".//pagamento[codigoReceita='5629']/valorReceita") is not None and root.find(".//pagamento[codigoReceita='5629']/valorReceita").text else 0.0
        icms_sc = re.search(r'ICMS-SC IMPORTAÇÃO....:\s*(.+?)[\n\r]', informacao_completa_str).group(1).strip() if re.search(r'ICMS-SC IMPORTAÇÃO....:\s*(.+?)[\n\r]', informacao_completa_str) else "N/A"
        taxa_cambial_usd = float(re.search(r'TAXA CAMBIAL\(USD\):\s*([\d\.,]+)', informacao_completa_str).group(1).replace(',', '.')) if re.search(r'TAXA CAMBIAL\(USD\):\s*([\d\.,]+)', informacao_completa_str) else 0.0

        taxa_siscomex_elem = root.find(".//pagamento[codigoReceita='7811']/valorReceita")
        taxa_siscomex = float(taxa_siscomex_elem.text.strip()) / 100 if taxa_siscomex_elem is not None and taxa_siscomex_elem.text else 0.0

        numero_invoice = "N/A"
        documentos_despacho = root.findall(".//documentoInstrucaoDespacho")
        for doc in documentos_despacho:
            nome_doc_elem = doc.find("nomeDocumentoDespacho")
            numero_doc_elem = doc.find("numeroDocumentoDespacho")
            if nome_doc_elem is not None and numero_doc_elem is not None:
                nome_doc = nome_doc_elem.text.strip().upper()
                if "FATURA COMERCIAL" in nome_doc:
                    numero_invoice = numero_doc_elem.text.strip()
                    break
        peso_bruto = float(root.find('.//declaracaoImportacao/cargaPesoBruto').text.strip()) / 100000.0 if root.find('.//declaracaoImportacao/cargaPesoBruto') is not None and root.find('.//declaracaoImportacao/cargaPesoBruto').text else 0.0
        peso_liquido = float(root.find('.//declaracaoImportacao/cargaPesoLiquido').text.strip()) / 100000.0 if root.find('.//declaracaoImportacao/cargaPesoLiquido') is not None and root.find('.//declaracaoImportacao/cargaPesoLiquido').text else 0.0
        cnpj_importador = root.find('.//declaracaoImportacao/importadorNumero').text.strip() if root.find('.//declaracaoImportacao/importadorNumero') is not None and root.find('.//declaracaoImportacao/importadorNumero').text else "N/A"
        importador_nome = root.find('.//declaracaoImportacao/importadorNome').text.strip() if root.find('.//declaracaoImportacao/importadorNome') is not None and root.find('.//declaracaoImportacao/importadorNome').text else "N/A"
        recinto = root.find('.//declaracaoImportacao/armazenamentoRecintoAduaneiroNome').text.strip() if root.find('.//declaracaoImportacao/armazenamentoRecintoAduaneiroNome') is not None and root.find('.//declaracaoImportacao/armazenamentoRecintoAduaneiroNome').text else "N/A"
        embalagem = root.find('.//declaracaoImportacao/embalagem/nomeEmbalagem').text.strip() if root.find('.//declaracaoImportacao/embalagem/nomeEmbalagem') is not None and root.find('.//declaracaoImportacao/embalagem/nomeEmbalagem').text else "N/A"
        quantidade_volumes = int(root.find('.//declaracaoImportacao/embalagem/quantidadeVolume').text.strip()) if root.find('.//declaracaoImportacao/embalagem/quantidadeVolume') is not None and root.find('.//declaracaoImportacao/embalagem/quantidadeVolume').text and root.find('.//declaracaoImportacao/embalagem/quantidadeVolume').text.isdigit() else 0
        acrescimo = sum(float(elem.text.strip()) / 100 for elem in root.findall('.//declaracaoImportacao/adicao/acrescimo/valorReais') if elem.text)
        imposto_importacao = sum(float(elem.text.strip()) / 100 for elem in root.findall(".//pagamento[codigoReceita='0086']/valorReceita") if elem.text)
        armazenagem_val = 0.0
        frete_nacional_val = 0.0
        valor_total_reais_xml = vmle
        arquivo_origem = "XML_Importado"
        data_importacao = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        di_data = {
            "numero_di": numero_di, "data_registro": data_registro_db, "valor_total_reais_xml": valor_total_reais_xml,
            "arquivo_origem": arquivo_origem, "data_importacao": data_importacao,
            "informacao_complementar": referencia_extraida, "vmle": vmle, "frete": frete, "seguro": seguro,
            "vmld": vmld, "ipi": ipi, "pis_pasep": pis_pasep, "cofins": cofins, "icms_sc": icms_sc,
            "taxa_cambial_usd": taxa_cambial_usd, "taxa_siscomex": taxa_siscomex, "numero_invoice": numero_invoice,
            "peso_bruto": peso_bruto, "peso_liquido": peso_liquido, "cnpj_importador": cnpj_importador,
            "importador_nome": importador_nome, "recinto": recinto, "embalagem": embalagem,
            "quantidade_volumes": quantidade_volumes, "acrescimo": acrescimo, "imposto_importacao": imposto_importacao,
            "armazenagem": armazenagem_val, "frete_nacional": frete_nacional_val
        }
        logger.debug(f"db_utils.py: Dados da DI parseados: {di_data.get('numero_di')}, Ref: {di_data.get('informacao_complementar')}")

        itens_data = []
        adicoes = root.findall('.//declaracaoImportacao/adicao')
        for adicao in adicoes:
            numero_adicao = adicao.find('numeroAdicao').text.strip() if adicao.find('numeroAdicao') is not None and adicao.find('numeroAdicao').text else "N/A"
            peso_liquido_total_adicao = float(adicao.find('dadosMercadoriaPesoLiquido').text.strip()) / 100000.0 if adicao.find('dadosMercadoriaPesoLiquido') is not None and adicao.find('dadosMercadoriaPesoLiquido').text else 0.0

            quantidade_total_adicao_from_items = 0.0
            mercadorias_in_current_adicao = adicao.findall('mercadoria')
            for mercadoria_elem_in_adicao in mercadorias_in_current_adicao:
                quantidade_item_str = mercadoria_elem_in_adicao.find('quantidade').text.strip() if mercadoria_elem_in_adicao.find('quantidade') is not None else "0"
                try:
                    quantidade_total_adicao_from_items += float(quantidade_item_str) / 10**5
                except ValueError:
                    logger.warning(f"db_utils.py: Erro de formato de quantidade em adição {numero_adicao}.")
                    pass

            peso_unitario_medio_adicao = peso_liquido_total_adicao / quantidade_total_adicao_from_items if quantidade_total_adicao_from_items > 0 else 0.0
            if quantidade_total_adicao_from_items == 0:
                peso_unitario_medio_adicao = 0.0

            ii_perc_adicao = float(adicao.find('iiAliquotaAdValorem').text.strip()) / 10000.0 if adicao.find('iiAliquotaAdValorem') is not None and adicao.find('iiAliquotaAdValorem').text else 0.0
            ipi_perc_adicao = float(adicao.find('ipiAliquotaAdValorem').text.strip()) / 10000.0 if adicao.find('ipiAliquotaAdValorem') is not None and adicao.find('ipiAliquotaAdValorem').text else 0.0
            pis_perc_adicao = float(adicao.find('pisPasepAliquotaAdValorem').text.strip()) / 10000.0 if adicao.find('pisPasepAliquotaAdValorem') is not None and adicao.find('pisPasepAliquotaAdValorem').text else 0.0
            cofins_perc_adicao = float(adicao.find('cofinsAliquotaAdValorem').text.strip()) / 10000.0 if adicao.find('cofinsAliquotaAdValorem') is not None and adicao.find('cofinsAliquotaAdValorem').text else 0.0
            icms_perc_adicao = 0.0

            mercadorias = adicao.findall('mercadoria')
            item_counter_in_adicao = 1
            for mercadoria_elem in mercadorias:
                descricao = mercadoria_elem.find('descricaoMercadoria').text.strip() if mercadoria_elem.find('descricaoMercadoria') is not None and mercadoria_elem.find('descricaoMercadoria').text else "N/A"
                quantidade_str = mercadoria_elem.find('quantidade').text.strip() if mercadoria_elem.find('quantidade') is not None and mercadoria_elem.find('quantidade').text else "0"
                unidade_medida = mercadoria_elem.find('unidadeMedida').text.strip() if mercadoria_elem.find('unidadeMedida') is not None and mercadoria_elem.find('unidadeMedida').text else "N/A"
                valor_unitario_str = mercadoria_elem.find('valorUnitario').text.strip() if mercadoria_elem.find('valorUnitario') is not None and mercadoria_elem.find('valorUnitario').text else "0"
                numero_item = mercadoria_elem.find('numeroSequencialItem').text.strip() if mercadoria_elem.find('numeroSequencialItem') is not None and mercadoria_elem.find('numeroSequencialItem').text else str(item_counter_in_adicao)
                codigo_ncm = adicao.find('dadosMercadoriaCodigoNcm').text.strip() if adicao.find('dadosMercadoriaCodigoNcm') is not None and adicao.find('dadosMercadoriaCodigoNcm').text else "N/A"

                quantidade = float(quantidade_str) / 10**5 if quantidade_str else 0.0
                valor_unitario_fob_usd = float(valor_unitario_str) / 10**7 if valor_unitario_str else 0.0
                valor_item_calculado_fob_brl = quantidade * valor_unitario_fob_usd * taxa_cambial_usd

                sku_item = re.match(r'([A-Z0-9-]+)', descricao).group(1) if re.match(r'([A-Z0-9-]+)', descricao) else "N/A"
                peso_liquido_item = peso_unitario_medio_adicao * quantidade
                custo_unit_di_usd = valor_unitario_fob_usd

                itens_data.append({
                    "id": f"temp_{numero_di}_{numero_adicao}_{numero_item}",
                    "declaracao_id": None,
                    "numero_adicao": numero_adicao,
                    "numero_item_sequencial": numero_item,
                    "descricao_mercadoria": descricao,
                    "quantidade": quantidade,
                    "unidade_medida": unidade_medida,
                    "valor_unitario": valor_unitario_fob_usd,
                    "valor_item_calculado": valor_item_calculado_fob_brl,
                    "peso_liquido_item": peso_liquido_item,
                    "ncm_item": codigo_ncm,
                    "sku_item": sku_item,
                    "custo_unit_di_usd": custo_unit_di_usd,
                    "ii_percent_item": ii_perc_adicao,
                    "ipi_percent_item": ipi_perc_adicao,
                    "pis_percent_item": pis_perc_adicao,
                    "cofins_percent_item": cofins_perc_adicao,
                    "icms_percent_item": icms_perc_adicao,
                    "codigo_erp_item": ""
                })
                item_counter_in_adicao += 1
        logger.info(f"db_utils.py: Parse do XML concluído. {len(itens_data)} itens processados.")
        return di_data, itens_data
    except ET.ParseError as pe:
        logger.error(f"db_utils.py: Erro ao analisar o conteúdo XML: {pe}")
        return None, None
    except Exception as e:
        logger.exception(f"db_utils.py: Erro inesperado ao processar o XML: {e}")
        return None, None

def update_processo_di_link(processo_novo_ref: str, di_id_vinculada: str) -> bool:
    """
    Atualiza o campo DI_ID_Vinculada de um processo na coleção 'followup_processos'.
    Usa 'Processo_Novo' como o ID do documento para o processo no Firestore.
    """
    logger.info(f"db_utils.py: Tentando atualizar link DI para processo: {processo_novo_ref} com DI ID: {di_id_vinculada}")
    if db_firestore:
        processos_ref = get_firestore_collection_ref("followup_processos")
        if not processos_ref:
            logger.error("db_utils.py: Falha ao acessar coleção 'followup_processos' no Firestore para atualizar link da DI.")
            return False
        try:
            # Assumimos que Processo_Novo é o ID do documento na coleção followup_processos
            process_doc_ref = processos_ref.document(_clean_reference_string(processo_novo_ref))
            process_doc = process_doc_ref.get()

            if process_doc.exists:
                # Atualiza apenas o campo DI_ID_Vinculada
                process_doc_ref.update({"DI_ID_Vinculada": di_id_vinculada})
                logger.info(f"db_utils.py: Processo '{processo_novo_ref}' atualizado com DI_ID_Vinculada: {di_id_vinculada}")
                return True
            else:
                logger.warning(f"db_utils.py: Processo '{processo_novo_ref}' não encontrado no Firestore para atualizar link da DI.")
                return False
        except Exception as e:
            logger.error(f"db_utils.py: Erro ao atualizar o link da DI para o processo '{processo_novo_ref}' no Firestore: {e}", exc_info=True)
            return False
    else:
        logger.warning("db_utils.py: Firestore client não inicializado ou desabilitado. Não é possível atualizar o link da DI para o processo.")
    return False

def save_parsed_di_data(di_data: Dict[str, Any], itens_data: List[Dict[str, Any]]):
    """
    Salva dados de DI e itens. SOMENTE Firestore.
    Após salvar, tenta vincular a DI ao processo correspondente.
    """
    logger.info(f"db_utils.py: Iniciando save_parsed_di_data para DI: {di_data.get('numero_di')}")
    success_firestore = True

    numero_di = di_data.get('numero_di')
    if not numero_di:
        logger.error(f"db_utils.py: Número da DI não fornecido para salvar. Abortando.")
        return False

    if db_firestore:
        logger.info(f"db_utils.py: Tentando salvar DI e itens no Firestore para DI: {numero_di}")
        declaracoes_ref_firestore = get_firestore_collection_ref("xml_declaracoes")
        itens_ref_firestore = get_firestore_collection_ref("xml_itens")
        processos_ref_firestore = get_firestore_collection_ref("followup_processos") # Referência para processos

        if declaracoes_ref_firestore and itens_ref_firestore and processos_ref_firestore: # Verifica também processos_ref_firestore
            try:
                existing_di_firestore = declaracoes_ref_firestore.document(numero_di).get()
                if existing_di_firestore.exists:
                    logger.error(f"db_utils.py: Erro de integridade: A DI {numero_di} já existe no Firestore. Abortando salvamento no Firestore.")
                    success_firestore = False
                else:
                    batch = db_firestore.batch()
                    di_doc_ref = declaracoes_ref_firestore.document(numero_di)
                    batch.set(di_doc_ref, di_data)
                    logger.debug(f"db_utils.py: DI {numero_di} adicionada ao batch do Firestore.")

                    for item in itens_data:
                        item_id_firestore = f"{numero_di}_{item.get('numero_adicao')}_{item.get('numero_item_sequencial')}"
                        item_data_firestore = item.copy()
                        item_data_firestore['declaracao_id'] = numero_di
                        if 'id' in item_data_firestore:
                            del item_data_firestore['id']

                        batch.set(itens_ref_firestore.document(item_id_firestore), item_data_firestore)
                        logger.debug(f"db_utils.py: Item {item_id_firestore} adicionado ao batch do Firestore.")

                    batch.commit()
                    logger.info(f"db_utils.py: DI {numero_di} e seus itens salvos com sucesso no Firestore.")

                    # --- NOVO: Lógica para vincular a DI ao processo correspondente ---
                    referencia_processo_da_di = di_data.get('informacao_complementar')
                    if referencia_processo_da_di and referencia_processo_da_di != "N/A":
                        logger.info(f"db_utils.py: Tentando vincular DI '{numero_di}' ao processo com referência '{referencia_processo_da_di}'.")
                        # Busca o processo usando a referência (Processo_Novo)
                        # No Firestore, o Processo_Novo é o ID do documento
                        processo_doc = processos_ref_firestore.document(_clean_reference_string(referencia_processo_da_di)).get()
                        if processo_doc.exists:
                            # Se o processo existe, atualiza seu campo DI_ID_Vinculada
                            processo_doc.reference.update({"DI_ID_Vinculada": numero_di})
                            logger.info(f"db_utils.py: Processo '{referencia_processo_da_di}' atualizado com DI_ID_Vinculada: {numero_di}.")
                        else:
                            logger.warning(f"db_utils.py: Nenhum processo encontrado com referência '{referencia_processo_da_di}' para vincular a DI '{numero_di}'.")
                    # --- FIM NOVO ---

            except Exception as e:
                logger.exception(f"db_utils.py: Erro ao salvar DI e itens no Firestore: {e}")
                success_firestore = False
        else:
            logger.error(f"db_utils.py: Falha ao obter referências de coleção 'xml_declaracoes', 'xml_itens' ou 'followup_processos' no Firestore para salvar DI.")
            success_firestore = False
    else:
        logger.warning("db_utils.py: Firestore client não inicializado ou desabilitado. Não é possível salvar DI e itens.")
        success_firestore = False
    return success_firestore


def delete_declaracao(declaracao_id: Any):
    """Deleta uma declaração e dados relacionados. SOMENTE Firestore."""
    logger.info(f"db_utils.py: Iniciando exclusão da declaração ID: {declaracao_id}")
    success_firestore = True

    if db_firestore:
        logger.info(f"db_utils.py: Tentando deletar declaração ID {declaracao_id} e dados relacionados do Firestore.")
        declaracoes_ref_firestore = get_firestore_collection_ref("xml_declaracoes")
        itens_ref_firestore = get_firestore_collection_ref("xml_itens")
        processo_dados_custo_ref = get_firestore_collection_ref("processo_dados_custo")
        processo_contratos_cambio_ref = get_firestore_collection_ref("processo_contratos_cambio")
        frete_internacional_ref = get_firestore_collection_ref("frete_internacional")
        processos_ref_firestore = get_firestore_collection_ref("followup_processos") # Adicionado para desvincular

        if declaracoes_ref_firestore and itens_ref_firestore and processo_dados_custo_ref and \
           processo_contratos_cambio_ref and frete_internacional_ref and processos_ref_firestore:
            try:
                batch = db_firestore.batch()
                di_doc_ref = declaracoes_ref_firestore.document(str(declaracao_id)) # ID é o numero_di
                batch.delete(di_doc_ref)
                logger.debug(f"db_utils.py: Declaração ID {declaracao_id} adicionada ao batch para exclusão no Firestore.")

                # Deleta itens relacionados
                docs_to_delete_itens = itens_ref_firestore.where("declaracao_id", "==", str(declaracao_id)).stream()
                for doc in docs_to_delete_itens:
                    batch.delete(doc.reference)
                logger.debug(f"db_utils.py: Itens relacionados à declaração ID {declaracao_id} adicionados ao batch para exclusão no Firestore.")

                # Deleta dados de custo associados
                cost_doc_ref = processo_dados_custo_ref.document(str(declaracao_id))
                batch.delete(cost_doc_ref)
                logger.debug(f"db_utils.py: Dados de custo para DI ID {declaracao_id} adicionados ao batch para exclusão no Firestore.")

                # Deleta contratos de câmbio associados
                contract_docs_to_delete = processo_contratos_cambio_ref.where("declaracao_id", "==", str(declaracao_id)).stream()
                for doc in contract_docs_to_delete:
                    batch.delete(doc.reference)
                logger.debug(f"db_utils.py: Contratos de câmbio para DI ID {declaracao_id} adicionados ao batch para exclusão no Firestore.")

                # Deleta frete internacional associado (assumindo que o ID é a referência do processo)
                di_data_temp = get_declaracao_by_id(declaracao_id) # Buscar a DI para pegar a referencia_processo
                if di_data_temp and di_data_temp.get('informacao_complementar'):
                    frete_int_ref_doc = frete_internacional_ref.document(di_data_temp['informacao_complementar'])
                    batch.delete(frete_int_ref_doc)
                    logger.debug(f"db_utils.py: Frete internacional para referência {di_data_temp['informacao_complementar']} adicionado ao batch para exclusão no Firestore.")

                # --- NOVO: Desvincular a DI do processo correspondente ---
                referencia_processo_da_di = di_data_temp.get('informacao_complementar') if di_data_temp else None
                if referencia_processo_da_di and referencia_processo_da_di != "N/A":
                    processo_doc_ref = processos_ref_firestore.document(_clean_reference_string(referencia_processo_da_di))
                    processo_doc = processo_doc_ref.get()
                    if processo_doc.exists:
                        # Se o processo existe e está vinculado a esta DI, remova a vinculação
                        if processo_doc.to_dict().get('DI_ID_Vinculada') == str(declaracao_id):
                            processo_doc.reference.update({"DI_ID_Vinculada": None})
                            logger.info(f"db_utils.py: Vinculação da DI {declaracao_id} removida do processo '{referencia_processo_da_di}'.")
                # --- FIM NOVO ---

                batch.commit()
                logger.info(f"db_utils.py: Declaração ID {declaracao_id} e dados relacionados excluídos com sucesso do Firestore.")
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao excluir declaração ID {declaracao_id} e dados relacionados do Firestore: {e}", exc_info=True)
                success_firestore = False
        else:
            logger.error(f"db_utils.py: Falha ao obter referências de coleção para deletar dados relacionados à DI.")
            success_firestore = False
    else:
        logger.warning(f"db_utils.py: Firestore client não inicializado ou desabilitado. Não é possível deletar declaração e dados relacionados.")
        success_firestore = False
    return success_firestore

def update_declaracao(declaracao_id: Any, di_data: Dict[str, Any]):
    """Atualiza uma declaração. SOMENTE Firestore."""
    logger.info(f"db_utils.py: Iniciando atualização da declaração ID: {declaracao_id}")
    success_firestore = True

    if db_firestore:
        logger.info(f"db_utils.py: Tentando atualizar declaração ID {declaracao_id} no Firestore.")
        declaracoes_ref_firestore = get_firestore_collection_ref("xml_declaracoes")
        if declaracoes_ref_firestore:
            try:
                # Firestore ID é o numero_di
                current_di_firestore_id = str(declaracao_id)

                doc_ref = declaracoes_ref_firestore.document(current_di_firestore_id)
                doc_ref.update(di_data)
                logger.info(f"db_utils.py: Declaração {current_di_firestore_id} (Firestore ID) atualizada com sucesso no Firestore.")
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao atualizar declaração ID {declaracao_id} no Firestore: {e}")
                success_firestore = False
        else:
            logger.error(f"db_utils.py: Falha ao obter referência da coleção 'xml_declaracoes' no Firestore para atualizar.")
            success_firestore = False
    else:
        logger.warning(f"db_utils.py: Firestore client não inicializado ou desabilitado. Não é possível atualizar declaração.")
        success_firestore = False
    return success_firestore

def update_declaracao_field(declaracao_id: Any, field_name: str, new_value: Any):
    """
    Updates a single field for a given declaracao_id. SOMENTE Firestore.
    """
    logger.info(f"db_utils.py: Atualizando campo '{field_name}' para declaração ID {declaracao_id} com valor '{new_value}'.")
    success_firestore = True

    allowed_fields = [
        'numero_di', 'data_registro', 'valor_total_reais_xml', 'arquivo_origem',
        'data_importacao', 'informacao_complementar', 'vmle', 'frete', 'seguro',
        'vmld', 'ipi', 'pis_pasep', 'cofins', 'icms_sc', 'taxa_cambial_usd',
        'taxa_siscomex', 'numero_invoice', 'peso_bruto', 'peso_liquido',
        'cnpj_importador', 'importador_nome', 'recinto', 'embalagem',
        'quantidade_volumes', 'acrescimo', 'imposto_importacao', 'armazenagem',
        'frete_nacional'
    ]
    if field_name not in allowed_fields:
        logger.error(f"db_utils.py: Tentativa de atualizar campo não permitido: {field_name}")
        return False

    if db_firestore:
        logger.info("db_utils.py: Usando Firestore para atualizar campo da declaração.")
        declaracoes_ref_firestore = get_firestore_collection_ref("xml_declaracoes")
        if declaracoes_ref_firestore:
            try:
                # Firestore ID é o numero_di
                current_di_firestore_id = str(declaracao_id)

                doc_ref = declaracoes_ref_firestore.document(current_di_firestore_id)
                doc_ref.update({field_name: new_value})
                logger.info(f"db_utils.py: Campo '{field_name}' da declaração {current_di_firestore_id} (Firestore ID) atualizado para '{new_value}' no Firestore.")
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao atualizar campo '{field_name}' para declaração ID {declaracao_id} no Firestore: {e}")
                success_firestore = False
        else:
            logger.error(f"db_utils.py: Falha ao obter referência da coleção 'xml_declaracoes' no Firestore para atualizar campo.")
            success_firestore = False
    else:
        logger.warning(f"db_utils.py: Firestore client não inicializado ou desabilitado. Não é possível atualizar campo da declaração.")
        success_firestore = False
    return success_firestore


def inserir_ou_atualizar_produto(produto: Tuple[str, str, str, str]):
    """
    Insere ou atualiza um produto. SOMENTE Firestore.
    produto: (id_key_erp, nome_part, descricao, ncm)
    """
    id_key_erp = produto[0]
    logger.info(f"db_utils.py: Inserindo/Atualizando produto com ID/Key ERP: {id_key_erp}")
    success_firestore = True

    data = {
        "id_key_erp": produto[0],
        "nome_part": produto[1],
        "descricao": produto[2],
        "ncm": produto[3]
    }

    if db_firestore:
        logger.info("db_utils.py: Usando Firestore para inserir/atualizar produto.")
        produtos_ref = get_firestore_collection_ref("produtos")
        if produtos_ref:
            try:
                produtos_ref.document(id_key_erp).set(data)
                logger.info(f"db_utils.py: Produto com ID/Key ERP '{id_key_erp}' inserido/atualizado com sucesso no Firestore.")
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao inserir/atualizar produto com ID/Key ERP '{id_key_erp}' no Firestore: {e}")
                success_firestore = False
        else:
            logger.error(f"db_utils.py: Falha ao obter referência da coleção 'produtos' no Firestore para inserir/atualizar.")
            success_firestore = False
    else:
        logger.warning(f"db_utils.py: Firestore client não inicializado ou desabilitado. Não é possível inserir/atualizar produto.")
        success_firestore = False
    return success_firestore

def selecionar_todos_produtos() -> List[Dict[str, Any]]:
    """
    Seleciona todos os produtos. SOMENTE Firestore.
    """
    logger.info(f"db_utils.py: Selecionando todos os produtos.")
    if db_firestore:
        logger.info(f"db_utils.py: Usando Firestore para selecionar todos os produtos.")
        produtos_ref = get_firestore_collection_ref("produtos")
        if not produtos_ref:
            logger.error(f"db_utils.py: Falha ao obter referência da coleção 'produtos' no Firestore para selecionar todos.")
            return []
        try:
            docs = produtos_ref.order_by("id_key_erp").order_by("nome_part").stream()
            produtos = [doc.to_dict() for doc in docs]
            logger.info(f"db_utils.py: Obtidos {len(produtos)} produtos do Firestore.")
            return produtos
        except Exception as e:
            logger.error(f"db_utils.py: Erro ao buscar todos os produtos do Firestore: {e}")
            return []
    else:
        logger.warning(f"db_utils.py: Firestore client não inicializado ou desabilitado. Não é possível selecionar todos os produtos.")
    return []

def selecionar_produto_por_id(id_key_erp: str) -> Optional[Dict[str, Any]]:
    """
    Seleciona um produto pelo ID. SOMENTE Firestore.
    """
    logger.info(f"db_utils.py: Selecionando produto por ID/Key ERP: {id_key_erp}")
    if db_firestore:
        logger.info(f"db_utils.py: Usando Firestore para selecionar produto por ID.")
        produtos_ref = get_firestore_collection_ref("produtos")
        if not produtos_ref:
            logger.error(f"db_utils.py: Falha ao obter referência da coleção 'produtos' no Firestore para selecionar por ID.")
            return None
        try:
            doc = produtos_ref.document(id_key_erp).get()
            if doc.exists:
                logger.info(f"db_utils.py: Produto com ID/Key ERP '{id_key_erp}' encontrado no Firestore.")
                return doc.to_dict()
            logger.warning(f"db_utils.py: Produto com ID/Key ERP '{id_key_erp}' não encontrado no Firestore.")
            return None
        except Exception as e:
            logger.error(f"db_utils.py: Erro ao buscar produto com ID/Key ERP '{id_key_erp}' no Firestore: {e}")
            return None
    else:
        logger.warning(f"db_utils.py: Firestore client não inicializado ou desabilitado. Não é possível selecionar produto por ID.")
    return None

def seleccionar_produtos_por_ids(ids: List[str]):
    """
    Seleciona produtos por uma lista de IDs. SOMENTE Firestore.
    """
    logger.info(f"db_utils.py: Selecionando produtos por IDs: {ids}")
    if not ids:
        logger.info(f"db_utils.py: Lista de IDs vazia para selecionar produtos.")
        return []

    if db_firestore:
        logger.info(f"db_utils.py: Usando Firestore para selecionar produtos por IDs.")
        produtos_ref = get_firestore_collection_ref("produtos")
        if not produtos_ref:
            logger.error(f"db_utils.py: Falha ao obter referência da coleção 'produtos' no Firestore para selecionar por IDs.")
            return []
        try:
            # Firestore tem um limite de 10 IDs para consultas 'in'
            if len(ids) > 10:
                logger.warning(f"db_utils.py: Query por múltiplos IDs no Firestore com mais de 10 IDs. Retornando apenas os 10 primeiros.")
                docs = produtos_ref.where(firestore.FieldPath.document_id(), 'in', ids[:10]).stream()
            else:
                docs = produtos_ref.where(firestore.FieldPath.document_id(), 'in', ids).stream()

            produtos_dict = {doc.id: doc.to_dict() for doc in docs}
            produtos_ordenados = [produtos_dict[id] for id in ids if id in produtos_dict]
            logger.info(f"db_utils.py: Obtidos {len(produtos_ordenados)} produtos por IDs do Firestore.")
            return produtos_ordenados
        except Exception as e:
            logger.error(f"db_utils.py: Erro ao buscar produtos por IDs no Firestore: {e}")
            return []
    else:
        logger.warning(f"db_utils.py: Firestore client não inicializado ou desabilitado. Não é possível selecionar produtos por IDs.")
    return []

def deletar_produto(id_key_erp: str):
    """
    Deleta um produto. SOMENTE Firestore.
    """
    logger.info(f"db_utils.py: Deletando produto com ID/Key ERP: {id_key_erp}")
    success_firestore = True

    if db_firestore:
        logger.info(f"db_utils.py: Usando Firestore para deletar produto.")
        produtos_ref = get_firestore_collection_ref("produtos")
        if produtos_ref:
            try:
                doc_ref = produtos_ref.document(id_key_erp)
                doc = doc_ref.get()
                if doc.exists:
                    doc_ref.delete()
                    logger.info(f"db_utils.py: Produto com ID/Key ERP '{id_key_erp}' excluído com sucesso do Firestore.")
                else:
                    logger.warning(f"db_utils.py: Produto com ID/Key ERP '{id_key_erp}' não encontrado no Firestore para exclusão.")
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao excluir produto com ID/Key ERP '{id_key_erp}' do Firestore: {e}")
                success_firestore = False
        else:
            logger.error(f"db_utils.py: Falha ao obter referência da coleção 'produtos' no Firestore para deletar.")
            success_firestore = False
    else:
        logger.warning(f"db_utils.py: Firestore client não inicializado ou desabilitado. Não é possível deletar produto.")
        success_firestore = False
    return success_firestore


# Funções para Frete Internacional (SOMENTE Firestore)
def inserir_ou_atualizar_frete_internacional(frete_data: Dict[str, Any]):
    """
    Insere ou atualiza um registro de frete internacional. SOMENTE Firestore.
    frete_data deve conter 'referencia_processo' como chave primária.
    """
    referencia_processo = frete_data.get('referencia_processo')
    if not referencia_processo:
        logger.error("db_utils.py: Referência de processo não fornecida para salvar frete internacional. Abortando.")
        return False

    logger.info(f"db_utils.py: Inserindo/Atualizando frete internacional para referência: {referencia_processo}")
    success_firestore = True

    if db_firestore:
        logger.info(f"db_utils.py: Usando Firestore para inserir/atualizar frete internacional.")
        frete_ref = get_firestore_collection_ref("frete_internacional")
        if frete_ref:
            try:
                doc_ref = frete_ref.document(referencia_processo)
                doc_ref.set(frete_data, merge=True)
                logger.info(f"db_utils.py: Frete internacional para '{referencia_processo}' inserido/atualizado com sucesso no Firestore.")
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao inserir/atualizar frete internacional para '{referencia_processo}' no Firestore: {e}")
                success_firestore = False
        else:
            logger.error(f"db_utils.py: Falha ao obter referência da coleção 'frete_internacional' no Firestore.")
            success_firestore = False
    else:
        logger.warning(f"db_utils.py: Firestore client não inicializado ou desabilitado. Não é possível inserir/atualizar frete internacional.")
        success_firestore = False
    return success_firestore


def get_frete_internacional_by_referencia(referencia_processo: str) -> Optional[Dict[str, Any]]:
    """
    Busca um registro de frete internacional pela referência do processo. SOMENTE Firestore.
    """
    logger.info(f"db_utils.py: Buscando frete internacional por referência: {referencia_processo}")
    if db_firestore:
        logger.info("db_utils.py: Usando Firestore para buscar frete internacional.")
        frete_ref = get_firestore_collection_ref("frete_internacional")
        if not frete_ref:
            logger.error(f"db_utils.py: Falha ao acessar coleção 'frete_internacional' no Firestore para buscar frete.")
            return None
        try:
            doc_ref = frete_ref.document(referencia_processo)
            doc = doc_ref.get()
            if doc.exists:
                data = doc.to_dict()
                logger.info(f"db_utils.py: Frete internacional para '{referencia_processo}' encontrado no Firestore.")
                return data
            else:
                logger.warning(f"db_utils.py: Frete internacional para '{referencia_processo}' não encontrado no Firestore.")
                return None
        except Exception as e:
            logger.error(f"Erro ao buscar frete internacional para '{referencia_processo}' no Firestore: {e}")
            return None
    else:
        logger.warning(f"db_utils.py: Firestore client não inicializado ou desabilitado. Não é possível buscar frete internacional.")
    return None

def get_all_xml_declaracoes_with_costs_from_firestore():
    """
    Obtém todas as declarações XML do Firestore e tenta unir com seus dados de custo
    (armazenagem, frete nacional) da coleção 'processo_dados_custo' e
    frete internacional da coleção 'frete_internacional'. SOMENTE Firestore.
    Retorna uma lista de dicionários, cada um representando uma declaração
    com os dados de custo agregados.
    """
    logger.info("db_utils.py: Obtendo todas as declarações XML com dados de custo do Firestore.")
    if not db_firestore:
        logger.error("Firestore não está pronto para obter dados.")
        return []

    declaracoes_data = []
    try:
        declaracoes_ref = get_firestore_collection_ref("xml_declaracoes")
        if not declaracoes_ref:
            logger.error("db_utils.py: Falha ao acessar coleção 'xml_declaracoes' no Firestore.")
            return []

        # Fetch all XML declarations
        docs = declaracoes_ref.stream()
        for doc in docs:
            data = doc.to_dict()
            data['id'] = doc.id  # O ID do documento em 'xml_declaracoes' é o numero_di
            declaracoes_data.append(data)
        logger.info(f"db_utils.py: Obtidas {len(declaracoes_data)} declarações XML.")

        # Convert to DataFrame for easier merging
        df_declaracoes = pd.DataFrame(declaracoes_data)
        if df_declaracoes.empty:
            return []

        # Ensure 'id' (numero_di) and 'informacao_complementar' are strings for merging
        df_declaracoes['id'] = df_declaracoes['id'].astype(str)
        df_declaracoes['informacao_complementar'] = df_declaracoes['informacao_complementar'].astype(str)

        # --- Fetch Process Cost Data (armazenagem, frete_nacional from processo_dados_custo) ---
        processo_dados_custo_ref = get_firestore_collection_ref("processo_dados_custo")
        if processo_dados_custo_ref:
            cost_docs = processo_dados_custo_ref.stream()
            costs_data = []
            for doc in cost_docs:
                cost_dict = doc.to_dict()
                cost_dict['declaracao_id_custo'] = doc.id # The document ID is the declaracao_id (numero_di)
                costs_data.append(cost_dict)
            df_costs = pd.DataFrame(costs_data)
            if not df_costs.empty:
                df_costs['declaracao_id_custo'] = df_costs['declaracao_id_custo'].astype(str)
                df_declaracoes = pd.merge(df_declaracoes, df_costs[['declaracao_id_custo', 'armazenagem', 'frete_nacional']],
                                          left_on='id', right_on='declaracao_id_custo', how='left')
                df_declaracoes.drop(columns=['declaracao_id_custo'], inplace=True) # Drop redundant ID column
            else:
                logger.info("db_utils.py: Nenhuns dados de custo de processo encontrados na coleção 'processo_dados_custo'.")
        else:
            logger.warning("db_utils.py: Coleção 'processo_dados_custo' não acessível ou não existe.")

        # --- Fetch International Freight Data (from frete_internacional) ---
        # Assuming frete_internacional is linked by 'referencia_processo' which is 'informacao_complementar' in xml_declaracoes
        frete_internacional_ref = get_firestore_collection_ref("frete_internacional")
        if frete_internacional_ref:
            frete_docs = frete_internacional_ref.stream()
            frete_data = []
            for doc in frete_docs:
                frete_dict = doc.to_dict()
                frete_dict['referencia_processo_id'] = doc.id # The document ID is 'referencia_processo'
                frete_data.append(frete_dict)
            df_frete = pd.DataFrame(frete_data)
            if not df_frete.empty:
                df_frete['referencia_processo_id'] = df_frete['referencia_processo_id'].astype(str)
                # O valor que queremos do frete internacional pode ser 'valor_usd' ou similar
                # Certifique-se de que o nome da coluna aqui corresponde ao que você salva
                df_declaracoes = pd.merge(df_declaracoes, df_frete[['referencia_processo_id', 'valor_usd']], # Assumindo 'valor_usd' é o campo
                                          left_on='informacao_complementar', right_on='referencia_processo_id', how='left',
                                          suffixes=('', '_frete_int'))
                df_declaracoes.rename(columns={'valor_usd_frete_int': 'frete_internacional_valor'}, inplace=True)
                df_declaracoes.drop(columns=['referencia_processo_id'], inplace=True) # Drop redundant ID column
            else:
                logger.info("db_utils.py: Nenhuns dados de frete internacional encontrados na coleção 'frete_internacional'.")
        else:
            logger.warning("db_utils.py: Coleção 'frete_internacional' não acessível ou não existe.")

        # Fill NaNs for new cost columns with 0.0 after merging
        df_declaracoes['armazenagem'] = pd.to_numeric(df_declaracoes.get('armazenagem', 0), errors='coerce').fillna(0.0)
        df_declaracoes['frete_nacional'] = pd.to_numeric(df_declaracoes.get('frete_nacional', 0), errors='coerce').fillna(0.0)
        df_declaracoes['frete_internacional_valor'] = pd.to_numeric(df_declaracoes.get('frete_internacional_valor', 0), errors='coerce').fillna(0.0)

        # Certifique-se de que os campos de status e previsão existem ou adicione-os com valores padrão
        if 'Status_Geral' not in df_declaracoes.columns:
            df_declaracoes['Status_Geral'] = 'Não Definido'
        if 'Previsao_Pichau' not in df_declaracoes.columns:
            df_declaracoes['Previsao_Pichau'] = ''

        # Convert back to list of dictionaries for consistency with previous function's return type
        return df_declaracoes.to_dict(orient='records')

    except Exception as e:
        logger.error(f"Erro ao obter declarações XML com dados de custo do Firestore: {e}", exc_info=True)
        return []

