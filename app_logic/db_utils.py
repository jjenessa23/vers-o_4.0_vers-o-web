import logging
import os
import hashlib
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime
import json
import re
import sqlite3
import pandas as pd
import xml.etree.ElementTree as ET

from google.cloud import firestore
from google.oauth2 import service_account

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

logger.info("db_utils.py: Módulo inicializado.")

_USE_FIRESTORE_AS_PRIMARY = True
_SQLITE_ENABLED = False # Alterado para False conforme sua solicitação

logger.info(f"db_utils.py: _USE_FIRESTORE_AS_PRIMARY = {_USE_FIRESTORE_AS_PRIMARY}")
logger.info(f"db_utils.py: _SQLITE_ENABLED = {_SQLITE_ENABLED}")

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
_XML_DI_DB_FILENAME = "analise_xml_di.db"
_USERS_DB_FILENAME = "users.db"
_PRODUTOS_DB_FILENAME = "banco_de_dados_descricao.db"
_NCM_DB_FILENAME = "banco_de_dados_ncm_draft_BL.db"
_PAGAMENTOS_DB_FILENAME = "pagamentos_container.db"
_FOLLOWUP_DB_FILENAME = "followup_importacao.db"
_NCM_IMPOSTOS_DB_FILENAME = "ncm_impostos.db"

_base_path = os.path.dirname(os.path.abspath(__file__))
_app_root_path = os.path.dirname(_base_path) if os.path.basename(_base_path) == 'app_logic' else _base_path

_DB_PATHS_SQLITE = {
    "xml_di": os.path.join(_app_root_path, _DEFAULT_DB_FOLDER, _XML_DI_DB_FILENAME),
    "users": os.path.join(_app_root_path, _DEFAULT_DB_FOLDER, _USERS_DB_FILENAME),
    "produtos": os.path.join(_app_root_path, _DEFAULT_DB_FOLDER, _PRODUTOS_DB_FILENAME),
    "ncm": os.path.join(_app_root_path, _DEFAULT_DB_FOLDER, _NCM_DB_FILENAME),
    "pagamentos": os.path.join(_app_root_path, _DEFAULT_DB_FOLDER, _PAGAMENTOS_DB_FILENAME),
    "followup": os.path.join(_app_root_path, _DEFAULT_DB_FOLDER, _FOLLOWUP_DB_FILENAME),
    "ncm_impostos": os.path.join(_app_root_path, _DEFAULT_DB_FOLDER, _NCM_IMPOSTOS_DB_FILENAME),
}

logger.info(f"db_utils.py: Caminhos SQLite definidos: {_DB_PATHS_SQLITE}")


def get_sqlite_db_path(db_type: str):
    """Retorna o caminho apropriado do banco de dados SQLite para um dado tipo."""
    path = _DB_PATHS_SQLITE.get(db_type)
    logger.debug(f"db_utils.py: get_sqlite_db_path para '{db_type}' retornou: {path}")
    return path

def connect_sqlite_db(db_path: str):
    """Conecta a um banco de dados SQLite."""
    logger.info(f"db_utils.py: Tentando conectar ao DB SQLite em: {db_path}")
    if not db_path:
        logger.error("db_utils.py: Caminho do DB SQLite não definido. Falha na conexão.")
        return None
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        logger.info(f"db_utils.py: Conectado com sucesso ao DB SQLite: {db_path}")
        return conn
    except Exception as e:
        logger.exception(f"db_utils.py: Erro ao conectar ao DB SQLite {db_path}: {e}")
        return None

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
    "followup_processos": "followup_processos",
    "followup_historico_processos": "followup_historico_processos",
    "followup_process_items": "followup_process_items",
    "followup_notifications": "followup_notifications",
    "followup_notification_history": "followup_notification_history",
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

def criar_tabela_users_sqlite(conn: sqlite3.Connection):
    """Cria a tabela 'users' no SQLite se não existir e adiciona a coluna allowed_screens."""
    logger.info("db_utils.py: Iniciando verificação/criação da tabela 'users' (SQLite).")
    try:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                is_admin INTEGER NOT NULL DEFAULT 0,
                allowed_screens TEXT
            )
        ''')
        conn.commit()
        logger.info("db_utils.py: Tabela 'users' (SQLite) verificada/criada com sucesso.")

        cursor.execute("PRAGMA table_info(users)")
        colunas = [info[1] for info in cursor.fetchall()]
        if 'allowed_screens' not in colunas:
            try:
                cursor.execute("ALTER TABLE users ADD COLUMN allowed_screens TEXT")
                conn.commit()
                logger.info("db_utils.py: Coluna 'allowed_screens' adicionada à tabela 'users' (SQLite).")
            except sqlite3.Error as e:
                logger.error(f"db_utils.py: Erro SQLite ao adicionar coluna 'allowed_screens': {e}")
            except Exception as e:
                 logger.exception("db_utils.py: Erro inesperado ao adicionar coluna 'allowed_screens' (SQLite)")

        cursor.execute("SELECT COUNT(*) FROM users")
        count = cursor.fetchone()[0]
        if count == 0:
             admin_username = "admin"
             admin_password_hash = hash_password("admin", admin_username)
             all_screens_default = "Home,Descrições,Listagem NCM,Follow-up Importação,Importar XML DI,Pagamentos,Custo do Processo,Cálculo Portonave,Análise de Documentos,Pagamentos Container,Cálculo de Tributos TTCE,Gerenciamento de Usuários,Cálculo Frete Internacional,Análise de Faturas/PL (PDF),Cálculo Futura,Cálculo Pac Log - Elo,Cálculo Fechamento,Cálculo FN Transportes"
             try:
                  cursor.execute("INSERT INTO users (username, password_hash, is_admin, allowed_screens) VALUES (?, ?, ?, ?)",
                                 (admin_username, admin_password_hash, 1, all_screens_default))
                  conn.commit()
                  logger.info("db_utils.py: Usuário admin padrão criado no SQLite com acesso a todas as telas.")
             except sqlite3.IntegrityError:
                  logger.warning("db_utils.py: Tentativa de criar usuário admin padrão, mas 'admin' já existe no SQLite.")
                  conn.rollback()
             except Exception as e:
                  logger.exception("db_utils.py: Erro ao criar usuário admin padrão no SQLite.")
                  conn.rollback()
        return True
    except Exception as e:
        logger.exception("db_utils.py: Erro ao criar ou atualizar a tabela 'users' (SQLite)")
        conn.rollback()
        return False

def criar_tabela_ncm_impostos_sqlite(conn: sqlite3.Connection):
    """Cria a tabela 'ncm_impostos_items' no SQLite se não existir."""
    logger.info("db_utils.py: Iniciando verificação/criação da tabela 'ncm_impostos_items' (SQLite).")
    try:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ncm_impostos_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ncm_code TEXT UNIQUE NOT NULL,
                descricao_item TEXT NOT NULL,
                ii_aliquota REAL,
                ipi_aliquota REAL,
                pis_aliquota REAL,
                cofins_aliquota REAL,
                icms_aliquota REAL
            )
        ''')
        conn.commit()
        logger.info("db_utils.py: Tabela 'ncm_impostos_items' (SQLite) verificada/criada com sucesso.")
        return True
    except Exception as e:
        logger.exception("db_utils.py: Erro ao criar ou atualizar a tabela 'ncm_impostos_items' (SQLite)")
        conn.rollback()
        return False

def criar_tabela_xml_di_sqlite(conn: sqlite3.Connection):
    """Cria as tabelas XML DI e Custo no SQLite se não existirem."""
    logger.info("db_utils.py: Iniciando verificação/criação das tabelas XML DI e Custo (SQLite).")
    try:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS xml_declaracoes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                numero_di TEXT UNIQUE,
                data_registro TEXT,
                valor_total_reais_xml REAL,
                arquivo_origem TEXT,
                data_importacao TEXT,
                informacao_complementar TEXT,
                vmle REAL,
                frete REAL,
                seguro REAL,
                vmld REAL,
                ipi REAL,
                pis_pasep REAL,
                cofins REAL,
                icms_sc TEXT,
                taxa_cambial_usd REAL,
                taxa_siscomex REAL,
                numero_invoice TEXT,
                peso_bruto REAL,
                peso_liquido REAL,
                cnpj_importador TEXT,
                importador_nome TEXT,
                recinto TEXT,
                embalagem TEXT,
                quantidade_volumes INTEGER,
                acrescimo REAL,
                imposto_importacao REAL,
                armazenagem REAL,
                frete_nacional REAL
            )
        ''')
        conn.commit()

        cursor.execute("PRAGMA table_info(xml_declaracoes)")
        columns = [info[1] for info in cursor.fetchall()]
        if 'armazenagem' not in columns:
            try:
                cursor.execute("ALTER TABLE xml_declaracoes ADD COLUMN armazenagem REAL")
                conn.commit()
                logger.info("db_utils.py: Coluna 'armazenagem' adicionada à tabela 'xml_declaracoes' (SQLite).")
            except sqlite3.Error as e:
                logger.error(f"db_utils.py: Erro SQLite ao adicionar coluna 'armazenagem': {e}")
                conn.rollback()
        
        if 'frete_nacional' not in columns:
            try:
                cursor.execute("ALTER TABLE xml_declaracoes ADD COLUMN frete_nacional REAL")
                conn.commit()
                logger.info("db_utils.py: Coluna 'frete_nacional' adicionada à tabela 'xml_declaracoes' (SQLite).")
            except sqlite3.Error as e:
                logger.error(f"db_utils.py: Erro SQLite ao adicionar coluna 'frete_nacional': {e}")
                conn.rollback()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS xml_itens (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                declaracao_id INTEGER,
                numero_adicao TEXT,
                numero_item_sequencial TEXT,
                descricao_mercadoria TEXT,
                quantidade REAL,
                unidade_medida TEXT,
                valor_unitario REAL,
                valor_item_calculado REAL,
                peso_liquido_item REAL,
                ncm_item TEXT,
                sku_item TEXT,
                custo_unit_di_usd REAL,
                ii_percent_item REAL,
                ipi_percent_item REAL,
                pis_percent_item REAL,
                cofins_percent_item REAL,
                icms_percent_item REAL,
                codigo_erp_item TEXT,
                FOREIGN KEY (declaracao_id) REFERENCES xml_declaracoes(id) ON DELETE CASCADE
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processo_dados_custo (
                declaracao_id INTEGER PRIMARY KEY,
                afrmm REAL,
                siscoserv REAL,
                descarregamento REAL,
                taxas_destino REAL,
                multa REAL,
                FOREIGN KEY (declaracao_id) REFERENCES xml_declaracoes(id) ON DELETE CASCADE
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processo_contratos_cambio (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                declaracao_id INTEGER,
                numero_contrato TEXT,
                dolar_cambio REAL,
                valor_usd REAL,
                FOREIGN KEY (declaracao_id) REFERENCES xml_declaracoes(id) ON DELETE CASCADE
            )
        ''')
        conn.commit()
        logger.info("db_utils.py: Tabelas XML DI e Custo (SQLite) verificadas/criadas.")
        return True
    except Exception as e:
        logger.error(f"db_utils.py: Erro ao criar tabelas XML DI/Custo (SQLite): {e}")
        conn.rollback()
        return False


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
                    "Home", "Descrições", "Listagem NCM", "Follow-up Importação",
                    "Importar XML DI", "Pagamentos", "Custo do Processo",
                    "Cálculo Portonave", "Análise de Documentos", "Pagamentos Container",
                    "Cálculo de Tributos TTCE", "Gerenciamento de Usuários",
                    "Cálculo Frete Internacional", "Análise de Faturas/PL (PDF)",
                    "Cálculo Futura", "Cálculo Pac Log - Elo", "Cálculo Fechamento",
                    "Cálculo FN Transportes", "Produtos", "Formulário Processo",
                    "Clonagem de Processo"
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
    Gerencia a criação de diretórios e tabelas para o SQLite, e inicializa dados para o Firestore.
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
            # Não defina success = False aqui para permitir que o app continue se o Firestore estiver habilitado.
            # A falha na criação do diretório de dados para o SQLite não deve impedir o Firestore.
    else:
        logger.info(f"db_utils.py: Diretório de dados '{data_dir}' já existe.")

    # --- SQLITE TABLE CREATION ---
    if _SQLITE_ENABLED:
        logger.info("db_utils.py: SQLite está HABILITADO. Iniciando criação/verificação de tabelas SQLite.")
        
        # Lista de funções de criação de tabela SQLite
        sqlite_table_creation_functions = [
            ("users", criar_tabela_users_sqlite),
            ("ncm_impostos", criar_tabela_ncm_impostos_sqlite),
            ("xml_di", criar_tabela_xml_di_sqlite),
            ("produtos", lambda conn: _create_produtos_table_sqlite(conn)), # Usar lambda para passar conn
            ("ncm", lambda conn: _create_ncm_table_sqlite(conn)), # Usar lambda para passar conn
            ("pagamentos", lambda conn: _create_pagamentos_table_sqlite(conn)) # Usar lambda para passar conn
        ]

        # Função auxiliar para criar a tabela de produtos SQLite
        def _create_produtos_table_sqlite(conn: sqlite3.Connection):
            try:
                cursor = conn.cursor()
                _COLS_MAP_PRODUTOS_STRUCT = {
                    "id": {"text": "ID/Key ERP", "width": 120, "col_id": "id_key_erp"},
                    "nome": {"text": "Nome/Part", "width": 200, "col_id": "nome_part"},
                    "desc": {"text": "Descrição", "width": 350, "col_id": "descricao"},
                    "ncm": {"text": "NCM", "width": 100, "col_id": "ncm"}
                }
                cursor.execute(f'''
                    CREATE TABLE IF NOT EXISTS produtos (
                        {_COLS_MAP_PRODUTOS_STRUCT['id']['col_id']} TEXT PRIMARY KEY,
                        {_COLS_MAP_PRODUTOS_STRUCT['nome']['col_id']} TEXT,
                        {_COLS_MAP_PRODUTOS_STRUCT['desc']['col_id']} TEXT,
                        {_COLS_MAP_PRODUTOS_STRUCT['ncm']['col_id']} TEXT
                    )
                ''')
                conn.commit()
                logger.info("db_utils.py: Tabela Produtos (SQLite) verificada/criada.")
                return True
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao criar tabela Produtos (SQLite): {e}")
                conn.rollback()
                return False

        # Função auxiliar para criar a tabela NCM SQLite
        def _create_ncm_table_sqlite(conn: sqlite3.Connection):
            try:
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS ncm_items (
                        item_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        item_name TEXT NOT NULL,
                        item_ncm TEXT,
                        parent_id INTEGER,
                        FOREIGN KEY(parent_id) REFERENCES ncm_items(item_id) ON DELETE CASCADE
                    )
                ''')
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_ncm_parent ON ncm_items (parent_id);")
                conn.commit()
                logger.info("db_utils.py: Tabela NCM (SQLite) verificada/criada.")
                return True
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao criar tabela NCM (SQLite): {e}")
                conn.rollback()
                return False
        
        # Função auxiliar para criar a tabela Pagamentos SQLite
        def _create_pagamentos_table_sqlite(conn: sqlite3.Connection):
            try:
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS pagamentos_container (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        DATA TEXT,
                        NOME TEXT,
                        QUANTIDADE INTEGER
                    )
                ''')
                conn.commit()
                logger.info("db_utils.py: Tabela Pagamentos (SQLite) verificada/criada.")
                return True
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao criar tabela Pagamentos (SQLite): {e}")
                conn.rollback()
                return False

        for db_type, create_func in sqlite_table_creation_functions:
            conn_sqlite = connect_sqlite_db(get_sqlite_db_path(db_type))
            if conn_sqlite:
                try:
                    if not create_func(conn_sqlite):
                        success = False
                except Exception as e:
                    logger.error(f"db_utils.py: Erro ao criar tabela {db_type} (SQLite): {e}")
                    if conn_sqlite: conn_sqlite.rollback()
                    success = False
                finally:
                    if conn_sqlite: conn_sqlite.close()
            else:
                logger.error(f"db_utils.py: Falha na conexão com o DB de {db_type} para criação de tabela (SQLite).")
                # Se a conexão falhar, isso é um problema. Mas não vamos parar o app todo.
                success = False

        try:
            logger.info("db_utils.py: Tentando importar followup_db_manager para criação de tabelas Follow-up (SQLite).")
            import followup_db_manager
            if not followup_db_manager.criar_tabela_followup():
                success = False
                logger.error("db_utils.py: Falha ao criar tabelas Follow-up via followup_db_manager (SQLite).")
            else:
                logger.info("db_utils.py: Tabelas Follow-up (SQLite) verificadas/criadas via followup_db_manager.")
        except ImportError:
            logger.warning("db_utils.py: Módulo 'followup_db_manager' não encontrado. As tabelas de Follow-up (SQLite) não serão criadas/verificadas.")
            # Não setar success = False aqui, pois o módulo pode não ser essencial se o SQLite for apenas um backup
        except Exception as e:
            logger.exception(f"db_utils.py: Erro inesperado ao lidar com followup_db_manager para SQLite: {e}")
            success = False
    else:
        logger.info("db_utils.py: SQLite está DESABILITADO. Ignorando criação/verificação de tabelas SQLite.")


    # --- FIRESTORE INITIAL DATA CREATION ---
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


def verify_credentials(username: str, password: str) -> Optional[Dict[str, Any]]:
    """Verifica as credenciais do usuário. Prefere Firestore se for primário."""
    logger.info(f"db_utils.py: Verificando credenciais para o usuário: {username}")
    if _USE_FIRESTORE_AS_PRIMARY and db_firestore:
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
    elif _SQLITE_ENABLED:
        logger.info("db_utils.py: Usando SQLite para verificar credenciais.")
        conn = connect_sqlite_db(get_sqlite_db_path("users"))
        if not conn:
            logger.error("db_utils.py: Falha na conexão com o DB de usuários para verificação de credenciais (SQLite).")
            return None
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT username, password_hash, is_admin, allowed_screens FROM users WHERE username = ?", (username,))
            user_data = cursor.fetchone()
            if user_data:
                db_username, stored_password_hash, is_admin, allowed_screens_str = user_data
                provided_password_hash = hash_password(password, db_username)
                if provided_password_hash == stored_password_hash:
                    logger.info(f"db_utils.py: Login bem-sucedido para o usuário: {username} (SQLite)")
                    allowed_screens_list = allowed_screens_str.split(',') if allowed_screens_str else []
                    return {'username': db_username, 'is_admin': bool(is_admin), 'allowed_screens': allowed_screens_list}
                else:
                    logger.warning(f"db_utils.py: Tentativa de login falhou para o usuário {username}: Senha incorreta (SQLite).")
                    return False
            else:
                logger.warning(f"db_utils.py: Tentativa de login falhou: Usuário '{username}' não encontrado (SQLite).")
                return False
        except Exception as e:
            logger.error(f"db_utils.py: Erro ao verificar credenciais para o usuário {username} (SQLite): {e}")
            return None
        finally:
            if conn: conn.close()
    else:
        logger.warning("db_utils.py: Nenhuma opção de DB disponível ou primário falhou e não há fallback para verificar credenciais.")
    return None

def get_all_users() -> List[Dict[str, Any]]:
    """Obtém todos os usuários. Prefere Firestore se for primário."""
    logger.info("db_utils.py: Obtendo todos os usuários.")
    if _USE_FIRESTORE_AS_PRIMARY and db_firestore:
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
    elif _SQLITE_ENABLED:
        logger.info("db_utils.py: Usando SQLite para obter todos os usuários.")
        conn = connect_sqlite_db(get_sqlite_db_path("users"))
        if not conn:
            logger.error("db_utils.py: Falha na conexão com o DB de usuários para obter todos os usuários (SQLite).")
            return []
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT id, username, is_admin, allowed_screens FROM users ORDER BY username ASC")
            users = cursor.fetchall()
            logger.info(f"db_utils.py: Obtidos {len(users)} usuários do SQLite.")
            return [
                {
                    'id': user[0],
                    'username': user[1],
                    'is_admin': bool(user[2]),
                    'allowed_screens': user[3].split(',') if user[3] else []
                } for user in users
            ]
        except Exception as e:
            logger.error(f"db_utils.py: Erro ao obter todos os usuários do DB (SQLite): {e}")
            return []
        finally:
            if conn: conn.close()
    else:
        logger.warning("db_utils.py: Nenhuma opção de DB disponível ou primário falhou para obter todos os usuários.")
    return []

def get_user_by_id_or_username(identifier: Any) -> Optional[Dict[str, Any]]:
    """
    Obtém um único usuário pelo seu ID (SQLite) ou username (Firestore).
    Retorna um dicionário com os dados do usuário, ou None se não encontrado.
    """
    logger.info(f"db_utils.py: Buscando usuário por identificador: {identifier}")
    if _USE_FIRESTORE_AS_PRIMARY and db_firestore:
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
                if isinstance(identifier, int):
                    query_users = users_ref.where('id', '==', identifier).limit(1).get()
                    for doc in query_users:
                        user_data = doc.to_dict()
                        logger.info(f"db_utils.py: Usuário com ID numérico '{identifier}' encontrado no Firestore via query.")
                        return {
                            'id': doc.id,
                            'username': user_data.get('username'),
                            'is_admin': user_data.get('is_admin', False),
                            'allowed_screens': user_data.get('allowed_screens', [])
                        }

                logger.warning(f"db_utils.py: Usuário com identificador '{identifier}' não encontrado no Firestore.")
                return None
        except Exception as e:
            logger.error(f"db_utils.py: Erro ao buscar usuário com identificador '{identifier}' no Firestore: {e}")
            return None
    elif _SQLITE_ENABLED:
        logger.info("db_utils.py: Usando SQLite para buscar usuário por ID ou username.")
        conn = connect_sqlite_db(get_sqlite_db_path("users"))
        if not conn:
            logger.error(f"db_utils.py: Falha na conexão com o DB de usuários para buscar usuário (SQLite).")
            return None
        try:
            cursor = conn.cursor()
            if isinstance(identifier, int):
                cursor.execute("SELECT id, username, is_admin, allowed_screens FROM users WHERE id = ?", (identifier,))
            else:
                cursor.execute("SELECT id, username, is_admin, allowed_screens FROM users WHERE username = ?", (str(identifier),))
            user_data = cursor.fetchone()
            if user_data:
                logger.info(f"db_utils.py: Usuário '{identifier}' encontrado no SQLite.")
                return {
                    'id': user_data[0],
                    'username': user_data[1],
                    'is_admin': bool(user_data[2]),
                    'allowed_screens': user_data[3].split(',') if user_data[3] else []
                }
            else:
                logger.warning(f"db_utils.py: Usuário com identificador '{identifier}' não encontrado no SQLite.")
                return None
        except Exception as e:
            logger.error(f"db_utils.py: Erro ao buscar usuário com identificador '{identifier}' (SQLite): {e}")
            return None
        finally:
            if conn: conn.close()
    else:
        logger.warning(f"db_utils.py: Nenhuma opção de DB disponível ou primário falhou para buscar usuário.")
    return None

def adicionar_ou_atualizar_usuario(user_id: Optional[int], username: str, password_hash: str, is_admin: bool, allowed_screens: List[str]) -> bool:
    """
    Adiciona um novo usuário ou atualiza um existente.
    user_id é o ID numérico (para SQLite) ou None para novo usuário.
    No Firestore, o username é usado como ID do documento.
    """
    logger.info(f"db_utils.py: Adicionando/Atualizando usuário: {username}")
    success_firestore = True
    success_sqlite = True

    user_data = {
        "username": username,
        "password_hash": password_hash,
        "is_admin": is_admin,
        "allowed_screens": allowed_screens
    }

    if _USE_FIRESTORE_AS_PRIMARY and db_firestore:
        logger.info(f"db_utils.py: Usando Firestore para adicionar/atualizar usuário: {username}")
        users_ref = get_firestore_collection_ref("users")
        if users_ref:
            try:
                doc_ref = users_ref.document(username)
                
                existing_doc = doc_ref.get()
                if existing_doc.exists:
                    pass
                
                doc_ref.set(user_data, merge=True)
                logger.info(f"db_utils.py: Usuário '{username}' inserido/atualizado com sucesso no Firestore.")
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao inserir/atualizar usuário '{username}' no Firestore: {e}")
                success_firestore = False
        else:
            logger.error(f"db_utils.py: Falha ao obter referência da coleção 'users' no Firestore.")
            success_firestore = False

    if _SQLITE_ENABLED:
        logger.info(f"db_utils.py: Usando SQLite para adicionar/atualizar usuário: {username}")
        conn_sqlite = connect_sqlite_db(get_sqlite_db_path("users"))
        if conn_sqlite:
            try:
                cursor_sqlite = conn_sqlite.cursor()
                allowed_screens_str = ",".join(allowed_screens)
                
                cursor_sqlite.execute("SELECT id FROM users WHERE username = ?", (username,))
                existing_user_sqlite = cursor_sqlite.fetchone()

                if existing_user_sqlite:
                    if not is_admin:
                        cursor_sqlite.execute("SELECT COUNT(*) FROM users WHERE is_admin = 1")
                        admin_count = cursor_sqlite.fetchone()[0]
                        if admin_count == 1 and existing_user_sqlite[0] == user_id:
                            st.error("Não é possível remover o status de administrador do último usuário administrador (SQLite).")
                            conn_sqlite.rollback()
                            success_sqlite = False
                        else:
                            cursor_sqlite.execute('''
                                UPDATE users
                                SET username = ?, password_hash = ?, is_admin = ?, allowed_screens = ?
                                WHERE id = ?
                            ''', (username, password_hash, 1 if is_admin else 0, allowed_screens_str, user_id))
                            logger.info(f"db_utils.py: Usuário '{username}' (ID: {user_id}) atualizado com sucesso no SQLite.")
                else:
                    cursor_sqlite.execute('''
                        INSERT INTO users (username, password_hash, is_admin, allowed_screens)
                        VALUES (?, ?, ?, ?)
                    ''', (username, password_hash, 1 if is_admin else 0, allowed_screens_str))
                    logger.info(f"db_utils.py: Novo usuário '{username}' inserido com sucesso no SQLite.")
                
                conn_sqlite.commit()
            except sqlite3.IntegrityError:
                logger.error(f"db_utils.py: Erro de integridade: Usuário '{username}' já existe no SQLite.")
                conn_sqlite.rollback()
                success_sqlite = False
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao inserir/atualizar usuário '{username}' no SQLite: {e}")
                conn_sqlite.rollback()
                success_sqlite = False
            finally:
                if conn_sqlite: conn_sqlite.close()
        else:
            logger.error(f"db_utils.py: Falha na conexão com o DB de usuários para adicionar/atualizar (SQLite).")
            success_sqlite = False
    
    return success_firestore and success_sqlite


def atualizar_senha_usuario(user_id: Any, new_password: str, username: str) -> bool:
    """Atualiza a senha de um usuário específico em ambos os DBs."""
    logger.info(f"db_utils.py: Atualizando senha para usuário: {username}")
    success_firestore = True
    success_sqlite = True

    new_password_hash = hash_password(new_password, username)

    if _USE_FIRESTORE_AS_PRIMARY and db_firestore:
        logger.info(f"db_utils.py: Usando Firestore para atualizar senha: {username}")
        users_ref = get_firestore_collection_ref("users")
        if users_ref:
            try:
                doc_ref = users_ref.document(username)
                doc_ref.update({"password_hash": new_password_hash})
                logger.info(f"db_utils.py: Senha do usuário '{username}' atualizada com sucesso no Firestore.")
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao atualizar senha do usuário '{username}' no Firestore: {e}")
                success_firestore = False
        else:
            logger.error(f"db_utils.py: Falha ao obter referência da coleção 'users' no Firestore para atualizar senha.")
            success_firestore = False

    if _SQLITE_ENABLED:
        logger.info(f"db_utils.py: Usando SQLite para atualizar senha: {username}")
        conn_sqlite = connect_sqlite_db(get_sqlite_db_path("users"))
        if conn_sqlite:
            try:
                cursor_sqlite = conn_sqlite.cursor()
                cursor_sqlite.execute("UPDATE users SET password_hash = ? WHERE id = ? OR username = ?", (new_password_hash, user_id, username))
                conn_sqlite.commit()
                if cursor_sqlite.rowcount > 0:
                    logger.info(f"db_utils.py: Senha do usuário '{username}' (ID: {user_id}) atualizada com sucesso no SQLite.")
                else:
                    logger.warning(f"db_utils.py: Usuário '{username}' (ID: {user_id}) não encontrado no SQLite para atualização de senha.")
                    success_sqlite = False
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao atualizar senha do usuário '{username}' no SQLite: {e}")
                conn_sqlite.rollback()
                success_sqlite = False
            finally:
                if conn_sqlite: conn_sqlite.close()
        else:
            logger.error(f"db_utils.py: Falha na conexão com o DB de usuários para atualizar senha (SQLite).")
            success_sqlite = False
    
    return success_firestore and success_sqlite


def deletar_usuario(user_identifier: Any) -> bool:
    """
    Deleta um usuário do banco de dados. Pode receber user_id (int) ou username (str).
    Considera a lógica de ser o último admin.
    """
    logger.info(f"db_utils.py: Deletando usuário: {user_identifier}")
    success_firestore = True
    success_sqlite = True

    user_to_delete = get_user_by_id_or_username(user_identifier)
    if not user_to_delete:
        logger.warning(f"db_utils.py: Usuário '{user_identifier}' não encontrado para exclusão.")
        return False

    all_users = get_all_users()
    admin_users = [u for u in all_users if u.get('is_admin')]

    if user_to_delete.get('is_admin') and len(admin_users) <= 1:
        logger.error(f"db_utils.py: Não é possível excluir o último usuário administrador: {user_to_delete.get('username')}.")
        return False

    if _USE_FIRESTORE_AS_PRIMARY and db_firestore:
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

    if _SQLITE_ENABLED:
        logger.info(f"db_utils.py: Usando SQLite para deletar usuário: {user_to_delete.get('username')}")
        conn_sqlite = connect_sqlite_db(get_sqlite_db_path("users"))
        if conn_sqlite:
            try:
                cursor_sqlite = conn_sqlite.cursor()
                cursor_sqlite.execute("DELETE FROM users WHERE id = ? OR username = ?", (user_to_delete.get('id'), user_to_delete.get('username')))
                conn_sqlite.commit()
                if cursor_sqlite.rowcount > 0:
                    logger.info(f"db_utils.py: Usuário '{user_to_delete.get('username')}' (ID: {user_to_delete.get('id')}) excluído com sucesso do SQLite.")
                else:
                    logger.warning(f"db_utils.py: Usuário '{user_to_delete.get('username')}' (ID: {user_to_delete.get('id')}) não encontrado no SQLite para exclusão.")
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao excluir usuário '{user_to_delete.get('username')}' do SQLite: {e}")
                conn_sqlite.rollback()
                success_sqlite = False
            finally:
                if conn_sqlite: conn_sqlite.close()
        else:
            logger.error(f"db_utils.py: Falha na conexão com o DB de usuários para deletar (SQLite).")
            success_sqlite = False
    
    return success_firestore and success_sqlite


def adicionar_ou_atualizar_ncm_item(ncm_code: str, descricao_item: str, ii_aliquota: float, ipi_aliquota: float, pis_aliquota: float, cofins_aliquota: float, icms_aliquota: float):
    """
    Adiciona/atualiza item NCM. Grava em ambos os bancos de dados se habilitado.
    """
    logger.info(f"db_utils.py: Adicionando/Atualizando item NCM: {ncm_code}")
    success_firestore = True
    success_sqlite = True
    
    data = {
        "ncm_code": ncm_code,
        "descricao_item": descricao_item,
        "ii_aliquota": ii_aliquota,
        "ipi_aliquota": ipi_aliquota,
        "pis_aliquota": pis_aliquota,
        "cofins_aliquota": cofins_aliquota,
        "icms_aliquota": icms_aliquota
    }

    if _USE_FIRESTORE_AS_PRIMARY and db_firestore:
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
    
    if _SQLITE_ENABLED:
        logger.info(f"db_utils.py: Usando SQLite para adicionar/atualizar NCM: {ncm_code}")
        conn_sqlite = connect_sqlite_db(get_sqlite_db_path("ncm_impostos"))
        if conn_sqlite:
            try:
                cursor_sqlite = conn_sqlite.cursor()
                cursor_sqlite.execute("SELECT ncm_code FROM ncm_impostos_items WHERE ncm_code = ?", (ncm_code,))
                if cursor_sqlite.fetchone():
                    cursor_sqlite.execute('''
                        UPDATE ncm_impostos_items
                        SET descricao_item = ?, ii_aliquota = ?, ipi_aliquota = ?, pis_aliquota = ?, cofins_aliquota = ?, icms_aliquota = ?
                        WHERE ncm_code = ?
                    ''', (descricao_item, ii_aliquota, ipi_aliquota, pis_aliquota, cofins_aliquota, icms_aliquota, ncm_code))
                    logger.info(f"db_utils.py: Item NCM '{ncm_code}' atualizado com sucesso no SQLite.")
                else:
                    cursor_sqlite.execute('''
                        INSERT INTO ncm_impostos_items (ncm_code, descricao_item, ii_aliquota, ipi_aliquota, pis_aliquota, cofins_aliquota, icms_aliquota)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (ncm_code, descricao_item, ii_aliquota, ipi_aliquota, pis_aliquota, cofins_aliquota, icms_aliquota))
                    logger.info(f"db_utils.py: Novo item NCM '{ncm_code}' inserido com sucesso no SQLite.")
                conn_sqlite.commit()
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao inserir/atualizar item NCM '{ncm_code}' no SQLite: {e}")
                conn_sqlite.rollback()
                success_sqlite = False
            finally:
                if conn_sqlite: conn_sqlite.close()
        else:
            logger.error(f"db_utils.py: Falha na conexão com o DB de NCM Impostos para adicionar/atualizar (SQLite).")
            success_sqlite = False
    
    return success_firestore and success_sqlite


def selecionar_todos_ncm_itens():
    """
    Seleciona todos os itens NCM. Prefere Firestore se for primário.
    """
    logger.info("db_utils.py: Selecionando todos os itens NCM.")
    if _USE_FIRESTORE_AS_PRIMARY and db_firestore:
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
    elif _SQLITE_ENABLED:
        logger.info("db_utils.py: Usando SQLite para selecionar todos os itens NCM.")
        conn = connect_sqlite_db(get_sqlite_db_path("ncm_impostos"))
        if not conn:
            logger.error(f"db_utils.py: Falha ao acessar DB 'ncm_impostos' no SQLite para obter todos os itens.")
            return []
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT id, ncm_code, descricao_item, ii_aliquota, ipi_aliquota, pis_aliquota, cofins_aliquota, icms_aliquota FROM ncm_impostos_items ORDER BY ncm_code ASC")
            itens = cursor.fetchall()
            logger.info(f"db_utils.py: Obtidos {len(itens)} itens NCM do SQLite.")
            return [dict(item) for item in itens]
        except Exception as e:
            logger.error(f"db_utils.py: Erro ao buscar todos os itens NCM (SQLite): {e}")
            return []
        finally:
            if conn: conn.close()
    else:
        logger.warning(f"db_utils.py: Nenhuma opção de DB disponível ou primário falhou para selecionar todos os itens NCM.")
    return []

def deletar_ncm_item(ncm_id: str):
    """
    Deleta um item NCM. Deleta em ambos os bancos de dados se habilitado.
    """
    logger.info(f"db_utils.py: Deletando item NCM: {ncm_id}")
    success_firestore = True
    success_sqlite = True

    if _USE_FIRESTORE_AS_PRIMARY and db_firestore:
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

    if _SQLITE_ENABLED:
        logger.info(f"db_utils.py: Usando SQLite para deletar NCM: {ncm_id}")
        conn_sqlite = connect_sqlite_db(get_sqlite_db_path("ncm_impostos"))
        if conn_sqlite:
            try:
                cursor_sqlite = conn_sqlite.cursor()
                cursor_sqlite.execute("DELETE FROM ncm_impostos_items WHERE ncm_code = ?", (ncm_id,))
                conn_sqlite.commit()
                if cursor_sqlite.rowcount > 0:
                    logger.info(f"db_utils.py: Item NCM com código '{ncm_id}' excluído com sucesso do SQLite.")
                else:
                    logger.warning(f"db_utils.py: Item NCM com código '{ncm_id}' não encontrado no SQLite para exclusão.")
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao excluir item NCM com código '{ncm_id}' do SQLite: {e}")
                conn_sqlite.rollback()
                success_sqlite = False
            finally:
                if conn_sqlite: conn_sqlite.close()
        else:
            logger.error(f"db_utils.py: Falha na conexão com o DB de NCM Impostos para deletar (SQLite).")
            success_sqlite = False
    
    return success_firestore and success_sqlite


def get_ncm_item_by_ncm_code(ncm_code: str):
    """
    Busca um item NCM pelo seu código NCM. Prefere Firestore se for primário.
    """
    logger.info(f"db_utils.py: Buscando item NCM pelo código: {ncm_code}")
    if _USE_FIRESTORE_AS_PRIMARY and db_firestore:
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
    elif _SQLITE_ENABLED:
        logger.info(f"db_utils.py: Usando SQLite para buscar item NCM.")
        conn = connect_sqlite_db(get_sqlite_db_path("ncm_impostos"))
        if not conn:
            logger.error(f"db_utils.py: Falha ao acessar DB 'ncm_impostos' no SQLite para buscar item.")
            return None
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT id, ncm_code, descricao_item, ii_aliquota, ipi_aliquota, pis_aliquota, cofins_aliquota, icms_aliquota FROM ncm_impostos_items WHERE ncm_code = ?", (ncm_code,))
            item = cursor.fetchone()
            if item:
                logger.info(f"db_utils.py: Item NCM '{ncm_code}' encontrado no SQLite.")
                return dict(item)
            else:
                logger.warning(f"db_utils.py: Item NCM com código '{ncm_code}' não encontrado no SQLite.")
                return None
        except Exception as e:
            logger.error(f"db_utils.py: Erro ao buscar item NCM com código '{ncm_code}' (SQLite): {e}")
            return None
        finally:
            if conn: conn.close()
    else:
        logger.warning(f"db_utils.py: Nenhuma opção de DB disponível ou primário falhou para buscar item NCM.")
    return None

def get_all_declaracoes():
    """Carrega e retorna todos os dados das declarações XML. Prefere Firestore."""
    logger.info("db_utils.py: Obtendo todas as declarações XML.")
    if _USE_FIRESTORE_AS_PRIMARY and db_firestore:
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
    elif _SQLITE_ENABLED:
        logger.info("db_utils.py: Usando SQLite para obter todas as declarações XML.")
        conn = connect_sqlite_db(get_sqlite_db_path("xml_di"))
        if not conn: 
            logger.error(f"db_utils.py: Falha ao conectar ao DB XML DI para obter declarações (SQLite).")
            return []
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, numero_di, data_registro, informacao_complementar, arquivo_origem, data_importacao
                FROM xml_declaracoes ORDER BY data_importacao DESC, numero_di DESC
            """)
            declaracoes = cursor.fetchall()
            logger.info(f"db_utils.py: Obtidas {len(declaracoes)} declarações XML do SQLite.")
            return [dict(d) for d in declaracoes]
        except Exception as e:
            logger.error(f"db_utils.py: Erro DB ao carregar todas as declarações XML DI (SQLite): {e}")
        finally:
            if conn: conn.close()
        return []
    else:
        logger.warning(f"db_utils.py: Nenhuma opção de DB disponível ou primário falhou para obter declarações XML.")
    return []

def get_declaracao_by_id(declaracao_id: Any):
    """Busca uma declaração pelo ID. Prefere Firestore."""
    logger.info(f"db_utils.py: Buscando declaração por ID: {declaracao_id}")
    if _USE_FIRESTORE_AS_PRIMARY and db_firestore:
        logger.info("db_utils.py: Usando Firestore para buscar declaração por ID.")
        declaracoes_ref = get_firestore_collection_ref("xml_declaracoes")
        if not declaracoes_ref: 
            logger.error(f"db_utils.py: Falha ao acessar coleção 'xml_declaracoes' no Firestore para buscar declaração por ID.")
            return None
        try:
            doc = declaracoes_ref.document(str(declaracao_id)).get() 
            if doc.exists:
                data = doc.to_dict()
                data['id'] = doc.id
                logger.info(f"db_utils.py: Declaração ID {declaracao_id} encontrada no Firestore.")
                return data
            logger.warning(f"db_utils.py: Declaração ID {declaracao_id} não encontrada no Firestore.")
            return None
        except Exception as e:
            logger.error(f"db_utils.py: Erro Firestore ao buscar declaração ID {declaracao_id}: {e}")
        return None
    elif _SQLITE_ENABLED:
        logger.info("db_utils.py: Usando SQLite para buscar declaração por ID.")
        conn = connect_sqlite_db(get_sqlite_db_path("xml_di"))
        if not conn: 
            logger.error(f"db_utils.py: Falha ao conectar ao DB XML DI para buscar declaração por ID (SQLite).")
            return None
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, numero_di, data_registro, valor_total_reais_xml, arquivo_origem, data_importacao,
                       informacao_complementar, vmle, frete, seguro, vmld, ipi, pis_pasep, cofins, icms_sc,
                       taxa_cambial_usd, taxa_siscomex, numero_invoice, peso_bruto, peso_liquido,
                       cnpj_importador, importador_nome, recinto, embalagem, quantidade_volumes, acrescimo,
                       imposto_importacao, armazenagem, frete_nacional
                FROM xml_declaracoes WHERE id = ?
            """, (declaracao_id,))
            declaracao = cursor.fetchone()
            if declaracao:
                logger.info(f"db_utils.py: Declaração ID {declaracao_id} encontrada no SQLite.")
                return dict(declaracao)
            else:
                logger.warning(f"db_utils.py: Declaração ID {declaracao_id} não encontrada no SQLite.")
                return None
        except Exception as e:
            logger.error(f"db_utils.py: Erro DB ao buscar declaração ID {declaracao_id} (SQLite): {e}")
        finally:
            if conn: conn.close()
        return None
    else:
        logger.warning(f"db_utils.py: Nenhuma opção de DB disponível ou primário falhou para buscar declaração por ID.")
    return None

def get_declaracao_by_referencia(referencia: str) -> Optional[Dict[str, Any]]:
    """
    Busca uma declaração de importação pela referência (informacao_complementar).
    Prefere Firestore se for primário.
    """
    logger.info(f"db_utils.py: Buscando declaração por referência: {referencia}")
    if _USE_FIRESTORE_AS_PRIMARY and db_firestore:
        logger.info("db_utils.py: Usando Firestore para buscar declaração por referência.")
        declaracoes_ref = get_firestore_collection_ref("xml_declaracoes")
        if not declaracoes_ref: 
            logger.error(f"db_utils.py: Falha ao acessar coleção 'xml_declaracoes' no Firestore para buscar declaração por referência.")
            return None
        try:
            query_val = referencia.strip().upper()
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
    elif _SQLITE_ENABLED:
        logger.info("db_utils.py: Usando SQLite para buscar declaração por referência.")
        conn = connect_sqlite_db(get_sqlite_db_path("xml_di"))
        if not conn: 
            logger.error(f"db_utils.py: Falha ao conectar ao DB XML DI para buscar declaração por referência (SQLite).")
            return None
        try:
            cursor = conn.cursor()
            query_val = referencia.strip().upper()
            cursor.execute("""
                SELECT id, numero_di, data_registro, valor_total_reais_xml, arquivo_origem, data_importacao,
                       informacao_complementar, vmle, frete, seguro, vmld, ipi, pis_pasep, cofins, icms_sc,
                       taxa_cambial_usd, taxa_siscomex, numero_invoice, peso_bruto, peso_liquido,
                       cnpj_importador, importador_nome, recinto, embalagem, quantidade_volumes, acrescimo,
                       imposto_importacao, armazenagem, frete_nacional
                FROM xml_declaracoes WHERE UPPER(TRIM(informacao_complementar)) = ?
            """, (query_val,))
            declaracao = cursor.fetchone()
            if declaracao:
                logger.info(f"db_utils.py: Declaração com referência '{referencia}' encontrada no SQLite.")
                return dict(declaracao)
            else:
                logger.warning(f"db_utils.py: Declaração com referência '{referencia}' não encontrada no SQLite.")
                return None
        except Exception as e:
            logger.error(f"db_utils.py: Erro DB ao buscar declaração por referência '{referencia}' (SQLite): {e}")
        finally:
            if conn: conn.close()
        return None
    else:
        logger.warning(f"db_utils.py: Nenhuma opção de DB disponível ou primário falhou para buscar declaração por referência.")
    return None

def get_itens_by_declaracao_id(declaracao_id: Any):
    """Obtém itens de declaração. Prefere Firestore."""
    logger.info(f"db_utils.py: Obtendo itens para declaração ID: {declaracao_id}")
    if _USE_FIRESTORE_AS_PRIMARY and db_firestore:
        logger.info("db_utils.py: Usando Firestore para obter itens da declaração.")
        itens_ref = get_firestore_collection_ref("xml_itens")
        if not itens_ref: 
            logger.error(f"db_utils.py: Falha ao acessar coleção 'xml_itens' no Firestore para obter itens.")
            return []
        try:
            docs = itens_ref.where("declaracao_id", "==", str(declaracao_id)).order_by("numero_adicao").order_by("numero_item_sequencial").stream()
            itens = []
            for doc in docs:
                data = doc.to_dict()
                data['id'] = doc.id
                itens.append(data)
            logger.info(f"db_utils.py: Obtidos {len(itens)} itens para declaração ID {declaracao_id} do Firestore.")
            return itens
        except Exception as e:
            logger.error(f"db_utils.py: Erro Firestore ao buscar itens para declaração ID {declaracao_id}: {e}")
        return []
    elif _SQLITE_ENABLED:
        logger.info(f"db_utils.py: Usando SQLite para obter itens da declaração.")
        conn = connect_sqlite_db(get_sqlite_db_path("xml_di"))
        if not conn: 
            logger.error(f"db_utils.py: Falha ao conectar ao DB XML DI para obter itens (SQLite).")
            return []
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, declaracao_id, numero_adicao, numero_item_sequencial, descricao_mercadoria, quantidade, unidade_medida,
                       valor_unitario, valor_item_calculado, peso_liquido_item, ncm_item, sku_item,
                       custo_unit_di_usd, ii_percent_item, ipi_percent_item, pis_percent_item, cofins_percent_item, icms_percent_item,
                       codigo_erp_item
                FROM xml_itens WHERE declaracao_id = ?
                ORDER BY numero_adicao ASC, numero_item_sequencial ASC
            """, (declaracao_id,))
            itens = cursor.fetchall()
            logger.info(f"db_utils.py: Obtidos {len(itens)} itens para declaração ID {declaracao_id} do SQLite.")
            return [dict(item) for item in itens]
        except Exception as e:
            logger.error(f"db_utils.py: Erro DB ao buscar itens para declaração ID {declaracao_id} (SQLite): {e}")
        finally:
            if conn: conn.close()
        return []
    else:
        logger.warning(f"db_utils.py: Nenhuma opção de DB disponível ou primário falhou para obter itens da declaração.")
    return []

def update_xml_item_erp_code(item_id: Any, new_erp_code: str):
    """Atualiza código ERP de um item. Grava em ambos os bancos de dados."""
    logger.info(f"db_utils.py: Atualizando código ERP para item ID {item_id} para '{new_erp_code}'.")
    success_firestore = True
    success_sqlite = True

    if _USE_FIRESTORE_AS_PRIMARY and db_firestore:
        logger.info("db_utils.py: Usando Firestore para atualizar código ERP.")
        itens_ref = get_firestore_collection_ref("xml_itens")
        if itens_ref:
            try:
                doc_ref = itens_ref.document(str(item_id))
                doc_ref.update({"codigo_erp_item": new_erp_code})
                logger.info(f"db_utils.py: Item ID {item_id} atualizado com Código ERP '{new_erp_code}' no Firestore.")
            except Exception as e:
                logger.error(f"db_utils.py: Erro Firestore ao atualizar Código ERP para item ID {item_id}: {e}")
                success_firestore = False
        else:
            logger.error(f"db_utils.py: Não foi possível obter referência da coleção 'xml_itens' no Firestore para atualizar código ERP.")
            success_firestore = False

    if _SQLITE_ENABLED:
        logger.info(f"db_utils.py: Usando SQLite para atualizar código ERP.")
        conn_sqlite = connect_sqlite_db(get_sqlite_db_path("xml_di"))
        if conn_sqlite:
            try:
                cursor_sqlite = conn_sqlite.cursor()
                cursor_sqlite.execute('''
                    UPDATE xml_itens
                    SET codigo_erp_item = ?
                    WHERE id = ?
                ''', (new_erp_code, item_id))
                conn_sqlite.commit()
                logger.info(f"db_utils.py: Item ID {item_id} atualizado com Código ERP '{new_erp_code}' no SQLite.")
            except Exception as e:
                logger.error(f"db_utils.py: Erro DB ao atualizar Código ERP para item ID {item_id} (SQLite): {e}")
                conn_sqlite.rollback()
                success_sqlite = False
            finally:
                if conn_sqlite: conn_sqlite.close()
        else:
            logger.error(f"db_utils.py: Falha na conexão com o DB XML DI para atualizar código ERP (SQLite).")
            success_sqlite = False
    
    return success_firestore and success_sqlite

def save_process_cost_data(declaracao_id: Any, afrmm: float, siscoserv: float, descarregamento: float, taxas_destino: float, multa: float, contracts_df: pd.DataFrame):
    """Salva dados de custo do processo. Grava em ambos os bancos de dados."""
    logger.info(f"db_utils.py: Salvando dados de custo para declaração ID: {declaracao_id}")
    success_firestore = True
    success_sqlite = True

    cost_data = {
        "afrmm": afrmm,
        "siscoserv": siscoserv,
        "descarregamento": descarregamento,
        "taxas_destino": taxas_destino,
        "multa": multa
    }

    if _USE_FIRESTORE_AS_PRIMARY and db_firestore:
        logger.info("db_utils.py: Usando Firestore para salvar dados de custo do processo.")
        processo_dados_custo_ref = get_firestore_collection_ref("processo_dados_custo")
        processo_contratos_cambio_ref = get_firestore_collection_ref("processo_contratos_cambio")
        if processo_dados_custo_ref and processo_contratos_cambio_ref:
            try:
                batch = db_firestore.batch()
                
                cost_doc_ref = processo_dados_custo_ref.document(str(declaracao_id))
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
                            "declaracao_id": str(declaracao_id),
                            "numero_contrato": num_contrato,
                            "dolar_cambio": dolar_cambio,
                            "valor_usd": valor_contrato_usd
                        }
                        batch.set(processo_contratos_cambio_ref.document(), contract_data)
                
                batch.commit()
                logger.info(f"db_utils.py: Despesas/contratos salvos para DI ID {declaracao_id} no Firestore.")
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao salvar despesas/contratos para DI ID {declaracao_id} no Firestore: {e}")
                success_firestore = False
        else:
            logger.error(f"db_utils.py: Não foi possível obter referência das coleções de custo/contrato no Firestore.")
            success_firestore = False

    if _SQLITE_ENABLED:
        logger.info(f"db_utils.py: Usando SQLite para salvar dados de custo do processo.")
        conn_sqlite = connect_sqlite_db(get_sqlite_db_path("xml_di"))
        if conn_sqlite:
            try:
                cursor_sqlite = conn_sqlite.cursor()

                cursor_sqlite.execute("SELECT declaracao_id FROM processo_dados_custo WHERE declaracao_id = ?", (declaracao_id,))
                if cursor_sqlite.fetchone():
                    cursor_sqlite.execute('''
                        UPDATE processo_dados_custo
                        SET afrmm = ?, siscoserv = ?, descarregamento = ?, taxas_destino = ?, multa = ?
                        WHERE declaracao_id = ?
                    ''', (afrmm, siscoserv, descarregamento, taxas_destino, multa, declaracao_id))
                    logger.info(f"db_utils.py: Despesas atualizadas para DI ID {declaracao_id} no SQLite.")
                else:
                    cursor_sqlite.execute('''
                        INSERT INTO processo_dados_custo (declaracao_id, afrmm, siscoserv, descarregamento, taxas_destino, multa)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (declaracao_id, afrmm, siscoserv, descarregamento, taxas_destino, multa))
                    logger.info(f"db_utils.py: Despesas inseridas para DI ID {declaracao_id} no SQLite.")
                conn_sqlite.commit()

                cursor_sqlite.execute("DELETE FROM processo_contratos_cambio WHERE declaracao_id = ?", (declaracao_id,))
                logger.debug(f"db_utils.py: Deletados contratos antigos para DI ID {declaracao_id} no SQLite.")
                for index, row in contracts_df.iterrows():
                    num_contrato = row['Nº Contrato']
                    dolar_cambio = row['Dólar']
                    valor_contrato_usd = row['Valor (US$)']

                    if dolar_cambio > 0 and valor_contrato_usd > 0 and num_contrato:
                        cursor_sqlite.execute('''
                            INSERT INTO processo_contratos_cambio (declaracao_id, numero_contrato, dolar_cambio, valor_usd)
                            VALUES (?, ?, ?, ?)
                        ''', (declaracao_id, num_contrato, dolar_cambio, valor_contrato_usd))
                conn_sqlite.commit()
                logger.info(f"db_utils.py: Contratos de câmbio salvos para DI ID {declaracao_id} no SQLite.")
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao salvar despesas/contratos para DI ID {declaracao_id} no SQLite: {e}")
                conn_sqlite.rollback()
                success_sqlite = False
            finally:
                if conn_sqlite: conn_sqlite.close()
        else:
            logger.error(f"db_utils.py: Falha na conexão com o DB XML DI para salvar dados de custo (SQLite).")
            success_sqlite = False
    
    return success_firestore and success_sqlite


def get_process_cost_data(declaracao_id: Any):
    """Obtém dados de custo do processo. Prefere Firestore."""
    logger.info(f"db_utils.py: Obtendo dados de custo para declaração ID: {declaracao_id}")
    if _USE_FIRESTORE_AS_PRIMARY and db_firestore:
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
            logger.error(f"db_utils.py: Erro ao carregar dados de custo para DI ID {declaracao_id} no Firestore: {e}")
        return None, []
    elif _SQLITE_ENABLED:
        logger.info("db_utils.py: Usando SQLite para obter dados de custo do processo.")
        conn = connect_sqlite_db(get_sqlite_db_path("xml_di"))
        if not conn: 
            logger.error(f"db_utils.py: Falha ao conectar ao DB XML DI para obter dados de custo (SQLite).")
            return None, []
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT afrmm, siscoserv, descarregamento, taxas_destino, multa FROM processo_dados_custo WHERE declaracao_id = ?", (declaracao_id,))
            expenses_db = cursor.fetchone()
            
            cursor.execute("SELECT numero_contrato, dolar_cambio, valor_usd FROM processo_contratos_cambio WHERE declaracao_id = ? ORDER BY id ASC", (declaracao_id,))
            contracts_db = cursor.fetchall()

            logger.info(f"db_utils.py: Obtidos dados de custo para DI ID {declaracao_id} do SQLite.")
            return expenses_db, contracts_db
        except Exception as e:
            logger.error(f"db_utils.py: Erro ao carregar dados de custo para DI ID {declaracao_id} (SQLite): {e}")
        finally:
            if conn: conn.close()
        return None, []
    else:
        logger.warning(f"db_utils.py: Nenhuma opção de DB disponível ou primário falhou para obter dados de custo do processo.")
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
            logger.debug(f"db_utils.py: Referência extraída do XML: {referencia_extraida}")
        
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

def save_parsed_di_data(di_data: Dict[str, Any], itens_data: List[Dict[str, Any]]):
    """
    Salva dados de DI e itens. Grava em ambos os bancos de dados se habilitado.
    """
    logger.info(f"db_utils.py: Iniciando save_parsed_di_data para DI: {di_data.get('numero_di')}")
    success_firestore = True
    success_sqlite = True
    
    numero_di = di_data.get('numero_di')
    if not numero_di:
        logger.error(f"db_utils.py: Número da DI não fornecido para salvar. Abortando.")
        return False

    if _USE_FIRESTORE_AS_PRIMARY and db_firestore:
        logger.info(f"db_utils.py: Tentando salvar DI e itens no Firestore para DI: {numero_di}")
        declaracoes_ref_firestore = get_firestore_collection_ref("xml_declaracoes")
        itens_ref_firestore = get_firestore_collection_ref("xml_itens")
        if declaracoes_ref_firestore and itens_ref_firestore:
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
            except Exception as e:
                logger.exception(f"db_utils.py: Erro ao salvar DI e itens no Firestore: {e}")
                success_firestore = False
        else:
            logger.error(f"db_utils.py: Falha ao obter referências de coleção 'xml_declaracoes' ou 'xml_itens' no Firestore para salvar DI.")
            success_firestore = False

    if _SQLITE_ENABLED:
        logger.info(f"db_utils.py: Tentando salvar DI e itens no SQLite para DI: {numero_di}")
        conn_sqlite = connect_sqlite_db(get_sqlite_db_path("xml_di"))
        if conn_sqlite:
            try:
                cursor_sqlite = conn_sqlite.cursor()
                cursor_sqlite.execute("SELECT id FROM xml_declaracoes WHERE numero_di = ?", (numero_di,))
                existing_di_sqlite = cursor_sqlite.fetchone()

                if existing_di_sqlite:
                    logger.error(f"db_utils.py: Erro de integridade: A DI {numero_di} já existe no SQLite. Abortando salvamento no SQLite.")
                    success_sqlite = False
                else:
                    cursor_sqlite.execute('''
                        INSERT INTO xml_declaracoes (
                            numero_di, data_registro, valor_total_reais_xml, arquivo_origem, data_importacao,
                            informacao_complementar, vmle, frete, seguro, vmld, ipi, pis_pasep, cofins, icms_sc,
                            taxa_cambial_usd, taxa_siscomex, numero_invoice, peso_bruto, peso_liquido,
                            cnpj_importador, importador_nome, recinto, embalagem, quantidade_volumes, acrescimo,
                            imposto_importacao, armazenagem, frete_nacional
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        di_data.get('numero_di'), di_data.get('data_registro'), di_data.get('valor_total_reais_xml'), di_data.get('arquivo_origem'), di_data.get('data_importacao'),
                        di_data.get('informacao_complementar'), di_data.get('vmle'), di_data.get('frete'), di_data.get('seguro'), di_data.get('vmld'),
                        di_data.get('ipi'), di_data.get('pis_pasep'), di_data.get('cofins'), di_data.get('icms_sc'),
                        di_data.get('taxa_cambial_usd'), di_data.get('taxa_siscomex'), di_data.get('numero_invoice'),
                        di_data.get('peso_bruto'), di_data.get('peso_liquido'), di_data.get('cnpj_importador'),
                        di_data.get('importador_nome'), di_data.get('recinto'), di_data.get('embalagem'),
                        di_data.get('quantidade_volumes'), di_data.get('acrescimo'), di_data.get('imposto_importacao'),
                        di_data.get('armazenagem'), di_data.get('frete_nacional')
                    ))
                    declaracao_id_sqlite = cursor_sqlite.lastrowid
                    logger.debug(f"db_utils.py: DI {numero_di} inserida no SQLite com ID: {declaracao_id_sqlite}.")

                    itens_a_salvar_tuples = []
                    for item in itens_data:
                        itens_a_salvar_tuples.append((
                            declaracao_id_sqlite,
                            item.get('numero_adicao'), item.get('numero_item_sequencial'), item.get('descricao_mercadoria'),
                            item.get('quantidade'), item.get('unidade_medida'), item.get('valor_unitario'),
                            item.get('valor_item_calculado'), item.get('peso_liquido_item'), item.get('ncm_item'),
                            item.get('sku_item'), item.get('custo_unit_di_usd'), item.get('ii_percent_item'),
                            item.get('ipi_percent_item'), item.get('pis_percent_item'), item.get('cofins_percent_item'),
                            item.get('icms_percent_item'), item.get('codigo_erp_item')
                        ))
                    
                    if itens_a_salvar_tuples:
                        cursor_sqlite.executemany('''
                            INSERT INTO xml_itens (
                                declaracao_id, numero_adicao, numero_item_sequencial, descricao_mercadoria, quantidade, unidade_medida,
                                valor_unitario, valor_item_calculado, peso_liquido_item, ncm_item, sku_item,
                                custo_unit_di_usd, ii_percent_item, ipi_percent_item, pis_percent_item, cofins_percent_item, icms_percent_item,
                                codigo_erp_item
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', itens_a_salvar_tuples)
                        logger.debug(f"db_utils.py: {len(itens_a_salvar_tuples)} itens inseridos para DI {numero_di} no SQLite.")
                    
                    conn_sqlite.commit()
                    logger.info(f"db_utils.py: DI {numero_di} e seus itens salvos com sucesso no SQLite.")
            except sqlite3.IntegrityError as e:
                logger.error(f"db_utils.py: Erro de integridade: A DI {numero_di} já existe no SQLite. {e}")
                success_sqlite = False
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao salvar DI e itens no banco de dados SQLite: {e}")
                conn_sqlite.rollback()
                success_sqlite = False
            finally:
                if conn_sqlite: conn_sqlite.close()
        else:
            logger.error(f"db_utils.py: Falha na conexão com o DB XML DI para salvar DI (SQLite).")
            success_sqlite = False

    return success_firestore and success_sqlite


def delete_declaracao(declaracao_id: Any):
    """Deleta uma declaração. Deleta em ambos os bancos de dados."""
    logger.info(f"db_utils.py: Iniciando exclusão da declaração ID: {declaracao_id}")
    success_firestore = True
    success_sqlite = True

    if _USE_FIRESTORE_AS_PRIMARY and db_firestore:
        logger.info(f"db_utils.py: Tentando deletar declaração ID {declaracao_id} e dados relacionados do Firestore.")
        declaracoes_ref_firestore = get_firestore_collection_ref("xml_declaracoes")
        itens_ref_firestore = get_firestore_collection_ref("xml_itens")
        if declaracoes_ref_firestore and itens_ref_firestore:
            try:
                batch = db_firestore.batch()
                di_doc_ref = declaracoes_ref_firestore.document(str(declaracao_id))
                batch.delete(di_doc_ref)
                logger.debug(f"db_utils.py: Declaração ID {declaracao_id} adicionada ao batch para exclusão no Firestore.")

                docs_to_delete = itens_ref_firestore.where("declaracao_id", "==", str(declaracao_id)).stream()
                for doc in docs_to_delete:
                    batch.delete(doc.reference)
                logger.debug(f"db_utils.py: Itens relacionados à declaração ID {declaracao_id} adicionados ao batch para exclusão no Firestore.")
                
                batch.commit()
                logger.info(f"db_utils.py: Declaração ID {declaracao_id} e dados relacionados excluídos com sucesso do Firestore.")
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao excluir declaração ID {declaracao_id} e dados relacionados do Firestore: {e}")
                success_firestore = False
        else:
            logger.error(f"db_utils.py: Falha ao obter referências de coleção 'xml_declaracoes' ou 'xml_itens' no Firestore para deletar.")
            success_firestore = False

    if _SQLITE_ENABLED:
        logger.info(f"db_utils.py: Tentando deletar declaração ID {declaracao_id} e dados relacionados do SQLite.")
        conn_sqlite = connect_sqlite_db(get_sqlite_db_path("xml_di"))
        if not conn_sqlite:
            logger.error(f"db_utils.py: Falha na conexão com o DB XML DI para deletar declaração (SQLite).")
            success_sqlite = False
        else:
            try:
                cursor_sqlite = conn_sqlite.cursor()
                cursor_sqlite.execute("DELETE FROM xml_declaracoes WHERE id = ?", (declaracao_id,))
                conn_sqlite.commit()
                logger.info(f"db_utils.py: Declaração ID {declaracao_id} e dados relacionados excluídos com sucesso do SQLite.")
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao excluir declaração ID {declaracao_id} do SQLite: {e}")
                conn_sqlite.rollback()
                success_sqlite = False
            finally:
                if conn_sqlite: conn_sqlite.close()
    
    return success_firestore and success_sqlite

def update_declaracao(declaracao_id: Any, di_data: Dict[str, Any]):
    """Atualiza uma declaração. Grava em ambos os bancos de dados."""
    logger.info(f"db_utils.py: Iniciando atualização da declaração ID: {declaracao_id}")
    success_firestore = True
    success_sqlite = True

    if _USE_FIRESTORE_AS_PRIMARY and db_firestore:
        logger.info(f"db_utils.py: Tentando atualizar declaração ID {declaracao_id} no Firestore.")
        declaracoes_ref_firestore = get_firestore_collection_ref("xml_declaracoes")
        if declaracoes_ref_firestore:
            try:
                current_di_data = None
                if isinstance(declaracao_id, int):
                    temp_conn_sqlite = None
                    try:
                        temp_conn_sqlite = connect_sqlite_db(get_sqlite_db_path("xml_di"))
                        if temp_conn_sqlite:
                            cursor = temp_conn_sqlite.cursor()
                            cursor.execute("SELECT numero_di FROM xml_declaracoes WHERE id = ?", (declaracao_id,))
                            result = cursor.fetchone()
                            if result:
                                current_di_data = result[0]
                            else:
                                logger.error(f"db_utils.py: Não foi possível encontrar numero_di para declaracao_id {declaracao_id} no SQLite.")
                                success_firestore = False
                        else:
                            logger.error("db_utils.py: Falha ao conectar ao SQLite para obter numero_di.")
                            success_firestore = False
                    finally:
                        if temp_conn_sqlite: temp_conn_sqlite.close()
                elif isinstance(declaracao_id, str):
                    current_di_data = declaracao_id

                if current_di_data:
                    doc_ref = declaracoes_ref_firestore.document(current_di_data)
                    doc_ref.update(di_data)
                    logger.info(f"db_utils.py: Declaração {current_di_data} (Firestore ID) atualizada com sucesso no Firestore.")
                else:
                    logger.error(f"db_utils.py: Não foi possível determinar o ID do documento Firestore para atualização. Abortando Firestore update.")
                    success_firestore = False
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao atualizar declaração ID {declaracao_id} no Firestore: {e}")
                success_firestore = False
        else:
            logger.error(f"db_utils.py: Falha ao obter referência da coleção 'xml_declaracoes' no Firestore para atualizar.")
            success_firestore = False

    if _SQLITE_ENABLED:
        logger.info(f"db_utils.py: Usando SQLite para atualizar declaração.")
        conn_sqlite = connect_sqlite_db(get_sqlite_db_path("xml_di"))
        if conn_sqlite:
            try:
                cursor_sqlite = conn_sqlite.cursor()
                cursor_sqlite.execute('''
                    UPDATE xml_declaracoes
                    SET
                        numero_di = ?,
                        data_registro = ?,
                        valor_total_reais_xml = ?,
                        arquivo_origem = ?,
                        data_importacao = ?,
                        informacao_complementar = ?,
                        vmle = ?,
                        frete = ?,
                        seguro = ?,
                        vmld = ?,
                        ipi = ?,
                        pis_pasep = ?,
                        cofins = ?,
                        icms_sc = ?,
                        taxa_cambial_usd = ?,
                        taxa_siscomex = ?,
                        numero_invoice = ?,
                        peso_bruto = ?,
                        peso_liquido = ?,
                        cnpj_importador = ?,
                        importador_nome = ?,
                        recinto = ?,
                        embalagem = ?,
                        quantidade_volumes = ?,
                        acrescimo = ?,
                        imposto_importacao = ?,
                        armazenagem = ?,
                        frete_nacional = ?
                    WHERE id = ?
                ''', (
                    di_data.get('numero_di'), di_data.get('data_registro'), di_data.get('valor_total_reais_xml'), di_data.get('arquivo_origem'), di_data.get('data_importacao'),
                    di_data.get('informacao_complementar'), di_data.get('vmle'), di_data.get('frete'), di_data.get('seguro'), di_data.get('vmld'),
                    di_data.get('ipi'), di_data.get('pis_pasep'), di_data.get('cofins'), di_data.get('icms_sc'),
                    di_data.get('taxa_cambial_usd'), di_data.get('taxa_siscomex'), di_data.get('numero_invoice'),
                    di_data.get('peso_bruto'), di_data.get('peso_liquido'), di_data.get('cnpj_importador'),
                    di_data.get('importador_nome'), di_data.get('recinto'), di_data.get('embalagem'),
                    di_data.get('quantidade_volumes'), di_data.get('acrescimo'), di_data.get('imposto_importacao'),
                    di_data.get('armazenagem'), di_data.get('frete_nacional'),
                    declaracao_id
                ))
                conn_sqlite.commit()
                logger.info(f"db_utils.py: Declaração ID {declaracao_id} atualizada com sucesso no SQLite.")
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao atualizar declaração ID {declaracao_id} no SQLite: {e}")
                conn_sqlite.rollback()
                success_sqlite = False
            finally:
                if conn_sqlite: conn_sqlite.close()
        else:
            logger.error(f"db_utils.py: Falha na conexão com o DB XML DI para atualizar declaração (SQLite).")
            success_sqlite = False
    
    return success_firestore and success_sqlite

def update_declaracao_field(declaracao_id: Any, field_name: str, new_value: Any):
    """
    Updates a single field for a given declaracao_id. Grava em ambos os bancos de dados.
    """
    logger.info(f"db_utils.py: Atualizando campo '{field_name}' para declaração ID {declaracao_id} com valor '{new_value}'.")
    success_firestore = True
    success_sqlite = True

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

    if _USE_FIRESTORE_AS_PRIMARY and db_firestore:
        logger.info("db_utils.py: Usando Firestore para atualizar campo da declaração.")
        declaracoes_ref_firestore = get_firestore_collection_ref("xml_declaracoes")
        if declaracoes_ref_firestore:
            try:
                current_di_firestore_id = None
                if isinstance(declaracao_id, int):
                    temp_conn_sqlite = None
                    try:
                        temp_conn_sqlite = connect_sqlite_db(get_sqlite_db_path("xml_di"))
                        if temp_conn_sqlite:
                            cursor = temp_conn_sqlite.cursor()
                            cursor.execute("SELECT numero_di FROM xml_declaracoes WHERE id = ?", (declaracao_id,))
                            result = cursor.fetchone()
                            if result:
                                current_di_firestore_id = result[0]
                            else:
                                logger.error(f"db_utils.py: Não foi possível encontrar numero_di para declaracao_id {declaracao_id} no SQLite para Firestore update.")
                                success_firestore = False
                        else:
                            logger.error("db_utils.py: Falha ao conectar ao SQLite para obter numero_di para Firestore update.")
                            success_firestore = False
                    finally:
                        if temp_conn_sqlite: temp_conn_sqlite.close()
                elif isinstance(declaracao_id, str):
                    current_di_firestore_id = declaracao_id

                if current_di_firestore_id:
                    doc_ref = declaracoes_ref_firestore.document(current_di_firestore_id)
                    doc_ref.update({field_name: new_value})
                    logger.info(f"db_utils.py: Campo '{field_name}' da declaração {current_di_firestore_id} (Firestore ID) atualizado para '{new_value}' no Firestore.")
                else:
                    logger.error(f"db_utils.py: Não foi possível determinar o ID do documento Firestore para atualização de campo. Abortando Firestore update.")
                    success_firestore = False
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao atualizar campo '{field_name}' para declaração ID {declaracao_id} no Firestore: {e}")
                success_firestore = False
        else:
            logger.error(f"db_utils.py: Falha ao obter referência da coleção 'xml_declaracoes' no Firestore para atualizar campo.")
            success_firestore = False

    if _SQLITE_ENABLED:
        logger.info("db_utils.py: Usando SQLite para atualizar campo da declaração.")
        conn_sqlite = connect_sqlite_db(get_sqlite_db_path("xml_di"))
        if conn_sqlite:
            try:
                cursor_sqlite = conn_sqlite.cursor()
                query = f"UPDATE xml_declaracoes SET {field_name} = ? WHERE id = ?"
                cursor_sqlite.execute(query, (new_value, declaracao_id))
                conn_sqlite.commit()
                logger.info(f"db_utils.py: Campo '{field_name}' da declaração ID {declaracao_id} atualizado para '{new_value}' no SQLite.")
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao atualizar campo '{field_name}' para declaração ID {declaracao_id} no SQLite: {e}")
                conn_sqlite.rollback()
                success_sqlite = False
            finally:
                if conn_sqlite: conn_sqlite.close()
        else:
            logger.error(f"db_utils.py: Falha na conexão com o DB XML DI para atualizar campo (SQLite).")
            success_sqlite = False
    
    return success_firestore and success_sqlite


def inserir_ou_atualizar_produto(produto: Tuple[str, str, str, str]):
    """
    Insere ou atualiza um produto. Grava em ambos os bancos de dados.
    produto: (id_key_erp, nome_part, descricao, ncm)
    """
    id_key_erp = produto[0]
    logger.info(f"db_utils.py: Inserindo/Atualizando produto com ID/Key ERP: {id_key_erp}")
    success_firestore = True
    success_sqlite = True

    data = {
        "id_key_erp": produto[0],
        "nome_part": produto[1],
        "descricao": produto[2],
        "ncm": produto[3]
    }

    if _USE_FIRESTORE_AS_PRIMARY and db_firestore:
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

    if _SQLITE_ENABLED:
        logger.info(f"db_utils.py: Usando SQLite para inserir/atualizar produto.")
        conn_sqlite = connect_sqlite_db(get_sqlite_db_path("produtos"))
        if conn_sqlite:
            try:
                cursor_sqlite = conn_sqlite.cursor()
                cursor_sqlite.execute("SELECT id_key_erp FROM produtos WHERE id_key_erp = ?", (id_key_erp,))
                if cursor_sqlite.fetchone():
                    cursor_sqlite.execute('''
                        UPDATE produtos
                        SET nome_part = ?, descricao = ?, ncm = ?
                        WHERE id_key_erp = ?
                    ''', (produto[1], produto[2], produto[3], produto[0]))
                    logger.info(f"db_utils.py: Produto com ID/Key ERP '{id_key_erp}' atualizado com sucesso no SQLite.")
                else:
                    cursor_sqlite.execute('''
                        INSERT INTO produtos (id_key_erp, nome_part, descricao, ncm)
                        VALUES (?, ?, ?, ?)
                    ''', produto)
                    logger.info(f"db_utils.py: Novo produto com ID/Key ERP '{id_key_erp}' inserido com sucesso no SQLite.")
                conn_sqlite.commit()
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao inserir/atualizar produto com ID/Key ERP '{id_key_erp}' no SQLite: {e}")
                conn_sqlite.rollback()
                success_sqlite = False
            finally:
                if conn_sqlite: conn_sqlite.close()
        else:
            logger.error(f"db_utils.py: Falha na conexão com o DB de Produtos para inserir/atualizar (SQLite).")
            success_sqlite = False
    
    return success_firestore and success_sqlite

def selecionar_todos_produtos() -> List[Dict[str, Any]]:
    """
    Seleciona todos os produtos. Prefere Firestore.
    """
    logger.info(f"db_utils.py: Selecionando todos os produtos.")
    if _USE_FIRESTORE_AS_PRIMARY and db_firestore:
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
    elif _SQLITE_ENABLED:
        logger.info(f"db_utils.py: Usando SQLite para selecionar todos os produtos.")
        conn = connect_sqlite_db(get_sqlite_db_path("produtos"))
        if not conn: 
            logger.error(f"db_utils.py: Falha na conexão com o DB de Produtos para selecionar todos (SQLite).")
            return []
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT id_key_erp, nome_part, descricao, ncm FROM produtos ORDER BY id_key_erp ASC, nome_part ASC")
            produtos = cursor.fetchall()
            logger.info(f"db_utils.py: Obtidos {len(produtos)} produtos do SQLite.")
            return [dict(p) for p in produtos]
        except Exception as e:
            logger.error(f"db_utils.py: Erro ao buscar todos os produtos (SQLite): {e}")
            return []
        finally:
            if conn: conn.close()
    else:
        logger.warning(f"db_utils.py: Nenhuma opção de DB disponível ou primário falhou para selecionar todos os produtos.")
    return []

def selecionar_produto_por_id(id_key_erp: str) -> Optional[Dict[str, Any]]:
    """
    Seleciona um produto pelo ID. Prefere Firestore.
    """
    logger.info(f"db_utils.py: Selecionando produto por ID/Key ERP: {id_key_erp}")
    if _USE_FIRESTORE_AS_PRIMARY and db_firestore:
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
    elif _SQLITE_ENABLED:
        logger.info(f"db_utils.py: Usando SQLite para selecionar produto por ID.")
        conn = connect_sqlite_db(get_sqlite_db_path("produtos"))
        if not conn: 
            logger.error(f"db_utils.py: Falha na conexão com o DB de Produtos para selecionar por ID (SQLite).")
            return None
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT id_key_erp, nome_part, descricao, ncm FROM produtos WHERE id_key_erp = ?", (id_key_erp,))
            produto = cursor.fetchone()
            if produto:
                logger.info(f"db_utils.py: Produto com ID/Key ERP '{id_key_erp}' encontrado no SQLite.")
                return dict(produto)
            else:
                logger.warning(f"db_utils.py: Produto com ID/Key ERP '{id_key_erp}' não encontrado no SQLite.")
                return None
        except Exception as e:
            logger.error(f"db_utils.py: Erro ao buscar produto com ID/Key ERP '{id_key_erp}' (SQLite): {e}")
            return None
        finally:
            if conn: conn.close()
    else:
        logger.warning(f"db_utils.py: Nenhuma opção de DB disponível ou primário falhou para selecionar produto por ID.")
    return None

def selecionar_produtos_por_ids(ids: List[str]):
    """
    Seleciona produtos por uma lista de IDs. Prefere Firestore.
    """
    logger.info(f"db_utils.py: Selecionando produtos por IDs: {ids}")
    if not ids: 
        logger.info(f"db_utils.py: Lista de IDs vazia para selecionar produtos.")
        return []

    if _USE_FIRESTORE_AS_PRIMARY and db_firestore:
        logger.info(f"db_utils.py: Usando Firestore para selecionar produtos por IDs.")
        produtos_ref = get_firestore_collection_ref("produtos")
        if not produtos_ref: 
            logger.error(f"db_utils.py: Falha ao obter referência da coleção 'produtos' no Firestore para selecionar por IDs.")
            return []
        try:
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
    elif _SQLITE_ENABLED:
        logger.info(f"db_utils.py: Usando SQLite para selecionar produtos por IDs.")
        conn = connect_sqlite_db(get_sqlite_db_path("produtos"))
        if not conn: 
            logger.error(f"db_utils.py: Falha na conexão com o DB de Produtos para selecionar por IDs (SQLite).")
            return []
        try:
            cursor = conn.cursor()
            placeholders = ', '.join('?' * len(ids))
            query = f"SELECT id_key_erp, nome_part, descricao, ncm FROM produtos WHERE id_key_erp IN ({placeholders}) ORDER BY INSTR(',{','.join(ids)},', ',' || id_key_erp || ',')"
            cursor.execute(query, tuple(ids))
            produtos_dict = {p['id_key_erp']: dict(p) for p in cursor.fetchall()}
            produtos_ordenados = [produtos_dict[id] for id in ids if id in produtos_dict]
            logger.info(f"db_utils.py: Obtidos {len(produtos_ordenados)} produtos por IDs do SQLite.")
            return produtos_ordenados
        except Exception as e:
            logger.error(f"db_utils.py: Erro ao buscar produtos por IDs (SQLite): {e}")
            return []
        finally:
            if conn: conn.close()
    else:
        logger.warning(f"db_utils.py: Nenhuma opção de DB disponível ou primário falhou para selecionar produtos por IDs.")
    return []

def deletar_produto(id_key_erp: str):
    """
    Deleta um produto. Deleta em ambos os bancos de dados.
    """
    logger.info(f"db_utils.py: Deletando produto com ID/Key ERP: {id_key_erp}")
    success_firestore = True
    success_sqlite = True

    if _USE_FIRESTORE_AS_PRIMARY and db_firestore:
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

    if _SQLITE_ENABLED:
        logger.info(f"db_utils.py: Usando SQLite para deletar produto.")
        conn_sqlite = connect_sqlite_db(get_sqlite_db_path("produtos"))
        if conn_sqlite:
            try:
                cursor_sqlite = conn_sqlite.cursor()
                cursor_sqlite.execute("DELETE FROM produtos WHERE id_key_erp = ?", (id_key_erp,))
                conn_sqlite.commit()
                if cursor_sqlite.rowcount > 0:
                    logger.info(f"db_utils.py: Produto com ID/Key ERP '{id_key_erp}' excluído com sucesso do SQLite.")
                else:
                    logger.warning(f"db_utils.py: Produto com ID/Key ERP '{id_key_erp}' não encontrado no SQLite para exclusão.")
            except Exception as e:
                logger.error(f"db_utils.py: Erro ao excluir produto com ID/Key ERP '{id_key_erp}' do SQLite: {e}")
                conn_sqlite.rollback()
                success_sqlite = False
            finally:
                if conn_sqlite: conn_sqlite.close()
        else:
            logger.error(f"db_utils.py: Falha na conexão com o DB de Produtos para deletar (SQLite).")
            success_sqlite = False
    
    return success_firestore and success_sqlite
