import streamlit as st
import pandas as pd
import logging
import hashlib # Para hashing de senha
import os # Para verificar a existência do DB
from typing import Dict, Optional, Any, List
from app_logic.utils import set_background_image, set_sidebar_background_image

# Importar funções do módulo de utilitários de banco de dados
import db_utils

# Configuração de logging para este módulo
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Lista de nomes de telas disponíveis (deve ser sincronizada com app_main.py)
# Esta lista é usada para as checkboxes de permissão
AVAILABLE_SCREENS_LIST = [
    "Home",
    "Descrições",
    "Listagem NCM",
    "Follow-up Importação",
    "Importar XML DI",
    "Pagamentos",
    "Custo do Processo",
    "Cálculo Portonave",
    "Análise de Documentos",
    "Pagamentos Container",
    "Cálculo de Tributos TTCE",
    "Gerenciamento de Usuários",
    "Cálculo Frete Internacional", # Adicionado
    "Análise de Faturas/PL (PDF)", # Adicionado
    "Cálculo Futura", # Adicionado
    "Cálculo Pac Log - Elo", # Adicionado
    "Cálculo Fechamento", # Adicionado
    "Cálculo FN Transportes" # Adicionado
]

# --- Funções de Lógica de Negócio (Interação com o DB) ---

# A função hash_password agora é usada diretamente do db_utils.
# def hash_password(password, username):
#     """Cria um hash SHA-256 da senha usando o nome de usuário como salt."""
#     # NOTA: Em produção, use bibliotecas mais seguras como bcrypt ou Argon2.
#     password_salted = password + username
#     return hashlib.sha256(password_salted.encode('utf-8')).hexdigest()

def adicionar_usuario_db(username: str, password: str, is_admin: bool = False, allowed_screens_list: Optional[List[str]] = None):
    """Adiciona um novo usuário ao banco de dados usando db_utils."""
    password_hash = db_utils.hash_password(password, username)
    if db_utils.adicionar_ou_atualizar_usuario(None, username, password_hash, is_admin, allowed_screens_list):
        st.success(f"Usuário '{username}' adicionado com sucesso!")
        return True
    else:
        st.error(f"Erro ao adicionar usuário '{username}'. Verifique os logs.")
        return False

def obter_todos_usuarios_db():
    """Obtém a lista de todos os usuários do banco de dados usando db_utils."""
    users = db_utils.get_all_users() # db_utils.get_all_users já retorna uma lista de dicionários
    if users is None: # Se houver falha na conexão, get_all_users pode retornar None ou [].
        st.error("Não foi possível conectar ao banco de dados de usuários para obter a lista.")
        return []
    return users

def obter_usuario_por_id_db(user_identifier: Any):
    """Obtém os dados de um usuário específico pelo ID ou username usando db_utils."""
    user_data = db_utils.get_user_by_id_or_username(user_identifier) # Retorna dicionário ou None
    if user_data is None:
        st.error(f"Usuário com ID/Nome '{user_identifier}' não encontrado no banco de dados.")
        return None
    return user_data

def atualizar_usuario_db(user_id_or_username: Any, username: str, is_admin: bool, allowed_screens_list: Optional[List[str]]):
    """Atualiza os dados de um usuário existente no banco de dados usando db_utils."""
    # Para atualizar, precisamos obter o hash da senha existente (se não for alterada).
    # db_utils.adicionar_ou_atualizar_usuario pode lidar com isso se o 'password_hash' for passado,
    # ou se a função adicionar_ou_atualizar_usuario for inteligente o suficiente para não sobrescrever.
    
    # A maneira mais segura é buscar o usuário primeiro para obter o hash atual, se for uma atualização.
    # Se user_id_or_username é o ID do SQLite, ou o username do Firestore.
    existing_user_data = db_utils.get_user_by_id_or_username(user_id_or_username)
    if not existing_user_data:
        st.error(f"Usuário '{username}' não encontrado para atualização.")
        return False
    
    # Mantém o hash da senha existente se não for fornecida uma nova.
    # No nosso caso, como a função é chamada sem a senha (apenas para metadados),
    # db_utils.adicionar_ou_atualizar_usuario deve usar o merge=True no Firestore
    # e a lógica correspondente no SQLite.
    
    # É importante passar o username correto para a função de hashing, mesmo que seja o antigo.
    # A função db_utils.adicionar_ou_atualizar_usuario espera um hash, não uma senha.
    # Se você for atualizar apenas is_admin e allowed_screens, NÃO deve passar o password_hash
    # para evitar sobrescrever com um valor vazio ou incorreto.
    # db_utils.adicionar_ou_atualizar_usuario deve ser capaz de lidar com a atualização parcial.
    
    # O user_id_or_username no db_utils.adicionar_ou_atualizar_usuario é o ID/Key do documento.
    # Para o Firestore, é o username. Para o SQLite, é o ID numérico.
    # Como as chamadas de db_utils.adicionar_ou_atualizar_usuario usam o username como chave principal no Firestore,
    # devemos usar o `username` como o identificador principal.
    
    # Se o username foi alterado, o Firestore criará um novo documento.
    # Se o modelo é que username é a chave primária imutável (o ID do documento),
    # então você precisaria DELETAR o antigo e ADICIONAR um novo.
    # Por simplicidade e consistência com o ID do documento, vamos considerar o username como imutável
    # se o usuário já existe e estamos editando. Se o user_id_or_username é o username.
    
    # Vamos assumir que, para ATUALIZAR, o `username` não muda e é a chave.
    # Se o user_id_or_username é um ID numérico do SQLite, precisamos obter o username.
    
    # Reutiliza o hash de senha existente para que a senha não seja redefinida inadvertidamente.
    # db_utils.adicionar_ou_atualizar_usuario aceita o `password_hash`.
    # É importante que a UI não envie uma senha vazia ou que `adicionar_ou_atualizar_usuario`
    # em db_utils não sobrescreva o hash existente se `password` não foi alterado na UI.
    
    # Se o `adicionar_ou_atualizar_usuario` é para ATUALIZAR, ele deve apenas atualizar os campos fornecidos.
    # A função `db_utils.adicionar_ou_atualizar_usuario` já usa `merge=True` no Firestore.
    # Para o SQLite, ela verifica se existe e atualiza.
    # Então, passamos os dados que queremos atualizar:
    
    updated_data = {
        "username": username, # O username pode ser atualizado, mas é a chave no Firestore
        "is_admin": is_admin,
        "allowed_screens": allowed_screens_list
    }
    
    # A função adicionar_ou_atualizar_usuario usa o username como ID no Firestore
    # e o user_id como ID no SQLite (se não for None).
    # Se estamos atualizando, precisamos do hash da senha atual, se não foi modificada.
    # db_utils.adicionar_ou_atualizar_usuario espera um `password_hash`.
    # A user_management_page.py atualmente não tem um campo para "senha antiga" para passar para hashing.
    # A função atualizar_usuario_db em db_utils já deve estar configurada para não sobrescrever a senha.
    
    # Melhor abordagem: criar uma função `atualizar_metadados_usuario` em db_utils
    # que NÃO inclua a senha, e chamá-la aqui.
    # Por agora, vamos chamar `adicionar_ou_atualizar_usuario` e garantir que o `password_hash`
    # seja o HASH EXISTENTE para não sobrescrever a senha se ela não foi alterada.
    
    # ATENÇÃO: É um ponto crucial. Se a função `adicionar_ou_atualizar_usuario` no db_utils
    # sobrescreve a senha sempre que chamada, teremos que ajustar `db_utils.py` para
    # aceitar um `password` opcional e hashá-lo, ou usar o hash existente.
    
    # Para fins desta refatoração, vamos assumir que `db_utils.adicionar_ou_atualizar_usuario`
    # será inteligente o suficiente para não sobrescrever a senha se `password_hash`
    # não for explicitamente fornecido (ou seja, se a senha não foi digitada no formulário de edição).
    # OU, que este `atualizar_usuario_db` na `user_management_page`
    # passará o `password_hash` existente do `existing_user_data`.
    
    # Se o user_id_or_username é o ID numérico do SQLite, precisamos obter o username dele.
    # Se é o username do Firestore, ele já é o identificador.
    actual_username = existing_user_data['username'] # Sempre usamos o username real do DB
    
    # Chamada ao db_utils, passando o hash da senha existente se não estiver atualizando a senha.
    # Esta função na user_management_page.py NÃO LIDA COM A MUDANÇA DE SENHA DIRETAMENTE.
    # Ela chama `atualizar_senha_usuario_db` separadamente se `edited_password` existir.
    # Portanto, devemos passar o HASH EXISTENTE para que o adicionar_ou_atualizar_usuario não redefina a senha.
    current_password_hash = existing_user_data.get('password_hash')
    
    if db_utils.adicionar_ou_atualizar_usuario(user_id_or_username, actual_username, current_password_hash, is_admin, allowed_screens_list):
        st.success(f"Usuário '{username}' atualizado com sucesso!")
        return True
    else:
        st.error(f"Erro ao atualizar usuário '{username}'. Verifique os logs.")
        return False


def atualizar_senha_usuario_db(user_id_or_username: Any, new_password: str, username: str):
    """Atualiza a senha de um usuário específico usando db_utils."""
    if db_utils.atualizar_senha_usuario(user_id_or_username, new_password, username):
        st.success(f"Senha do usuário '{username}' atualizada com sucesso!")
        return True
    else:
        st.error(f"Erro ao atualizar senha do usuário '{username}'. Verifique os logs.")
        return False

def deletar_usuario_db(user_id_or_username: Any):
    """Deleta um usuário do banco de dados pelo ID ou username usando db_utils."""
    if db_utils.deletar_usuario(user_id_or_username):
        st.success(f"Usuário excluído com sucesso!")
        return True
    else:
        st.error(f"Falha ao excluir usuário '{user_id_or_username}'. Verifique os logs.")
        return False


# --- Funções de UI (Streamlit) ---

# Adicionado @st.cache_data para otimização
@st.cache_data(ttl=3600)
def load_users_data():
    """Carrega os usuários do DB e atualiza o estado da sessão para exibição."""
    users_list = db_utils.get_all_users() # db_utils.get_all_users já retorna uma lista de dicionários
    st.session_state.users_data_for_display = users_list
    logger.info(f"Carregados {len(users_list)} usuários para exibição.")


def display_add_user_form():
    """Exibe o formulário para adicionar um novo usuário."""
    with st.form("add_user_form", clear_on_submit=True):
        st.markdown("### Adicionar Novo Usuário")
        new_username = st.text_input("Nome de Usuário", key="new_username_input")
        new_password = st.text_input("Senha", type="password", key="new_password_input")
        new_is_admin = st.checkbox("É Administrador", key="new_is_admin_checkbox")

        st.markdown("##### Permissões de Tela:")
        selected_screens = []
        for screen in AVAILABLE_SCREENS_LIST:
            if st.checkbox(screen, key=f"add_perm_{screen}"):
                selected_screens.append(screen)

        if st.form_submit_button("Adicionar Usuário"):
            if new_username and new_password:
                # Chama a função de lógica de negócio que usa db_utils
                if adicionar_usuario_db(new_username, new_password, new_is_admin, selected_screens):
                    load_users_data.clear() # Limpa o cache para recarregar os dados
                    load_users_data() # Recarrega a lista de usuários
                    st.session_state.show_add_user_form = False # Opcional: fechar formulário após sucesso
                    st.rerun()
            else:
                st.warning("Nome de usuário e senha são obrigatórios.")


def display_edit_user_form():
    """Exibe o formulário para editar um usuário existente."""
    user_id_to_edit = st.session_state.get('editing_user_id')
    
    if user_id_to_edit is None:
        st.error("Nenhum usuário selecionado para edição.")
        st.session_state.show_edit_user_form = False
        return

    # Obtém os dados do usuário a ser editado usando db_utils
    user_data = obter_usuario_por_id_db(user_id_to_edit) # Já retorna dicionário
    if user_data is None:
        st.error(f"Usuário com ID/Nome '{user_id_to_edit}' não encontrado no banco de dados.")
        st.session_state.show_edit_user_form = False
        return

    initial_username = user_data['username']
    initial_is_admin = user_data['is_admin']
    initial_allowed_screens_list = user_data['allowed_screens']

    with st.form(f"edit_user_form_{initial_username}"): # Usar username para a key do formulário
        st.markdown(f"### Editar Usuário: {initial_username}")
        
        edited_username = st.text_input("Nome de Usuário", value=initial_username, key=f"edit_username_{initial_username}")
        edited_password = st.text_input("Nova Senha (deixe em branco para não alterar)", type="password", key=f"edit_password_{initial_username}")
        edited_is_admin = st.checkbox("É Administrador", value=initial_is_admin, key=f"edit_is_admin_{initial_username}")

        st.markdown("##### Permissões de Tela:")
        edited_screens = []
        for screen in AVAILABLE_SCREENS_LIST:
            # Pre-seleciona as checkboxes com base nas permissões atuais
            if st.checkbox(screen, value=(screen in initial_allowed_screens_list), key=f"edit_perm_{initial_username}_{screen}"):
                edited_screens.append(screen)

        col_save, col_cancel = st.columns(2)
        with col_save:
            if st.form_submit_button("Salvar Alterações"):
                # Passa o user_id_to_edit (pode ser ID ou username) e os dados atualizados.
                # A função `atualizar_usuario_db` agora usará o username como chave primária no Firestore.
                # Se o username foi alterado no input, e o modo primário é Firestore,
                # isso pode resultar em um novo documento no Firestore e a perda do antigo.
                # Para evitar isso, se `initial_username != edited_username`, precisaríamos de
                # uma lógica de exclusão do antigo e criação de um novo.
                # POR SIMPLICIDADE, VAMOS ASSUMIR QUE O USERNAME É A CHAVE IMUTÁVEL PARA EDIÇÃO NO FIRESTORE.
                # Ou seja, `edited_username` DEVE ser igual a `initial_username`.
                
                # Vamos forçar o username do formulário a ser o original para evitar complexidade.
                final_username_for_update = initial_username
                if initial_username != edited_username:
                    st.warning("A alteração do nome de usuário não é permitida diretamente neste formulário para evitar problemas de ID. Salve com o nome original e, se necessário, exclua e recrie o usuário.")
                    return # Não prossegue com o salvamento.

                if atualizar_usuario_db(user_id_to_edit, final_username_for_update, edited_is_admin, edited_screens):
                    if edited_password: # Se uma nova senha foi fornecida
                        # Passa user_id_to_edit, que pode ser ID (SQLite) ou username (Firestore)
                        atualizar_senha_usuario_db(user_id_to_edit, edited_password, final_username_for_update)
                    load_users_data.clear() # Limpa o cache para recarregar os dados
                    load_users_data() # Recarrega a lista de usuários
                    st.session_state.show_edit_user_form = False
                    st.session_state.editing_user_id = None
                    st.rerun()
        with col_cancel:
            if st.form_submit_button("Cancelar"):
                st.session_state.show_edit_user_form = False
                st.session_state.editing_user_id = None
                st.rerun()


def display_delete_user_confirm_popup():
    """Exibe um pop-up de confirmação para exclusão de usuário."""
    user_id_to_delete = st.session_state.get('delete_user_id_to_confirm')
    user_name_to_delete = st.session_state.get('delete_user_name_to_confirm')

    if user_id_to_delete is None:
        st.session_state.show_delete_user_confirm_popup = False
        return

    with st.form(key=f"delete_user_confirm_form_{user_id_to_delete}"):
        st.markdown(f"### Confirmar Exclusão de Usuário")
        st.warning(f"Tem certeza que deseja excluir o usuário '{user_name_to_delete}' (ID: {user_id_to_delete})?")
        
        col_yes, col_no = st.columns(2)
        with col_yes:
            if st.form_submit_button("Sim, Excluir"):
                # Passa o user_id_to_delete (pode ser ID ou username) para db_utils
                if deletar_usuario_db(user_id_to_delete):
                    load_users_data.clear() # Limpa o cache para recarregar os dados
                    load_users_data() # Recarrega a lista de usuários
                    st.session_state.show_delete_user_confirm_popup = False
                    st.session_state.delete_user_id_to_confirm = None
                    st.session_state.delete_user_name_to_confirm = None
                    st.rerun()
        with col_no:
            if st.form_submit_button("Não, Cancelar"):
                st.session_state.show_delete_user_confirm_popup = False
                st.session_state.delete_user_id_to_confirm = None
                st.session_state.delete_user_name_to_confirm = None
                st.rerun()


def show_page():
    background_image_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'assets', 'logo_navio_atracado.png')
    set_background_image(background_image_path)
    
    st.title("Gerenciamento de Usuários")
    logger.debug("Executando show_page da user_management_page.") # Debugging

    # Inicialização de variáveis de estado da sessão para esta página
    if 'users_data_for_display' not in st.session_state:
        st.session_state.users_data_for_display = []
    if 'show_add_user_form' not in st.session_state:
        st.session_state.show_add_user_form = False
    if 'show_edit_user_form' not in st.session_state:
        st.session_state.show_edit_user_form = False
    if 'editing_user_id' not in st.session_state:
        st.session_state.editing_user_id = None
    if 'show_delete_user_confirm_popup' not in st.session_state:
        st.session_state.show_delete_user_confirm_popup = False
    if 'delete_user_id_to_confirm' not in st.session_state:
        st.session_state.delete_user_id_to_confirm = None
    if 'delete_user_name_to_confirm' not in st.session_state:
        st.session_state.delete_user_name_to_confirm = None
    if 'show_change_password_form' not in st.session_state: # Novo estado para o formulário de alteração de senha
        st.session_state.show_change_password_form = False
    if 'change_password_user_id' not in st.session_state:
        st.session_state.change_password_user_id = None
    if 'change_password_username' not in st.session_state:
        st.session_state.change_password_username = None


    # Exibir pop-ups se ativos
    if st.session_state.show_add_user_form:
        display_add_user_form()
        return # Impede que o restante da página seja renderizado enquanto o formulário está ativo
    
    if st.session_state.show_edit_user_form:
        display_edit_user_form()
        return # Impede que o restante da página seja renderizado enquanto o formulário está ativo

    if st.session_state.show_delete_user_confirm_popup:
        display_delete_user_confirm_popup()
        return # Impede que o restante da página seja renderizado enquanto o pop-up está ativo

    # NOVO: Formulário de alteração de senha
    if st.session_state.show_change_password_form:
        display_change_password_form()
        return # Impede que o restante da página seja renderizado

    # Botão para abrir o formulário de adição de usuário
    if st.button("Adicionar Novo Usuário", key="open_add_user_form_btn"):
        st.session_state.show_add_user_form = True
        st.session_state.editing_user_id = None # Garante que é um novo
        st.rerun()

    st.markdown("---")
    st.markdown("### Lista de Usuários")

    # Carregar dados dos usuários para exibição
    if not st.session_state.users_data_for_display:
        load_users_data()

    df_users = pd.DataFrame(st.session_state.users_data_for_display)

    if not df_users.empty:
        # Colunas a serem exibidas e configuradas
        column_config = {
            "id": st.column_config.TextColumn("ID", width="small"), # ID pode ser string (username do Firestore)
            "username": st.column_config.TextColumn("Usuário", width="medium"),
            "is_admin": st.column_config.TextColumn("Admin?", width="small"),
            "allowed_screens": st.column_config.TextColumn("Telas Permitidas", width="large")
        }

        # Reordenar as colunas do DataFrame
        df_users_display_ordered = df_users[["id", "username", "is_admin", "allowed_screens"]]

        # Exibir a tabela de usuários
        selected_user_row = st.dataframe(
            df_users_display_ordered, 
            column_config=column_config,
            hide_index=False, # Manter False para mostrar o checkbox de seleção
            use_container_width=True,
            selection_mode="single-row",
            key="users_table",
            on_select="rerun" # Força um rerun quando uma linha é selecionada
        )

        # Lógica para botões de edição/exclusão baseada na seleção da tabela
        # Estes botões aparecerão ABAIXO da tabela quando uma linha for selecionada
        if selected_user_row and selected_user_row.get('selection', {}).get('rows'):
            selected_index = selected_user_row['selection']['rows'][0]
            
            # Obter o ID do usuário da linha selecionada na tabela exibida (que é o `id` retornado pelo db_utils)
            selected_user_id_from_display = df_users_display_ordered.iloc[selected_index]['id']
            selected_username_from_display = df_users_display_ordered.iloc[selected_index]['username'] # Para exibição no botão
            
            # Usaremos selected_user_id_from_display como o identificador para as funções de db_utils
            # pois ele é o que db_utils.get_user_by_id_or_username espera (ID numérico ou username string).
            
            st.write(f"DEBUG: Usuário selecionado na tabela - ID: {selected_user_id_from_display}, Nome: {selected_username_from_display}")
            
            # Botões de ação abaixo da tabela
            col1, col2, col3 = st.columns(3)
            with col1:
                if st.button(f"Editar Usuário: {selected_username_from_display}", key=f"edit_user_{selected_user_id_from_display}"):
                    st.session_state.editing_user_id = selected_user_id_from_display # Passa o identificador
                    st.session_state.show_edit_user_form = True
                    st.rerun()
            with col2:
                if st.button(f"Excluir Usuário: {selected_username_from_display}", key=f"delete_user_{selected_user_id_from_display}"):
                    st.session_state.delete_user_id_to_confirm = selected_user_id_from_display
                    st.session_state.delete_user_name_to_confirm = selected_username_from_display
                    st.session_state.show_delete_user_confirm_popup = True
                    st.rerun()
            with col3:
                if st.button(f"Alterar Senha: {selected_username_from_display}", key=f"change_password_{selected_user_id_from_display}"):
                    st.session_state.change_password_user_id = selected_user_id_from_display
                    st.session_state.change_password_username = selected_username_from_display
                    st.session_state.show_change_password_form = True
                    st.rerun()
        else:
            st.info("Selecione um usuário na tabela para editar, excluir ou alterar a senha.")

    else:
        st.info("Nenhum usuário cadastrado. Adicione um novo usuário.")

    st.markdown("---")
    st.write("Esta tela permite gerenciar usuários da aplicação, incluindo suas permissões de acesso às diferentes telas.")

# NOVO: Função para exibir o formulário de alteração de senha
def display_change_password_form():
    user_id_or_username = st.session_state.get('change_password_user_id')
    username = st.session_state.get('change_password_username')

    if user_id_or_username is None or username is None:
        st.error("Nenhum usuário selecionado para alterar a senha.")
        st.session_state.show_change_password_form = False
        return

    with st.form(key=f"change_password_form_{user_id_or_username}"): # Usar id_or_username para key
        st.markdown(f"### Alterar Senha para: {username}")
        new_password = st.text_input("Nova Senha", type="password", key=f"new_password_input_{user_id_or_username}")
        confirm_password = st.text_input("Confirmar Nova Senha", type="password", key=f"confirm_password_input_{user_id_or_username}")

        col_save, col_cancel = st.columns(2)
        with col_save:
            if st.form_submit_button("Salvar Nova Senha"):
                if new_password and confirm_password:
                    if new_password == confirm_password:
                        # Chama a função de lógica de negócio que usa db_utils
                        if atualizar_senha_usuario_db(user_id_or_username, new_password, username):
                            st.session_state.show_change_password_form = False
                            st.session_state.change_password_user_id = None
                            st.session_state.change_password_username = None
                            st.rerun()
                        # Mensagem de erro já é tratada por atualizar_senha_usuario_db
                    else:
                        st.error("As senhas não coincidem.")
                else:
                    st.warning("Por favor, preencha ambos os campos de senha.")
        with col_cancel:
            if st.form_submit_button("Cancelar"):
                st.session_state.show_change_password_form = False
                st.session_state.change_password_user_id = None
                st.session_state.change_password_username = None
                st.rerun()

