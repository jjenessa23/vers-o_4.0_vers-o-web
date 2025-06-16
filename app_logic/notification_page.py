import streamlit as st
from datetime import datetime
import json # Para lidar com a string JSON de target_users

# Importar o db_manager e db_utils usando importação relativa,
# pois notification_page.py está em app_logic e os dbs estão no diretório pai.
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import followup_db_manager as db_manager
import db_utils # Assumindo que db_utils também está no diretório pai
from app_logic.utils import set_background_image, set_sidebar_background_image

# Helper function to remove a notification
def _remove_notification(notification_id, deleted_by):
    """
    Marca uma notificação como excluída no banco de dados.
    """
    if db_manager.mark_notification_as_deleted(notification_id, deleted_by):
        st.success("Notificação excluída com sucesso!")
    else:
        st.error("Erro ao excluir notificação.")
    st.rerun()

def _restore_notification(notification_id, restored_by):
    """
    Restaura uma notificação excluída no banco de dados.
    """
    if db_manager.restore_notification(notification_id, restored_by):
        st.success("Notificação restaurada com sucesso!")
    else:
        st.error("Erro ao restaurar notificação.")
    st.rerun()

def _delete_history_entry(history_entry_id: int, deleted_by: str):
    """
    Exclui permanentemente uma entrada do histórico de notificações.
    """
    if db_manager.delete_history_entry_permanently(history_entry_id, deleted_by):
        st.success("Entrada do histórico excluída permanentemente!")
    else:
        st.error("Erro ao excluir permanentemente a entrada do histórico.")
    st.rerun()

# Nova função para obter a contagem de notificações ativas para um usuário
def get_notification_count_for_user(username: str) -> int:
    """
    Retorna o número de notificações ativas para um usuário específico.
    """
    notifications = db_manager.get_active_notifications(username)
    return len(notifications)

# Esta função será chamada pela página inicial (app_main.py)
def display_notifications_on_home(current_username: str):
    """
    Exibe as notificações ativas para o usuário logado na tela inicial do programa.
    """
    st.subheader("Central de Notificações")

    notifications = db_manager.get_active_notifications(current_username)

    if not notifications:
        st.info("Nenhuma notificação recente.")
    else:
        for notification in notifications:
            col_notif_text, col_notif_delete = st.columns([0.9, 0.1])
            with col_notif_text:
                st.warning(f"**Notificação:** {notification['message']} (Criada por: {notification['created_by']} em {notification['created_at']})")
            with col_notif_delete:
                if st.button("🗑️", key=f"delete_home_notif_{notification['id']}", help="Excluir Notificação"):
                    _remove_notification(notification['id'], current_username)

# Nova função para a página de administração de notificações
def show_admin_notification_page():
    """
    Página para administradores criarem, gerenciarem e visualizarem o histórico de notificações.
    """
    st.subheader("Gerenciar Notificações (Admin)")

    current_admin_username = st.session_state.get('user_info', {}).get('username', 'Admin Desconhecido')

    background_image_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'assets', 'logo_navio_atracado.png')
    set_background_image(background_image_path)
    # --- Criar Nova Notificação ---
    st.markdown("#### Criar Nova Notificação")
    with st.form(key="new_notification_form"):
        new_message = st.text_area("Mensagem da Notificação:", height=100)
        
        # Obter lista de usuários para seleção
        all_users = db_manager.get_all_users_from_db() 
        user_options = ["ALL"] + [user['username'] for user in all_users]
        
        selected_users = st.multiselect(
            "Enviar para (selecione um ou mais usuários, ou 'ALL' para todos):",
            options=user_options,
            default=["ALL"]
        )

        submit_button = st.form_submit_button("Criar Notificação")

        if submit_button:
            if new_message:
                # Se "ALL" estiver selecionado, crie uma única notificação para "ALL"
                if "ALL" in selected_users:
                    if db_manager.add_notification(new_message, "ALL", current_admin_username):
                        st.success("Notificação criada e enviada para TODOS os usuários!")
                        st.rerun()
                    else:
                        st.error("Falha ao criar notificação para TODOS.")
                else: # Caso contrário, crie uma notificação para cada usuário selecionado
                    success_count = 0
                    for user in selected_users:
                        if db_manager.add_notification(new_message, user, current_admin_username):
                            success_count += 1
                    if success_count > 0:
                        st.success(f"Notificações criadas e enviadas para {success_count} usuário(s) selecionado(s)!")
                        st.rerun()
                    else:
                        st.error("Falha ao criar notificações para os usuários selecionados.")
            else:
                st.warning("A mensagem da notificação não pode estar vazia.")

    st.markdown("---")

    # --- Notificações Ativas ---
    st.markdown("#### Notificações Ativas")
    active_notifications = db_manager.get_active_notifications() # Busca todas as ativas para admin
    if not active_notifications:
        st.info("Nenhuma notificação ativa no momento.")
    else:
        for notification in active_notifications:
            col_msg, col_target, col_created_by, col_created_at, col_actions = st.columns([0.4, 0.2, 0.15, 0.15, 0.1])
            with col_msg:
                st.write(notification['message'])
            with col_target:
                st.write(notification['target_users']) # target_users agora é uma string simples
            with col_created_by:
                st.write(notification['created_by'])
            with col_created_at:
                st.write(notification['created_at'])
            with col_actions:
                if st.button("🗑️", key=f"delete_admin_notif_{notification['id']}", help="Excluir Notificação"):
                    _remove_notification(notification['id'], current_admin_username)

    st.markdown("---")

    # --- Histórico de Notificações Excluídas ---
    st.markdown("#### Histórico de Notificações Excluídas")
    deleted_notifications = db_manager.get_deleted_notifications()
    if not deleted_notifications:
        st.info("Nenhuma notificação excluída no histórico.")
    else:
        for notification in deleted_notifications:
            col_hist_msg, col_hist_action_at, col_hist_action_by, col_hist_restore, col_hist_delete_perm = st.columns([0.4, 0.2, 0.15, 0.1, 0.1]) # Adicionado mais uma coluna para o novo botão
            with col_hist_msg:
                st.write(notification['original_message']) 
            with col_hist_action_at:
                st.write(notification['action_at'])
            with col_hist_action_by:
                st.write(notification['action_by'])
            with col_hist_restore:
                if st.button("↩️", key=f"restore_notif_{notification['history_entry_id']}", help="Restaurar Notificação"):
                    _restore_notification(notification['original_notification_id'], current_admin_username)
            with col_hist_delete_perm: # Botão de exclusão permanente
                if st.button("❌", key=f"delete_perm_notif_{notification['history_entry_id']}", help="Excluir Permanentemente"):
                    _delete_history_entry(notification['history_entry_id'], current_admin_username)
