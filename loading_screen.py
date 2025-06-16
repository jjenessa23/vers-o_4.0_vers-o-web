# -*- coding: utf-8 -*-
import tkinter as tk
from tkinter import ttk, messagebox
from PIL import Image, ImageTk
import time
import os
import hashlib # Importar hashlib para hashing de senha
import sqlite3 # Importar sqlite3 para conectar ao DB de usuários
import logging # Importar logging

# Configuração de logging para este módulo
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) # Defina o nível de log apropriado

class TelaCarregamento:
    # --- MODIFICADO: Adiciona logo_path como argumento ---
    def __init__(self, root, callback_app_principal, db_path_usuarios, app_version_str="", logo_path="image_loading.png"): # Usando image_loading.png como padrão
    # --- FIM MODIFICADO ---
        self.root = root
        self.callback_app_principal = callback_app_principal
        self.db_path_usuarios = db_path_usuarios # Caminho do DB de usuários
        self.app_version_str = app_version_str # Versão da aplicação
        self.logo_path = logo_path # Caminho para o arquivo do logo

        self.janela_carregamento = tk.Toplevel(root)
        # --- MODIFICADO: Título inicial pode ser "Carregando..." ou vazio ---
        self.janela_carregamento.title("Gerenciamento COMEX")
        # --- FIM MODIFICADO ---
        # Remove as decorações da janela
        self.janela_carregamento.overrideredirect(True)

        # --- MODIFICADO: Define o tamanho da janela (aumentado ligeiramente a altura) ---
        largura_janela = 400
        altura_janela = 480 # Aumentado um pouco mais para melhor espaçamento
        # --- FIM MODIFICADO ---

        # Obtém as dimensões da tela e centraliza a janela
        largura_tela = self.janela_carregamento.winfo_screenwidth()
        altura_tela = self.janela_carregamento.winfo_screenheight()
        centro_x = int(largura_tela/2 - largura_janela/2)
        centro_y = int(altura_tela/2 - altura_janela/2)
        self.janela_carregamento.geometry(f'{largura_janela}x{altura_janela}+{centro_x}+{centro_y}')

        # Define a cor de fundo para um tema escuro
        cor_fundo_escuro = '#2E2E2E' # Exemplo de cor escura
        cor_texto_claro = '#EAEAEA' # Cor do texto para contraste
        cor_entrada = '#4A4A4A' # Cor para campos de entrada
        cor_borda_entrada = '#6A6A6A' # Cor da borda para campos de entrada
        cor_botao = '#4A4A4A' # Cor para botões
        cor_botao_ativo = '#6A6A6A' # Cor para botão ativo
        cor_texto_botao = '#EAEAEA' # Cor do texto para botões
        cor_texto_botao_ativo = '#FFFF00' # Cor do texto para botão ativo

        self.janela_carregamento.config(bg=cor_fundo_escuro)

        # Configurações de estilo para os widgets
        style = ttk.Style()
        style.theme_use('clam') # Use o tema clam como base
        style.configure('Login.TButton',
                        background=cor_botao,
                        foreground=cor_texto_botao,
                        font=('Segoe UI', 10, 'bold'),
                        padding=(10, 5))
        style.map('Login.TButton',
                  background=[('active', cor_botao_ativo)],
                  foreground=[('active', cor_texto_botao_ativo)])
        style.configure('TLabel', background=cor_fundo_escuro, foreground=cor_texto_claro) # Estilo base para labels
        style.configure('TEntry', fieldbackground=cor_entrada, foreground=cor_texto_claro, insertbackground=cor_texto_claro) # Estilo base para entradas
        style.configure('TProgressbar', background='#0078D7', troughcolor='#4A4A4A') # Estilo para barra de progresso


        # --- MODIFICADO: Frame principal para conter os elementos ---
        self.frame_conteudo = tk.Frame(self.janela_carregamento, bg=cor_fundo_escuro)
        self.frame_conteudo.pack(expand=True, fill=tk.BOTH, padx=20, pady=20)
        # --- FIM MODIFICADO ---

        # --- MODIFICADO: Elementos visíveis inicialmente (Logo, Login, Botões) ---

        # Adiciona uma imagem (logo) na parte superior
        self.foto = None # Inicializa como None
        self.label_imagem = None # Inicializa como None
        if os.path.exists(self.logo_path):
            try:
                img = Image.open(self.logo_path)
                # Redimensiona a imagem para caber na janela inicial (ajuste conforme necessário)
                # Mantém a proporção, ajustando para a menor dimensão da janela
                max_size = 150 # Tamanho maior para o logo no topo
                img.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)

                self.foto = ImageTk.PhotoImage(img)
                # Define o fundo do label da imagem para a cor escura
                self.label_imagem = tk.Label(self.frame_conteudo, image=self.foto, bg=cor_fundo_escuro)
                self.label_imagem.pack(pady=(0, 15)) # Ajusta padding
            except Exception as e:
                logger.warning(f"Aviso: Não foi possível carregar a imagem '{self.logo_path}'. Erro: {e}")
                # Define o fundo e a cor do texto do placeholder para o tema escuro
                self.label_imagem = tk.Label(self.frame_conteudo, text="Placeholder da Logo", bg=cor_fundo_escuro, fg=cor_texto_claro, font=("Segoe UI", 12))
                self.label_imagem.pack(pady=(0, 15)) # Ajusta padding
        else:
            logger.warning(f"Aviso: Arquivo '{self.logo_path}' não encontrado. A imagem não será exibida na tela de carregamento.")
            # Define o fundo e a cor do texto do placeholder para o tema escuro
            self.label_imagem = tk.Label(self.frame_conteudo, text="Placeholder da Logo", bg=cor_fundo_escuro, fg=cor_texto_claro, font=("Segoe UI", 12))
            self.label_imagem.pack(pady=(0, 15)) # Ajusta padding


        # --- REMOVIDO: Label para o título da aplicação ---
        # lbl_app_title = tk.Label(self.frame_conteudo, text="Gerenciamento COMEX", font=("Segoe UI", 14, "bold"), bg=cor_fundo_escuro, fg=cor_texto_claro)
        # lbl_app_title.pack(pady=(0, 15)) # Ajusta padding
        # --- FIM REMOVIDO ---


        # Frame para os campos de login
        self.frame_campos_login = tk.Frame(self.frame_conteudo, bg=cor_fundo_escuro)

        lbl_usuario = tk.Label(self.frame_campos_login, text="Usuário:", font=("Segoe UI", 10), bg=cor_fundo_escuro, fg=cor_texto_claro)
        lbl_usuario.pack(anchor=tk.W)
        self.entry_usuario = tk.Entry(self.frame_campos_login, font=("Segoe UI", 10), bg=cor_entrada, fg=cor_texto_claro, insertbackground=cor_texto_claro, relief=tk.FLAT, borderwidth=1)
        self.entry_usuario.pack(fill=tk.X, pady=5)

        lbl_senha = tk.Label(self.frame_campos_login, text="Senha:", font=("Segoe UI", 10), bg=cor_fundo_escuro, fg=cor_texto_claro)
        lbl_senha.pack(anchor=tk.W)
        self.entry_senha = tk.Entry(self.frame_campos_login, show="*", font=("Segoe UI", 10), bg=cor_entrada, fg=cor_texto_claro, insertbackground=cor_texto_claro, relief=tk.FLAT, borderwidth=1)
        self.entry_senha.pack(fill=tk.X, pady=5)

        self.lbl_mensagem = tk.Label(self.frame_campos_login, text="", font=("Segoe UI", 9), bg=cor_fundo_escuro, fg="red")
        self.lbl_mensagem.pack(pady=(10, 0))

        self.frame_campos_login.pack(fill=tk.X, pady=(0, 15)) # Empacota o frame de campos de login com mais padding


        # Frame para os botões de login
        self.frame_botoes_login = tk.Frame(self.frame_conteudo, bg=cor_fundo_escuro)

        btn_login = ttk.Button(self.frame_botoes_login, text="Entrar", command=self.tentar_login, style='Login.TButton')
        btn_login.pack(side=tk.LEFT, padx=5)

        btn_cancelar = ttk.Button(self.frame_botoes_login, text="Cancelar", command=self.cancelar_login, style='Login.TButton')
        btn_cancelar.pack(side=tk.LEFT, padx=5)

        self.frame_botoes_login.pack(pady=(0, 25)) # Empacota o frame de botões de login com mais padding

        # --- FIM MODIFICADO ---

        # --- MODIFICADO: Barra de progresso (inicialmente oculta) ---
        self.barra_progresso = ttk.Progressbar(self.frame_conteudo, orient="horizontal", length=300, mode="determinate", style='TProgressbar')
        # Não empacota a barra de progresso aqui, ela será empacotada após o login
        # self.barra_progresso.pack(pady=10)
        # --- FIM MODIFICADO ---


        # Adiciona label para a versão da aplicação na parte inferior (fora dos frames de conteúdo)
        lbl_versao = tk.Label(self.janela_carregamento, text=f"v{self.app_version_str}",
                                     bg=cor_fundo_escuro, fg=cor_texto_claro, font=("Segoe UI", 8))
        lbl_versao.pack(side=tk.BOTTOM, pady=10) # Aumentado o padding inferior

        # Foca no campo de usuário ao abrir a janela
        self.entry_usuario.focus_set()

        # Liga a tecla Enter ao botão de login
        self.janela_carregamento.bind('<Return>', lambda event=None: btn_login.invoke())

        # Variáveis para controlar a animação da barra de progresso
        self._after_id_atualizar = None
        self._after_id_fechar = None
        self._progress_start_time = None
        # Define a duração total da tela de carregamento APÓS o login
        self._duracao_loading_ms = 3000 # 3 segundos para a barra ir de 0 a 100%


    def conectar_db_usuarios(self):
        """Conecta ao banco de dados de usuários."""
        if not self.db_path_usuarios:
            logger.error("Caminho do DB de usuários não definido.")
            messagebox.showerror("Erro DB", "Caminho do banco de dados de usuários não definido.")
            return None
        try:
            conn = sqlite3.connect(self.db_path_usuarios)
            return conn
        except Exception as e:
            logger.exception(f"Erro ao conectar ao DB de usuários em {self.db_path_usuarios}")
            messagebox.showerror("Erro DB Usuários", f"Não foi possível conectar ao banco de dados de usuários:\n{self.db_path_usuarios}\n{e}")
            return None

    def verificar_credenciais(self, username, password):
        """Verifica as credenciais do usuário no banco de dados."""
        conn = self.conectar_db_usuarios()
        if conn is None:
            return None # Retorna None em caso de erro de conexão

        try:
            cursor = conn.cursor()
            # Busca o usuário pelo nome de usuário
            cursor.execute("SELECT username, password_hash, is_admin FROM users WHERE username = ?", (username,))
            user_data = cursor.fetchone()

            if user_data:
                db_username, stored_password_hash, is_admin = user_data
                # Cria o hash da senha fornecida com um salt (usando o username como salt simples)
                provided_password_hash = hashlib.sha256((password + db_username).encode('utf-8')).hexdigest()

                # Compara o hash da senha fornecida com o hash armazenado
                if provided_password_hash == stored_password_hash:
                    logger.info(f"Login bem-sucedido para o usuário: {username}")
                    # Retorna os dados do usuário (username e status de admin)
                    return {'username': db_username, 'is_admin': bool(is_admin)}
                else:
                    logger.warning(f"Tentativa de login falhou para o usuário {username}: Senha incorreta.")
                    return False # Credenciais incorretas
            else:
                logger.warning(f"Tentativa de login falhou: Usuário '{username}' não encontrado.")
                return False # Usuário não encontrado

        except Exception as e:
            logger.exception(f"Erro ao verificar credenciais para o usuário {username}")
            messagebox.showerror("Erro DB", f"Erro ao verificar credenciais no banco de dados:\n{e}")
            return None # Retorna None em caso de erro durante a consulta

        finally:
            if conn:
                conn.close()

    def tentar_login(self):
        """Tenta autenticar o usuário com as credenciais fornecidas."""
        username = self.entry_usuario.get().strip()
        password = self.entry_senha.get().strip()

        if not username or not password:
            self.lbl_mensagem.config(text="Por favor, insira usuário e senha.")
            return

        # Limpa a mensagem de erro anterior
        self.lbl_mensagem.config(text="")

        # Verifica as credenciais no banco de dados
        user_info = self.verificar_credenciais(username, password)

        if user_info is not None: # user_info é um dicionário (sucesso) ou False (falha)
            if user_info: # Login bem-sucedido
                logger.info("Login bem-sucedido. Iniciando animação de carregamento.")
                # --- MODIFICADO: Inicia a animação de carregamento ---
                self.iniciar_carregamento_apos_login(user_info)
                # --- FIM MODIFICADO ---
            else: # Login falhou (credenciais incorretas)
                self.lbl_mensagem.config(text="Usuário ou senha incorretos.")
                logger.warning("Login falhou: Usuário ou senha incorretos.")
                # Limpa o campo de senha para nova tentativa
                self.entry_senha.delete(0, tk.END)
                self.entry_senha.focus_set()
        else: # Erro durante a verificação (erro de DB, etc.)
             self.lbl_mensagem.config(text="Erro ao verificar login. Verifique o log.")
             # O erro já foi logado e mostrado via messagebox pela função verificar_credenciais

    # --- NOVO: Função para iniciar a animação de carregamento após o login ---
    def iniciar_carregamento_apos_login(self, user_info):
        """Esconde os campos de login e botões, mostra a barra de loading e inicia a animação."""
        # Esconde os frames de campos e botões de login
        self.frame_campos_login.pack_forget()
        self.frame_botoes_login.pack_forget()

        # Empacota a barra de progresso
        self.barra_progresso.pack(pady=10)

        # Agenda o fechamento da tela de carregamento após a duração total
        # Passa as informações do usuário para o callback principal
        self._after_id_fechar = self.janela_carregamento.after(
            self._duracao_loading_ms,
            lambda: self.fechar_tela_carregamento(user_info) # Passa user_info para o callback
        )

        # Agenda a primeira atualização da barra imediatamente
        self._progress_start_time = time.time() # Reseta o tempo de início
        self.atualizar_progresso() # Inicia a atualização da barra

    # --- FIM NOVO ---

    # --- MODIFICADO: Função para atualizar a barra de progresso (reintroduzida) ---
    def atualizar_progresso(self):
        """Atualiza o valor da barra de progresso."""
        # Verifica se a janela ainda existe antes de tentar interagir
        if not self.janela_carregamento.winfo_exists():
            # Se a janela não existe mais, para a atualização
            return

        # Registra o tempo de início da barra na primeira chamada
        if self._progress_start_time is None:
            self._progress_start_time = time.time()

        # Calcula o tempo decorrido desde o início da barra
        tempo_decorrido_s = time.time() - self._progress_start_time
        tempo_decorrido_ms = tempo_decorrido_s * 1000

        # Calcula a porcentagem de progresso baseada no tempo decorrido e na duração da barra
        # Garante que o progresso não ultrapasse 100%
        progresso_porcentagem = min(100, (tempo_decorrido_ms / self._duracao_loading_ms) * 100)

        self.barra_progresso['value'] = progresso_porcentagem

        # Agenda a próxima atualização SOMENTE se o progresso ainda não atingiu 100%
        if progresso_porcentagem < 100:
             self._after_id_atualizar = self.janela_carregamento.after(50, self.atualizar_progresso) # Atualiza a cada 50ms para suavidade
        else:
             # O progresso chegou a 100%, não agendamos mais atualizações
             self._after_id_atualizar = None
    # --- FIM MODIFICADO ---


    # --- MODIFICADO: Função para fechar a tela de carregamento (reintroduzida) ---
    # Agora aceita user_info como argumento para passar para o callback principal
    def fechar_tela_carregamento(self, user_info):
        """Fecha a tela de carregamento e chama a função da aplicação principal."""
        # Tenta cancelar a atualização da barra de progresso se ela ainda estiver agendada
        if self._after_id_atualizar is not None:
            try:
                self.janela_carregamento.after_cancel(self._after_id_atualizar)
                # print(f"Debug: Cancelado after ID: {self._after_id_atualizar}")
            except tk.TclError:
                # Ocorre se o after já foi executado ou cancelado de outra forma
                pass # Ignora o erro, a atualização já parou
            self._after_id_atualizar = None # Limpa o ID após tentar cancelar

        # Verifica se a janela ainda existe antes de tentar destruir
        if self.janela_carregamento.winfo_exists():
             self.janela_carregamento.destroy()

        # Chama a função que inicia a aplicação principal, passando as informações do usuário
        self.callback_app_principal(user_info)
    # --- FIM MODIFICADO ---

    # --- NOVO: Função para cancelar o login ---
    def cancelar_login(self):
        """Fecha a janela de login e encerra a aplicação."""
        logger.info("Login cancelado pelo usuário. Encerrando aplicação.")
        if self.janela_carregamento.winfo_exists():
            self.janela_carregamento.destroy()
        # Encerrar a aplicação principal (root)
        self.root.quit() # Usa quit() para sair do mainloop
    # --- FIM NOVO ---
