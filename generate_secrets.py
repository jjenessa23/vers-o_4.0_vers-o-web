import json
import os

# SUBSTITUA 'prucomex-firebase-adminsdk-fbsvc-4911573050.json'
# PELO NOME EXATO DO NOVO ARQUIVO JSON QUE VOCÊ BAIXOU DO FIREBASE.
json_file_path = 'prucomex-firebase-adminsdk-fbsvc-b88db6bddd.json' # <-- ATUALIZE ESTE CAMINHO/NOME DO ARQUIVO

# Se o arquivo JSON estiver na mesma pasta que este script, você pode deixá-lo assim.
# Caso contrário, forneça o caminho completo para o arquivo.
# Exemplo: json_file_path = '/caminho/completo/para/seu/novo/prucomex-firebase-adminsdk-fbsvc-NOVACHAVE.json'

if not os.path.exists(json_file_path):
    print(f"ERRO: O arquivo '{json_file_path}' não foi encontrado.")
    print("Por favor, verifique se o nome do arquivo e o caminho estão corretos.")
    print("Certifique-se de que o arquivo JSON do Firebase foi baixado e está acessível.")
    exit()

try:
    with open(json_file_path, 'r') as f:
        service_account_json_data = json.load(f)

    # Escapa as quebras de linha na private_key
    # Isso é CRÍTICO para que o JSON dentro do TOML seja válido
    if 'private_key' in service_account_json_data:
        service_account_json_data['private_key'] = service_account_json_data['private_key'].replace('\n', '\\n')
    else:
        print("AVISO: A chave 'private_key' não foi encontrada no seu arquivo JSON de credenciais.")
        print("Isso pode causar problemas. Por favor, verifique se o arquivo está correto.")

    # Converte o dicionário modificado para uma string JSON formatada para o secrets.toml.
    # json.dumps cuidará de escapar outras aspas e caracteres especiais,
    # e `indent=2` tornará a string mais legível dentro do TOML.
    json_string_for_toml = json.dumps(service_account_json_data, indent=2)

    # Imprime o conteúdo final para copiar e colar no secrets.toml
    print("\n--- COPIE O CONTEÚDO ABAIXO PARA O SEU secrets.toml ---")
    print("[firestore_service_account]")
    print('credentials_json = """')
    print(json_string_for_toml)
    print('"""')
    print("--- FIM DO CONTEÚDO ---")
    print("\nLembre-se de substituir TODO o conteúdo do seu secrets.toml por este.")
    print("E verifique se NÃO há espaços ou linhas extras antes ou depois das aspas triplas.")

except json.JSONDecodeError as e:
    print(f"ERRO: O arquivo JSON '{json_file_path}' está malformado. Detalhes: {e}")
    print("Por favor, verifique se é um JSON válido.")
except Exception as e:
    print(f"Ocorreu um erro inesperado ao processar o arquivo: {e}")

