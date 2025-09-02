# app.py - Microserviço para rodar no Railway
# Responsável por receber pedidos da Shopee via n8n, processá-los e salvar no Firestore.

import os
import re
import json # Adicionado para processar o JSON da variável de ambiente
from datetime import datetime
from flask import Flask, request, jsonify
import firebase_admin
from firebase_admin import credentials
# CORREÇÃO: Importa diretamente o cliente do Google Cloud Firestore
from google.cloud import firestore
# CORREÇÃO: Importa a biblioteca correta para criar as credenciais
from google.oauth2 import service_account

# --- INICIALIZAÇÃO DO FLASK ---
app = Flask(__name__)

# --- CONFIGURAÇÃO SEGURA DO FIREBASE ---
# As credenciais do Firebase são carregadas de uma variável de ambiente no Railway.
try:
    # 1. Pega o conteúdo JSON da variável de ambiente 'FIREBASE_CREDENTIALS_JSON'.
    creds_json_str = os.environ.get('FIREBASE_CREDENTIALS_JSON')
    if not creds_json_str:
        raise ValueError("A variável de ambiente FIREBASE_CREDENTIALS_JSON não foi definida no Railway.")
    
    # 2. Converte a string JSON em um dicionário Python.
    creds_dict = json.loads(creds_json_str)
    
    # 3. CORREÇÃO: Cria o objeto de credenciais usando a biblioteca 'google.oauth2', que é a esperada pelo firestore.Client.
    google_creds = service_account.Credentials.from_service_account_info(creds_dict)
    
    # A inicialização do app do firebase_admin ainda é útil para outros serviços (como Storage, Auth, etc).
    if not firebase_admin._apps:
        firebase_admin_cred = credentials.Certificate(creds_dict)
        firebase_admin.initialize_app(firebase_admin_cred)
    
    # 4. Cria o cliente do Firestore usando as credenciais no formato correto.
    db = firestore.Client(
        project=creds_dict['project_id'],
        credentials=google_creds,
        database='shopee-pedidos-creativusfabrica'
    )
    print("Conexão com Firestore 'shopee-pedidos-creativusfabrica' estabelecida com sucesso!")

except Exception as e:
    print(f"ERRO CRÍTICO: Não foi possível conectar ao Firestore. Verifique as credenciais e o nome do banco de dados. Erro: {e}")
    db = None

# --- MAPEAMENTOS INTERNOS ---
# Conforme discutido, os mapeamentos estão diretamente no código para agilidade.
rendimentoPlacas = {
    "2020": 50, "3015": 50, "4020": 25, "1515": 55, "3030": 24,
    "3010": 70, "5151": 10, "3013": 50, "4010": 50, "4015": 35,
    "2508": 105, "3510": 60, "1414": 90, "3918": 30
}
coresMap = {
    "DOU": "Dourado", "PRA": "Prata", "ROS": "Rose", "CRU": "Cru",
    "BRA": "Branco", "TRA": "Transparente"
}
formatoMap = {
    "REDO": "Redondo", "AVIA": "Aviãozinho", "CORA": "Coração",
    "PLAC": "Plaquinha", "MOLD": "Moldurinha", "NUVM": "Nuvenzinha",
    "NUVE": "Nuvem", "PLAO": "Plaquinha Oval", "PLAR": "Plaquinha com bolinha redonda no começo",
    "URSI": "Ursinho", "PING": "Pingente", "BORB": "Borboleta",
    "BO3D": "Borboleta 3D", "PROT": "Passante retangular oval no topo",
    "FLOP": "Flor Passante", "APCA": "Aplique Casamento", "MIPA": "Mini Palito",
    "MASC": "Máscara Carnaval", "ARVC": "Arvórezinha"
}
furosMap = {
    "1FS": "1 furo Superior", "1FH": "1 Furo Lateral", "2FH": "2 Furos Lateral",
    "2FV": "2 Furos Vertical", "4FL": "4 Furos, 2 na horizontal e 2 na vertical",
    "4FC": "4 Furos nos cantos", "0SF": "SEM FURO", "2PV": "DOIS FUROS PASSANTES VERTICAL",
    "1PC": "UM PASSANTE NO CENTRO"
}
variacoesMap = {
    "0001F": "Escrita/Logo", "0002F": "Ramo coração data", "0003F": "Três Corações data",
    "0004F": "Coração Barra data", "0005F": "Coração Barra", "0006F": "Três corações",
    "0007F": "&", "0008F": "& e data", "0009F": "Cheguei", "0010F": "Escrita+Estrelas",
    "0011F": "Chá do", "0012F": "Escrita+Corações", "1001P": "Gratidão",
    "1002P": "Você é especial", "1003P": "Feito a mão + novelo",
    "1004P": "Feito a mão com coração vazado no meio", "1005P": "Feito com amor + novelo",
    "1006P": "Caderneta de Saúde", "1007P": "Feliz dia das Mães", "1008P": "Ramo de flor 1",
    "1009P": "Feito com amor", "1010P": "Feliz Páscoa", "1011P": "Gratidão modelo 2",
    "1012P": "Fé", "1013P": "Coração", "1014P": "Fé + Cruz", "1015P": "Ele Vive + Cruz",
    "1016P": "Mãe + Coração", "1017P": "Seja Luz", "1018P": "Bíblia Sagrada",
    "1019P": "Feliz Natal", "0000P": "Sem gravação", "0000F": "Sem gravação"
}

# NOVA FUNÇÃO: Converte um valor para float de forma segura
def safe_float(value, default=0.0):
    """Tenta converter um valor para float, retornando um padrão em caso de falha."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

def process_webhook_order(order_data):
    """
    Processa um único item de pedido do JSON do n8n e o mapeia para o formato do sistema.
    """
    try:
        sku_original = order_data.get("item_sku", "")
        quantidade_original = int(order_data.get("quantidade", 1))
        order_id = order_data.get("pedido", "ID_DESCONHECIDO")
        
        partes = sku_original.split("-")
        if len(partes) < 7 or not partes[5].isdigit():
            print(f"AVISO: SKU '{sku_original}' para o pedido '{order_id}' é inválido. Usando SKU padrão.")
            sku_original = "XXXX-XXXX-XXX-XX-XXX-XXX-XXXXF"
            partes = sku_original.split("-")

        qtd_sku_in_part = int(partes[5]) if partes[5].isdigit() else 1
        nova_qtd = qtd_sku_in_part * quantidade_original
        partes[5] = str(nova_qtd).zfill(3)
        sku_final = "-".join(partes)

        partes_mapeamento = sku_final.split("-")
        grupo1, grupo2, grupo3, _, grupo5, _, grupo7 = partes_mapeamento
        
        placas = rendimentoPlacas.get(grupo2, 0)
        if placas != 0:
            placas = (nova_qtd + placas - 1) // placas

        cor = coresMap.get(grupo5, "Desconhecido")
        formato = formatoMap.get(grupo1, "Desconhecido")
        furo = furosMap.get(grupo3, "Desconhecido")

        tamanho_x_str, tamanho_y_str = grupo2[0:2], grupo2[2:]
        tamanho_x = int(tamanho_x_str) / 10 if tamanho_x_str.isdigit() else "N/A"
        tamanho_y = int(tamanho_y_str) / 10 if tamanho_y_str.isdigit() else "N/A"
        tamanho_formatado = f"{tamanho_x}x{tamanho_y} Cm" if "N/A" not in (tamanho_x, tamanho_y) else "Desconhecido"
        
        variacao = variacoesMap.get(grupo7, "Desconhecido")
        situacao = "Fazer arquivo" if sku_final.endswith("F") else "Arquivo Padronizado"
        ship_by_date_str = order_data.get("ship_by_at", "").split(",")[0]

        return {
            'id': order_id, 'situacao': situacao, 'material': cor, 'qntPlacas': placas,
            'formato': formato, 'tamanho': tamanho_formatado, 'furo': furo, 'planoCorte': '',
            'skuPlanoCorte': f'{"-".join(partes_mapeamento[0:5])}.dxf' if "XXXX" not in sku_final else "N/A",
            'tipoArte': variacao, 'sku': sku_final, 'motivoRetrabalho': '',
            'dataEntrega': ship_by_date_str, 'ecommerce': 'Shopee', 'contaEcommerce': 'Conta Padrão',
            'dataPedidoFeito': order_data.get("created_at"), 'cliente': order_data.get("cliente"),
            # ATUALIZAÇÃO: Usa a função safe_float para garantir que os valores são numéricos
            'valorTotal': safe_float(order_data.get("valor_total_pedido")),
            'comissaoEcommerce': safe_float(order_data.get("comissao_total_pedido")),
            'taxaEcommerce': safe_float(order_data.get("taxa_servico_total_pedido")),
            'frete': safe_float(order_data.get("frete_pago_total")),
        }
    except Exception as e:
        print(f"ERRO ao processar dados do pedido '{order_data.get('pedido')}': {e}")
        return None

def save_order_to_firestore(order_data):
    """Salva o dicionário do pedido processado no Firestore."""
    if not db:
        print("ERRO: Firestore não está conectado. Não é possível salvar o pedido.")
        return False
    try:
        order_id = order_data.get('id')
        doc_ref = db.collection('pedidos_ativos').document(str(order_id))
        doc_ref.set(order_data, merge=True)
        print(f"Pedido '{order_id}' salvo/atualizado com sucesso no Firestore.")
        return True
    except Exception as e:
        print(f"ERRO ao salvar pedido '{order_data.get('id')}' no Firestore: {e}")
        return False

# --- ROTA DO WEBHOOK ---
@app.route('/webhook/shopee/new-order', methods=['POST'])
def webhook_shopee_new_order():
    """Endpoint para receber novos pedidos processados pelo n8n."""
    if not db:
        return jsonify(message="Erro interno: Serviço de banco de dados indisponível."), 503

    # CORREÇÃO: Lógica mais robusta para lidar com o payload do n8n
    try:
        # Tenta obter o JSON. request.json só funciona se o mimetype for application/json.
        data = request.get_json()
        if not data:
             # Se falhar, tenta forçar a leitura do corpo da requisição e converter para JSON.
             # Isso ajuda quando o n8n envia os dados, mas não o header de mimetype correto.
            data = json.loads(request.data)
    except Exception as e:
        print(f"ERRO: Falha ao decodificar o JSON do payload. Erro: {e}")
        print(f"Dados recebidos (brutos): {request.data}")
        return jsonify(message="Erro ao decodificar o JSON. Verifique o formato enviado pelo n8n."), 400

    # Agora que temos 'data', verificamos se é uma lista
    if isinstance(data, list):
        orders_data = data
    else:
        # Se não for uma lista (ex: o n8n enviou apenas o objeto),
        # nós a colocamos dentro de uma lista para padronizar o processamento.
        orders_data = [data]

    success_count = 0
    errors = []
    
    for order_item in orders_data:
        processed_order = process_webhook_order(order_item)
        if processed_order:
            if save_order_to_firestore(processed_order):
                success_count += 1
            else:
                errors.append(f"Falha ao salvar no banco de dados o pedido: {order_item.get('pedido', 'N/A')}")
        else:
            errors.append(f"Falha ao processar o item de pedido: {order_item.get('pedido', 'N/A')}")

    if not errors:
        return jsonify(message=f"{success_count} pedido(s) recebido(s) e salvo(s) com sucesso!"), 200
    else:
        return jsonify(
            message=f"{success_count} pedido(s) salvo(s) com sucesso, mas ocorreram erros.",
            errors=errors
        ), 207

# --- ROTA DE VERIFICAÇÃO ---
@app.route('/')
def health_check():
    """Endpoint simples para verificar se a aplicação está online."""
    return "Webhook para Pedidos Shopee está online e pronto para receber dados do n8n."

# --- EXECUÇÃO DO SERVIDOR ---
if __name__ == '__main__':
    # A porta é definida pelo Railway através da variável de ambiente PORT
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)

