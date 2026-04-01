# app.py - Microserviço para rodar no Railway
# Responsável por receber pedidos da Shopee via n8n, processá-los e salvar no Firestore

import os
import re
import json
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
import firebase_admin
from firebase_admin import credentials
from google.cloud import firestore
from google.oauth2 import service_account

# --- INICIALIZAÇÃO DO FLASK ---
app = Flask(__name__)

# --- 1. CONFIGURAÇÃO DO FIREBASE PRINCIPAL (PEDIDOS) ---
try:
    creds_json_str = os.environ.get('FIREBASE_CREDENTIALS_JSON')
    if not creds_json_str:
        raise ValueError("A variável de ambiente FIREBASE_CREDENTIALS_JSON não foi definida.")
    
    creds_dict = json.loads(creds_json_str)
    google_creds = service_account.Credentials.from_service_account_info(creds_dict)
    
    if not firebase_admin._apps:
        firebase_admin_cred = credentials.Certificate(creds_dict)
        firebase_admin.initialize_app(firebase_admin_cred)
    
    db = firestore.Client(
        project=creds_dict['project_id'],
        credentials=google_creds,
        database='shopee-pedidos-creativusfabrica'
    )
    print("Conexão com Firestore Principal (Pedidos) estabelecida com sucesso!")

except Exception as e:
    print(f"ERRO CRÍTICO: Não foi possível conectar ao Firestore de Pedidos. Erro: {e}")
    db = None

# --- 2. CONFIGURAÇÃO DO FIREBASE SECUNDÁRIO (MAPEAMENTOS) ---
try:
    mapping_creds_json_str = os.environ.get('FIREBASE_MAPPING_JSON')
    if not mapping_creds_json_str:
        raise ValueError("A variável de ambiente FIREBASE_MAPPING_JSON não foi definida.")
    
    mapping_creds_dict = json.loads(mapping_creds_json_str)
    mapping_google_creds = service_account.Credentials.from_service_account_info(mapping_creds_dict)
    
    mapping_db = firestore.Client(
        project=mapping_creds_dict['project_id'],
        credentials=mapping_google_creds
    )
    print("Conexão com Firestore Secundário (Mapeamentos) estabelecida com sucesso!")
    
    # --- LOG DE INICIALIZAÇÃO (PROVA DE VIDA) ---
    print("\n" + "="*50)
    print("--- INICIANDO LEITURA DOS MAPEAMENTOS NO BANCO DE DADOS ---")
    COMPANY_ID = "gswfIc8n97NyFPnlJFnf"
    docs = mapping_db.collection("mappings").where("companyId", "==", COMPANY_ID).stream()
    
    count = 0
    for doc in docs:
        data = doc.to_dict()
        print(f"Mapeamento Carregado -> Código: {data.get('code')}, Tipo: {data.get('type')}, Tradução: {data.get('translation')}")
        count += 1
    print(f"--- TOTAL DE MAPEAMENTOS CARREGADOS NA INICIALIZAÇÃO: {count} ---")
    print("="*50 + "\n")

except Exception as e:
    print(f"ERRO CRÍTICO: Não foi possível conectar ao Firestore de Mapeamentos. Erro: {e}")
    mapping_db = None


# --- MAPEAMENTOS INTERNOS MANTIDOS (NÃO APAGAR) ---

shopIdMap = {
    334593801: "Creativus Fábrica",
    1300792309: "Rei dos Apliques"
}

rendimentoPlacas = {
    "2020": 50, "3015": 50, "4020": 25, "1515": 55, "3030": 24,
    "3010": 70, "5151": 10, "3013": 50, "4010": 50, "4015": 35,
    "2508": 105, "3510": 60, "1414": 90, "1313": 110, "3918": 30, "1525": 55
}


# --- FUNÇÕES AUXILIARES ---

def get_dynamic_mappings():
    """
    Consulta o banco de mapeamentos em tempo real e devolve 4 dicionários
    separados por tipo para serem usados na tradução do SKU.
    """
    dyn_cores = {}
    dyn_formatos = {}
    dyn_furos = {}
    dyn_variacoes = {}
    
    if not mapping_db:
        print("AVISO: Banco de Mapeamentos indisponível. Traduções não serão feitas.")
        return dyn_cores, dyn_formatos, dyn_furos, dyn_variacoes
        
    try:
        COMPANY_ID = "gswfIc8n97NyFPnlJFnf"
        docs = mapping_db.collection("mappings").where("companyId", "==", COMPANY_ID).stream()
        
        for doc in docs:
            data = doc.to_dict()
            code = data.get("code")
            m_type = data.get("type")
            translation = data.get("translation")
            
            if not code or not m_type or not translation:
                continue
                
            code = str(code).upper()
            
            if m_type == "COR":
                dyn_cores[code] = translation
            elif m_type == "FORMATO":
                dyn_formatos[code] = translation
            elif m_type == "FURO":
                dyn_furos[code] = translation
            elif m_type == "VARIACAO":
                dyn_variacoes[code] = translation
                
    except Exception as e:
        print(f"ERRO ao buscar mapeamentos em tempo real: {e}")
        
    return dyn_cores, dyn_formatos, dyn_furos, dyn_variacoes


def safe_float(value, default=0.0):
    """Tenta converter um valor para float, retornando um padrão em caso de falha."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

def calculate_fallback_delivery_date(creation_date_str):
    """Calcula a data de entrega base com fallback."""
    if not creation_date_str:
        return ""

    try:
        creation_date = datetime.strptime(str(creation_date_str), "%d/%m/%Y, %H:%M:%S")
    except (ValueError, TypeError):
        try:
            if isinstance(creation_date_str, (int, float)):
                creation_date = datetime.fromtimestamp(creation_date_str)
            else:
                date_str_iso = str(creation_date_str)
                if date_str_iso.endswith('Z'):
                    date_str_iso = date_str_iso[:-1] + '+00:00'
                creation_date = datetime.fromisoformat(date_str_iso)
        except (ValueError, TypeError) as e:
            creation_date = datetime.now()

    weekday = creation_date.weekday()
    hour = creation_date.hour
    delivery_date = creation_date.date()

    if weekday == 0: delivery_date += timedelta(days=1)
    elif weekday == 1 and hour >= 11: delivery_date += timedelta(days=1)
    elif weekday == 2 and hour >= 11: delivery_date += timedelta(days=1)
    elif weekday == 3 and hour >= 11: delivery_date += timedelta(days=1)
    elif weekday == 4 and hour >= 11: delivery_date += timedelta(days=3)
    elif weekday == 5: delivery_date += timedelta(days=2)
    elif weekday == 6: delivery_date += timedelta(days=2)

    return delivery_date.strftime('%d/%m/%Y')

def process_shop_id_update(order_data):
    """Processa a atualização da conta do e-commerce."""
    try:
        order_id = order_data.get("pedido")
        shop_id = order_data.get("shop_id")

        if not order_id: return None
        
        try: shop_id_int = int(shop_id)
        except (ValueError, TypeError): return None

        nome_loja = shopIdMap.get(shop_id_int, "Loja Desconhecida")
        return {'id': order_id, 'contaEcommerce': nome_loja}
    except Exception:
        return None

def process_user_cliente_update(order_data):
    """Processa a atualização do user_cliente Shopee."""
    try:
        order_id = order_data.get("pedido") or order_data.get("order_sn")
        user_cliente = order_data.get("user_cliente", "")

        if not order_id: return None
        return {'id': order_id, 'userCliente': user_cliente}
    except Exception:
        return None

# ATENÇÃO: Os dicionários dinâmicos agora são passados como parâmetros!
def process_webhook_order(order_data, dyn_cores, dyn_formatos, dyn_furos, dyn_variacoes):
    """Processa um único item e mapeia com os dados dinâmicos."""
    try:
        sku_bruto = order_data.get("item_sku", "")
        sku_original = sku_bruto.split(" ")[0]
        
        quantidade_original = int(order_data.get("quantidade", 1))
        order_id = order_data.get("pedido", "ID_DESCONHECIDO")
        
        partes = sku_original.split("-")
        
        # --- LÓGICA DO "PLA" MANTIDA INTACTA ---
        placas_pre_calculadas = None
        if len(partes) == 8 and partes[7].startswith("PLA"):
            try:
                qtd_placa_sku = int(partes[7].replace("PLA", ""))
                placas_pre_calculadas = qtd_placa_sku * quantidade_original
            except ValueError:
                placas_pre_calculadas = None
            partes.pop()

        if len(partes) < 7 or not partes[5].isdigit():
            sku_padrao = "XXXX-XXXX-XXX-XX-XXX-XXX-XXXXF"
            partes = sku_padrao.split("-")

        qtd_sku_in_part = int(partes[5]) if partes[5].isdigit() else 1
        nova_qtd = qtd_sku_in_part * quantidade_original
        partes[5] = str(nova_qtd).zfill(3)
        sku_final = "-".join(partes)

        partes_mapeamento = sku_final.split("-")
        grupo1, grupo2, grupo3, _, grupo5, _, grupo7 = partes_mapeamento
        
        # --- CÁLCULO DE PLACAS MANTIDO INTACTO ---
        if placas_pre_calculadas is not None:
            placas = placas_pre_calculadas
        else:
            placas = rendimentoPlacas.get(grupo2, 0)
            if placas != 0:
                placas = (nova_qtd + placas - 1) // placas

        # --- A MÁGICA DA TRADUÇÃO DINÂMICA ---
        cor = dyn_cores.get(grupo5, "Desconhecido")
        formato = dyn_formatos.get(grupo1, "Desconhecido")
        furo = dyn_furos.get(grupo3, "Desconhecido")
        variacao = dyn_variacoes.get(grupo7, "Desconhecido")

        tamanho_x_str, tamanho_y_str = grupo2[0:2], grupo2[2:]
        tamanho_x = int(tamanho_x_str) / 10 if tamanho_x_str.isdigit() else "N/A"
        tamanho_y = int(tamanho_y_str) / 10 if tamanho_y_str.isdigit() else "N/A"
        tamanho_formatado = f"{tamanho_x}x{tamanho_y} Cm" if "N/A" not in (tamanho_x, tamanho_y) else "Desconhecido"
        
        situacao = "Fazer arquivo" if sku_final.endswith("F") else "Arquivo Padronizado"
        
        ship_by_at_raw = order_data.get("ship_by_at")
        data_entrega_final = ""

        if ship_by_at_raw and str(ship_by_at_raw).strip():
            date_str_part = str(ship_by_at_raw).split(",")[0].strip()
            try:
                dt_obj = datetime.strptime(date_str_part, '%Y-%m-%d')
                data_entrega_final = dt_obj.strftime('%d/%m/%Y')
            except ValueError:
                data_entrega_final = date_str_part
        else:
            creation_date = order_data.get("created_at")
            data_entrega_final = calculate_fallback_delivery_date(creation_date)

        return {
            'id': order_id, 'situacao': situacao, 'material': cor, 'qntPlacas': placas,
            'formato': formato, 'tamanho': tamanho_formatado, 'furo': furo, 'planoCorte': '',
            'skuPlanoCorte': f'{"-".join(partes_mapeamento[0:5])}.dxf' if "XXXX" not in sku_final else "N/A",
            'tipoArte': variacao, 'sku': sku_final, 'motivoRetrabalho': '',
            'dataEntrega': data_entrega_final, 'ecommerce': 'Shopee', 
            'dataPedidoFeito': order_data.get("created_at"), 'cliente': order_data.get("cliente"),
            'valorTotal': safe_float(order_data.get("valor_total_pedido")),
            'comissaoEcommerce': safe_float(order_data.get("comissao_total_pedido")),
            'taxaEcommerce': safe_float(order_data.get("taxa_servico_total_pedido")),
            'frete': safe_float(order_data.get("frete_pago_total")),
        }
    except Exception as e:
        print(f"ERRO ao processar dados do pedido '{order_data.get('pedido')}': {e}")
        return None

def save_order_to_firestore(order_data):
    """Salva no banco de PEDIDOS."""
    if not db: return False
    try:
        order_id = order_data.get('id')
        doc_ref = db.collection('pedidos_ativos').document(str(order_id))
        doc_ref.set(order_data, merge=True)
        return True
    except Exception:
        return False

def delete_order_from_firestore(order_id):
    """Exclui do banco de PEDIDOS."""
    if not db: return False
    try:
        doc_ref = db.collection('pedidos_ativos').document(str(order_id))
        doc_ref.delete()
        return True
    except Exception:
        return False

# --- ROTA DO WEBHOOK ---
@app.route('/webhook/shopee/new-order', methods=['POST'])
def webhook_shopee_new_order():
    if not db:
        return jsonify(message="Erro interno: Serviço de banco de dados indisponível."), 503

    try:
        data = request.get_json()
        if not data: data = json.loads(request.data)
    except Exception:
        return jsonify(message="Erro ao decodificar o JSON."), 400

    orders_data = data if isinstance(data, list) else [data]

    # ATENÇÃO: Consulta o banco de mapeamentos em TEMPO REAL para este lote de pedidos.
    # Fazemos isso fora do loop para não sobrecarregar o banco lendo os mesmos dados 10 vezes se chegarem 10 itens!
    dyn_cores, dyn_formatos, dyn_furos, dyn_variacoes = get_dynamic_mappings()

    successful_orders_info = []
    successful_shop_id_updates = 0
    successful_user_cliente_updates = 0
    delete_count = 0
    errors = []
    
    for order_item in orders_data:
        status = order_item.get("status")
        order_id = order_item.get("pedido") or order_item.get("order_sn") 
        shop_id = order_item.get("shop_id")
        item_sku = order_item.get("item_sku")
        user_cliente = order_item.get("user_cliente")

        if status == "CANCELLED":
            if not order_id:
                errors.append(f"Recebido pedido de cancelamento sem 'pedido'.")
                continue
            if delete_order_from_firestore(order_id): delete_count += 1
            else: errors.append(f"Falha ao excluir o pedido cancelado: {order_id}")

        elif shop_id and order_id:
            processed_data = process_shop_id_update(order_item)
            if processed_data:
                if save_order_to_firestore(processed_data): successful_shop_id_updates += 1
                else: errors.append(f"Falha ao salvar shop_id no banco de dados: {order_id}")
            else: errors.append(f"Falha ao processar atualização de shop_id: {order_id}")

        elif user_cliente and order_id:
            processed_data = process_user_cliente_update(order_item)
            if processed_data:
                if save_order_to_firestore(processed_data): successful_user_cliente_updates += 1
                else: errors.append(f"Falha ao salvar user_cliente no banco de dados: {order_id}")
            else: errors.append(f"Falha ao processar atualização de user_cliente: {order_id}")

        elif item_sku and order_id:
            # Passa os dicionários fresquinhos para a função de processamento
            processed_order = process_webhook_order(order_item, dyn_cores, dyn_formatos, dyn_furos, dyn_variacoes)
            if processed_order:
                if save_order_to_firestore(processed_order):
                    successful_orders_info.append({'id': processed_order.get('id'), 'dataEntrega': processed_order.get('dataEntrega', 'N/A')})
                else: errors.append(f"Falha ao salvar item de pedido no banco de dados: {order_id}")
            else: errors.append(f"Falha ao processar o item de pedido: {order_id}")
        
        else:
            errors.append(f"Payload desconhecido ou incompleto para: {order_id}")

    message_parts = []
    if successful_orders_info:
        if len(successful_orders_info) == 1:
            message_parts.append(f"1 item de pedido salvo/atualizado com sucesso. Data de entrega: {successful_orders_info[0]['dataEntrega']}")
        else:
            message_parts.append(f"{len(successful_orders_info)} item(ns) de pedido salvo(s)/atualizado(s) com sucesso")

    if successful_shop_id_updates > 0: message_parts.append(f"{successful_shop_id_updates} pedido(s) atualizado(s) com a conta")
    if successful_user_cliente_updates > 0: message_parts.append(f"{successful_user_cliente_updates} pedido(s) atualizado(s) com o usuário")
    if delete_count > 0: message_parts.append(f"{delete_count} pedido(s) excluído(s)")
    
    if not message_parts and not errors: final_message = "Nenhuma ação realizada."
    else: final_message = ". ".join(message_parts) + "."

    if not errors: return jsonify(message=final_message), 200
    else: return jsonify(message=f"Operação concluída com erros. {final_message}", errors=errors), 207

# --- ROTA DE VERIFICAÇÃO ---
@app.route('/')
def health_check():
    return "Webhook para Pedidos Shopee está online e pronto para receber dados do n8n."

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
