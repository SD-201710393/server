import json
import os
import threading
import time
from flask import Flask
from flask import request

app = Flask(__name__)
# Server details that get pulled from '/info' in their respective order
componente = "server"
ver = "0.3"
desc = "serve os clientes com servicos variados"
acess_point = "https://sd-rdm.herokuapp.com"
is_busy = False                                # If true, it's busy or 'down'. Otherwise, it's 'up'
uid = 2
is_leader = False
elect_running = False
election = "valentao"
web_servers = ["https://sd-201620236.herokuapp.com", "https://sd-jhsq.herokuapp.com",
               "https://sd-mgs.herokuapp.com", "https://sd-app-server-jesulino.herokuapp.com"]


@app.route('/eleicao/coordenador', methods=['POST'])      # Call this to say this server is a leader
def set_coord():
    # TODO
    pass


@app.route('/eleicao', methods=['POST'])      # Call this to say this server is a leader
def elected():
    # TODO
    pass


@app.route('/eleicao', methods=['GET'])      # Call this to acknowledge an election is in process
def ack_election():
    out = {
        "tipo_de_eleicao_ativa": election,
        "eleicao_em_andamento": elect_running
    }
    return json.dumps(out), 200


@app.route('/shutdown', methods=['GET'])    # Call this to simulate a server shutdown
def shtdwn():
    pass


@app.route('/recurso', methods=['GET'])    # Call this to get the resource status
def res_get():
    # global is_busy    # Talvez seja desnecessario ja que e somente acesso
    server_res = {"ocupado": is_busy}
    return json.dumps(server_res), 200


@app.route('/recurso', methods=['POST'])    # Call this to simulate handling a resource
def res():
    global is_busy
    return_code: int
    thr = threading.Thread(target=thr_func, args=())
    if is_busy:
        return_code = 409
    else:
        is_busy = True
        thr.start()
        return_code = 200
    server_res = {"ocupado": is_busy}
    return json.dumps(server_res), return_code


@app.route('/info', methods=['POST'])       # Call this to manually set info (DEBUG function)
def d_set_info():
    return_code: int
    exp_amount = 8          # We expect the incoming JSON to have 8 keys
    global componente
    global ver
    global desc
    global acess_point
    global is_busy
    global uid
    global is_leader
    global election
    # 400 - Bad Request (Data was invalid)
    # 409 - Conflict
    # 200 - OK
    req = request.json
    out = {
        "atualizado": None,
        "falha": None,
        "info_atual": None
    }

    if len(req) != exp_amount:
        return_code = 400
        out["falha"] = f"Quantidade de argumentos diferente de {exp_amount}"
        print("[DEBUG] Invalid length")
    else:
        try:
            t_componente = req["componente"]
            t_ver = req["versao"]
            t_desc = req["descricao"]
            t_acess_point = req["ponto_de_acesso"]
            t_status = req["status"]
            t_uid = req["identificacao"]
            t_is_leader = req["lider"]
            t_election = req["eleicao"]
            # If we reached here, all entries were read successfully
            componente = t_componente
            ver = t_ver
            desc = t_desc
            acess_point = t_acess_point
            is_busy = (True if t_status == "down" else False)
            uid = t_uid
            is_leader = t_is_leader
            election = t_election
            return_code = 200
        except KeyError:    # If we reached here, something was wrong on the dictionary
            return_code = 409
            out["falha"] = "Uma ou mais chaves invalidas"
            print("[DEBUG] One or more keys missing / invalid")

    curr_server_details = {
        "componente": componente,
        "versao": ver,
        "descricao": desc,
        "ponto_de_acesso": acess_point,
        "status": ("down" if is_busy else "up"),
        "identificacao": uid,
        "lider": int(is_leader),
        "eleicao": election
    }
    out["atualizado"] = (True if return_code == 200 else False)
    out["info_atual"] = curr_server_details

    return out, return_code


@app.route('/info', methods=['GET'])        # Call this to pull info about this module
def info():
    known_servers = []
    for i in range(len(web_servers)):
        server = {
            "id": "server"+str(i+1),
            "url": web_servers[i]
        }
        known_servers.append(server)
    out = {
        "componente": componente,
        "versao": ver,
        "descricao": desc,
        "ponto_de_acesso": acess_point,
        "status": ("down" if is_busy else "up"),
        "identificacao": uid,
        "lider": int(is_leader),
        "eleicao": election,
        "servidores_conhecidos": known_servers
    }

    return json.dumps(out), 200


def thr_func():
    global is_busy
    print("[DEBUG] Thread fired, waiting 10s...")
    time.sleep(10)
    print("[DEBUG] Thread ended")
    is_busy = False


def main():
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 3002)))


if __name__ == "__main__":
    main()
