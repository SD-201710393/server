import json
import os
import threading
import time
import requests
from flask import Flask
from flask import request

app = Flask(__name__)
# Server details that get pulled from '/info' in their respective order
componente = "server"
ver = "0.5"
desc = "serve os clientes com servicos variados"
acess_point = "https://sd-rdm.herokuapp.com"
is_busy = False                                # If true, it's busy or 'down'. Otherwise, it's 'up'
uid = 2
is_leader = False
have_competition = True                        # If true, at least one server have an UID higher than this
elect_running = False
cur_election: str
election = "valentao"
urls = ["https://sd-201620236.herokuapp.com", "https://sd-jhsq.herokuapp.com",
        "https://sd-mgs.herokuapp.com", "https://sd-app-server-jesulino.herokuapp.com",
        "https://sd-dmss.herokuapp.com"]

is_shadow = False                           # If true, use the shadow servers for communication
shadow_servers = ["https://sd-rdm-shadow1.herokuapp.com", "https://sd-rdm-shadow2.herokuapp.com"]


@app.route('/eleicao/reset', methods=['GET'])             # Resets the election counter
def reset_elec_count():
    global cur_election
    cur_election = 0
    out = {
        "id": str(cur_election)
    }
    return json.dumps(out), 200


@app.route('/eleicao/coordenador', methods=['POST'])      # Call this to say this server is a leader
def set_coord():
    global is_leader
    global elect_running
    global cur_election
    success = False
    req = request.json
    if len(req) == 2 and req["id_eleicao"] == cur_election:
        is_leader = False
        elect_running = False
        cur_election = ""
        success = True
    else:
        print("[DEBUG] Invalid coordinator request. Either invalid amount of arguments or invalid election!")
    out = {
        "successo": success
    }
    return json.dumps(out), 200

@app.route('/eleicao', methods=['POST'])                 # Call this to start an election
def elected():
    global cur_election
    global elect_running
    try:
        cur_election = request.json["id"]
        if elect_running is False:
            elect_running = True
            run_election()
    except KeyError:
        print("[DEBUG] Key is missing")
    out = {
        "id": cur_election
    }
    return json.dumps(out), 200


@app.route('/eleicao', methods=['GET'])                 # Call this to acknowledge an election is in process
def ack_election():
    out = {
        "tipo_de_eleicao_ativa": election,
        "eleicao_em_andamento": elect_running
    }
    return json.dumps(out), 200


@app.route('/shutdown', methods=['GET'])                # Call this to simulate a server shutdown
def shtdwn():
    pass


@app.route('/recurso', methods=['GET'])                 # Call this to get the resource status
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
    for i in range(len(urls)):
        server = {
            "id": "server"+str(i+1),
            "url": urls[i]
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


def become_coord():
    global is_leader
    global elect_running
    out = {
        "coordenador": uid,
        "id_eleicao": cur_election
    }
    request_post_all("/eleicao/coordenador", out)
    is_leader = True
    elect_running = False


def request_get_all(route, out_json):
    for u in urls:
        threading.Thread(target=(lambda: requests.get(u + route, json=out_json))).start()


def request_post_all(route, out_json):
    for u in urls:
        threading.Thread(target=(lambda: requests.post(u + route, json=out_json))).start()


def run_election():
    global elect_running
    global cur_election
    global have_competition
    have_competition = False
    thr = []
    if election == "valentao":
        for server in urls:
            thr.append(threading.Thread(target=elec_valentao, args=(server, )))
            thr[-1].start()
        for i in range(len(thr)):
            thr[i].join()
        if have_competition is False:   # No one opposed this server, set it as coordinator
            print('[DEBUG] This server is the new coordinator')
            become_coord()
        else:
            print("[DEBUG] This server have competitors")
    elif election == "anel":
        pass
    else:
        print(f"[DEBUG] Unknown election type: '{election}'")


def elec_valentao(target):
    # Fire a GET at target/info and get the response...
    # ... if it's id < your id
    global have_competition
    global cur_election
    try:
        target_info = requests.get(target + "/info").json()
        if target_info["identificacao"] > uid:
            have_competition = True
            requests.post(target + "/eleicao", json={"id": cur_election})
            print(f"[DEBUG] Lose against '{target}'")
        else:
            print(f"[DEBUG] Won against '{target}'")
    except requests.ConnectionError:
        print(f"[DEBUG] Server '{target}' if offline")
    except KeyError:
        print(f"[DEBUG] Couldn't get info on server '{target}'")


def thr_func():
    global is_busy
    print("[DEBUG] Thread fired, waiting 10s...")
    time.sleep(10)
    print("[DEBUG] Thread ended")
    is_busy = False


def main():
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))


if __name__ == "__main__":
    main()
