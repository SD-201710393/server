import json
import os
import threading
import time
import requests
from operator import itemgetter
from flask import Flask
from flask import request

app = Flask(__name__)
# Server details that get pulled from '/info' in their respective order
componente = "server"
ver = "0.5"
desc = "serve os clientes com servicos variados"
acess_point = "https://sd-rdm.herokuapp.com"
is_busy = False                                # If true, it's busy or 'down'. Otherwise, it's 'up'
uid = 3
is_leader = False
have_competition = True                        # If true, at least one server have an UID higher than this
elect_running = False
started_ring = False                           # If true, this server started a 'ring election'
cur_election = ""
election_type = "valentao"
urls = ["https://sd-201620236.herokuapp.com", "https://sd-jhsq.herokuapp.com",
        "https://sd-mgs.herokuapp.com", "https://sd-app-server-jesulino.herokuapp.com",
        "https://sd-dmss.herokuapp.com"]

is_shadow = False                           # If true, use the shadow servers for communication
shadow_servers = ["https://sd-rdm-shadow1.herokuapp.com", "https://sd-rdm-shadow2.herokuapp.com"]


@app.route('/eleicao/reset', methods=['GET'])             # Resets the election counter
def reset():
    out = {
        "id": str(cur_election)
    }
    requests.post(acess_point + '/eleicao/coordenador', json={"coordenador": uid, "id_eleicao": cur_election})
    return json.dumps(out), 200


@app.route('/eleicao/coordenador', methods=['POST'])      # Call this to say this server is a leader
def coord_decision():
    global is_leader
    success = False
    req = request.json
    return_code: int
    try:
        if len(req) == 2 and req["id_eleicao"] == cur_election:
            is_leader = req["coordenador"] == uid
            print(f"[DEBUG] Election '{cur_election}' ended")
            set_coord()
            success = True
            return_code = 200
            print(f"[DEBUG] Election '{cur_election}' ended")
        else:
            print("[DEBUG] Invalid coordinator request. Either invalid amount of arguments or invalid election!")
            return_code = 400
    except KeyError:
        success = False
        return_code = 400
    out = {
        "successo": success
    }
    return json.dumps(out), return_code


@app.route('/eleicao', methods=['POST'])                 # Call this to start an election
def elected():
    global cur_election
    global elect_running
    global started_ring
    return_code: int
    try:
        cur_election = request.json["id"]
        if cur_election is None:
            print("[DEBUG] Current Election id is NULL!")
            return_code = 400
        elif elect_running is False:
            elect_running = True
            if election_type == "anel" and ("-" not in cur_election):
                started_ring = True
            run_election()
            return_code = 200
        elif election_type == "anel" and ("-" + str(uid) in cur_election) and started_ring is True:
            # Only goes here if it's a ring election, it's ID is present and it started the election...
            # ... then we can finish it!
            ids_str = cur_election.split("-")   # Get all ids
            ids = map(int, ids_str)             # Convert to numbers
            ids.sort()                          # Sort them
            if ids[-1] < uid:                   # Our id is higher, then, set ourselves as the new coordinator
                requests.post(acess_point + '/eleicao/coordenador',
                              json={"coordenador": uid, "id_eleicao": cur_election})
            else:                               # But if not, search the server with this id and set it
                for server in urls:
                    try:
                        new_coord = requests.get(server + "/info").json()
                        if new_coord["identificacao"] == uid:
                            requests.post(new_coord + "/eleicao/coordenador",
                                          json={"coordenador": ids[-1], "id_eleicao": cur_election})
                        elif new_coord["status"] == "down":
                            print(f"[DEBUG] New Coordinator '{server}' became down")
                    except requests.ConnectionError:
                        print(f"[DEBUG] New Coordinator '{server}' became offline")
                    except KeyError:
                        print(f"[DEBUG] Couldn't get info on New Coordinator '{server}'")
            return_code = 200
        else:
            print(f"[DEBUG] Election '{cur_election}' is still running")
            return_code = 409   # Return 'conflict' if there is an election running
    except KeyError:
        print("[DEBUG] Key is missing")
        return_code = 400
    out = {
        "id": cur_election
    }
    return json.dumps(out), return_code


@app.route('/eleicao', methods=['GET'])                 # Call this to acknowledge an election is in process
def ack_election():
    out = {
        "tipo_de_eleicao_ativa": election_type,
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
    # global componente
    # global ver
    # global desc
    # global acess_point
    global is_busy
    global uid
    global is_leader
    global election_type
    # 400 - Bad Request (Data was invalid)
    # 409 - Conflict
    # 200 - OK
    req = request.json
    return_code = 200   # If something goes wrong, this will be updated to either 400 or 409
    out = {
        "atencao": None,
        "info_atual": None
    }
    try:
        if req["status"] == "down":
            is_busy = True
        elif req["status"] == "up":
            is_busy = False
        else:
            return_code = 400
    except KeyError:  # If we reached here, something was wrong on the dictionary
        pass
    try:
        uid = req["identificacao"]
    except KeyError:
        pass
    try:
        if req["lider"] == 1:
            is_leader = True
        elif req["lider"] == 0:
            is_leader = False
        else:
            return_code = 400
    except KeyError:
        pass
    try:
        if req["eleicao"] == "valentao":
            election_type = "valentao"
        elif req["eleicao"] == "anel":
            election_type = "anel"
        else:
            return_code = 400
    except KeyError:
        pass

    if return_code == 400:
        out["atencao"] = "Uma ou mais chaves invalidas"
        print("[DEBUG] One or more keys missing / invalid")

    curr_server_details = {
        "componente": componente,
        "versao": ver,
        "descricao": desc,
        "ponto_de_acesso": acess_point,
        "status": ("down" if is_busy else "up"),
        "identificacao": uid,
        "lider": int(is_leader),
        "eleicao": election_type
    }
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
        "eleicao": election_type,
        "servidores_conhecidos": known_servers
    }
    return json.dumps(out), 200


def set_coord():
    global elect_running
    global started_ring
    global cur_election
    if is_leader is True:
        out = {
            "coordenador": uid,
            "id_eleicao": cur_election
        }
        request_post_all("/eleicao/coordenador", out)
    cur_election = ""
    elect_running = False
    started_ring = False


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
    if election_type == "valentao":
        for server in urls:
            thr.append(threading.Thread(target=elec_valentao, args=(server, )))
            thr[-1].start()
        for i in range(len(thr)):
            thr[i].join()
        if have_competition is False:   # No one opposed this server, set it as coordinator
            print('[DEBUG] This server is the new coordinator')
            requests.post(acess_point + '/eleicao/coordenador', json={"coordenador": uid, "id_eleicao": cur_election})
        else:
            print("[DEBUG] This server have competitors")
    elif election_type == "anel":
        id_list = []
        for i in range(len(urls)):
            id_list.append((urls[i], -1))
            thr.append(threading.Thread(target=elec_anel, args=(urls[i], id_list, i)))
            thr[-1].start()
        for i in range(len(thr)):
            thr[i].join()
        id_list.sort(key=itemgetter(1))     # Sort using the [1] element of the tuple
        for server_id in id_list:
            if server_id[1] > uid:
                requests.post(server_id[0] + "/eleicao", json={"id": cur_election + '-' + str(uid)})
                return
        # If we reached here, none servers have an ID higher than this, then, send to the lowest...
        # ... but check first if all of them failed...
        valid_servers = 0
        for server_id in id_list:
            if server_id[1] > -1:
                valid_servers += 1
        if valid_servers == 0:      # ... since all of them failed, set ourselves as the new coordinator
            requests.post(acess_point + '/eleicao/coordenador', json={"coordenador": uid, "id_eleicao": cur_election})
        else:                       # ... otherwise, send a request to the lowest ID available
            requests.post(id_list[0][0] + "/eleicao", json={"id": cur_election + '-' + str(uid)})
    else:
        print(f"[DEBUG] Unknown election type: '{election_type}'")


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
            print(f"[DEBUG] Lost against '{target}'")
        elif target_info["status"] == "down":
            print(f"[DEBUG] Server '{target}' is down")
        elif target_info["eleicao"] == "anel":
            print(f"[DEBUG] Server '{target}' is using a different election type")
        else:
            print(f"[DEBUG] Won against '{target}'")
    except requests.ConnectionError:
        print(f"[DEBUG] Server '{target}' if offline")
    except KeyError:
        print(f"[DEBUG] Couldn't get info on server '{target}'")
    except TypeError:
        print(f"[DEBUG] Server '{target}' sent data in an invalid format")


def elec_anel(target, id_list, i):
    try:
        target_info = requests.get(target + "/info").json()
        if target_info["status"] == "down":
            print(f"[DEBUG] Server '{target}' is down")
        elif target_info["eleicao"] == "valentao":
            print(f"[DEBUG] Server '{target}' is using a different election type")
        else:
            id_list[i] = (target, target_info["identificacao"])
            return
    except requests.ConnectionError:
        print(f"[DEBUG] Server '{target}' if offline")
    except KeyError:
        print(f"[DEBUG] Couldn't get info on server '{target}'")
    except TypeError:
        print(f"[DEBUG] Server '{target}' sent data in an invalid format")
    id_list[i] = (target, -1)


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
