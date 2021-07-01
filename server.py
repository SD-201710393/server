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
componente = "Server"
ver = "0.8"
desc = "Serve os clientes com servicos variados"
access_point = "https://sd-rdm.herokuapp.com"
is_busy = False                                # If true, it's busy or 'down'. Otherwise, it's 'up'
uid = 3
is_leader = True
have_competition = True                        # If true, at least one server have an UID higher than this
elect_running = False
started_ring = False                           # If true, this server started a 'ring election'
election_timeout = 10                          # Timeout, in seconds, for an election to be canceled
cur_election = ""
election_type = "valentao"
urls = ["https://sd-201620236.herokuapp.com", "https://sd-jhsq.herokuapp.com",
        "https://sd-mgs.herokuapp.com", "https://sd-app-server-jesulino.herokuapp.com",
        "https://sd-dmss.herokuapp.com"]
log_url = "https://sd-log-server.herokuapp.com/log"

is_shadow = False                           # If true, use the shadow servers for communication
shadow_servers = ["https://sd-rdm-shadow1.herokuapp.com", "https://sd-rdm-shadow2.herokuapp.com"]


@app.route('/eleicao/reset', methods=['GET'])             # Resets the election counter
def reset():
    out = {
        "id": str(cur_election)
    }
    requests.post(access_point + '/eleicao/coordenador', json={"coordenador": uid, "id_eleicao": "reset"})
    return json.dumps(out), 200


@app.route('/eleicao/coordenador', methods=['POST'])      # Call this to say this server is a leader
def coord_decision():
    global is_leader
    global cur_election
    success = False
    req = request.json
    return_code: int
    try:
        if len(req) == 2:
            if req["id_eleicao"] != "canceled":
                is_leader = req["coordenador"] == uid
                if cur_election == "":
                    cur_election = req["id_eleicao"]
                if is_leader:
                    log_success(comment=f"This server is the new coordinator. Election '{cur_election}' ended",
                                body=req)
                else:
                    log_success(comment=f"Election '{cur_election}' ended", body=req)
                print(f"[DEBUG] Election '{cur_election}' ended")
                set_coord()
                success = True
                return_code = 200
            else:
                log_warning(comment="Election timed out and was canceled", body=req)
                print(f"[DEBUG] Election was canceled!")
                set_coord()
                success = True
                return_code = 200
        else:
            log_attention(comment="Invalid Coordinator Request (Amount of Arguments)", body=req)
            print("[DEBUG] Invalid coordinator request. Either invalid amount of arguments!")
            return_code = 400
    except KeyError:
        log_error(comment="Invalid coordinator Request (Missing Key)", body=req)
        print("[DEBUG] Invalid coordinator request. A required key is missing")
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
            log_error(comment="Current Election id is NULL!", body=cur_election)
            print("[DEBUG] Current Election id is NULL!")
            return_code = 400
        elif elect_running is False:
            elect_running = True
            if election_type == "anel":
                if "-" not in cur_election or ('participantes' in request.json and len(request.json['participantes']) == 0):
                    started_ring = True
            log(comment=f"Election started with id [{uid}] and mode '{election_type}'"
                        f"{(' and it started the ring' if started_ring and election_type == 'anel' else '')}",
                body=cur_election)
            run_election(request.json)
            return_code = 200
        elif election_type == "anel" and started_ring is True:
            # Only goes here if it's a ring election, it's ID is present and it started the election...
            if 'participantes' in request.json and uid in request.json['participantes']:
                # Starter is using the new method
                ids = request.json['participantes']
            elif "-" + str(uid) in cur_election:
                # Starter is using the old method
                log_attention(comment="Election starter doesn't have field 'participantes'. Using old method",
                              body=request.json)
                ids_str = cur_election.split("-")   # Get all ids
                ids = []
                for num in ids_str[1:]:
                    ids.append(int(num))
            else:
                print(f"[DEBUG] Election '{cur_election}' is still running")
                log_attention(comment=f"Election '{cur_election}' is still running")
                return json.dumps({"id": cur_election}), 409

            ids.sort()                          # Sort them
            if ids[-1] <= uid:                  # Our id is higher, then, set ourselves as the new coordinator
                requests.post(access_point + '/eleicao/coordenador',
                              json={"coordenador": uid, "id_eleicao": cur_election})
                log(comment="Won ring election", body={"coordenador": uid, "id_eleicao": cur_election})
            else:                               # But if not, search the server with this id and set it
                new_coord = {}
                for server in urls:
                    try:
                        new_coord = requests.get(server + "/info").json()
                        if new_coord["identificacao"] == ids[-1]:
                            request_post_all("/eleicao/coordenador",
                                             out_json={"coordenador": ids[-1], "id_eleicao": cur_election})
                            requests.post(access_point + '/eleicao/coordenador',
                                          json={"coordenador": ids[-1], "id_eleicao": cur_election})
                            log_success(comment=f"'{new_coord['ponto_de_acesso'] }' Won ring election",
                                        body={"coordenador": ids[-1], "id_eleicao": cur_election})
                            break
                        elif new_coord["status"] == "down":
                            print(f"[DEBUG] New Coordinator '{server}' became down")
                            log_warning(comment=f"New Coordinator '{server}' became down")
                    except requests.ConnectionError:
                        print(f"[DEBUG] New Coordinator '{server}' became offline")
                        log_warning(comment=f"New Coordinator '{server}' became offline")
                    except KeyError:
                        print(f"[DEBUG] Couldn't get info on New Coordinator '{server}'")
                        log_warning(comment=f"Couldn't get info on New Coordinator '{server}'")
                    except TypeError:
                        print("[DEBUG] Data type mismatch!")
                        log_error(comment="Data type mismatch", body=new_coord)
            return_code = 200
        else:
            print(f"[DEBUG] Election '{cur_election}' is still running")
            log_attention(comment=f"Election '{cur_election}' is still running")
            return_code = 409   # Return 'conflict' if there is an election running
    except KeyError:
        print("[DEBUG] Key is missing")
        log_error(comment=f"Election '{cur_election}' is still running")
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


@app.route('/recurso', methods=['GET'])                 # Call this to get the resource status
def res_get():
    server_res = {"ocupado": is_busy}
    return json.dumps(server_res), 200


@app.route('/recurso', methods=['POST'])    # Call this to simulate handling a resource
def res():
    global is_busy
    return_code: int
    thr = threading.Thread(target=make_busy, args=())
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
    global is_busy
    global uid
    global is_leader
    global election_type
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
        "ponto_de_acesso": access_point,
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
        "ponto_de_acesso": access_point,
        "status": ("down" if is_busy else "up"),
        "identificacao": uid,
        "lider": int(is_leader),
        "eleicao": election_type,
        "servidores_conhecidos": known_servers
    }
    return json.dumps(out), 200


@app.route('/shadow', methods=['POST'])
def enable_shadow():
    global is_shadow
    global urls
    global access_point
    global uid
    is_shadow = True
    req = request.json
    try:
        uid = req["id"]
        access_point = req["access"]
        urls = req["urls"]
    except KeyError:    # If here, the caller is the main server
        urls = shadow_servers
    url_list = {
        "urls": urls
    }
    log(s_from=access_point, comment=f"Shadow mode is ENABLED. URL list changed. UID: {uid}", body=url_list)
    return "Shadow was enabled", 200


def log_success(comment="Not Specified", body=None):
    log(severity="Success", comment=comment, body=body)


def log_warning(comment="Not Specified", body=None):
    log(severity="Warning", comment=comment, body=body)


def log_attention(comment="Not Specified", body=None):
    log(severity="Attention", comment=comment, body=body)


def log_error(comment="Not Specified", body=None):
    log(severity="Error", comment=comment, body=body)


def log_critical(comment="Not Specified", body=None):
    log(severity="Critical", comment=comment, body=body)


def log(s_from=None, severity="Information", comment="Not Specified", body=None):
    log_data = {
        "from": (access_point if s_from is None else s_from),
        "severity": severity,
        "comment": comment,
        "body": body
    }
    threading.Thread(target=(lambda: requests.post(log_url, json=log_data))).start()


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
    print(f"[DEBUG] Firing on all servers' [GET]  '{route}'")
    log(comment=f"Firing @ [GET] '{route}' of all servers", body=out_json)
    for u in urls:
        threading.Thread(target=(lambda: requests.get(u + route, json=out_json))).start()


def request_post_all(route, out_json):
    print(f"[DEBUG] Firing on all servers' [POST] '{route}'")
    log(comment=f"Firing @ [POST] '{route}' of all servers", body=out_json)
    for u in urls:
        threading.Thread(target=(lambda: requests.post(u + route, json=out_json))).start()


def run_election(req_json):
    global elect_running
    global cur_election
    global have_competition
    have_competition = False
    thr = []
    threading.Thread(target=elec_timeout).start()   # If an election takes too long, cancel it
    if election_type == "valentao":
        for server in urls:
            thr.append(threading.Thread(target=elec_valentao, args=(server, )))
            thr[-1].start()
        for i in range(len(thr)):
            thr[i].join()
        if have_competition is False:   # No one opposed this server, set it as coordinator
            requests.post(access_point + '/eleicao/coordenador', json={"coordenador": uid, "id_eleicao": cur_election})
        else:
            print("[DEBUG] This server have competitors")
            log(comment=f"This server [{uid}] have competitors")
    elif election_type == "anel":
        id_list = []
        for i in range(len(urls)):
            id_list.append((urls[i], -1))
            thr.append(threading.Thread(target=elec_anel, args=(urls[i], id_list, i)))
            thr[-1].start()
        for i in range(len(thr)):
            thr[i].join()
        id_list.sort(key=itemgetter(1))   # Sort using the [1] element of the tuple
        for server_id in id_list:
            if server_id[1] > uid:        # Send a request to the first server that have an ID higher than this
                print(f"[DEBUG] Sending -{uid} to '{server_id[0]}'")
                if 'participantes' in req_json:
                    part = req_json['participantes']
                    part.append(int(uid))
                    out = {
                        "id": cur_election,
                        "participantes": part
                    }
                else:
                    out = {
                        "id": cur_election + '-' + str(uid)
                    }
                log(comment=f"Sending -{uid} to '{server_id[0]}'", body=out)
                requests.post(server_id[0] + "/eleicao", json=out)
                return

        # If we reached here, none servers have an ID higher than this, then, send to the lowest...
        # ... but check first if all of them failed...
        valid_servers = []
        for server_id in id_list:
            if server_id[1] > -1:
                valid_servers.append(server_id)
        if len(valid_servers) == 0:      # ... since all of them failed, set ourselves as the new coordinator
            print("[DEBUG] There was no valid response from any server")
            log_warning(comment="There was no valid response from any server")
            requests.post(access_point + '/eleicao/coordenador', json={"coordenador": uid, "id_eleicao": cur_election})
        else:                            # ... otherwise, send a request to the lowest, valid ID available
            print(f"[DEBUG] Sending -{uid} to '{valid_servers[0][0]}'")
            log(comment=f"Sending -{uid} to '{valid_servers[0][0]}'", body={"id": cur_election + '-' + str(uid)})
            requests.post(valid_servers[0][0] + "/eleicao", json={"id": cur_election + '-' + str(uid)})
    else:
        print(f"[DEBUG] Unknown election type: '{election_type}'")
        log_attention(comment=f"Request an unknown election type: '{election_type}'")


def elec_valentao(target):
    # Fire a GET at target/info and get the response...
    # ... if it's id < your id
    global have_competition
    global cur_election
    target_info = {}
    try:
        target_info = requests.get(target + "/info").json()
        if target_info["identificacao"] > uid:
            have_competition = True
            requests.post(target + "/eleicao", json={"id": cur_election})
            print("[DEBUG] Lost against '%s' [%d]" % (target, target_info["identificacao"]))
            log(comment=f"Lost against '{target} [{target_info['identificacao']}]")
        elif target_info["status"] == "down":
            print(f"[DEBUG] Server '{target}' is down")
            log_warning(comment=f"Server '{target}' is down")
        elif target_info["eleicao"] == "anel":
            print(f"[DEBUG] Server '{target}' is using a different election type")
            log_warning(comment=f"Server '{target}' is using a different election type")
        else:
            print(f"[DEBUG] Won against '{target}'")
            log(comment=f"Won against '{target}'")
    except requests.ConnectionError:
        print(f"[DEBUG] Server '{target}' if offline")
        log_warning(comment=f"Server '{target}' is offline")
    except KeyError:
        print(f"[DEBUG] Couldn't get info on server '{target}'")
        log_attention(comment=f"Couldn't get info on server '{target}'")
    except TypeError:
        print(f"[DEBUG] Server '{target}' sent data in an invalid format")
        log_error(comment=f"Server '{target}' sent data in an invalid format", body=target_info)


def elec_anel(target, id_list, i):
    target_info = {}
    try:
        target_info = requests.get(target + "/info").json()
        if target_info["status"] == "down":
            print(f"[DEBUG] Server '{target}' is down")
            log_warning(comment=f"Server '{target}' is down")
        elif target_info["eleicao"] == "valentao":
            print(f"[DEBUG] Server '{target}' is using a different election type")
            log_warning(comment=f"Server '{target}' is using a different election type")
        else:
            id_list[i] = (target, target_info["identificacao"])
            print(f"[DEBUG] Server '{target}' is valid")
            return
    except requests.ConnectionError:
        print(f"[DEBUG] Server '{target}' if offline")
        log_warning(comment=f"Server '{target}' is offline")
    except KeyError:
        print(f"[DEBUG] Couldn't get info on server '{target}'")
        log_attention(comment=f"Couldn't get info on server '{target}'")
    except TypeError:
        print(f"[DEBUG] Server '{target}' sent data in an invalid format")
        log_error(comment=f"Server '{target}' sent data in an invalid format", body=target_info)
    id_list[i] = (target, -1)


def elec_timeout():
    time.sleep(election_timeout)
    if elect_running is True:
        print("[DEBUG] Election timed out. Canceling...")
        cancel_election()


def cancel_election():
    out = {
        "id": str(cur_election)
    }
    requests.post(access_point + '/eleicao/coordenador', json={"coordenador": -1, "id_eleicao": "canceled"})
    return json.dumps(out), 200


def make_busy():
    global is_busy
    print("[DEBUG] Thread fired, waiting 10s...")
    time.sleep(10)
    print("[DEBUG] Thread ended")
    is_busy = False


def main():
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))


if __name__ == "__main__":
    main()
