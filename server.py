import json
import os
import sys
import threading
import time
from flask import Flask

app = Flask(__name__)
thr_lock = threading.Lock()


@app.route('/shutdown', methods=['GET'])    # Call this to simulate a server shutdown
def shtdwn():
    pass


@app.route('/recurso', methods=['POST'])    # Call this to simulate handling a resource
def res():
    return_code: int
    thr = threading.Thread(target=thr_func, args=(thr_lock, ))
    available = not thr_lock.locked()
    if available:
        thr_lock.acquire()
        thr.start()
        return_code = 200
    else:
        return_code = 409
    server_res = {"disponivel": available}
    return json.dumps(server_res), return_code


@app.route('/info', methods=['GET'])        # Call this to pull info about this module
def home():
    componente = "server"
    desc = "serve os clientes com servicos variados"
    ver = "0.1"
    out = {
        "componente": componente,
        "descricao": desc,
        "versao": ver
    }
    return json.dumps(out)


def thr_func(lock):
    print("Thread fired, waiting 10s...")
    time.sleep(10)
    print("Thread ended")
    lock.release()


def main():
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 3002)))


if __name__ == "__main__":
    main()
