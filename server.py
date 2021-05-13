import json
from flask import Flask

app = Flask(__name__)


@app.route('/info', methods=['GET'])
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


if __name__ == '__main__':
    print()
    app.run()
