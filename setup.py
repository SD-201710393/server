from setuptools import setup

setup(
    name="orchestrator_docker",
    version="0.1.0",
    author="Ramon Darwich de Menezes",
    description="Handles and organizes connection between clients and worker servers",
    license="GNU",
    install_requires=["flask", "requests"],
    entry_points={
        "console_scripts": [
            # <Nome do Comando>=<Modulo (Arquivo)>:<Funcao>
            "run_server=server:main"
        ]
    }
)