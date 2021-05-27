FROM python:3.8

COPY . /app
WORKDIR /app
# O -e vai linkar os arquivos para o projeto, rodando sempre a mais nova modificação
RUN pip install -e .
# Comando valido do setup.py
ENTRYPOINT ["run_server"]
CMD ["$PORT"]