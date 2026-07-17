"""
Contrato de codigo de saida entre os jobs e o orquestrador.

POR QUE ISSO EXISTE
-------------------
Retry so faz sentido pra falha TRANSIENTE: rede caiu, servidor do governo
devolveu 502, MinIO ainda estava subindo. Tenta de novo em 10 minutos e
provavelmente passa.

Falha de QUALIDADE e o oposto: e deterministica. Se a silver tem duplicata
na chave, tentar de novo daqui 10 minutos vai encontrar exatamente a mesma
duplicata. O retry so queima 20 minutos, polui o log e falha igual - com a
agravante de esconder o sinal real no meio de 3 tracebacks iguais.

Sem distinguir os dois casos, o orquestrador trata tudo como transiente.
Este modulo e o vocabulario minimo pra ele saber a diferenca:

    exit 0  -> deu certo
    exit 3  -> dado ruim. NAO adianta tentar de novo, precisa de gente.
    exit !=0-> qualquer outra coisa (rede, OOM, bug). Retry faz sentido.

O DAG le esse codigo e decide entre AirflowFailException (falha seca) e
AirflowException (falha com retry). Ver dags/lakehouse_pipeline.py.

Por que exit code e nao parsear a mensagem de erro do stderr: contrato
explicito e estavel. Grep em traceback quebra no dia que alguem reescreve
a mensagem de excecao.
"""

# Sucesso.
EXIT_OK = 0

# Falha generica: rede, permissao, OOM, bug. Retry pode resolver.
EXIT_ERRO = 1

# Falha de qualidade de dados (DataQualityError). Deterministica:
# retry NAO resolve, precisa de intervencao humana.
# 3 e arbitrario, mas fora da faixa que o Python usa sozinho (1 = excecao,
# 2 = erro de argparse) e longe de 99, que o Airflow reserva pra "skip".
EXIT_QUALIDADE = 3
