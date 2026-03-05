import polars as pl
from datetime import datetime

ENDERECO_DADOS = './../DADOS/PARQUET/NovoBolsaFamilia/'

try:
    print('Iniciando o processamento Lazy()')
    inicio = datetime.now()

    # Método Lazy "scan_parquet" cria um plano de execução, não carregando os TODOS os dados diretamente
    # diretamente na memória, porém o plano é implementado
    lazy_plan = (
        pl.scan_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')
        .select(['NOME MUNICÍPIO', 'VALOR PARCELA'])
        .group_by('NOME MUNICÍPIO')             # agrupa pelo município
        .agg(pl.col('VALOR PARCELA').sum())     # soma o valor da parcela
        .sort('VALOR PARCELA', descending=True)
    )

    # print(lazy_plan)
    # collect() executa o plano de execução, ele traz de fato os dados que estão estabelecidos no plano
    df_bolsa_familia = lazy_plan.collect()
    print(df_bolsa_familia.head(10))

    fim = datetime.now()
    print(f'Tempo de execução: {fim - inicio}')
    print('Leitura do Arquivo parquet realizada com sucesso')

except Exception as e:
    print(f'Erro ao obter dados {e}')