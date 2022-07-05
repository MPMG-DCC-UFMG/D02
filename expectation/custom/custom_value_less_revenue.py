"""
Verifica se existe alguma licitação cujo valor excede o total arrecadado pelo município
naquele ano.

Args:
    table_id (string): Chave primária da tabela de origem para identificar as instâncias
        no data_docs.
    df_licitacao (list): tabela contendo valores de licitações referenciadas na tabela 
        de origem.
    df_receita (list): tabela contendo os dados da receita já agrupados por entidade e ano.
        Necessária para obter informações de receita do município.
    df_tempo (list): tabela contendo as informações de data para join com a tabela em 
        questão usando a chave sk_tempo.
    value_columns (list): lista com as colunas de valores que serão verificadas
"""

import pandas as pd
import numpy as np
from datetime import date

from great_expectations.expectations.expectation import TableExpectation
from great_expectations.expectations.metrics import metric_value
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.expectations.metrics.table_metric_provider import TableMetricProvider
from great_expectations.expectations.util import render_evaluation_parameter_string

from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine, SparkDFExecutionEngine
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.core import ExpectationConfiguration

from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent, RenderedTableContent
from great_expectations.render.util import substitute_none_for_missing 

from great_expectations.expectations.metrics.import_manager import F

from typing import Any, Dict, Optional, Tuple


class ValueLessRevenue(TableMetricProvider):
    metric_name = "table.custom.value_less_revenue"
    value_keys = ("table_id", "value_columns", "df_licitacao", "df_receita", "df_tempo")


    @metric_value(engine=PandasExecutionEngine)
    def _pandas(
        cls,
        execution_engine: "PandasExecutionEngine",
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        df, _, _ = execution_engine.get_compute_domain(
            domain_kwargs=metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )

        table_id = metric_value_kwargs.get("table_id")

        df_receita = metric_value_kwargs.get("df_receita")
        df_receita = pd.DataFrame(df_receita)

        df_tempo = metric_value_kwargs.get("df_tempo")
        df_tempo = pd.DataFrame(df_tempo)

        table_size = df.shape[0]

        df_licitacao = metric_value_kwargs.get("df_licitacao")
        # Se não for a fato_licitacao, fazemos um join
        if df_licitacao is not None:
            df_licitacao = pd.DataFrame(df_licitacao)

            # Join com a fato_licitacao para obter informações de valores
            df = pd.merge(df, df_licitacao, on='sk_licitacao', how='inner')

        # Join com a dim_tempo para obter o ano da licitação
        df = pd.merge(df, df_tempo, on='sk_tempo', how='inner')

        # Se a licitação for do ano corrente, seu valor será atribuído ao ano anterior
        df['ano_tmp'] = df['ano']
        df.loc[df['ano'] == date.today().year, 'ano'] = date.today().year - 1

        # Join com a fato_receita para obter informações do município
        df = pd.merge(df, df_receita, on=['sk_ibge', 'ano'], how='inner')

        # Ordenando por município e ano
        df = df.sort_values(by=['nome_entidade', 'ano'])

        # Se a receita for 0, é substituída pela do ano anterior
        df['vlr_arrecadado'] = np.where(df['vlr_arrecadado'] != 0, df['vlr_arrecadado'], np.nan)
        df['vlr_arrecadado'].ffill(inplace=True)
        
        # Colunas de valores a serem comparados com a receita
        value_columns = metric_value_kwargs.get("value_columns")

        # Entradas da tabela resultante onde algum valor supera a receita total arrecadada
        df_iter = None
        df_previous = df.copy()
        for column in value_columns:
            
            df_current = df_previous.copy()
            df_current['vlr_comparado'] = df_current[column]
            df_current['coluna_comparada'] = column

            # Transforma valores nulos em 0
            df_current[column].fillna(0, inplace=True)

            # Filtra registros com valores maiores que o valor arrecadado
            df_current = df_current[df_current[column] > df_current['vlr_arrecadado']]

            if df_iter is None:
                df_iter = df_current.copy()
            else:
                df_iter = pd.concat([df_iter, df_current])

            df = df_iter
        
        # Resgatando os valores originais do ano corrente
        df['ano'] = df['ano_tmp']
        df.drop('ano_tmp', axis=1, inplace=True)

        df['table_id'] = df[table_id]
        
        unexpected_records = len(df[table_id].unique())
        unexpected_percent = 100 * (unexpected_records / table_size)

        # Convertendo o DataFrame para o formato necessário
        df = df.head(100)
        # Selecionando as colunas de interesse
        arr_fields = []
        arr_fields.append("table_id")
        arr_fields.append("sk_ibge")
        arr_fields.append("nome_entidade")
        arr_fields.append("ano")
        arr_fields.append("coluna_comparada")
        arr_fields.append("vlr_comparado")
        arr_fields.append("vlr_arrecadado")

        df = df[arr_fields]
        return_df = df.to_dict('records')

        return return_df, unexpected_percent
    

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        execution_engine: "SparkDFExecutionEngine",
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        df, _, _ = execution_engine.get_compute_domain(
            domain_kwargs=metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )

        # Imports necessários para o pyspark
        from pyspark.context import SparkContext
        from pyspark.sql.session import SparkSession
        from pyspark.sql import Row
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        from pyspark.sql.functions import lit
        from pyspark.sql.functions import col
        
        sc = SparkContext.getOrCreate()
        spark = SparkSession(sc)

        # Resgatando os parâmetros da entrada
        table_id = metric_value_kwargs.get("table_id")

        df_receita = metric_value_kwargs.get("df_receita")
        df_receita = spark.createDataFrame(Row(**x) for x in df_receita)

        df_tempo = metric_value_kwargs.get("df_tempo")
        df_tempo = spark.createDataFrame(Row(**x) for x in df_tempo)

        table_size = df.count()

        # Flag para indicar se a tabela é a própria fato_licitacao
        df_licitacao = metric_value_kwargs.get("df_licitacao")
        flag = df_licitacao is None
        # Se não for a fato_licitacao, fazemos um join
        if flag == False:
            df_licitacao = spark.createDataFrame(Row(**x) for x in df_licitacao)

            # Join com a fato_licitacao para obter informações de valores
            df = df.join(df_licitacao, on="sk_licitacao", how="inner")

        # Join com a dim_tempo para obter o ano da licitação
        df = df.join(df_tempo, on="sk_tempo", how="inner")

        # Se a licitação for do ano corrente, seu valor será atribuído ao ano anterior
        df = df.withColumn("ano_tmp", df.ano)
        df = df.withColumn(
            "ano", F.when(df.ano == date.today().year, date.today().year - 1)
            .otherwise(df.ano)
        )

        # Join com a fato_receita para obter informações do município
        df = df.join(df_receita, on=["sk_ibge", "ano"], how="inner")

        # Se o valor arrecadado for 0, seu valor é substituída pelo do ano anterior
        df = df.withColumn(
            "vlr_arrecadado", F.when(df.vlr_arrecadado == 0, None)
            .otherwise(df.vlr_arrecadado)
        )
        
        window = (
            Window
            .partitionBy("vlr_arrecadado")
            .orderBy(["cod_entidade", "ano"])
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        df = (
            df
            .withColumn("vlr_arrecadado", F.last("vlr_arrecadado", ignorenulls=True).over(window))
        )

        # Colunas de valores a serem comparados com a receita
        value_columns = metric_value_kwargs.get("value_columns")

        # Entradas da tabela resultante onde algum valor supera a receita total arrecadada
        df_iter = None
        df_previous = df
        for column in value_columns:
            
            df_current = df_previous
            df_current = df_current.withColumn("vlr_comparado", col(column))
            df_current = df_current.withColumn("coluna_comparada", lit(column))
            
            # Transforma valores nulos em 0
            df_current = df_current.fillna(0, subset=[column])

            # Filtra registros com valores maiores que o valor arrecadado
            df_current = df.filter(col(column) > df.vlr_arrecadado).alias('df_current')
            
            if df_iter is None:
                df_iter = df_current.alias('df_iter')
            else:
                df_iter = df_iter.union(df_current)
        
            df = df_iter

        # Resgatando os valores originais do ano corrente
        df = df.withColumn("ano", df.ano_tmp)
        df = df.drop("ano_tmp")

        df = df.withColumn("table_id", col(table_id))

        unexpected_records = df[table_id].countDistinct()
        unexpected_percent = 100 * (unexpected_records / table_size)

        # Convertendo o DataFrame para o formato necessário
        df = df.limit(100)
        # Selecionando as colunas de interesse
        arr_fields = []
        arr_fields.append("table_id")
        arr_fields.append("sk_ibge")
        arr_fields.append("nome_entidade")
        arr_fields.append("ano")
        arr_fields.append("coluna_comparada")
        arr_fields.append("vlr_comparado")
        arr_fields.append("vlr_arrecadado")
        df = df.select(*arr_fields)

        return_df = list(map(lambda row: row.asDict(), df.collect()))

        return return_df, unexpected_percent


class ExpectValueLessRevenue(TableExpectation):
    # Setting necessary computation metric dependencies and defining kwargs,
    # as well as assigning kwargs default values
    metric_dependencies = ("table.custom.value_less_revenue",)
    success_keys = (
        "table_id",
        "value_columns",
        "df_licitacao",
        "df_receita",
        "df_tempo"
    )

    # Default values
    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,
        "table_id": None,
        "value_columns": None,
        "df_licitacao": None,
        "df_receita": None,
        "df_tempo": None
    }


    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        return_df, unexpected_percent = metrics["table.custom.value_less_revenue"]

        success = (len(return_df) == 0)
        table_id = self.default_kwarg_values.get("table_id")
        return {
            "success": success,
            "result": {
                "dataframe": return_df,
                "unexpected_percent": unexpected_percent,
                "table_id":table_id
            }
        }


    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        """
        Valida se uma configuração foi definida e se os parâmetros foram fornecidos
        corretamente.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]):
                Parâmetro opcional de configuração.
        Retorna:
            Verdadeiro se tudo foi configurado corretamente e falso caso contrário.
        """

        # Setting up a configuration
        if not super().validate_configuration(configuration):
            return False

        if configuration is None:
            configuration = self.configuration
        
        parameters = {
            "table_id": "str",
            "value_columns": "list",
            "df_licitacao": "list",
            "df_receita": "list",
            "df_tempo": "list"
        }

        for p in parameters.keys():
            # Conferindo se os argumentos foram fornecidos
            try:
                assert(p in configuration.kwargs)
            except AssertionError:
                print("{} parameter is required for this expectation".format(p))
                return False
        
            arg = configuration.kwargs[p]
            
            # Validando se o parâmetro não é nulo
            try:
                assert(arg is not None)
            except AssertionError:
                print("{} parameter is None".format(p))
                return False

            # Validando se o parâmetro é do tipo correto
            try:
                assert(isinstance(arg, eval(parameters[p])))
            except AssertionError:
                print("{} parameter is not an instance of {}".format(p, parameters[p]))
                return False

        return True


    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs
    ):
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name", True)
        include_column_name = (
            include_column_name if include_column_name is not None else True
        )
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(
            configuration.kwargs,
            [],
        )

        template_str = "O valor comparado não deve ser maior do que a receita do município/estado."

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]


    @classmethod
    @renderer(renderer_type="renderer.diagnostic.unexpected_table")
    def _diagnostic_unexpected_table_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        try:
            result_dict = result.result
        except KeyError:
            return None

        if result_dict is None:
            return None
        
        table_rows = []

        if not result_dict.get("dataframe"):
            return None
        else:
            df = result_dict.get("dataframe")
            for row in df:

                for col_name in df[0].keys():
                    if "vlr" in col_name:
                        # Modificando o formato das colunas de valores
                        if row[col_name] is not None:
                            value = round(float(row[col_name]), 2)
                            row[col_name] = f"R$ {value:,.2f}".replace(',','v').replace('.',',').replace('v','.')
                table_rows.append([
                    row["ano"],
                    row["nome_entidade"],
                    row["table_id"],
                    row["coluna_comparada"],
                    row["vlr_comparado"],
                    row["vlr_arrecadado"]
                ])

        header_row = [
            "Ano exercicio",
            "Nome entidade",
            "Id tabela",
            "Coluna comparada",
            "Valor comparado",
            "Valor arrecadado"
        ]
        unexpected_table_content_block = RenderedTableContent(
            **{
                "content_block_type": "table",
                "table": table_rows,
                "header_row": header_row,
                "styling": {
                    "body": {"classes": ["table-bordered", "table-sm", "mt-3"]}
                },
            }
        )

        return unexpected_table_content_block


    @classmethod
    @renderer(renderer_type="renderer.diagnostic.observed_value")
    def _diagnostic_observed_value_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        result_dict = result.result
        if result_dict is None:
            return "--"

        if result_dict.get("unexpected_percent") is not None:
            return (
                str(round(result_dict.get("unexpected_percent"), 3)) + "% inesperado"
            )
        else:
            return "--"