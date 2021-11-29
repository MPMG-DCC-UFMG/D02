
"""
Verifica se existe alguma licitação cujo valor excede o total arrecadado pelo município
naquele ano.

Entrada:
    df_receita (pandas DataFrame ou Spark table) - tabela contendo os dados da receita
    já agrupados por entidade e ano.
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


    @metric_value(engine=PandasExecutionEngine)
    def _pandas(
        cls,
        execution_engine: "PandasExecutionEngine",
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        df_licitacao, _, _ = execution_engine.get_compute_domain(
            domain_kwargs=metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )

        return df_licitacao
    

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        execution_engine: "SparkDFExecutionEngine",
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        df_licitacao, _, _ = execution_engine.get_compute_domain(
            domain_kwargs=metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )

        return df_licitacao


class ExpectValueLessRevenue(TableExpectation):
    # Setting necessary computation metric dependencies and defining kwargs,
    # as well as assigning kwargs default values
    metric_dependencies = ("table.custom.value_less_revenue",)
    success_keys = ("df_receita", "columns")

    # Default values
    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,
        "df_receita": None,
        "columns": ['id_licitacao', 'nome_entidade', 'ano_exercicio', 'vlr_licitacao']
    }


    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        df_licitacao = metrics["table.custom.value_less_revenue"]

        df_receita = self.get_success_kwargs(configuration).get("df_receita")
        columns = self.get_success_kwargs(configuration).get("columns")

        # Implementação Pandas: não foi possível passar os parâmetros para a métrica "table.custom.value_less_revenue"
        try:
            # Se for um pandas DataFrame
            df_licitacao = pd.DataFrame(df_licitacao)
        
            df_receita = pd.DataFrame(df_receita)
        
            # Selecionando as colunas de interesse e excluindo duplicatas
            df_licitacao = df_licitacao[columns]
            df_licitacao = df_licitacao.drop_duplicates()

            # Se a licitação for do ano corrente, seu valor será atribuído ao ano anterior
            df_licitacao['ano_exercicio_tmp'] = df_licitacao['ano_exercicio']
            df_licitacao.loc[df_licitacao['ano_exercicio'] == date.today().year, 'ano_exercicio'] = date.today().year - 1

            # Inner join de ambas as tabelas
            df = pd.merge(df_licitacao, df_receita, on=['nome_entidade', 'ano_exercicio'], how='inner')

            # Ordenando por município e ano
            df = df.sort_values(by=['nome_entidade', 'ano_exercicio'])

            # Se a receita for 0, é substituída pela do ano anterior
            df["vlr_arrecadado"] = np.where(df["vlr_arrecadado"] != 0, df["vlr_arrecadado"], np.nan)
            df["vlr_arrecadado"].ffill(inplace=True)

            # Entradas da tabela resultante onde o valor da licitação supera a receita total arrecadada
            df = df[df['vlr_licitacao'] > df['vlr_arrecadado']]

            df['ano_exercicio'] = df['ano_exercicio_tmp']
            success = (len(df) == 0)
            return_df = df.to_dict('records')
            unexpected_percent = 100 * (df.shape[0] / df_licitacao.shape[0])

        # Implementação Spark
        except ValueError:

            # [NOTE] Precisei adicionar esses imports para usar a variável spark ao criar o dataframe df_receita
            from pyspark.context import SparkContext
            from pyspark.sql.session import SparkSession
            from pyspark.sql import Row
            sc = SparkContext.getOrCreate()
            spark = SparkSession(sc)

            # Se for um spark DataFrame
            df_licitacao = df_licitacao

            #df_receita = spark.createDataFrame(df_receita)
            df_receita = spark.createDataFrame(Row(**x) for x in df_receita)
        
            # Selecionando as colunas de interesse e excluindo duplicatas
            df_licitacao = df_licitacao.select(*columns)
            df_licitacao = df_licitacao.dropDuplicates()

            # Se a licitação for do ano corrente, seu valor será atribuído ao ano anterior
            df_licitacao = df_licitacao.withColumn("ano_exercicio_tmp", df_licitacao.ano_exercicio)
            df_licitacao = df_licitacao.withColumn("ano_exercicio", F.when(df_licitacao.ano_exercicio == date.today().year, date.today().year - 1).otherwise(df_licitacao.ano_exercicio))

            # Inner join de ambas as tabelas
            df = df_licitacao.join(df_receita, on=["nome_entidade", "ano_exercicio"], how="inner")

            # Ordenando por município e ano
            df = df.orderBy(["nome_entidade", "ano_exercicio"])

            # Se a receita for 0, é substituída pela do ano anterior
            df = df.withColumn("vlr_arrecadado", F.when(df.vlr_arrecadado != 0, df.vlr_arrecadado).otherwise(np.nan))
            #df["vlr_arrecadado"].ffill(inplace=True) # Esta função ainda não está disponível no pyspark

            # Entradas da tabela resultante onde o valor da licitação supera a receita total arrecadada
            df = df.filter(df.vlr_licitacao > df.vlr_arrecadado)
            
            df = df.withColumn("ano_exercicio", df.ano_exercicio)
            success = (df.count() == 0)
            return_df = list(map(lambda row: row.asDict(), df.collect()))
            unexpected_percent = 100 * (df.count() / df_licitacao.count())


        

        return {
            "success": success,
            "result": {
                "dataframe": return_df,
                "unexpected_percent": unexpected_percent
            }
        }


    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]):
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            True if the configuration has been validated successfully. Otherwise, raises an exception
        """

        # Setting up a configuration
        if not super().validate_configuration(configuration):
            return False

        if configuration is None:
            configuration = self.configuration

        # Ensuring basic configuration parameters are properly set
        try:
            assert("df_receita" in configuration.kwargs)
        except AssertionError:
            print("'df_receita' parameter is required for this expectation")
            return False
        
        try:
            assert("columns" in configuration.kwargs)
        except AssertionError:
            print("'columns' parameter is required for this expectation")
            return False

        # Validating that 'df_receita' is of the proper format and type
        if "df_receita" in configuration.kwargs:
            df_receita = configuration.kwargs["df_receita"]
        
        if "columns" in configuration.kwargs:
            columns = configuration.kwargs["columns"]

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

        template_str = """
            Possível erro de digitação. Valores de licitação maiores que a receita do município/estado.
        """

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
            df_receita = result_dict.get("dataframe")
            header_row = ["id_licitacao", "ano_exercicio", "nome_entidade", "vlr_licitacao", "vlr_arrecadado"]

            for row in df_receita:
                # Modificando o formato das colunas de valores
                vlr_licitacao = round(float(row["vlr_licitacao"]),2)
                vlr_arrecadado = round(float(row["vlr_arrecadado"]),2)

                # Adiciona máscara para formato brasileiro de moeda
                row["vlr_licitacao"] = f"R$ {vlr_licitacao:,.2f}".replace(',','v').replace('.',',').replace('v','.')
                row["vlr_arrecadado"] = f"R$ {vlr_arrecadado:,.2f}".replace(',','v').replace('.',',').replace('v','.')

                # Selecionando as colunas restantes
                columns = [row[k] for k in header_row]
                table_rows.append(columns)
        
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
                str(round(result_dict.get("unexpected_percent"), 5)) + "% unexpected"
            )
        else:
            return "--"
