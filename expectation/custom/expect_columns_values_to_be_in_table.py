"""
Verifica se um conjunto de valores de uma ou mais colunas se encontra em outra tabela.

Args:
    table_id (string): Chave primária da tabela de origem para identificar as instâncias
        no data_docs.
    columns (list): Lista de colunas da tabela de origem cujos valores devem
        existir em outra tabela.
    target_table (list): Tabela que deve conter os valores. Por limitações de memória,
        deve ser fornecida no formato de lista de dicionários.
"""

import pandas as pd

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

from typing import Any, Dict, Optional, Tuple


class ColumnsValuesToBeInTable(TableMetricProvider):
    metric_name = "table.custom.columns_values_to_be_in_table"
    value_keys = ("table_id", "columns", "target_table")


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

        table_size = df.shape[0]

        table_id = metric_value_kwargs.get("table_id")
        columns = metric_value_kwargs.get("columns")
        df_target = metric_value_kwargs.get("target_table")

        # Selecionando as colunas de interesse e excluindo duplicatas
        df = df[[table_id] + columns]
        df = df.drop_duplicates()

        # Definindo um dataframe pandas a partir da tabela de argumento
        df_target = pd.DataFrame(df_target)

        # Left join de ambas as tabelas
        df = pd.merge(
            df, df_target,
            how='left',
            left_on=columns,
            right_on=list(df_target)
        )

        # Filtrando entradas da tabela resultante onde os valores da tabela alvo são nulos
        null_filter = " | ".join([
            "pd.isnull(df['{}'])".format(col) for col in list(df_target)
        ])
        df = df[eval(null_filter)]

        # Calculando a porcentagem de valores que falharam a expectation
        unexpected_percent = 100*(df.shape[0] / table_size)

        # Selecionando colunas de interesse e convertendo o dataframe
        # para uma lista de dicionários
        df = df[[table_id] + columns]
        
        df = df.head(100)
        df = df.to_dict(orient='records')

        return df, unexpected_percent


    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        execution_engine: "SparkDFExecutionEngine",
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        # Imports necessários para utilizar dataframes spark
        from pyspark.context import SparkContext
        from pyspark.sql.session import SparkSession
        from pyspark.sql import Row

        df, _, _ = execution_engine.get_compute_domain(
            domain_kwargs=metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )

        table_size = df.count()

        table_id = metric_value_kwargs.get("table_id")
        columns = metric_value_kwargs.get("columns")
        df_target = metric_value_kwargs.get("target_table")

        # Configurando o spark
        sc = SparkContext.getOrCreate()
        spark = SparkSession(sc)

        # Selecionando as colunas de interesse e excluindo duplicatas
        df = df.select(*([table_id] + columns))
        df = df.dropDuplicates()

        # Definindo um dataframe spark a partir da tabela de argumento
        df_target = spark.createDataFrame(Row(**x) for x in df_target)

        # Renomeando colunas da tabela alvo para evitar colunas duplicadas
        for column in df_target.columns:
            df_target = df_target.withColumnRenamed(column, "target_" + column)

        # Definindo uma expressão para o join utilizando múltiplas colunas
        cond = " & ".join([
            "(df.{} == df_target.{})".format(left_col, right_col)
            for left_col, right_col in zip(columns, df_target.columns)
        ])
        # Left join de ambas as tabelas
        df = df.join(df_target, on=eval(cond), how="left")

        # Filtrando entradas da tabela resultante onde os valores da tabela alvo são nulos
        null_filter = " | ".join([
            "(df.{}.isNull())".format(col) for col in df_target.columns
        ])
        df = df.filter(eval(null_filter))

        # Calculando a porcentagem de valores que falharam a expectation
        unexpected_percent = 100*(df.count() / table_size)

        # Selecionando colunas de interesse e convertendo o datafram 
        # para uma lista de dicionários
        df = df.select(*([table_id] + columns))
        
        df = df.limit(100)
        df = list(map(lambda row: row.asDict(), df.collect()))

        return df, unexpected_percent


class ExpectColumnsValuesToBeInTable(TableExpectation):
    # Setting necessary computation metric dependencies and defining kwargs,
    # as well as assigning kwargs default values
    metric_dependencies = ("table.custom.columns_values_to_be_in_table",)
    success_keys = ("table_id", "columns", "target_table")

    # Default values
    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,
        "table_id": None,
        "columns": None,
        "target_table": None
    }


    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        df_result, unexpected_percent = metrics["table.custom.columns_values_to_be_in_table"]

        # A expectation teve êxito se não há valores na tabela resultante
        success = (len(df_result) == 0)

        return {
            "success": success,
            "result": {
                "dataframe": df_result,
                "unexpected_percent": unexpected_percent
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
            "columns": "list",
            "target_table": "list"
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


    # Funções para a renderização do Data Docs

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
        """
        Método para renderizar a mensagem de erro da expectation.
        """

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
        
        params = substitute_none_for_missing(
            configuration.kwargs, ["columns", "target_table", "table_id"],
        )

        template_str = "Todas as instâncias das colunas {} devem estar presentes na tabela alvo.".format(
            params.get("columns")
        )

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
        """
        Método para renderizar a tabela dos valores que não obedeceram à expectation.

        Args:
            renderer_type: por padrão, utiliza o renderizador de uma tabela.
        Retorna:
            'None' se a expectation teve êxito ou um dicionário com os parâmetros da tabela
            caso contrário.
        """

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
                table_rows.append([row[k] for k in row.keys()])
        
        header_row = df[0].keys()
        
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
        """
        Método para renderizar a porcentagem dos valores que não atenderam
        à expectation.
        """

        result_dict = result.result

        if result_dict is None:
            return "--"

        if result_dict.get("unexpected_percent") is not None:
            return (
                str(round(result_dict.get("unexpected_percent"), 3))
                + "% inesperado"
            )
        else:
            return "--"