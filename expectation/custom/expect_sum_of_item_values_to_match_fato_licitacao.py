from typing import Any, Dict, Optional, Tuple
import numpy as np

from great_expectations.execution_engine import (
   ExecutionEngine,
   PandasExecutionEngine,
   SparkDFExecutionEngine,
   SqlAlchemyExecutionEngine,
)
from great_expectations.execution_engine.execution_engine import MetricDomainTypes

from great_expectations.core import ExpectationConfiguration
from great_expectations.core.expectation_configuration import parse_result_format

from great_expectations.expectations.metrics import (
   metric_value, metric_partial
)
from great_expectations.expectations.metrics.table_metric_provider import TableMetricProvider
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    _format_map_output,
    TableExpectation
)
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent, RenderedTableContent
from great_expectations.render.util import (
    num_to_str,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)

class SumOfItemValuesToMatchFatoLicitacao(TableMetricProvider):
    metric_name = "table.custom.sum_of_item_values_to_match_fato_licitacao"
    value_keys = ('offset', 'fato_licitacao')

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
        offset = float(metric_value_kwargs.get('offset'))
        fato_licitacao = metric_value_kwargs.get('fato_licitacao')

        # Criando pandas DataFrame
        df_licitacao = pd.DataFrame(df_licitacao)
        fato_licitacao = pd.DataFrame(fato_licitacao)

        # Selecionando as colunas de interesse e excluindo duplicatas
        df_licitacao = df_licitacao[['id_licitacao', 'qtde_item_cotado', 'vlr_item_cotado']]
        df_licitacao = df_licitacao.drop_duplicates()
        fato_licitacao = fato_licitacao[['id_licitacao', 'vlr_licitacao']]

        # Criando coluna com valores totais
        valor_total = df_licitacao['qtde_item_cotado'] * df_licitacao['vlr_item_cotado']
        df_licitacao['valor_total'] = valor_total

        # Agrupando valores por id_licitacao
        valores = df_licitacao.groupby('id_licitacao').valor_total.sum()
        valores = pd.DataFrame({'id_licitacao': valores.index, 'valor_total': valores.values})

        # Comparando os valores
        valores = valores.merge(fato_licitacao, how='left', on='id_licitacao')
        valores['diferenca'] = valores['valor_total'] - valores['vlr_licitacao']
        valores['diferenca_abs'] = valores['diferenca'].abs()
        
        df = valores[valores['diferenca_abs'] > offset][['id_licitacao',  'valor_total', 'vlr_licitacao','diferenca_abs']]

        success = (df.shape[0] == 0)
        return_df = df.head(10).to_dict('records')
        unexpected_percent = 100 * (df.shape[0] / valores.shape[0])

        return success, return_df, unexpected_percent

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        execution_engine: "SparkDFExecutionEngine",
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        import pyspark.sql.functions as F
        from pyspark.sql.types import IntegerType

        df_licitacao, _, _ = execution_engine.get_compute_domain(
            domain_kwargs=metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )
        offset = float(metric_value_kwargs.get('offset'))
        fato_licitacao = metric_value_kwargs.get('fato_licitacao')

        # [NOTE] Precisei adicionar esses imports para usar a variável spark ao criar o dataframe fato_licitacao
        from pyspark.context import SparkContext
        from pyspark.sql.session import SparkSession
        from pyspark.sql import Row
        sc = SparkContext.getOrCreate()
        spark = SparkSession(sc)

        # Criando spark DataFrame
        fato_licitacao = spark.createDataFrame(Row(**x) for x in fato_licitacao)
        #fato_licitacao = spark.createDataFrame(fato_licitacao)

        # Selecionando as colunas de interesse e excluindo duplicatas
        df_licitacao = df_licitacao.select('id_licitacao', 'qtde_item_cotado', 'vlr_item_cotado')
        df_licitacao = df_licitacao.dropDuplicates()
        fato_licitacao = fato_licitacao.select('id_licitacao', 'vlr_licitacao')

        # Criando coluna com valores totais
        df_licitacao = df_licitacao.withColumn('valor_total', F.col('qtde_item_cotado')*F.col('vlr_item_cotado'))

        # Agrupando valores por id_licitacao
        valores = df_licitacao.groupBy('id_licitacao').agg(F.sum('valor_total')).withColumnRenamed("sum(valor_total)", "valor_total")

        # Comparando os valores
        valores = valores.join(fato_licitacao,['id_licitacao'],how='inner')
        valores = valores.withColumn('diferenca', F.col('valor_total') - F.col('vlr_licitacao'))
        valores = valores.withColumn('diferenca_abs', F.abs(valores.diferenca))
        valores_filter = valores.filter(valores.diferenca_abs > offset)
        valores_filter = valores_filter.withColumn('id_licitacao', valores_filter['id_licitacao'].cast(IntegerType()))
        
        success = (valores_filter.count() == 0)
        unexpected_percent = 100 * valores_filter.count() / valores.count()
        valores_filter = valores_filter.limit(100)
        return_df = list(map(lambda row: row.asDict(), valores_filter.select('id_licitacao', 'valor_total','vlr_licitacao','diferenca_abs').collect()))

        return success, return_df, unexpected_percent

        

class ExpectSumOfItemValuesToMatchFatoLicitacao(TableExpectation):
    # Setting necessary computation metric dependencies and defining kwargs,
    # as well as assigning kwargs default values
    metric_dependencies = ("table.custom.sum_of_item_values_to_match_fato_licitacao",)
    success_keys = ('offset', 'fato_licitacao')

    # Default values
    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,
        "offset": 0.0,
        "fato_licitacao": None
    }

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        success, return_df, unexpected_percent = metrics["table.custom.sum_of_item_values_to_match_fato_licitacao"]

        offset = self.get_success_kwargs(configuration).get("offset")
        
        return {
            "success": success,
            "result": {
                "dataframe": return_df,
                "unexpected_percent": unexpected_percent,
                "offset": offset
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
            assert("offset" in configuration.kwargs)
        except AssertionError:
            print("'offset' parameter is required for this expectation")
            return False
        
        try:
            assert("fato_licitacao" in configuration.kwargs)
        except AssertionError:
            print("'fato_licitacao' parameter is required for this expectation")
            return False

        # Validating that 'offset' is of the proper format and type
        if "offset" in configuration.kwargs:
            offset = configuration.kwargs["offset"]
        
        if "fato_licitacao" in configuration.kwargs:
            fato_licitacao = configuration.kwargs["fato_licitacao"]

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

        template_str = "Soma dos valores dos itens de licitação diferente do valor da licitação."

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

        if result_dict.get("dataframe"):
            result_dict = result_dict.get("dataframe")
            for id_dict in result_dict:
                id = id_dict.get("id_licitacao")
                sum_itens = id_dict.get("valor_total")
                sum_itens = round(float(sum_itens), 2)
                sum_itens = f"R$ {sum_itens:,.2f}".replace(',','v').replace('.',',').replace('v','.')
                vlr_licit = id_dict.get("vlr_licitacao")
                vlr_licit = round(float(vlr_licit), 2)
                vlr_licit = f"R$ {vlr_licit:,.2f}".replace(',','v').replace('.',',').replace('v','.')
                dif = id_dict.get("diferenca_abs")
                dif = round(float(dif), 2)
                dif = f"R$ {dif:,.2f}".replace(',','v').replace('.',',').replace('v','.')
                table_rows.append([id, sum_itens, vlr_licit, dif])

        header_row = ["ID Licitação", "Soma itens", "Vlr. Licitação", "Diferença"]
        
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