import pandas as pd

from great_expectations.core import ExpectationConfiguration, ExpectationValidationResult
from great_expectations.execution_engine import (
   ExecutionEngine,
   PandasExecutionEngine,
   SparkDFExecutionEngine,
   SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import TableExpectation
from great_expectations.expectations.metrics import (
#   TableMetricProvider,
   metric_value, metric_partial,
)

from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.expectations.metrics.table_metric_provider import (
    TableMetricProvider,
)

from great_expectations.execution_engine.execution_engine import (
    MetricDomainTypes,
    MetricPartialFunctionTypes,
)

from great_expectations.expectations.metrics.import_manager import F, sa
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent, RenderedTableContent, RenderedBulletListContent, RenderedGraphContent
from great_expectations.render.util import substitute_none_for_missing, num_to_str

from typing import Any, Dict, List, Optional, Union, Tuple

class TableCountLicitacoesConviteWithActivityAboveOneYear(TableMetricProvider):
    """MetricProvider Class for Custom Aggregate UniqueValueRate MetricProvider"""

    metric_name = "table.custom.activity_above_one_year"
    value_keys = ("licitacao_id_field", "year_column_name",)

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

        licitacao_id_field = metric_value_kwargs.get("licitacao_id_field")
        year_column_name = metric_value_kwargs.get("year_column_name")

        df = df[[licitacao_id_field, year_column_name]]
        df_grouped = df.groupby([licitacao_id_field]).nunique().reset_index()
        df_grouped = df_grouped.sort_values(by=year_column_name, ascending=False)
        df_unexpected = df_grouped[df_grouped[year_column_name] > 1]

        count_licitacao = df_unexpected.shape[0]
        unexpected_values = list(df_unexpected[licitacao_id_field])
        
        if len(unexpected_values) > 100:
            unexpected_values = unexpected_values[:100]

        return count_licitacao, unexpected_values
    
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

        licitacao_id_field = metric_value_kwargs.get("licitacao_id_field")
        year_column_name = metric_value_kwargs.get("year_column_name")
        
        df.select(F.col(licitacao_id_field), F.col(year_column_name))
        df_grouped = df.groupBy(licitacao_id_field).agg(F.countDistinct(year_column_name).alias("ano_referencia_count"))
        df_grouped = df_grouped.sort("ano_referencia_count", ascending=False)
        df_unexpected = df_grouped.filter(df_grouped.ano_referencia_count > 1)

        count_licitacao = df_unexpected.count()
        unexpected_values = df_unexpected.select(licitacao_id_field).rdd.flatMap(list).collect()
        
        if len(unexpected_values) > 100:
            unexpected_values = unexpected_values[:100]
        
        return count_licitacao, unexpected_values


class ExpectOnlyOneYearOfActivity(TableExpectation):
    # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values
    metric_dependencies = ("table.custom.activity_above_one_year",)
    success_keys = ("licitacao_id_field", "year_column_name",)

    # Default values
    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,
        "licitacao_id_field": None,
        "year_column_name": None,
    }
    
    
    

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        """Validates the given data against the set minimum and maximum value thresholds for the column max"""
        count_licitacao, unexpected_values = metrics["table.custom.activity_above_one_year"]

        # Checking if mean lies between thresholds
        success = (count_licitacao == 0)

        return {"success": success, "result": {"observed_value": count_licitacao, "partial_unexpected_list": unexpected_values}}
        
        
        
        

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.
        Args:
            configuration (OPTIONAL[ExpectationConfiguration]):                 An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            True if the configuration has been validated successfully. Otherwise, raises an exception
        """

        # Setting up a configuration
        # super().validate_configuration(configuration)
        # if configuration is None:
        #     configuration = self.configuration
        return True



    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration: ExpectationConfiguration = None,
        result: ExpectationValidationResult = None,
        language: str = None,
        runtime_configuration: dict = None,
        **kwargs,
    ) -> List[Union[dict, str, RenderedStringTemplateContent, RenderedTableContent, RenderedBulletListContent,
                    RenderedGraphContent, Any]]:

        assert configuration or result, "Must provide renderers either a configuration or result."
        
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
        #template_str = "Must have exactly $value rows."
        template_str = "As licitações devem possuir somente um único ano de atividade."

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

        if not result_dict.get("partial_unexpected_list") and not result_dict.get(
            "partial_unexpected_counts"
        ):
            return None

        table_rows = []

        if result_dict.get("partial_unexpected_counts"):
            # We will check to see whether we have *all* of the unexpected values
            # accounted for in our count, and include counts if we do. If we do not,
            # we will use this as simply a better (non-repeating) source of
            # "sampled" unexpected values

            total_count = 0
            for unexpected_count_dict in result_dict.get("partial_unexpected_counts"):
                if not isinstance(unexpected_count_dict, dict):
                    # handles case: "partial_exception_counts requires a hashable type"
                    # this case is also now deprecated (because the error is moved to an errors key
                    # the error also *should have* been updated to "partial_unexpected_counts ..." long ago.
                    # NOTE: JPC 20200724 - Consequently, this codepath should be removed by approximately Q1 2021
                    continue
                value = unexpected_count_dict.get("value")
                count = unexpected_count_dict.get("count")
                total_count += count
                if value is not None and value != "":
                    table_rows.append([value, count])
                elif value == "":
                    table_rows.append(["EMPTY", count])
                else:
                    table_rows.append(["null", count])

            # Check to see if we have *all* of the unexpected values accounted for. If so,
            # we show counts. If not, we only show "sampled" unexpected values.
            if total_count == result_dict.get("unexpected_count"):
                header_row = ["Unexpected Value", "Count"]
            else:
                header_row = ["IDs de licitações com vários anos de atividade"]
                table_rows = [[row[0]] for row in table_rows]
        else:
            header_row = ["IDs de licitações com vários anos de atividade"]
            sampled_values_set = set()
            for unexpected_value in result_dict.get("partial_unexpected_list"):
                if unexpected_value:
                    string_unexpected_value = str(unexpected_value)
                elif unexpected_value == "":
                    string_unexpected_value = "EMPTY"
                else:
                    string_unexpected_value = "null"
                if string_unexpected_value not in sampled_values_set:
                    table_rows.append([unexpected_value])
                    sampled_values_set.add(string_unexpected_value)

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

        if result_dict.get("observed_value"):
            observed_value = result_dict.get("observed_value")
            if isinstance(observed_value, (int, float)) and not isinstance(observed_value, bool):
                if (observed_value == 1):
                    return num_to_str(observed_value, precision=10, use_locale=True) + " licitação"
                else:
                    return num_to_str(observed_value, precision=10, use_locale=True) + " licitações"
            return str(observed_value)
        elif result_dict.get("unexpected_percent") is not None:
            return (
                num_to_str(result_dict.get("unexpected_percent"), precision=5)
                + "% unexpected"
            )
        else:
            return "--"