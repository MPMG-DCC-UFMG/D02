from great_expectations.expectations.metrics.table_metric_provider import (
    TableMetricProvider
)
from great_expectations.expectations.expectation import TableExpectation
from great_expectations.expectations.metrics import (
   metric_value, metric_partial
)
from great_expectations.execution_engine import (
   ExecutionEngine,
   PandasExecutionEngine,
   SparkDFExecutionEngine,
   SqlAlchemyExecutionEngine,
)
from great_expectations.core import ExpectationConfiguration, ExpectationValidationResult
from great_expectations.render.renderer.renderer import renderer
from great_expectations.expectations.util import render_evaluation_parameter_string

from great_expectations.execution_engine.execution_engine import (
    MetricDomainTypes,
    MetricPartialFunctionTypes,
)

from great_expectations.render.types import RenderedStringTemplateContent, RenderedTableContent
from great_expectations.render.util import substitute_none_for_missing 

from typing import Any, Dict, List, Optional, Union, Tuple

import pandas as pd

class DatesToMatchAcrossTables(TableMetricProvider):
    metric_name = "table.custom.expect_dates_to_match_across_tables"
    value_keys = ("list_dates", "df_join")
    
    @metric_value(engine=PandasExecutionEngine)
    def _pandas(
        cls,
        execution_engine: "PandasExecutionEngine",
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        df_join = metric_value_kwargs.get("df_join")
        df_join = pd.DataFrame(df_join)
        
        list_dates = metric_value_kwargs.get("list_dates")
        
        total_licit = df_join.shape[0]
        dict_unexpected_values = {"date1": [], "date2": [], "amount": [],"percent":[]}
        
        for l1 in range(len(list_dates)):
            if l1 != (len(list_dates)-1):
                for l2 in range(l1+1, len(list_dates)):
                    list_date_1 = list_dates[l1]
                    list_date_2 = list_dates[l2]
                    for date_1 in list_date_1:
                        for date_2 in list_date_2:
                            df_join_temp = df_join.filter(df_join[date_1] > df_join[date_2])
                            df_join_temp = df_join[df_join[date_1] > df_join[date_2]]
                            amount = df_join_temp.shape[0]
                            if amount > 0 :
                                dict_unexpected_values["date1"].append(date_1)
                                dict_unexpected_values["date2"].append(date_2)
                                dict_unexpected_values["amount"].append(amount)
                                percent = (amount/total_licit)*100
                                percent = round(float(f"{percent:,}"), 2)
                                dict_unexpected_values["percent"].append(percent)
        
        len_unexpected_values = len(dict_unexpected_values["date1"])
        success = (len_unexpected_values == 0)
        dict_unexpected_values["date1"] = dict_unexpected_values["date1"][:100]
        dict_unexpected_values["date2"] = dict_unexpected_values["date2"][:100]
        dict_unexpected_values["amount"] = dict_unexpected_values["amount"][:100]
        dict_unexpected_values["percent"] = dict_unexpected_values["percent"][:100]
        
        return success, len_unexpected_values, dict_unexpected_values
    
    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        execution_engine: "SparkDFExecutionEngine",
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        from pyspark.context import SparkContext
        from pyspark.sql.session import SparkSession
        from pyspark.sql import Row
        
        sc = SparkContext.getOrCreate()
        spark = SparkSession(sc)
        
        df_join = metric_value_kwargs.get("df_join")
        df_join = spark.createDataFrame(Row(**x) for x in df_join)
        
        list_dates = metric_value_kwargs.get("list_dates")
        
        total_licit = df_join.count()
        dict_unexpected_values = {"date1": [], "date2": [], "amount": [],"percent":[]}

        for l1 in range(len(list_dates)):
            if l1 != (len(list_dates)-1):
                for l2 in range(l1+1, len(list_dates)):
                    list_date_1 = list_dates[l1]
                    list_date_2 = list_dates[l2]
                    for date_1 in list_date_1:
                        for date_2 in list_date_2:
                            df_join_temp = df_join.filter(df_join[date_1] > df_join[date_2])
                            amount = df_join_temp.count()
                            if amount > 0 :
                                dict_unexpected_values["date1"].append(date_1)
                                dict_unexpected_values["date2"].append(date_2)
                                dict_unexpected_values["amount"].append(amount)
                                percent = (amount/total_licit)*100
                                percent = round(float(f"{percent:,}"), 2)
                                dict_unexpected_values["percent"].append(percent)
        
        len_unexpected_values = len(dict_unexpected_values["date1"])
        success = (len_unexpected_values == 0)
        dict_unexpected_values["date1"] = dict_unexpected_values["date1"][:100]
        dict_unexpected_values["date2"] = dict_unexpected_values["date2"][:100]
        dict_unexpected_values["amount"] = dict_unexpected_values["amount"][:100]
        dict_unexpected_values["percent"] = dict_unexpected_values["percent"][:100]
        
        return success, len_unexpected_values, dict_unexpected_values


class ExpectDatesToMatchAcrossTables(TableExpectation):
    metric_dependencies = ("table.custom.expect_dates_to_match_across_tables",)
    success_keys = ("list_dates", "df_join")

    # Default values
    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,
        "list_dates": None,
        "df_join": None
    }

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        success, len_unexpected_values, dict_unexpected_values = metrics["table.custom.expect_dates_to_match_across_tables"]
                
        return {"success": success, "result": {"observed_value": len_unexpected_values, "unexpected_values": dict_unexpected_values}}
    

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

        template_str = 'As datas apresentadas em \"Data 1\" não devem ser maiores do que em \"Data 2\".'

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

        if not result_dict.get("unexpected_values"):
            return None
        else:
            unexpected_values = result_dict.get("unexpected_values")
            date1_l = unexpected_values["date1"]
            date2_l = unexpected_values["date2"]
            amount_l = unexpected_values["amount"]
            percent_l = unexpected_values["percent"]
            for date1, date2, amount, percent in zip(date1_l, date2_l, amount_l, percent_l):
                table_rows.append([
                    date1,
                    date2,
                    amount,
                    percent
                ])
        
        header_row = [
            "Data 1",
            "Data 2",
            "Qtd. Registros",
            "%"
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