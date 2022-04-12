from typing import Dict, Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.util import render_evaluation_parameter_string

from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.render.util import (
    handle_strict_min_max,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)
#from great_expectations.expectation import ColumnExpectation
from great_expectations.expectations.core import ExpectColumnMinToBeBetween

class ExpectColumnMinToBeBetweenCustomMessage(ExpectColumnMinToBeBetween):
    """Expect the column minimum to be between an min and max value

            expect_column_min_to_be_between is a \
            :func:`column_aggregate_expectation
                <great_expectations.execution_engine.MetaExecutionEngine.column_aggregate_expectation>`.

            Args:
                column (str): \
                    The column name
                min_value (comparable type or None): \
                    The minimal column minimum allowed.
                max_value (comparable type or None): \
                    The maximal column minimum allowed.
                strict_min (boolean):
                    If True, the minimal column minimum must be strictly larger than min_value, default=False
                strict_max (boolean):
                    If True, the maximal column minimum must be strictly smaller than max_value, default=False

            Keyword Args:
                parse_strings_as_datetimes (Boolean or None): \
                    If True, parse min_value, max_values, and all non-null column values to datetimes before making \
                    comparisons.
                output_strftime_format (str or None): \
                    A valid strfime format for datetime output. Only used if parse_strings_as_datetimes=True.

            Other Parameters:
                result_format (str or None): \
                    Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`. \
                    For more detail, see :ref:`result_format <result_format>`.
                include_config (boolean): \
                    If True, then include the expectation config as part of the result object. \
                    For more detail, see :ref:`include_config`.
                catch_exceptions (boolean or None): \
                    If True, then catch exceptions and include them as part of the result object. \
                    For more detail, see :ref:`catch_exceptions`.
                meta (dict or None): \
                    A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
                    modification. For more detail, see :ref:`meta`.

            Returns:
                An ExpectationSuiteValidationResult

                Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
                :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

            Notes:
                These fields in the result object are customized for this expectation:
                ::

                    {
                        "observed_value": (list) The actual column min
                    }


                * min_value and max_value are both inclusive unless strict_min or strict_max are set to True.
                * If min_value is None, then max_value is treated as an upper bound
                * If max_value is None, then min_value is treated as a lower bound

            """

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name", True)
        include_column_name = (
            include_column_name if include_column_name is not None else True
        )
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(
            configuration.kwargs,
            [
                "column",
                "min_value",
                "max_value",
                "parse_strings_as_datetimes",
                "row_condition",
                "condition_parser",
                "strict_min",
                "strict_max",
            ],
        )

        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "Valor mínimo deve ter qualquer valor numérico."
        else:
            if params["min_value"] is not None and params["max_value"] is not None:
                template_str = f"Valor mínimo deve ser ao menos $min_value e até $max_value."
            elif params["min_value"] is None:
                template_str = f"Valor mínimo deve ser até $max_value."
            elif params["max_value"] is None:
                template_str = f"Valor mínimo deve ser ao menos $min_value."
            else:
                template_str = ""

        if params.get("parse_strings_as_datetimes"):
            template_str += " Valores devem ser considerados no formato datetime."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", então " + template_str
            params.update(conditional_params)

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
    @renderer(renderer_type="renderer.descriptive.stats_table.min_row")
    def _descriptive_stats_table_min_row_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        assert result, "Must pass in result."
        return [
            {
                "content_block_type": "string_template",
                "string_template": {
                    "template": "Minimum",
                    "tooltip": {"content": "expect_column_min_to_be_between"},
                },
            },
            "{:f}".format(result.result["observed_value"]),
        ]