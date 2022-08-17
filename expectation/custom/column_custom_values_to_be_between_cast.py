from dateutil.parser import parse
import pandas as pd

from great_expectations.core import ExpectationConfiguration, ExpectationValidationResult
from great_expectations.execution_engine import (
   ExecutionEngine,
   PandasExecutionEngine,
   SparkDFExecutionEngine,
   SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnExpectation
from great_expectations.expectations.metrics import (
   ColumnMapMetricProvider,
   column_aggregate_value, column_condition_partial,
)
from great_expectations.expectations.metrics.import_manager import F, sa

from great_expectations.expectations.core import ExpectColumnValuesToBeBetween

from great_expectations.expectations.util import render_evaluation_parameter_string

from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent, RenderedTableContent, RenderedBulletListContent, RenderedGraphContent
from great_expectations.render.util import (
    substitute_none_for_missing,
    num_to_str
)

from typing import Any, Dict, List, Optional, Union



def handle_strict_min_max_custom(params: dict) -> (str, str):
    """
    Utility function for the at least and at most conditions based on strictness.
    Args:
        params: dictionary containing "strict_min" and "strict_max" booleans.
    Returns:
        tuple of strings to use for the at least condition and the at most condition
    """

    at_least_str = (
        "maiores que"
        if params.get("strict_min") is True
        else "maiores ou iguais a"
    )
    at_most_str = (
        "menores que" if params.get("strict_max") is True else "menores ou iguais a"
    )

    return at_least_str, at_most_str


# Adaptei o código da métrica original (ColumnValuesBetween) para inserir o cast
class ColumnValuesBetweenCast(ColumnMapMetricProvider):

    condition_metric_name = "column_values.between.cast"
    condition_value_keys = (
        "min_value",
        "max_value",
        "strict_min",
        "strict_max",
        "parse_strings_as_datetimes",
        "allow_cross_type_comparisons",
    )

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(
        cls,
        column,
        min_value=None,
        max_value=None,
        strict_min=None,
        strict_max=None,
        parse_strings_as_datetimes: Optional[bool] = False,
        allow_cross_type_comparisons=None,
        **kwargs
    ):
        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        if parse_strings_as_datetimes:
            warnings.warn(
                """The parameter "parse_strings_as_datetimes" is no longer supported and will be deprecated in a \
future release.  Please update code accordingly.
""",
                DeprecationWarning,
            )

            if min_value is not None:
                try:
                    min_value = parse(min_value)
                except TypeError:
                    pass

            if max_value is not None:
                try:
                    max_value = parse(max_value)
                except TypeError:
                    pass

            try:
                temp_column = column.map(parse)
            except TypeError:
                temp_column = column

        else:
            # Realiza o cast dos valores da coluna para numérico
            try:
                temp_column = pd.to_numeric(column)
            except TypeError:
                temp_column = column

        if min_value is not None and max_value is not None and min_value > max_value:
            raise ValueError("min_value cannot be greater than max_value")

        def is_between(val):
            # TODO Might be worth explicitly defining comparisons between types (for example, between strings and ints).
            # Ensure types can be compared since some types in Python 3 cannot be logically compared.
            # print type(val), type(min_value), type(max_value), val, min_value, max_value

            if type(val) is None:
                return False

            if min_value is not None and max_value is not None:
                if allow_cross_type_comparisons:
                    try:
                        if strict_min and strict_max:
                            return (min_value < val) and (val < max_value)
                        elif strict_min:
                            return (min_value < val) and (val <= max_value)
                        elif strict_max:
                            return (min_value <= val) and (val < max_value)
                        else:
                            return (min_value <= val) and (val <= max_value)
                    except TypeError:
                        return False

                else:
                    if (isinstance(val, str) != isinstance(min_value, str)) or (
                        isinstance(val, str) != isinstance(max_value, str)
                    ):
                        raise TypeError(
                            "Column values, min_value, and max_value must either be None or of the same type."
                        )

                    if strict_min and strict_max:
                        return (min_value < val) and (val < max_value)
                    elif strict_min:
                        return (min_value < val) and (val <= max_value)
                    elif strict_max:
                        return (min_value <= val) and (val < max_value)
                    else:
                        return (min_value <= val) and (val <= max_value)

            elif min_value is None and max_value is not None:
                if allow_cross_type_comparisons:
                    try:
                        if strict_max:
                            return val < max_value
                        else:
                            return val <= max_value
                    except TypeError:
                        return False

                else:
                    if isinstance(val, str) != isinstance(max_value, str):
                        raise TypeError(
                            "Column values, min_value, and max_value must either be None or of the same type."
                        )

                    if strict_max:
                        return val < max_value
                    else:
                        return val <= max_value

            elif min_value is not None and max_value is None:
                if allow_cross_type_comparisons:
                    try:
                        if strict_min:
                            return min_value < val
                        else:
                            return min_value <= val
                    except TypeError:
                        return False

                else:
                    if isinstance(val, str) != isinstance(min_value, str):
                        raise TypeError(
                            "Column values, min_value, and max_value must either be None or of the same type."
                        )

                    if strict_min:
                        return min_value < val
                    else:
                        return min_value <= val

            else:
                return False

        return temp_column.map(is_between)

    @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(
        cls,
        column,
        min_value=None,
        max_value=None,
        strict_min=None,
        strict_max=None,
        parse_strings_as_datetimes: Optional[bool] = False,
        **kwargs
    ):
        if parse_strings_as_datetimes:
            warnings.warn(
                """The parameter "parse_strings_as_datetimes" is no longer supported and will be deprecated in a \
future release.  Please update code accordingly.
""",
                DeprecationWarning,
            )

            if min_value is not None:
                try:
                    min_value = parse(min_value)
                except TypeError:
                    pass

            if max_value is not None:
                try:
                    max_value = parse(max_value)
                except TypeError:
                    pass
        else:
            # Realiza o cast dos valores da coluna para numérico
            try:
                column = sa.cast(column, sa.Float)
            except TypeError:
                pass

        if min_value is not None and max_value is not None and min_value > max_value:
            raise ValueError("min_value cannot be greater than max_value")

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        if min_value is None:
            if strict_max:
                return column < max_value
            else:
                return column <= max_value

        elif max_value is None:
            if strict_min:
                return min_value < column
            else:
                return min_value <= column

        else:
            if strict_min and strict_max:
                return sa.and_(min_value < column, column < max_value)
            elif strict_min:
                return sa.and_(min_value < column, column <= max_value)
            elif strict_max:
                return sa.and_(min_value <= column, column < max_value)
            else:
                return sa.and_(min_value <= column, column <= max_value)

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        column,
        min_value=None,
        max_value=None,
        strict_min=None,
        strict_max=None,
        parse_strings_as_datetimes: Optional[bool] = False,
        **kwargs
    ):
        if parse_strings_as_datetimes:
            warnings.warn(
                """The parameter "parse_strings_as_datetimes" is no longer supported and will be deprecated in a \
future release.  Please update code accordingly.
""",
                DeprecationWarning,
            )

            if min_value is not None:
                try:
                    min_value = parse(min_value)
                except TypeError:
                    pass

            if max_value is not None:
                try:
                    max_value = parse(max_value)
                except TypeError:
                    pass
        else:
            # Realiza o cast dos valores da coluna para numérico
            try:
                column = column.cast('float')
            except TypeError:
                pass

        if min_value is not None and max_value is not None and min_value > max_value:
            raise ValueError("min_value cannot be greater than max_value")

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        if min_value is None:
            if strict_max:
                return column < max_value
            else:
                return column <= max_value

        elif max_value is None:
            if strict_min:
                return min_value < column
            else:
                return min_value <= column

        else:
            if strict_min and strict_max:
                return (min_value < column) & (column < max_value)
            elif strict_min:
                return (min_value < column) & (column <= max_value)
            elif strict_max:
                return (min_value <= column) & (column < max_value)
            else:
                return (min_value <= column) & (column <= max_value)



# Expectation customizada que herda a expectation original
# A diferença será a métrica utilizada
class ExpectColumnValuesToBeBetweenCast(ExpectColumnValuesToBeBetween):

    map_metric = "column_values.between.cast"


    @classmethod
    def _atomic_prescriptive_template(
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
                "mostly",
                "row_condition",
                "condition_parser",
                "strict_min",
                "strict_max",
            ],
        )
        params_with_json_schema = {
            "column": {"schema": {"type": "string"}, "value": params.get("column")},
            "min_value": {
                "schema": {"type": "number"},
                "value": params.get("min_value"),
            },
            "max_value": {
                "schema": {"type": "number"},
                "value": params.get("max_value"),
            },
            "mostly": {"schema": {"type": "number"}, "value": params.get("mostly")},
            "mostly_pct": {
                "schema": {"type": "string"},
                "value": params.get("mostly_pct"),
            },
            "row_condition": {
                "schema": {"type": "string"},
                "value": params.get("row_condition"),
            },
            "condition_parser": {
                "schema": {"type": "string"},
                "value": params.get("condition_parser"),
            },
            "strict_min": {
                "schema": {"type": "boolean"},
                "value": params.get("strict_min"),
            },
            "strict_max": {
                "schema": {"type": "boolean"},
                "value": params.get("strict_max"),
            },
        }

        template_str = ""
        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str += "pode possuir qualquer valor numérico."
        else:
            at_least_str, at_most_str = handle_strict_min_max_custom(params)

            mostly_str = ""
            if params["mostly"] is not None and params["mostly"] < 1.0:
                params_with_json_schema["mostly_pct"]["value"] = num_to_str(
                    params["mostly"] * 100, precision=15, no_scientific=True
                )
                # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
                mostly_str = ", ao menos $mostly_pct % das vezes"

            if params["min_value"] is not None and params["max_value"] is not None:
                template_str += f"Valores devem ser {at_least_str} $min_value e {at_most_str} $max_value{mostly_str}."

            elif params["min_value"] is None:
                template_str += f"Valores devem ser {at_most_str} $max_value{mostly_str}."

            elif params["max_value"] is None:
                template_str += f"Valores devem ser {at_least_str} $min_value{mostly_str}."

        if include_column_name:
            template_str = f"$column {template_str}"

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(
                params["row_condition"], with_schema=True
            )
            template_str = f"{conditional_template_str}, então {template_str}"
            params_with_json_schema.update(conditional_params)

        return (template_str, params_with_json_schema, styling)

    # NOTE: This method is a pretty good example of good usage of `params`.
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
                "mostly",
                "row_condition",
                "condition_parser",
                "strict_min",
                "strict_max",
            ],
        )

        template_str = ""
        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str += "pode possuir qualquer valor numérico."
        else:
            at_least_str, at_most_str = handle_strict_min_max_custom(params)

            mostly_str = ""
            if params["mostly"] is not None and params["mostly"] < 1.0:
                params["mostly_pct"] = num_to_str(
                    params["mostly"] * 100, precision=15, no_scientific=True
                )
                # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
                mostly_str = ", ao menos $mostly_pct % das vezes"

            if params["min_value"] is not None and params["max_value"] is not None:
                template_str += f"Valores devem ser {at_least_str} $min_value e {at_most_str} $max_value{mostly_str}."

            elif params["min_value"] is None:
                template_str += f"Valores devem ser {at_most_str} $max_value{mostly_str}."

            elif params["max_value"] is None:
                template_str += f"Valores devem ser {at_least_str} $min_value{mostly_str}."

        if include_column_name:
            template_str = f"$column {template_str}"

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = f"{conditional_template_str}, então {template_str}"
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
    @renderer(renderer_type="renderer.descriptive.example_values_block")
    def _descriptive_example_values_block_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs
    ):
        assert result, "Must pass in result."
        if "partial_unexpected_counts" in result.result:
            partial_unexpected_counts = result.result["partial_unexpected_counts"]
            values = [str(v["value"]) for v in partial_unexpected_counts]
        elif "partial_unexpected_list" in result.result:
            values = [str(item) for item in result.result["partial_unexpected_list"]]
        else:
            return

        classes = ["col-3", "mt-1", "pl-1", "pr-1"]

        if any(len(value) > 80 for value in values):
            content_block_type = "bullet_list"
            content_block_class = RenderedBulletListContent
        else:
            content_block_type = "value_list"
            content_block_class = ValueListContent

        new_block = content_block_class(
            **{
                "content_block_type": content_block_type,
                "header": RenderedStringTemplateContent(
                    **{
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": "Valores de exemplo",
                            "tooltip": {"content": "expect_column_values_to_be_between_cast"},
                            "tag": "h6",
                        },
                    }
                ),
                content_block_type: [
                    {
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": "$value",
                            "params": {"value": value},
                            "styling": {
                                "default": {
                                    "classes": ["badge", "badge-info"]
                                    if content_block_type == "value_list"
                                    else [],
                                    "styles": {"word-break": "break-all"},
                                },
                            },
                        },
                    }
                    for value in values
                ],
                "styling": {
                    "classes": classes,
                },
            }
        )

        return new_block


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
                    return num_to_str(observed_value, precision=3, use_locale=True) + " valores inesperados encontrados"
                else:
                    return num_to_str(observed_value, precision=3, use_locale=True) + " valores inesperados encontrados"
            return str(observed_value)
        elif result_dict.get("unexpected_percent") is not None:
            return (
                num_to_str(result_dict.get("unexpected_percent"), precision=3).replace(".", ",")
                + "% inesperado"
            )
        else:
            return "--"

    @classmethod
    @renderer(renderer_type="renderer.diagnostic.unexpected_statement")
    def _diagnostic_unexpected_statement_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        assert result, "Must provide a result object."
        success = result.success
        result_dict = result.result
        if result.exception_info["raised_exception"]:
            exception_message_template_str = (
                "\n\n$expectation_type raised an exception:\n$exception_message"
            )

            exception_message = RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": exception_message_template_str,
                        "params": {
                            "expectation_type": result.expectation_config.expectation_type,
                            "exception_message": result.exception_info[
                                "exception_message"
                            ],
                        },
                        "tag": "strong",
                        "styling": {
                            "classes": ["text-danger"],
                            "params": {
                                "exception_message": {"tag": "code"},
                                "expectation_type": {
                                    "classes": ["badge", "badge-danger", "mb-2"]
                                },
                            },
                        },
                    },
                }
            )

            exception_traceback_collapse = CollapseContent(
                **{
                    "collapse_toggle_link": "Show exception traceback...",
                    "collapse": [
                        RenderedStringTemplateContent(
                            **{
                                "content_block_type": "string_template",
                                "string_template": {
                                    "template": result.exception_info[
                                        "exception_traceback"
                                    ],
                                    "tag": "code",
                                },
                            }
                        )
                    ],
                }
            )

            return [exception_message, exception_traceback_collapse]

        if success or not result_dict.get("unexpected_count"):
            return []
        else:
            unexpected_count = num_to_str(
                result_dict["unexpected_count"], use_locale=True, precision=20
            )
            unexpected_percent = (
                num_to_str(result_dict["unexpected_percent"], precision=3) + "%"
            )
            element_count = num_to_str(
                result_dict["element_count"], use_locale=True, precision=20
            )
    
            template_str = (
                "\n\n$unexpected_count valores inesperados encontrados. "
                "$unexpected_percent de $element_count entradas."
            )

            return [
                RenderedStringTemplateContent(
                    **{
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": template_str,
                            "params": {
                                "unexpected_count": unexpected_count,
                                "unexpected_percent": unexpected_percent,
                                "element_count": element_count,
                            },
                            "tag": "strong",
                            "styling": {"classes": ["text-danger"]},
                        },
                    }
                )
            ]
        
