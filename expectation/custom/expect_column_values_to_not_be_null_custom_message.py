from typing import Dict

from great_expectations.core import ExpectationConfiguration
from great_expectations.core.expectation_configuration import parse_result_format
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    _format_map_output,
)
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.render.util import (
    num_to_str,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)
from great_expectations.expectations.core import ExpectColumnValuesToNotBeNull

class ExpectColumnValuesToNotBeNullCustomMessage(ExpectColumnValuesToNotBeNull):
    """Expect column values to not be null.

    To be counted as an exception, values must be explicitly null or missing, such as a NULL in PostgreSQL or an
    np.NaN in pandas. Empty strings don't count as null unless they have been coerced to a null type.

    expect_column_values_to_not_be_null is a \
    :func:`column_map_expectation <great_expectations.execution_engine.execution_engine.MetaExecutionEngine
    .column_map_expectation>`.

    Args:
        column (str): \
            The column name.

    Keyword Args:
        mostly (None or a float between 0 and 1): \
            Return `"success": True` if at least mostly fraction of values match the expectation. \
            For more detail, see :ref:`mostly`.

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
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

    See Also:
        :func:`expect_column_values_to_be_null \
        <great_expectations.execution_engine.execution_engine.ExecutionEngine.expect_column_values_to_be_null>`

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
            ["column", "mostly", "row_condition", "condition_parser"],
        )

        if params["mostly"] is not None:
            params["mostly_pct"] = num_to_str(
                params["mostly"] * 100, precision=3, no_scientific=True
            )
            # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
            if include_column_name:
                template_str = "Valores em $column não devem ser nulos, pelo menos $mostly_pct % das vezes."
            else:
                template_str = (
                    "Valores da coluna não devem ser nulos, pelo menos $mostly_pct % das vezes."
                )
        else:
            if include_column_name:
                template_str = "Valores em $column nunca devem ser nulos."
            else:
                template_str = "Valores nunca devem ser nulos."

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
    @renderer(renderer_type="renderer.diagnostic.observed_value")
    def _diagnostic_observed_value_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs
    ):
        result_dict = result.result

        try:
            null_percent = result_dict["unexpected_percent"]
            return (
                num_to_str(100 - null_percent, precision=3, use_locale=True).replace(".", ",")
                + "% não nulo"
            )
        except KeyError:
            return "% não nulo desconhecido"
        except TypeError:
            return "NaN% não nulo"
        return "--"




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

        return None #new_block


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
        
