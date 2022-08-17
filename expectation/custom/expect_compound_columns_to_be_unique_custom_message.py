from typing import Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.expectations.expectation import MulticolumnMapExpectation
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.render.util import (
    num_to_str,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)


class ExpectCompoundColumnsToBeUniqueCustomMessage(ExpectCompoundColumnsToBeUnique):

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
        styling = runtime_configuration.get("styling")

        params = substitute_none_for_missing(
            configuration.kwargs,
            [
                "column_list",
                "ignore_row_if",
                "row_condition",
                "condition_parser",
                "mostly",
            ],
        )
        params_with_json_schema = {
            "column_list": {
                "schema": {"type": "array"},
                "value": params.get("column_list"),
            },
            "ignore_row_if": {
                "schema": {"type": "string"},
                "value": params.get("ignore_row_if"),
            },
            "row_condition": {
                "schema": {"type": "string"},
                "value": params.get("row_condition"),
            },
            "condition_parser": {
                "schema": {"type": "string"},
                "value": params.get("condition_parser"),
            },
            "mostly": {
                "schema": {"type": "number"},
                "value": params.get("mostly"),
            },
            "mostly_pct": {
                "schema": {"type": "string"},
                "value": params.get("mostly_pct"),
            },
        }

        if params["mostly"] is not None and params["mostly"] < 1.0:
            params_with_json_schema["mostly_pct"]["value"] = num_to_str(
                params["mostly"] * 100, precision=15, no_scientific=True
            )
            template_str = f"Valores para as seguintes colunas devem ser únicos em conjunto, ao menos $mostly_pct % das vezes: "
        else:
            template_str = (
                f"Valores para as seguintes colunas devem ser únicos em conjunto: "
            )

        column_list = params.get("column_list") if params.get("column_list") else []

        if len(column_list) > 0:
            for idx, val in enumerate(column_list[:-1]):
                param = f"$column_list_{idx}"
                template_str += f"{param}, "
                params[param] = val

            last_idx = len(column_list) - 1
            last_param = f"$column_list_{last_idx}"
            template_str += last_param
            params[last_param] = column_list[last_idx]

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(
                params["row_condition"], with_schema=True
            )
            template_str = (
                conditional_template_str
                + ", então "
                + template_str[0].lower()
                + template_str[1:]
            )
            params_with_json_schema.update(conditional_params)

        return (template_str, params_with_json_schema, styling)

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
        styling = runtime_configuration.get("styling")

        params = substitute_none_for_missing(
            configuration.kwargs,
            [
                "column_list",
                "ignore_row_if",
                "row_condition",
                "condition_parser",
                "mostly",
            ],
        )

        if params["mostly"] is not None and params["mostly"] < 1.0:
            params_with_json_schema["mostly_pct"]["value"] = num_to_str(
                params["mostly"] * 100, precision=15, no_scientific=True
            )
            template_str = f"Valores para as seguintes colunas devem ser únicos em conjunto, ao menos $mostly_pct % das vezes: "
        else:
            template_str = (
                f"Valores para as seguintes colunas devem ser únicos em conjunto: "
            )

        for idx in range(len(params["column_list"]) - 1):
            template_str += f"$column_list_{str(idx)}, "
            params[f"column_list_{str(idx)}"] = params["column_list"][idx]

        last_idx = len(params["column_list"]) - 1
        template_str += f"$column_list_{str(last_idx)}"
        params[f"column_list_{str(last_idx)}"] = params["column_list"][last_idx]

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = (
                conditional_template_str
                + ", then "
                + template_str[0].lower()
                + template_str[1:]
            )
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