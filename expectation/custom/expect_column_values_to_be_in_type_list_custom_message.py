import inspect
import logging
from typing import Dict, Optional

import numpy as np
import pandas as pd

from great_expectations.core import ExpectationConfiguration
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.core.expect_column_values_to_be_of_type import (
    _get_dialect_type_module,
    _native_type_type_map,
)
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.registry import get_metric_kwargs
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.render.util import (
    num_to_str,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)
from great_expectations.validator.metric_configuration import MetricConfiguration

logger = logging.getLogger(__name__)

try:
    import pyspark.sql.types as sparktypes
except ImportError as e:
    logger.debug(str(e))
    logger.debug(
        "Unable to load spark context; install optional spark dependency for support."
    )

from great_expectations.expectations.core import ExpectColumnValuesToBeInTypeList

class ExpectColumnValuesToBeInTypeListCustomMessage(ExpectColumnValuesToBeInTypeList):
    """
    Expect a column to contain values from a specified type list.

    expect_column_values_to_be_in_type_list is a \
    :func:`column_map_expectation <great_expectations.execution_engine.execution_engine.MetaExecutionEngine
    .column_map_expectation>` for typed-column backends,
    and also for PandasDataset where the column dtype provides an unambiguous constraints (any dtype except
    'object'). For PandasDataset columns with dtype of 'object' expect_column_values_to_be_of_type is a
    :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>` and will
    independently check each row's type.

    Args:
        column (str): \
            The column name.
        type_list (str): \
            A list of strings representing the data type that each column should have as entries. Valid types are
            defined by the current backend implementation and are dynamically loaded. For example, valid types for
            PandasDataset include any numpy dtype values (such as 'int64') or native python types (such as 'int'),
            whereas valid types for a SqlAlchemyDataset include types named by the current driver such as 'INTEGER'
            in most SQL dialects and 'TEXT' in dialects such as postgresql. Valid types for SparkDFDataset include
            'StringType', 'BooleanType' and other pyspark-defined type names.

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

    See also:
        :func:`expect_column_values_to_be_of_type \
        <great_expectations.dataset.dataset.Dataset.expect_column_values_to_be_of_type>`
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
            ["column", "type_list", "mostly", "row_condition", "condition_parser"],
        )

        if params["type_list"] is not None:
            for i, v in enumerate(params["type_list"]):
                params["v__" + str(i)] = v
            values_string = " ".join(
                ["$v__" + str(i) for i, v in enumerate(params["type_list"])]
            )

            if params["mostly"] is not None:
                params["mostly_pct"] = num_to_str(
                    params["mostly"] * 100, precision=3, no_scientific=True
                )
                # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
                if include_column_name:
                    template_str = (
                        "$column tipos de valores devem pertencer a este set: "
                        + values_string
                        + ", pelo menos $mostly_pct % das vezes."
                    )
                else:
                    template_str = (
                        "Tipos de valores devem pertencer a este set: "
                        + values_string
                        + ", pelo menos $mostly_pct % das vezes."
                    )
            else:
                if include_column_name:
                    template_str = (
                        "$column tipos de valores devem pertencer a este set: "
                        + values_string
                        + "."
                    )
                else:
                    template_str = (
                        "Tipos de valores devem pertencer a este set: " + values_string + "."
                    )
        else:
            if include_column_name:
                template_str = "$column valores podem ter qualquer valor, valores observados serão repostados"
            else:
                template_str = (
                    "Valores podem ter qualquer valor, valores observados serão repostados"
                )

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
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
