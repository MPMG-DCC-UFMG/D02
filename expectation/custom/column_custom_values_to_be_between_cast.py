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

# Adaptei o código da métrica original (ColumnValuesBetween) para inserir o cast
# [27/10/2021] Por enquanto, só alterei a função _pandas()
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