# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------
from typing import Optional, Union, Dict
from azure.ai.evaluation._common._experimental import experimental


@experimental
class Mean:
    def __init__(self):
        pass

@experimental
class Sum:
    def __init__(self):
        pass

@experimental
class DefectRate:
    def __init__(self, threshold: Optional[float]= 1.0):
        self.threshold = threshold


class AggregationMixin:
    def __init__(self):
        self._aggregator = Mean()

    @property
    def aggregator(self) -> Union[Mean, Sum, DefectRate, str]:
        """
        """
        return self._aggregator
    
    @aggregator.setter
    def aggregator(self, value: Union[Mean, Sum, DefectRate, str]) -> None:
        """
        """
        if isinstance(value, (Mean, Sum, DefectRate)):
            self._aggregator = value
        elif value.lower() == "mean":
            self._aggregator = Mean
        elif value.lower() == "sum":
            self._aggregator = Sum
        else:
            raise ValueError("Aggregator must be one of 'mean', 'sum', Mean(), Sum(), or DefectRate().")


    



