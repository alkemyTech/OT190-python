from __future__ import absolute_import, division, print_function

import helpers
import operators

from airflow.plugins_manager import AirflowPlugin


class MyPlugins(AirflowPlugin):
    name = "my_plugin"

    operators = [operators.SQLToLocalCsv]
    helpers = [helpers.transform_data]
