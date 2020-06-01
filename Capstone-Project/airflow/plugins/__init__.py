from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class MyOwnPlugin(AirflowPlugin):
    name = "my_own_plugin"
    operators = [
        operators.GenerateCsvOperator
    ]