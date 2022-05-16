from airflow.plugins_manager import AirflowPlugin
import helpers
import operators
import etl_universidades_d

#Define the plugin class
class MyPlugins(AirflowPlugin):
    #Name your AirflowPlugin
    name = "data_transform"
    
    #List all plugins you want to use in dag operation
    helpers = [helpers.DataTransformer, helpers.transform_data]
    operators = [operators.SQLToLocalCsv]
    etl = [etl_universidades_d.DataProcessor]
