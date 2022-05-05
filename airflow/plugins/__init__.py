from airflow.plugins_manager import AirflowPlugin
import helpers

#Define the plugin class
class MyPlugins(AirflowPlugin):
    #Name your AirflowPlugin
    name = "data_transform"
    
    #List all plugins you want to use in dag operation
    helpers = [helpers.DataTransformer]