from airflow.plugins_manager import AirflowPlugin
import helpers

#Define the plugin class
class MyPlugins(AirflowPlugin):
    #Name your AirflowPlugin
    name = "transform_datas"

    helpers = [helpers.transform_datas]

