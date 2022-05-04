'''
Genera los DAG dinámicamente de:
- Universidad de Palermo
- Universidad Nacional de Jujuy
A partir del archivo DAG_D_Universidades_C.yml
'''
from airflow import DAG
import dagfactory
import pathlib

# Busqueda del path donde se está ejecutando el archivo, subimos un nivel para
# situarnos en la carpeta airflow
path_p = (pathlib.Path(__file__).parent.absolute()).parent

config_file = f"{path_p}/dags_dinamicos/DAG_D_Universidades_C.yml"
dag_factory = dagfactory.DagFactory(config_file)

dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())