from jinja2 import Environment, FileSystemLoader
import yaml
import pathlib
import os

path_p = (pathlib.Path(__file__).parent.absolute()).parent
# file_dir = os.path.dirname(os.path.abspath(__file__)) # Current file directory
file_dir = f'{path_p}/templates'
env = Environment(loader=FileSystemLoader(file_dir))
template = env.get_template('template_dag.jinja2')


for filename in os.listdir(file_dir):
    if filename.endswith(".yaml"):
        with open(f'{file_dir}/{filename}', "r") as configfile: # open each file
            config = yaml.safe_load(configfile)
            with open(f"dags_dinamicos/ETL_{config['dag_id']}.py", "w") as f:
                f.write(template.render(config))
