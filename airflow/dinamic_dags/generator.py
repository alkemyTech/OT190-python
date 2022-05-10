from jinja2 import Environment, FileSystemLoader
import yaml
import pathlib
import os

path_p = (pathlib.Path(__file__).parent.absolute()).parent
file_dir = f'{path_p}/templates'
env = Environment(loader=FileSystemLoader(file_dir))
template = env.get_template('template_universidades.jinja2')


for filename in os.listdir(file_dir):
    if filename.endswith(".yaml"):
        with open(f'{file_dir}/{filename}', "r") as configfile:
            config = yaml.safe_load(configfile)
            with open(f"dinamic_dags/ETL_{config['dag_id']}.py", "w") as f:
                f.write(template.render(config))