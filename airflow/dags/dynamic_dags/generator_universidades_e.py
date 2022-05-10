import os

import yaml
from jinja2 import Environment, FileSystemLoader

abspath_file = os.path.abspath(__file__)
file_dir = os.path.dirname(abspath_file)
parent_dirname = os.path.split(file_dir)[0]

env = Environment(loader=FileSystemLoader(file_dir))
template = env.get_template('template_universidades_e.jinja2')

for filename in os.listdir(file_dir):

    if filename.endswith(".yaml"):

        with open(f"{file_dir}/{filename}", "r") as configfile:
            config = yaml.safe_load(configfile)

            with open(f"{parent_dirname}/{config['dag_id']}.py", "w") as f:
                f.write(template.render(config))
