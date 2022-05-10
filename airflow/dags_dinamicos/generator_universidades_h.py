from jinja2 import Environment, FileSystemLoader
import yaml
import os


file_dir = os.path.dirname(os.path.abspath(__file__))
templ_folder = 'templates'
env = Environment(loader=FileSystemLoader(f'{file_dir}'))

template = env.get_template(f'{templ_folder}/universidades_h_template.jinja2')

for filename in os.listdir(f'{file_dir}/{templ_folder}'):
    if filename.endswith('.yaml'):
        with open(f"{file_dir}/{templ_folder}/{filename}") as f:
            config = yaml.safe_load(f)
            with open(f"dags_dinamicos/{config['dag_id']}.py", 'w') as f2:
                f2.write(template.render(config))