from jinja2 import Environment
from jinja2 import FileSystemLoader
import yaml
import os

path_p= os.path.dirname(os.path.abspath(__file__) )
file_dir= f'{path_p}/templates'
env = Environment(loader= FileSystemLoader(file_dir) )
template = env.get_template('template_universidades_b.jinja2')


#print (file_dir)
#print (env)
#print (template)


#print ('inicio')
for filename in os.listdir(file_dir):
     if filename.endswith(".yaml"):
          with open(f'{file_dir}/{filename}','r') as configfile:
              config = yaml.safe_load(configfile)
              #print (config,f"{file_dir}/{config['dag_id']}.py")
              with open (f"{path_p}/ETL_{config['dag_id']}.py", 'w') as f:
                  f.write(template.render(config))

#print ('fin')
