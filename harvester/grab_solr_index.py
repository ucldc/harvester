#Use the ansible runnner to run the specific playbook
import os
import  ansible_run_pb

def main():
	code_dir = os.path.abspath(os.path.join(os.environ.get('HOME', '~'), 'code/harvester/harvester/'))
	playbook = os.path.join(code_dir, 'grab-solr-index-playbook.yml')
	inventory = os.path.join(code_dir, 'host_inventory')
	print("======================PLAYBOOK:{0} INV:{1}".format(playbook, inventory))
	main(playbook, inventory)
