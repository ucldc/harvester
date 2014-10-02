# Use the ansible runnner to run the specific playbook
import os
import ansible_run_pb


def main():
    dir_code = os.environ['DIR_CODE'] if 'DIR_CODE' in os.environ \
        else \
        os.path.join(os.path.join(os.environ.get('HOME', '~'),
                                  'code/harvester'))
    dir_code = os.path.abspath(dir_code)
    dir_pb = os.path.join(dir_code, 'harvester')
    playbook = os.path.join(dir_pb, 'grab-solr-index-playbook.yml')
    inventory = os.path.join(dir_pb, 'host_inventory')
    ansible_run_pb.main(playbook, inventory)

if __name__ == '__main__':
    main()
