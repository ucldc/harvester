# execute a playbook from within python
# for use from an rqworker
# fullpath to playbook This is copied & modified from the
# ansible-playbook standalone script
import sys
import os
from ansible import errors
from ansible import utils
import ansible.playbook
import ansible.constants as C
from ansible import callbacks


def get_args(args):    # create parser for CLI options
    usage = "%prog playbook.yml"
    parser = utils.base_parser(
        constants=C,
        usage=usage,
        connect_opts=True,
        runas_opts=True,
        subset_opts=True,
        check_opts=True,
        diff_opts=True
    )
    return parser.parse_args(args)


def main(playbook, inventory, remote_user=None, private_key_file=None):
    if not os.path.exists(playbook):
        raise errors.AnsibleError("the playbook: %s could not be found" % playbook)
    if not os.path.isfile(playbook):
        raise errors.AnsibleError("the playbook: %s does not appear to be a file" % playbook)

    if not os.path.exists(inventory):
        raise errors.AnsibleError("the inventory: %s could not be found" % inventory)
    if not os.path.isfile(inventory):
        raise errors.AnsibleError("the inventory: %s does not appear to be a file" % inventory)

    inventory = ansible.inventory.Inventory(inventory)
    if len(inventory.list_hosts()) == 0:
        raise errors.AnsibleError("provided hosts list is empty")

    # let inventory know which playbooks are using so it can know the basedirs
    inventory.set_playbook_basedir(os.path.dirname(playbook))

    stats = callbacks.AggregateStats()
    playbook_cb = callbacks.PlaybookCallbacks(verbose=utils.VERBOSITY)
    runner_cb = callbacks.PlaybookRunnerCallbacks(stats, verbose=utils.VERBOSITY)
    pb = ansible.playbook.PlayBook(
        playbook=playbook,
        inventory=inventory,
        callbacks=playbook_cb,
        runner_callbacks=runner_cb,
        stats=stats,
    )

    if remote_user:
        pb.remote_user = remote_user
    if private_key_file:
        pb.private_key_file = private_key_file
    pb.run()

if __name__ == '__main__':
    options, args = get_args(sys.argv)
    pb = args[1]
    main(pb, options.inventory, remote_user=options.remote_user, private_key_file=options.private_key_file)
