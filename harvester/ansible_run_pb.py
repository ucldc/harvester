#execute a playbook from within python
# for use from an rqworker
# fullpath to playbook
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
###    parser.add_option('-e', '--extra-vars', dest="extra_vars", default=None,
###        help="set additional key=value variables from the CLI")
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
    #if options.step:
    #    playbook_cb.step = options.step
    #if options.start_at:
    #    playbook_cb.start_at = options.start_at
    runner_cb = callbacks.PlaybookRunnerCallbacks(stats, verbose=utils.VERBOSITY)
    pb = ansible.playbook.PlayBook(
        playbook=playbook,
###        module_path=options.module_path,
        inventory=inventory,
###        forks=options.forks,
       remote_user=remote_user,
###        remote_pass=sshpass,
        callbacks=playbook_cb,
        runner_callbacks=runner_cb,
        stats=stats,
###        timeout=options.timeout,
###        transport=options.connection,
###        sudo=options.sudo,
###        sudo_user=options.sudo_user,
###        sudo_pass=sudopass,
###        extra_vars=extra_vars,
        private_key_file=private_key_file,
###        only_tags=only_tags,
###        check=options.check,
###        diff=options.diff
    )

    pb.run()

if __name__=='__main__':
    options, args = get_args(sys.argv)
    pb = args[1]
    main(pb, options.inventory, remote_user=options.remote_user, private_key_file=options.private_key_file)
