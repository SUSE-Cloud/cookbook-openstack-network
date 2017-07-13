#! /usr/bin/env python

# Copyright 2013 AT&T Services, Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

# Exit codes:
#   0 - successful run, or no action required
#   1 - unexpected error
#   2 - router migration required (with --l3-agent-check)


import argparse
import collections
import contextlib
import datetime
import logging
from logging.handlers import SysLogHandler
import os
import random
import retrying
import sys
import time
import paramiko
import socket
import signal

from neutronclient.common.exceptions import NeutronException
from neutronclient.neutron import client as nclient
import keystoneclient.v2_0.client as kclientv2
import keystoneclient.v3.client as kclientv3


LOG = logging.getLogger('neutron-ha-tool')
LOG_FORMAT = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
LOG_DATE = '%m-%d %H:%M'
DESCRIPTION = "neutron High Availability Tool"
TAKEOVER_DELAY = random.randrange(30, 60)
OS_PASSWORD_FILE = '/etc/neutron/os_password'


IDENTITY_API_VERSIONS = {
    '2.0': kclientv2,
    '2': kclientv2,
    '3': kclientv3
}

ROUTER_CACHE_MAX_AGE_SECONDS = 5 * 60
TERM_SIGNAL_RECEIVED = False
SHOULD_NOT_TERMINATE = False


def make_argparser():
    ap = argparse.ArgumentParser(description=DESCRIPTION)
    ap.add_argument('-d', '--debug', action='store_true',
                    default=False, help='Show debugging output')
    ap.add_argument('-q', '--quiet', action='store_true', default=False,
                    help='Only show error and warning messages')
    ap.add_argument('-n', '--noop', action='store_true', default=False,
                    help='Do not do any modifying operations (dry-run)')
    ap.add_argument('--l3-agent-check', action='store_true', default=False,
                    help='Show routers associated with offline l3 agents')
    ap.add_argument('--l3-agent-migrate', action='store_true', default=False,
                    help='Migrate routers away from offline l3 agents')
    ap.add_argument('--l3-agent-evacuate', default=None, metavar='HOST',
                    help='Migrate routers away from l3 agent living on HOST')
    ap.add_argument('--l3-agent-rebalance', action='store_true', default=False,
                    help='Rebalance router count on all l3 agents')
    ap.add_argument('--replicate-dhcp', action='store_true', default=False,
                    help='Replicate DHCP configuration to all agents')
    ap.add_argument('--now', action='store_true', default=False,
                    help='Migrate Routers immediately without a delay.')
    ap.add_argument('-r', '--retry', action='store_true', default=False,
                    help='Retry neutronclient exceptions with exponential '
                         'backoff')
    ap.add_argument('--retry-max-interval', action='store', type=int,
                    default=10000, metavar='MS',
                    help='Maximum retry interval in milliseconds at which to '
                    'cap back-off')
    ap.add_argument('--insecure', action='store_true', default=False,
                    help='Explicitly allow neutron-ha-tool to perform '
                         '"insecure" SSL (https) requests. The server\'s '
                         'certificate will not be verified against any '
                         'certificate authorities. This option should be used '
                         'with caution.')
    ap.add_argument('--ssh-delete-namespace', action='store_true',
                    default=False,
                    help='After migrating a router, try to ssh to '
                         'the node the router was migrated from and '
                         'delete the router\'s network namespace. This '
                         'is mainly useful when evacuating routers from '
                         'a node where the l3-agent is not running for '
                         'some reason.')
    ap.add_argument('--agent-selection-mode', choices=['random', 'least-busy'],
                    default='least-busy',
                    help='Determines how target agent is selected for routers '
                         '"random" selects target agent randomly, '
                         '"least-busy" selects the least busy agent.')
    ap.add_argument('--router-list-file', default=None,
                    help='Only routers specified in the list file will be '
                         'moved. The router list file should specify one '
                         'router id per line. This only applies for '
                         'agent evacuation.')
    ap.add_argument('--exit-on-first-failure', action='store_true',
                    dest='fail_fast', default=False,
                    help='When evacuating an l3-agent exit with an error code '
                         'after the first failed router migration. The '
                         'standard behaviour is to continue and report errors '
                         'after trying to evacuate all routers.')
    target_agent_parser = ap.add_mutually_exclusive_group(required=False)
    target_agent_parser.add_argument(
        '--target-agent-id', default=None,
        help='Explicitly select a target agent by specifying an agent id.')
    target_agent_parser.add_argument(
        '--target-host', default=None,
        help='Explicitly select a target agent by specifying its host.')
    wait_parser = ap.add_mutually_exclusive_group(required=False)
    wait_parser.add_argument('--wait-for-router', action='store_true',
                             dest='wait_for_router')
    wait_parser.add_argument('--no-wait-for-router', action='store_false',
                             dest='wait_for_router',
                             help='When migrating routers, do not wait for '
                                  'its ports and floating IPs to be ACTIVE '
                                  'again on the target agent.')
    wait_parser.set_defaults(wait_for_router=True)
    return ap


def parse_args():
    ap = make_argparser()
    args = ap.parse_args()

    # ensure environment has necessary items to authenticate
    for key in ['OS_USERNAME', 'OS_AUTH_URL', 'OS_REGION_NAME']:
        if key not in os.environ.keys():
            raise SystemExit("Your environment is missing '%s'" % key)
    keys = ['OS_TENANT_NAME', 'OS_PROJECT_NAME']
    if not any(key in os.environ.keys() for key in keys):
        raise SystemExit("Your environment is missing "
                         "'OS_TENANT_NAME' or 'OS_PROJECT_NAME")
    modes = [
        args.l3_agent_check,
        args.l3_agent_migrate,
        args.l3_agent_evacuate,
        args.l3_agent_rebalance,
        args.replicate_dhcp,
    ]
    if sum(1 for x in modes if x) != 1:
        args_error(ap, "You must choose exactly one action")

    if args.retry and args.l3_agent_check:
        args_error(ap, "--l3-agent-check doesn't support --retry")

    return args


# Replacement for ArgumentParser.error() which is hardcoded to exit 2,
# clashing with our meaning of exit code 2.
def args_error(ap, message):
    ap.print_usage()
    ap.exit(3, '%s: error: %s\n' % (ap.prog, message))


def setup_logging(args):
    level = logging.INFO
    if args.quiet:
        level = logging.WARN
    if args.debug:
        level = logging.DEBUG
    logging.basicConfig(level=level, format=LOG_FORMAT, date_fmt=LOG_DATE)
    handler = SysLogHandler(address='/dev/log')
    syslog_formatter = logging.Formatter('%(name)s: %(levelname)s %(message)s')
    handler.setFormatter(syslog_formatter)
    LOG.addHandler(handler)


def retry_neutron_exceptions(exception):
    LOG.error(exception)
    return isinstance(exception, NeutronException)


def retry_on_errors(num_errors):
    return num_errors > 0


def retry_with_backoff(fn, args):
    if not args.retry:
        return fn

    return retrying.retry(
        wait_exponential_multiplier=250,
        wait_exponential_max=args.retry_max_interval,
        retry_on_exception=retry_neutron_exceptions,
        retry_on_result=retry_on_errors
    )(fn)


def run(args):
    try:
        ca = os.environ['OS_CACERT']
    except KeyError:
        ca = None

    # Allow environment to override config file.  This retains
    # backwards-compatibility and follows conventional precedence.
    if os.getenv('OS_PASSWORD'):
        os_password = os.environ['OS_PASSWORD']
    elif os.path.exists(OS_PASSWORD_FILE):
        with open(OS_PASSWORD_FILE) as f:
            os_password = f.readline()
    else:
        LOG.fatal("Couldn't retrieve password from $OS_PASSWORD environment "
                  "or from %s; aborting!" % OS_PASSWORD_FILE)
        sys.exit(1)

    auth_version = os.getenv('OS_AUTH_VERSION', None)
    if not auth_version:
        auth_version = os.getenv('OS_IDENTITY_API_VERSION', None)
        if not auth_version:
            auth_version = '2.0'

    kclient = IDENTITY_API_VERSIONS[auth_version]
    kclient_kwargs = dict()
    kclient_kwargs['username'] = os.environ['OS_USERNAME']
    kclient_kwargs['password'] = os_password
    kclient_kwargs['insecure'] = args.insecure
    kclient_kwargs['ca_cert'] = ca
    kclient_kwargs['auth_url'] = os.environ['OS_AUTH_URL']
    kclient_kwargs['region_name'] = os.environ['OS_REGION_NAME']

    tenant_name = os.getenv('OS_TENANT_NAME')
    if tenant_name:
        kclient_kwargs['tenant_name'] = tenant_name
    else:
        kclient_kwargs['project_name'] = os.environ['OS_PROJECT_NAME']

    endpoint_type = os.getenv('OS_ENDPOINT_TYPE', 'internalURL')

    # Instantiate Keystone client
    keystone = kclient.Client(**kclient_kwargs)

    # Instantiate Neutron client
    qclient = nclient.Client(
        '2.0',
        endpoint_url=keystone.service_catalog.url_for(
            service_type='network',
            endpoint_type=endpoint_type
        ),
        token=keystone.get_token(keystone.session),
        **kclient_kwargs
    )

    # set json return type
    qclient.format = 'json'

    if args.agent_selection_mode == 'random':
        agent_picker = RandomAgentPicker()
    elif args.agent_selection_mode == 'least-busy':
        agent_picker = LeastBusyAgentPicker(qclient)
    else:
        raise ValueError('Invalid agent_selection_mode')

    if args.target_agent_id:
        agent_picker = AgentIdBasedAgentPicker(args.target_agent_id)
    elif args.target_host:
        agent_picker = HostBasedAgentPicker(args.target_host)

    if args.l3_agent_check:
        LOG.info("Performing L3 Agent Health Check")
        # We don't want the health check to retry - if it fails, we
        # need to take remedial action immediately
        migrations_required = l3_agent_check(qclient)
        return 2 if migrations_required > 0 else 0

    elif args.l3_agent_migrate:
        LOG.info("Performing L3 Agent Migration for Offline L3 Agents")
        # Move all routers by not filtering them
        router_filter = NullRouterFilter()
        errors = retry_with_backoff(l3_agent_migrate, args)(
            qclient, agent_picker, router_filter, args.noop, args.now,
            args.wait_for_router, args.ssh_delete_namespace, args.fail_fast
        )

    elif args.l3_agent_evacuate:
        LOG.info("Performing L3 Agent Evacuation from host %s",
                 args.l3_agent_evacuate)
        if args.router_list_file:
            router_id_whitelist = load_router_ids(args.router_list_file)
            router_filter = WhitelistRouterFilter(router_id_whitelist)
        else:
            router_filter = NullRouterFilter()
        errors = retry_with_backoff(l3_agent_evacuate, args)(
            qclient, args.l3_agent_evacuate, agent_picker, router_filter,
            args.noop, args.wait_for_router, args.ssh_delete_namespace,
            args.fail_fast
        )

    elif args.l3_agent_rebalance:
        LOG.info("Rebalancing L3 Agent Router Count")
        errors = retry_with_backoff(l3_agent_rebalance, args)(
            qclient, args.noop, args.wait_for_router)

    elif args.replicate_dhcp:
        LOG.info("Performing DHCP Replication of Networks to Agents")
        errors = retry_with_backoff(replicate_dhcp, args)(qclient, args.noop)

    return 1 if errors > 0 else 0


def l3_agent_rebalance(qclient, noop=False, wait_for_router=True):
    """
    Rebalance l3 agent router count across agents.  The number of routers
    on each l3 agent will be as close as possible which should help
    distribute load as new l3 agents come online.

    :param qclient: A neutronclient
    :param noop: Optional noop flag
    """
    agent_list = live_agent_list(qclient)
    router_mover = RouterMover(qclient, noop, wait_for_router)

    while agent_list.calculate_maximal_router_count_difference() > 1:
        router_mover.move_one_router(
            agent_list.agent_with_most_number_of_routers(),
            agent_list.agent_with_least_number_of_routers()
        )

    return router_mover.errors


class Agent(object):
    def __init__(self, agent_dict, routers):
        self.agent_dict = agent_dict
        self.routers = routers

    def count_routers(self):
        return len(self.routers)

    def pop_router(self):
        return self.routers.pop()

    def add_router(self, router):
        self.routers.append(router)


def live_agent_list(qclient):
    """Create an AgentList populated with the live agents of the system

    :param qclient: neuton client.
    :return: an AgentList populated with the live agents listed by neutron.
    """
    agents = []
    all_agents = list_agents(qclient)
    for agent_dict in list_alive_l3_agents(all_agents):
        agent_id = agent_dict['id']
        routers = list_routers_on_l3_agent(qclient, agent_id)
        agents.append(Agent(agent_dict, routers))
    return AgentList(agents)


class RouterMover(object):
    def __init__(self, qclient, noop, wait_for_router):
        self.qclient = qclient
        self.noop = noop
        self.wait_for_router = wait_for_router
        self.errors = 0

    def move_one_router(self, source_agent, target_agent):
        """Move one router between agents

        Maintain the agent structures, and talk to neutron to do the actual
        router migration. Also count errors.

        :param source_agent: source agent
        :param target_agent: target agent
        :return: None
        """
        router_to_move = source_agent.pop_router()
        target_agent.add_router(router_to_move)
        migration_result = migrate_router_safely(
            self.qclient,
            self.noop,
            router_to_move,
            source_agent.agent_dict,
            target_agent.agent_dict,
            self.wait_for_router
        )
        if migration_result.failed:
            self.errors += 1


class AgentList(object):
    def __init__(self, agents):
        self.agents = agents

    @property
    def agents_sorted_by_router_count(self):
        return sorted(self.agents, key=lambda agent: agent.count_routers())

    def agent_with_least_number_of_routers(self):
        return self.agents_sorted_by_router_count[0]

    def agent_with_most_number_of_routers(self):
        return self.agents_sorted_by_router_count[-1]

    def calculate_maximal_router_count_difference(self):
        if not self.agents:
            return 0

        return (
            self.agent_with_most_number_of_routers().count_routers()
            - self.agent_with_least_number_of_routers().count_routers()
        )


def l3_agent_check(qclient):
    """
    Walk the l3 agents searching for agents that are offline.  Show routers
    that are offline.

    :param qclient: A neutronclient
    :returns: total numbers of migrations required

    """

    migration_count = 0
    agent_list = list_agents(qclient)
    agent_dead_list = list_dead_l3_agents(agent_list)
    agent_alive_list = list_alive_l3_agents(agent_list)
    LOG.info("There are %d offline L3 agents and %d online L3 agents"
             " (excluding those configured in dvr mode)",
             len(agent_dead_list), len(agent_alive_list))

    if len(agent_dead_list) == 0:
        return 0

    for agent in agent_dead_list:
        LOG.info("Querying agent_id=%s for routers to migrate", agent['id'])
        routers = list_routers_on_l3_agent(qclient, agent['id'])

        for router in routers:
            LOG.warn("Would like to migrate router=%s", router['id'])
            migration_count += 1

    return migration_count


def l3_agent_migrate(qclient, agent_picker, router_filter, noop=False,
                     now=False, wait_for_router=True,
                     ssh_delete_namespace=False, fail_fast=False):
    """
    Walk the l3 agents searching for agents that are offline.  For those that
    are offline, we will retrieve a list of routers on them and migrate them to
    an l3 agent that is online and has the same mode (snat_dvr, legacy).

    :param qclient: A neutronclient
    :param noop: Optional noop flag
    :param now: Optional. If false (the default), we'll wait for a random
                amount of time (between 30 and 60 seconds) before migration,
                and if an agent comes online, migration is abandoned. If
                true, routers are migrated immediately.
    :returns: total number of errors encountered
    """

    agent_list = list_agents(qclient)
    agent_dead_list = list_dead_l3_agents(agent_list)
    agent_alive_list = list_alive_l3_agents(agent_list)
    LOG.info("There are %d offline L3 agents and %d online L3 agents"
             " (excluding those configured in dvr mode)",
             len(agent_dead_list), len(agent_alive_list))

    if len(agent_dead_list) == 0:
        return 0

    if len(agent_alive_list) < 1:
        LOG.error("There are no l3 agents alive to migrate routers onto - "
                  "aborting!")
        return 1

    timeout = 0
    if not now:
        while timeout < TAKEOVER_DELAY:
            agent_list_new = list_agents(qclient)
            agent_dead_list_new = list_dead_l3_agents(agent_list_new)
            if len(agent_dead_list_new) < len(agent_dead_list):
                LOG.info("Skipping router failover since an agent came "
                         "online while ensuring agents offline for %d "
                         "seconds", TAKEOVER_DELAY)
                return 0

            LOG.info("Agent found offline for seconds=%d but waiting "
                     "seconds=%d before migration",
                     timeout, TAKEOVER_DELAY)
            timeout += 1
            time.sleep(1)

    total_errors = 0
    total_migrations = 0
    for agent in agent_dead_list:
        (migrations, errors) = \
            migrate_l3_routers_from_agent(qclient, agent, agent_alive_list,
                                          agent_picker, router_filter,
                                          noop, wait_for_router,
                                          ssh_delete_namespace, fail_fast,
                                          skip_migration_for_live_agents=True)
        total_migrations += migrations
        total_errors += errors

    LOG.info("%d routers required migration from offline L3 agents",
             total_migrations)
    if total_errors > 0:
        LOG.error("%d errors encountered during migration", total_errors)

    return total_errors


def l3_agent_evacuate(qclient, agent_host, agent_picker, router_filter,
                      noop=False, wait_for_router=True,
                      ssh_delete_namespace=False, fail_fast=False):
    """
    Retreive a list of routers scheduled on the listed agent, and move that
    to another agent.

    :param qclient: A neutronclient
    :param noop: Optional noop flag
    :param agent_host: the hostname of the L3 agent to migrate routers from
    :returns: total number of errors encountered

    """

    agent_list = list_agents(qclient, agent_type='L3 agent')

    agent_to_evacuate = None
    for agent in agent_list:
        if agent.get('host', None) == agent_host:
            agent_to_evacuate = agent
            break

    if not agent_to_evacuate:
        LOG.error("Could not locate agent to evacuate; aborting!")
        return 0

    if agent_to_evacuate['configurations']['agent_mode'] == 'dvr':
        LOG.error("Cannot evacuate an l3 agent configured in dvr-only mode;"
                  " aborting!")
        return 1

    target_list = list_alive_l3_agents_except(agent_list,
                                              exclude_agent=agent_to_evacuate)

    if len(target_list) < 1:
        LOG.error("There are no l3 agents alive to migrate routers onto")
        return 0

    (migrations, errors) = \
        migrate_l3_routers_from_agent(qclient, agent_to_evacuate,
                                      target_list, agent_picker, router_filter,
                                      noop, wait_for_router,
                                      ssh_delete_namespace, fail_fast)
    LOG.info("%d routers %s evacuated from L3 agent %s", migrations,
             "would have been" if noop else "were", agent_host)
    if errors > 0:
        LOG.error("%d errors encountered during evacuation", errors)

    return errors


def replicate_dhcp(qclient, noop=False):
    """
    Retrieve a network list and then probe each DHCP agent to ensure
    they have that network assigned.

    :param qclient: A neutronclient
    :param noop: Optional noop flag
    :returns: total number of errors encountered

    """

    added = 0
    errors = 0
    networks = list_networks(qclient)
    network_id_list = [n['id'] for n in networks]
    agents = list_agents(qclient, agent_type='DHCP agent')
    LOG.info("Replicating %d networks to %d DHCP agents", len(networks),
             len(agents))
    for dhcp_agent_id in [a['id'] for a in agents]:
        networks_on_agent = \
            qclient.list_networks_on_dhcp_agent(dhcp_agent_id)['networks']
        network_id_on_agent = [n['id'] for n in networks_on_agent]
        for network_id in network_id_list:
            if network_id in network_id_on_agent:
                continue
            try:
                dhcp_body = {'network_id': network_id}
                if not noop:
                    qclient.add_network_to_dhcp_agent(dhcp_agent_id,
                                                      dhcp_body)
                LOG.info("Added missing network=%s to dhcp agent=%s",
                         network_id, dhcp_agent_id)
                added += 1
            except:
                LOG.exception("Failed to add network_id=%s to"
                              "dhcp_agent=%s", network_id, dhcp_agent_id)
                errors += 1
                continue

    LOG.info("Added %d networks to DHCP agents", added)
    if errors > 0:
        LOG.error("%d errors encountered during DHCP replication", errors)

    return errors


def migrate_l3_routers_from_agent(qclient, agent, targets, agent_picker,
                                  router_filter, noop, wait_for_router,
                                  delete_namespace, fail_fast,
                                  skip_migration_for_live_agents=False):
    LOG.info("Querying agent_id=%s for routers to migrate away", agent['id'])
    routers = list_routers_on_l3_agent(qclient, agent['id'])
    router_id_list = router_filter.filter_routers(
        [router['id'] for router in routers]
    )
    routers = [router for router in routers if router['id'] in router_id_list]

    migrations = 0
    errors = 0
    agent_picker.set_agents(targets)
    for router in routers:
        target = agent_picker.pick()
        migration_result = migrate_router_safely(
            qclient, noop, router, agent, target, wait_for_router,
            delete_namespace, skip_migration_for_live_agents)
        if migration_result.succeeded:
            migrations += 1
        elif migration_result.failed:
            errors += 1
            if fail_fast:
                break
        elif migration_result.skipped:
            LOG.info(
                "Migration of router %s was skipped, no more routers will be "
                "moved away from agent %s", router['id'], agent['id']
            )
            return (migrations, errors)

    return (migrations, errors)


def migrate_router_safely(qclient, noop, router, agent, target,
                          wait_for_router=True, delete_namespace=False,
                          skip_migration_for_live_agents=False):
    if skip_migration_for_live_agents:
        all_agents = list_agents(qclient)
        live_agents = list_alive_l3_agents(all_agents)
        if agent['id'] in [_agent['id'] for _agent in live_agents]:
            LOG.info(
                "Agent %s is online, not migrating router %s",
                agent['id'], router['id']
            )
            return SKIPPED_MIGRATION
    if noop:
        LOG.info("Would try to migrate router=%s from agent=%s "
                 "to agent=%s", router['id'], agent['id'], target['id'])
        return SUCCESS_MIGRATION

    try:
        with term_disabled():
            migrate_router(qclient, router, agent, target,
                           wait_for_router, delete_namespace)
        return SUCCESS_MIGRATION
    except:
        LOG.exception("Failed to migrate router=%s from agent=%s "
                      "to agent=%s", router['id'], agent['id'], target['id'])
        return FAILED_MIGRATION


def migrate_router(qclient, router, agent, target,
                   wait_for_router=True, delete_namespace=False):
    """
    Returns nothing, and raises exceptions on errors.

    :param qclient: A neutronclient
    :param router: The router to migrate
    :param agent: The l3 agent to migrate/remove from
    :param target: The l3 agent to migrate to
    """

    LOG.info("Migrating router=%s from agent=%s to agent=%s",
             router['id'], agent['id'], target['id'])

    # N.B. The neutron API will return "success" even when there is a
    # subsequent failure during the add or remove process so we must check to
    # ensure the router has been added or removed

    # Remove the router from the original agent
    qclient.remove_router_from_l3_agent(agent['id'], router['id'])
    LOG.debug("Removed router from agent=%s" % agent['id'])

    # ensure it is removed
    router_ids = [
        r['id'] for r in list_routers_on_l3_agent(qclient, agent['id'])
    ]
    if router['id'] in router_ids:
        if router['distributed']:
            # Because of bsc#1016943, the router is not completely removed
            # from the agent. As a workaround, issuing a second remove
            # seems to bring the router/agent intro correct state
            qclient.remove_router_from_l3_agent(agent['id'], router['id'])
            LOG.debug("The router was not correctly deleted from agent=%s, "
                      "retrying." % agent['id'])

        if router in list_routers_on_l3_agent(qclient, agent['id']):
            raise RuntimeError("Failed to remove router_id=%s from agent_id="
                               "%s" % (router['id'], agent['id']))

    # add the router id to a live agent
    router_body = {'router_id': router['id']}
    qclient.add_router_to_l3_agent(target['id'], router_body)
    LOG.debug("Added router to agent=%s" % target['id'])

    # ensure it is added or log an error
    router_ids = [
        r['id'] for r in list_routers_on_l3_agent(qclient, target['id'])
    ]
    if router['id'] not in router_ids:
        raise RuntimeError("Failed to add router_id=%s from agent_id=%s" %
                           (router['id'], agent['id']))
    if wait_for_router:
        wait_router_migrated(qclient, router['id'], target['host'])

    if delete_namespace:
        nscleanup = RemoteRouterNsCleanup(agent['host'])
        nscleanup.delete_router_namespace(router['id'])


def wait_router_migrated(qclient, router_id, target_host, maxtries=60):
    """
    Returns nothing. Waits for all non-distributed ports and floating IPs
    of a router for being in the ACTIVE again after a migration.

    :param qclient: A neutron client
    :param router_id: The id of the router to check
    :param target_host: The host on which the ports should be ACTIVE
                        (reflected by the binding:host_id attribute of
                        a port)
    :param maxtries: Maximum number of times the ports' status is polled,
                     an expection is raised there are still ports with a
                     non ACTIVE status after that.
    """

    LOG.info("Wait for the ports and floating IPs of router_id=%s "
             "to be ACTIVE", router_id)
    remaining_ports = ["dummy"]
    remaining_fips = ["dummy"]
    remaining_iterations = maxtries
    while remaining_iterations:
        if remaining_ports:
            router_port_list = qclient.list_ports(device_id=router_id,
                                                  fields=['id',
                                                          'status',
                                                          'binding:host_id',
                                                          'binding:vif_type'])
            remaining_ports = [
                port['id'] for port in router_port_list['ports']
                if (port['binding:vif_type'] != 'distributed' and
                    not (port['status'] == 'ACTIVE' and
                         port['binding:host_id'] == target_host))
            ]
            LOG.debug("Ports not ACTIVE on router_id=%s: [%s]",
                      router_id, ", ".join(remaining_ports))
            if not remaining_ports:
                # avoid an unneeded sleep when all ports back and before
                # starting to check the floating IPs
                continue
        elif remaining_fips:
            floating_ips = qclient.list_floatingips(router_id=router_id)
            remaining_fips = [fip['id'] for fip in floating_ips['floatingips']
                              if fip['status'] != 'ACTIVE']
            LOG.debug("Floating IPs not active: [%s]",
                      ", ".join(remaining_fips))
            if not remaining_fips:
                break

        remaining_iterations -= 1
        if remaining_iterations:
            time.sleep(1)

    if remaining_ports:
        raise RuntimeError("Some ports are not ACTIVE on router_id=%s: [%s]" %
                           (router_id, ", ".join(remaining_ports)))
    elif remaining_fips:
        raise RuntimeError("Some floating ips are not ACTIVE "
                           "on router_id=%s: [%s]" %
                           (router_id, ", ".join(remaining_fips)))


def list_networks(qclient):
    """
    Return a list of network objects

    :param qclient: A neutronclient
    """

    resp = qclient.list_networks()
    LOG.debug("list_networks: %s", resp)
    return resp['networks']


def list_dhcp_agent_networks(qclient, agent_id):
    """
    Return a list of network ids assigned to a particular DHCP agent

    :param qclient: A neutronclient
    :param agent_id: A DHCP agent id
    """

    resp = qclient.list_networks_on_dhcp_agent(agent_id)
    LOG.debug("list_networks_on_dhcp_agent: %s", resp)
    return [s['id'] for s in resp['networks']]


def list_routers(qclient):
    """
    Return a list of router objects

    :param qclient: A neutronclient

    # {'routers': [
    #    {u'status': u'ACTIVE',
    #     u'external_gateway_info':
    #        {u'network_id': u'b970297c-d80e-4527-86d7-e49d2da9fdef'},
    #     u'name': u'router1',
    #     u'admin_state_up': True,
    #     u'tenant_id': u'5603b97ee7f047ea999e25492c7fcb23',
    #     u'routes': [],
    #     u'id': u'0a122e5c-1623-412e-8c53-a1e21d1daff8'}
    # ]}

    """

    resp = qclient.list_routers()
    LOG.debug("list_routers: %s", resp)
    # Filter routers to not include HA routers
    return [i for i in resp['routers'] if not i.get('ha') == True]  # noqa


def list_routers_on_l3_agent(qclient, agent_id):
    """
    Return a list of router ids on an agent

    :param qclient: A neutronclient
    """

    resp = qclient.list_routers_on_l3_agent(agent_id)
    LOG.debug("list_routers_on_l3_agent: %s", resp)
    return [r for r in resp['routers'] if not r.get('ha') == True]  # noqa


def list_agents(qclient, agent_type=None):
    """Return a list of agent objects

    :param qclient: A neutronclient


    # {u'agents': [

    #   {u'binary': u'neutron-openvswitch-agent',
    #    u'description': None,
    #    u'admin_state_up': True,
    #    u'heartbeat_timestamp': u'2013-07-02 22:20:25',
    #    u'alive': True,
    #    u'topic': u'N/A',
    #    u'host': u'o3r3.int.san3.attcompute.com',
    #    u'agent_type': u'Open vSwitch agent',
    #    u'created_at': u'2013-07-02 14:50:57',
    #    u'started_at': u'2013-07-02 14:50:57',
    #    u'id': u'3a577f1d-d86e-4f1a-a395-8d4c8e4df1e2',
    #    u'configurations': {u'devices': 10}},

    #   {u'binary': u'neutron-dhcp-agent',
    #    u'description': None,
    #    u'admin_state_up': True,
    #    u'heartbeat_timestamp': u'2013-07-02 22:20:23',
    #    u'alive': True,
    #    u'topic': u'dhcp_agent',
    #    u'host': u'o5r4.int.san3.attcompute.com',
    #    u'agent_type': u'DHCP agent',
    #    u'created_at': u'2013-06-26 16:21:02',
    #    u'started_at': u'2013-06-28 13:32:52',
    #    u'id': u'3e8be28e-05a0-472b-9288-a59f8d8d2271',
    #    u'configurations': {
    #         u'subnets': 4,
    #         u'use_namespaces': True,
    #         u'dhcp_driver': u'neutron.agent.linux.dhcp.Dnsmasq',
    #         u'networks': 4,
    #         u'dhcp_lease_time': 120,
    #         u'ports': 38}},


    #   {u'binary': u'neutron-l3-agent',
    #    u'description': None,
    #    u'admin_state_up': True,
    #    u'heartbeat_timestamp': u'2013-07-02 22:20:23',
    #    u'alive': True,
    #    u'topic': u'l3_agent',
    #    u'host': u'o3r3.int.san3.attcompute.com',
    #    u'agent_type': u'L3 agent',
    #    u'created_at': u'2013-07-02 14:50:58',
    #    u'started_at': u'2013-07-02 18:00:55',
    #    u'id': u'6efe494a-616c-41ea-9c8f-2c592f4d46ff',
    #    u'configurations': {
    #         u'router_id': u'',
    #         u'gateway_external_network_id': u'',
    #         u'handle_internal_only_routers': True,
    #         u'use_namespaces': True,
    #         u'routers': 5,
    #         u'interfaces': 3,
    #         u'floating_ips': 9,
    #         u'interface_driver':
    #             u'neutron.agent.linux.interface.OVSInterfaceDriver',
    #         u'ex_gw_ports': 3}},

    """

    resp = qclient.list_agents()
    LOG.debug("list_agents: %s", resp)
    if agent_type:
        return [agent for agent in resp['agents']
                if agent['agent_type'] == agent_type]
    return resp['agents']


def list_alive_l3_agents(agent_list):
    """
    Return a list of l3 agents that are alive from an API list of agents.
    The l3 agents configured in a dvr mode are filtered out because
    routers cannot be migrated to them.

    :param agent_list: API response for list_agents()

    """
    return [agent for agent in agent_list
            if agent['agent_type'] == 'L3 agent' and
            agent['alive'] is True and
            agent['admin_state_up'] is True and
            agent['configurations']['agent_mode'] != 'dvr']


def list_alive_l3_agents_except(agent_list, exclude_agent):
    """
    Return a list of l3 agents that are alive, excluding the one we want to
    migrate from.
    The l3 agents configured in a dvr mode are filtered out because
    routers cannot be migrated to them. Implicitly, this also means that
    the returned l3 agents are of the same mode as the excluded agent.
    

    :param agent_list: API response for list_agents()
    :param exclude_agent: agent to exclude from the list

    """
    return [agent for agent in list_alive_l3_agents(agent_list)
            if agent['id'] != exclude_agent['id']]

def list_dead_l3_agents(agent_list):
    """
    Return a list of l3 agents that are dead from an API list of agents.
    The l3 agents configured in a dvr mode are filtered out because
    routers cannot be migrated away from them.

    :param agent_list: API response for list_agents()

    """
    return [agent for agent in agent_list
            if agent['agent_type'] == 'L3 agent' and
            agent['alive'] is False and
            agent['configurations']['agent_mode'] != 'dvr']


class RandomAgentPicker(object):
    def __init__(self):
        self.agents = []

    def set_agents(self, agents):
        self.agents = agents

    def pick(self):
        return random.choice(self.agents)


class LeastBusyAgentPicker(object):
    def __init__(self, qclient):
        self.cache_created_at = None
        self.qclient = qclient
        self.agents_by_id = {}
        self.router_count_per_agent_id = {}

    def set_agents(self, agents):
        self.agents_by_id = {agent['id']: agent for agent in agents}
        self.refresh_router_count_per_agent_id()

    def refresh_router_count_per_agent_id(self):
        LOG.info("Refreshing router count per agent cache")
        self.router_count_per_agent_id = {}
        for agent_id in self.agents_by_id:
            self.router_count_per_agent_id[agent_id] = len(
                list_routers_on_l3_agent(self.qclient, agent_id)
            )
        self.cache_created_at = datetime.datetime.now()

    def cache_expired(self):
        cache_life = datetime.datetime.now() - self.cache_created_at
        return cache_life.total_seconds() > ROUTER_CACHE_MAX_AGE_SECONDS

    def pick(self):
        if self.cache_expired():
            self.refresh_router_count_per_agent_id()

        agent_id_number_of_routers = sorted(
            self.router_count_per_agent_id.items(),
            key=lambda x: (x[1], x[0])
        )
        agent_id = agent_id_number_of_routers[0][0]
        self.router_count_per_agent_id[agent_id] += 1
        return self.agents_by_id[agent_id]


class NullRouterFilter(object):
    def filter_routers(self, router_id_list):
        return router_id_list


class WhitelistRouterFilter(object):
    def __init__(self, router_id_whitelist):
        self.router_id_whitelist = set(router_id_whitelist)

    def filter_routers(self, router_id_list):
        return list(self.router_id_whitelist & set(router_id_list))


def load_router_ids(path):
    router_ids = []
    with open(path, 'r') as router_list_file:
        for line in router_list_file.read().split():
            router_ids.append(line.strip())
    return router_ids


class AgentIdBasedAgentPicker(object):
    def __init__(self, agent_id):
        self.agents = []
        self.agent_id = agent_id

    def set_agents(self, agents):
        self.agents = agents

    def pick(self):
        for agent in self.agents:
            if agent.get('id') == self.agent_id:
                return agent
        raise IndexError(
            'Cannot find agent with agent id: {}'.format(self.agent_id)
        )


class HostBasedAgentPicker(object):
    def __init__(self, host):
        self.agents = []
        self.host = host

    def set_agents(self, agents):
        self.agents = agents

    def pick(self):
        for agent in self.agents:
            if agent.get('host') == self.host:
                return agent
        raise IndexError(
            'Cannot find agent with host: {}'.format(self.host)
        )


class RemoteNodeCleanup(object):
    def __init__(self, host, timeout=10):
        self.target_host = host
        self.timeout = timeout
        self.netns_del = "ip netns delete "
        self.netns_pids = "ip netns pids "
        self.netns_list = "ip netns list"

    def _ssh_connect(self):
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(
            paramiko.AutoAddPolicy())
        self.ssh_client.load_system_host_keys()
        self.ssh_client.connect(self.target_host, timeout=self.timeout)

    def _simple_ssh_command(self, command):
        # Note, that when get_pty is True, paramiko will never return anything
        # on the stderr channel, that's why we ignore it here. (stderr output
        # will endup in the stdout channel)
        _, stdout, _ = self.ssh_client.exec_command(command,
                                                    timeout=self.timeout,
                                                    get_pty=True)
        out_lines = stdout.readlines()
        rc = stdout.channel.recv_exit_status()
        return [rc, [line.strip() for line in out_lines]]

    def _namespace_exists(self, namespace):
        rc, out_lines = self._simple_ssh_command(self.netns_list)
        return namespace in out_lines

    def _get_namespace_pids(self, namespace):
        rc, out_lines = self._simple_ssh_command(self.netns_pids + namespace)
        if rc:
            if out_lines and "No such file or directory" in out_lines[0]:
                # Assume the namespace was delete meanwhile
                return []
            else:
                raise RuntimeError("Failed to get pids for namespace %s",
                                   namespace)
        else:
            return out_lines

    def _kill_pids_in_namespace(self, namespace):
        LOG.debug("Trying to terminate all processes namespace "
                  "%s on host %s", namespace, self.target_host)
        remaining = 3
        pids = self._get_namespace_pids(namespace)
        while pids:
            LOG.debug("Processes still running: [%s]", ", ".join(pids))
            for pid in pids:
                killcmd = "kill "
                # use more force (kill -9) on the last try
                if remaining == 1:
                    LOG.debug("Last try. Using SIGKILL now")
                    killcmd = "kill -9 "

                rc, out_lines = self._simple_ssh_command(killcmd + pid)
                if rc:
                    if out_lines and "No such process" in out_lines[0]:
                        # Assume the process was stopped meanwhile
                        return None
                    else:
                        raise RuntimeError("Failed to kill %s on host %s",
                                           pid, self.target_host)

            remaining -= 1
            if remaining:
                pids = self._get_namespace_pids(namespace)
                if pids:
                    LOG.debug("Some processes are still running in namespace "
                              "%s on host %s. Retrying.", namespace,
                              self.target_host)
                    time.sleep(1)
            else:
                break

    def delete_remote_namespace(self, namespace):
        LOG.debug("Deleting namespace %s on host %s.", namespace,
                  self.target_host)
        self._ssh_connect()
        with self.ssh_client:
            try:
                if self._namespace_exists(namespace):
                    self._kill_pids_in_namespace(namespace)
                    self._simple_ssh_command(self.netns_del + namespace)
            except socket.timeout:
                LOG.warn("SSH timeout exceeded. Failed to delete namespace "
                         "%s on %s", namespace, self.target_host)


class RemoteRouterNsCleanup(RemoteNodeCleanup):

    def delete_router_namespace(self, router_id):
        namespace = "qrouter-" + router_id
        self.delete_remote_namespace(namespace)


def term_signal_handler(signum, frame):
    global TERM_SIGNAL_RECEIVED
    TERM_SIGNAL_RECEIVED = True
    if SHOULD_NOT_TERMINATE:
        return
    sys.exit(0)


def register_term_signal_handler():
    signal.signal(signal.SIGTERM, term_signal_handler)


@contextlib.contextmanager
def term_disabled():
    global SHOULD_NOT_TERMINATE
    SHOULD_NOT_TERMINATE = True
    yield None
    SHOULD_NOT_TERMINATE = False
    if TERM_SIGNAL_RECEIVED:
        sys.exit(0)


MigrationResult = collections.namedtuple(
    'MigrationResult', 'succeeded failed skipped'
)


def make_migration_result(succeeded=False, failed=False, skipped=False):
    return MigrationResult(succeeded=succeeded, failed=failed, skipped=skipped)


SUCCESS_MIGRATION = make_migration_result(succeeded=True)
FAILED_MIGRATION = make_migration_result(failed=True)
SKIPPED_MIGRATION = make_migration_result(skipped=True)


if __name__ == '__main__':
    args = parse_args()
    setup_logging(args)
    register_term_signal_handler()

    try:
        ret = run(args)
        sys.exit(ret)
    except NeutronException as e:
        LOG.error(e)
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(1)
