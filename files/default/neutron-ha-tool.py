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
from collections import OrderedDict
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

from neutronclient.common.exceptions import NeutronException
from neutronclient.neutron import client as nclient
import keystoneclient.v2_0.client as kclientv2
import keystoneclient.v3.client as kclientv3


LOG = logging.getLogger('neutron-ha-tool')
LOG_FORMAT = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
LOG_DATE = '%m-%d %H:%M'
DESCRIPTION = "neutron High Availability Tool"
TAKEOVER_DELAY = int(random.random() * 30 + 30)
OS_PASSWORD_FILE = '/etc/neutron/os_password'


IDENTITY_API_VERSIONS = {
    '2.0': kclientv2,
    '2': kclientv2,
    '3': kclientv3
}

ROUTER_CACHE_MAX_AGE_SECONDS = 5 * 60


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
                    help='Migrate routers away from a particular l3 agent')
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
    ap.add_argument('--target-agent', default=None,
                    help='Explicitly select a target agent either by specifying '
                         'an agent id or a host id.')
    ap.add_argument('--router-list-file', default=None,
                    help='Only routers specified in the list file will be '
                         'moved. The router list file should specify one '
                         'router id per line.')
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
    # ensure environment has necessary items to authenticate
    for key in ['OS_USERNAME', 'OS_AUTH_URL', 'OS_REGION_NAME']:
        if key not in os.environ.keys():
            raise SystemExit("Your environment is missing '%s'" % key)
    keys = ['OS_TENANT_NAME', 'OS_PROJECT_NAME']
    if not any(key in os.environ.keys() for key in keys):
        raise SystemExit("Your environment is missing "
                         "'OS_TENANT_NAME' or 'OS_PROJECT_NAME")

    ap = make_argparser()
    args = ap.parse_args()
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
        token=keystone.get_token(keystone.session)
    )

    # set json return type
    qclient.format = 'json'

    configure(args, qclient)
    Configuration.router_filter.load()

    if args.l3_agent_check:
        LOG.info("Performing L3 Agent Health Check")
        # We don't want the health check to retry - if it fails, we
        # need to take remedial action immediately
        migrations_required = l3_agent_check(qclient)
        return 2 if migrations_required > 0 else 0

    elif args.l3_agent_migrate:
        LOG.info("Performing L3 Agent Migration for Offline L3 Agents")
        errors = retry_with_backoff(l3_agent_migrate, args)(
            qclient, args.noop, args.now, args.wait_for_router,
            args.ssh_delete_namespace)

    elif args.l3_agent_evacuate:
        LOG.info("Performing L3 Agent Evacuation from host %s",
                 args.l3_agent_evacuate)
        errors = retry_with_backoff(l3_agent_evacuate, args)(
            qclient, args.l3_agent_evacuate, args.noop,
            args.wait_for_router, args.ssh_delete_namespace)

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

    # {u'binary': u'neutron-l3-agent',
    #  u'description': None,
    #  u'admin_state_up': True,
    #  u'heartbeat_timestamp': u'2013-07-02 22:20:23',
    #  u'alive': True,
    #  u'topic': u'l3_agent',
    #  u'host': u'o3r3.int.san3.attcompute.com',
    #  u'agent_type': u'L3 agent',
    #  u'created_at': u'2013-07-02 14:50:58',
    #  u'started_at': u'2013-07-02 18:00:55',
    #  u'id': u'6efe494a-616c-41ea-9c8f-2c592f4d46ff',
    #  u'configurations': {
    #      u'router_id': u'',
    #      u'gateway_external_network_id': u'',
    #      u'handle_internal_only_routers': True,
    #      u'use_namespaces': True,
    #      u'routers': 5,
    #      u'interfaces': 3,
    #      u'floating_ips': 9,
    #      u'interface_driver':
    #           u'neutron.agent.linux.interface.OVSInterfaceDriver',
    #      u'ex_gw_ports': 3}
    #  }

    l3_agent_dict = {}
    routers_on_l3_agent_dict = {}
    agents = list_agents(qclient, agent_type='L3 agent')
    num_agents = len(agents)
    if num_agents <= 1:
        LOG.info("No rebalancing required for 1 or fewer agents")
        return 0

    for l3_agent in agents:
        l3_agent_dict[l3_agent['id']] = l3_agent
        routers_on_l3_agent_dict[l3_agent['id']] = \
            list_routers_on_l3_agent(qclient, l3_agent['id'])

    ordered_l3_agent_dict = OrderedDict(
        sorted(routers_on_l3_agent_dict.items(), key=lambda t: len(t[0])))
    ordered_l3_agent_list = list(ordered_l3_agent_dict)
    num_agents = len(ordered_l3_agent_list)
    LOG.info("Agent list: %s",
             ordered_l3_agent_list[0:(num_agents - 1 / 2) + 1])
    i = 0
    migrations = 0
    errors = 0
    for agent in ordered_l3_agent_list[0:num_agents - 1 / 2]:
        low_agent_id = ordered_l3_agent_list[i]
        hgh_agent_id = ordered_l3_agent_list[-(i + 1)]

        # do nothing if we end up comparing the same router
        if low_agent_id == hgh_agent_id:
            continue

        LOG.info("Examining low_agent=%s, high_agent=%s",
                 low_agent_id, hgh_agent_id)

        low_agent_router_count = len(routers_on_l3_agent_dict[low_agent_id])
        hgh_agent_router_count = len(routers_on_l3_agent_dict[hgh_agent_id])

        LOG.info("Low Count=%d, High Count=%d",
                 low_agent_router_count, hgh_agent_router_count)

        for router_id in routers_on_l3_agent_dict[hgh_agent_id]:
            if low_agent_router_count >= hgh_agent_router_count:
                break

            if migrate_router_safely(qclient, noop, router_id,
                                     l3_agent_dict[hgh_agent_id],
                                     l3_agent_dict[low_agent_id],
                                     wait_for_router):
                low_agent_router_count += 1
                hgh_agent_router_count -= 1
                migrations += 1
            else:
                errors += 1

        i += 1

    return errors


def l3_agent_check(qclient):
    """
    Walk the l3 agents searching for agents that are offline.  Show routers
    that are offline and where we would migrate them to.

    :param qclient: A neutronclient
    :returns: total numbers of migrations required

    """

    migration_count = 0
    agent_list = list_agents(qclient)
    agent_dead_list = list_dead_agents(agent_list, 'L3 agent')
    agent_alive_list = list_alive_agents(agent_list, 'L3 agent')
    LOG.info("There are %d offline L3 agents and %d online L3 agents",
             len(agent_dead_list), len(agent_alive_list))

    if len(agent_dead_list) == 0:
        return 0

    for agent in agent_dead_list:
        LOG.info("Querying agent_id=%s for routers to migrate", agent['id'])
        router_id_list = list_routers_on_l3_agent(qclient, agent['id'])

        for router_id in router_id_list:
            try:
                target = random.choice(agent_alive_list)
            except IndexError:
                LOG.warn("There are no l3 agents alive we could "
                         "migrate routers onto.")
                target = {'id': None}

            migration_count += 1
            LOG.warn("Would like to migrate router=%s to agent=%s",
                     router_id, target['id'])

    return migration_count


def l3_agent_migrate(qclient, noop=False, now=False,
                     wait_for_router=True, ssh_delete_namespace=False):
    """
    Walk the l3 agents searching for agents that are offline.  For those that
    are offline, we will retrieve a list of routers on them and migrate them to
    an l3 agent that is online using the strategy specified by
    Configuration.agent_picker.

    :param qclient: A neutronclient
    :param noop: Optional noop flag
    :param now: Optional. If false (the default), we'll wait for a random
                amount of time (between 30 and 60 seconds) before migration,
                and if an agent comes online, migration is abandoned. If
                true, routers are migrated immediately.
    :returns: total number of errors encountered
    """

    agent_list = list_agents(qclient)
    agent_dead_list = list_dead_agents(agent_list, 'L3 agent')
    agent_alive_list = list_alive_agents(agent_list, 'L3 agent')
    LOG.info("There are %d offline L3 agents and %d online L3 agents",
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
            agent_dead_list_new = list_dead_agents(agent_list_new,
                                                   'L3 agent')
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
                                          noop, wait_for_router,
                                          ssh_delete_namespace)
        total_migrations += migrations
        total_errors += errors

    LOG.info("%d routers required migration from offline L3 agents",
             total_migrations)
    if total_errors > 0:
        LOG.error("%d errors encountered during migration", total_errors)

    return total_errors


def l3_agent_evacuate(qclient, agent_host, noop=False,
                      wait_for_router=True, ssh_delete_namespace=False):
    """
    Retreive a list of routers scheduled on the listed agent, and move that
    to another agent.

    :param qclient: A neutronclient
    :param noop: Optional noop flag
    :param agent_host: the hostname of the L3 agent to migrate routers from
    :returns: total number of errors encountered

    """

    agent_list = list_agents(qclient, agent_type='L3 agent')
    target_list = target_agent_list(agent_list, 'L3 agent', agent_host)

    if len(target_list) < 1:
        LOG.error("There are no l3 agents alive to migrate routers onto")
        return 0

    agent_to_evacuate = None
    for agent in agent_list:
        if agent.get('host', None) == agent_host:
            agent_to_evacuate = agent
            break

    if not agent_to_evacuate:
        LOG.error("Could not locate agent to evacuate; aborting!")
        return 1

    (migrations, errors) = \
        migrate_l3_routers_from_agent(qclient, agent_to_evacuate,
                                      target_list, noop, wait_for_router,
                                      ssh_delete_namespace)
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


def migrate_l3_routers_from_agent(qclient, agent, targets,
                                  noop, wait_for_router, delete_namespace):
    LOG.info("Querying agent_id=%s for routers to migrate away", agent['id'])
    router_id_list = list_routers_on_l3_agent(qclient, agent['id'])
    router_id_list = Configuration.router_filter.filter_routers(router_id_list)

    migrations = 0
    errors = 0
    agent_picker = Configuration.agent_picker
    agent_picker.set_agents(targets)

    for router_id in router_id_list:
        target = agent_picker.pick()
        if migrate_router_safely(qclient, noop, router_id, agent,
                                 target, wait_for_router, delete_namespace):
            migrations += 1
        else:
            errors += 1

    return (migrations, errors)


def migrate_router_safely(qclient, noop, router_id, agent, target,
                          wait_for_router=True, delete_namespace=False):
    if noop:
        LOG.info("Would try to migrate router=%s from agent=%s "
                 "to agent=%s", router_id, agent['id'], target['id'])
        return True

    try:
        migrate_router(qclient, router_id, agent, target,
                       wait_for_router, delete_namespace)
        return True
    except:
        LOG.exception("Failed to migrate router=%s from agent=%s "
                      "to agent=%s", router_id, agent['id'], target['id'])
        return False


def migrate_router(qclient, router_id, agent, target,
                   wait_for_router=True, delete_namespace=False):
    """
    Returns nothing, and raises exceptions on errors.

    :param qclient: A neutronclient
    :param router_id: The id of the router to migrate
    :param agent_id: The id of the l3 agent to migrate from
    :param target_id: The id of the l3 agent to migrate to
    """

    LOG.info("Migrating router=%s from agent=%s to agent=%s",
             router_id, agent['id'], target['id'])

    # N.B. The neutron API will return "success" even when there is a
    # subsequent failure during the add or remove process so we must check to
    # ensure the router has been added or removed

    # Remove the router from the original agent
    qclient.remove_router_from_l3_agent(agent['id'], router_id)
    LOG.debug("Removed router from agent=%s" % agent['id'])

    # ensure it is removed or log an error
    if router_id in list_routers_on_l3_agent(qclient, agent['id']):
        raise RuntimeError("Failed to remove router_id=%s from agent_id=%s" %
                           (router_id, agent['id']))

    # add the router id to a live agent
    router_body = {'router_id': router_id}
    qclient.add_router_to_l3_agent(target['id'], router_body)

    # ensure it is removed or log an error
    if router_id not in list_routers_on_l3_agent(qclient, target['id']):
        raise RuntimeError("Failed to add router_id=%s from agent_id=%s" %
                           (router_id, agent['id']))
    if wait_for_router:
        wait_router_migrated(qclient, router_id, target['host'])

    if delete_namespace:
        ssh_delete_router_namespace(agent['host'], router_id)


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


def ssh_delete_router_namespace(target_host, router_id):
    namespace = "qrouter-" + router_id
    command = "ip netns delete " + namespace

    with paramiko.SSHClient() as ssh_client:
        # Accept unknown host keys (reflects a "StrictHostKeyChecking no"
        # setting in the ssh_config)
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.load_system_host_keys()

        try:
            ssh_client.connect(target_host, timeout=10)
            stdin, stdout, stderr = ssh_client.exec_command(command,
                                                            timeout=10,
                                                            get_pty=True)
            out_lines = stdout.readlines()
            err_lines = stderr.readlines()
            rc = stdout.channel.recv_exit_status()
            ssh_client.close()

            if rc != 0:
                LOG.warn("Failed to delete namespace %s delete on %s",
                         namespace, target_host)
                LOG.warn("SSH stdout: %s", "".join(out_lines))
                LOG.warn("SSH stderr: %s", "".join(err_lines))
            else:
                LOG.info("namespace %s deleted on %s", namespace, target_host)

        except socket.timeout:
            LOG.warn("SSH timeout exceeded. "
                     "Failed to delete namespace %s on %s.",
                     namespace, target_host)


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
    return [r['id'] for r in resp['routers'] if not r.get('ha') == True]  # noqa


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


def list_alive_agents(agent_list, agent_type):
    """
    Return a list of agents that are alive from an API list of agents

    :param agent_list: API response for list_agents()

    """
    return [agent for agent in agent_list
            if agent['agent_type'] == agent_type and
            agent['alive'] is True and
            agent['admin_state_up'] is True]


def target_agent_list(agent_list, agent_type, exclude_agent_host):
    """
    Return a list of agents that are alive, excluding the one we want to
    migrate from

    :param agent_list: API response for list_agents()
    :param agent_type: used to filter the type of agent
    :param exclude_agent_host: hostname of agent to exclude from the list

    """
    agent_info = [
        i for i in agent_list
        if i.get('host', None) == exclude_agent_host
    ]
    if not agent_info:
        LOG.error("Cannot find agent %s information.", exclude_agent_host)
        return []
    agent_info = agent_info[0]
    agent_mode = agent_info['configurations']['agent_mode']

    return [agent for agent in agent_list
            if agent['agent_type'] == agent_type and
            agent['alive'] and
            agent['host'] != exclude_agent_host and
            agent['configurations']['agent_mode'] == agent_mode]


def list_dead_agents(agent_list, agent_type):
    """
    Return a list of agents that are dead from an API list of agents

    :param agent_list: API response for list_agents()

    """
    return [agent for agent in agent_list
            if agent['agent_type'] == agent_type and agent['alive'] is False]


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
        self.router_count_per_agent_id = dict()
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


class SingleAgentPicker(object):
    agent_keys = ('id', 'host')

    def __init__(self, agent_selection_value):
        self.agent_selection_value = agent_selection_value

    def set_agents(self, agents):
        self.agents = agents

    def find_agent(self, value):
        for agent in self.agents:
            agent_values = [agent.get(key) for key in self.agent_keys]
            if value in agent_values:
                return agent
        raise IndexError('Cannot find desired agent')

    def pick(self):
        if not self.agent_selection_value:
            raise ValueError('agent_selection_value is None')

        return self.find_agent(self.agent_selection_value)


class NullRouterFilter(object):
    def load(self):
        pass

    def filter_routers(self, router_id_list):
        return router_id_list


class ListFileBasedRouterFilter(object):
    def __init__(self, router_list_file):
        self.router_list_file = router_list_file
        self.router_list = []

    def load(self):
        self.router_list = []
        with open(self.router_list_file, 'r') as fh_router_list:
            for line in fh_router_list.read().split():
                router_id = line.strip()
                if router_id:
                    self.router_list.append(router_id)

    def filter_routers(self, router_id_list):
        return [
            router_id
            for router_id in router_id_list
            if router_id in self.router_list
        ]


class Configuration(object):
    """
    Registry for storing application's configuration
    """

    agent_picker = RandomAgentPicker()
    router_filter = NullRouterFilter()


def configure(args, qclient):
    if args.agent_selection_mode == 'random':
        Configuration.agent_picker = RandomAgentPicker()
    elif args.agent_selection_mode == 'least-busy':
        Configuration.agent_picker = LeastBusyAgentPicker(qclient)
    else:
        raise ValueError('Invalid agent_selection_mode')

    if args.target_agent is not None:
        Configuration.agent_picker = SingleAgentPicker(args.target_agent)

    if args.router_list_file:
        Configuration.router_filter = ListFileBasedRouterFilter(
            args.router_list_file
        )


if __name__ == '__main__':
    args = parse_args()
    setup_logging(args)

    try:
        ret = run(args)
        sys.exit(ret)
    except NeutronException as e:
        LOG.error(e)
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(1)
