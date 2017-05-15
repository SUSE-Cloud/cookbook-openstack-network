#!/usr/bin/python
#
# Copyright 2017, SUSE LINUX GmbH
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import importlib
import socket
import sqlalchemy
import sys
from oslo_utils import timeutils
from oslo_config import cfg
from oslo_log import log as logging

LOG = logging.getLogger(__name__)

# Try to fall back loading the neutron-ha-tool from an absolute path.
# As on the installed system we're installing neutron-ha-tool without
# the ".py" extension, which causes importlib.import_module to fail.
try:
    hatool = importlib.import_module("neutron-ha-tool")
except ImportError:
    import imp
    import os
    dirname = os.path.dirname(os.path.abspath(__file__))
    hatool = imp.load_source(
        '', os.path.join(dirname, '/usr/bin/neutron-ha-tool'))


class EvacuateLbaasV2Agent(object):

    def __init__(self):
        _default_opts = [
            cfg.StrOpt('host',
                       required=True,
                       help='name of the host to evacuate'),
            cfg.IntOpt('agent_down_time', default=75),
            cfg.BoolOpt('use_crm',
                        default=False,
                        help='When restarting an agent, assume a pacemaker '
                        'setup and use crm to switch the node into '
                        'maintenance mode before restarting the agent.'),
            cfg.BoolOpt('source_agent_restart',
                        default=True,
                        help='Restart the neutron-lbaasv2-agent '
                        'on the node that is being evacuated.'),
            cfg.BoolOpt('delete_namespaces',
                        default=False,
                        help='After evacuating an agent, ssh to it, '
                        'delete all related loadbalancer namespaces and '
                        'terminate all processes still running in those '
                        'namespaces.')
        ]

        _db_opts = [
            cfg.StrOpt('connection',
                       deprecated_name='sql_connection',
                       default='',
                       secret=True,
                       help='URL to database'),
        ]

        cfg.CONF.register_cli_opts(_db_opts, 'database')
        cfg.CONF.register_cli_opts(_default_opts)
        logging.register_options(cfg.CONF)
        logging.set_defaults()

        self.host_to_evacuate = ""

    # Find all possible destination agents for our migration. Ignore
    # agent that have set admin_state_down or did not send  their
    # heartbeat recently
    def available_destination_agents(self):
        ret = self.connection.execute(
            "SELECT id,host,heartbeat_timestamp "
            "FROM agents WHERE "
            "agents.host != %s "
            "AND agents.binary='neutron-lbaasv2-agent' "
            "AND agents.admin_state_up=true;",
            self.host_to_evacuate)

        return [{'id': id, 'host': host}
                for id, host, timestamp in ret.fetchall()
                if not timeutils.is_older_than(timestamp,
                                               cfg.CONF.agent_down_time)]

    def loadbalancers_on_agent(self, agent_hostname):
        ret = self.connection.execute(
            "SELECT loadbalancer_id "
            "FROM lbaas_loadbalanceragentbindings,agents "
            "WHERE agents.host = %s "
            "AND agents.binary='neutron-lbaasv2-agent' "
            "AND agents.id=lbaas_loadbalanceragentbindings.agent_id;",
            agent_hostname)
        return [id[0] for id in ret.fetchall()]

    def reassign_loadbalancers(self, load_balancer_ids, target_agents):
        for agent in target_agents:
            agent['load'] = len(self.loadbalancers_on_agent(agent['host']))

        agents_to_restart = set()
        for loadbalancer in load_balancer_ids:
            agent = min(target_agents, key=lambda x: x['load'])
            LOG.info("Reassigning loadbalancer %s to agent %s" %
                     (loadbalancer, agent))

            self.connection.execute(
                "UPDATE lbaas_loadbalanceragentbindings "
                "SET agent_id = %s "
                "WHERE loadbalancer_id = %s;",
                agent['id'], loadbalancer
            )
            agents_to_restart.add(agent['host'])
            agent['load'] = agent['load'] + 1
        return agents_to_restart

    def setup(self):
        cfg.CONF(project='neutron')
        # Always log to stderr, ignore setting in neutron.conf
        cfg.CONF.set_override("use_stderr", True)
        logging.setup(cfg.CONF, "neutron")
        self.host_to_evacuate = cfg.CONF.host

        db_uri = cfg.CONF.database.connection
        db = sqlalchemy.create_engine(db_uri)
        try:
            self.connection = db.connect()
        except sqlalchemy.exc.SQLAlchemyError as e:
            LOG.error('Cannot connect to database: %s' % e)
            raise

    def run(self):
        load_balancers = self.loadbalancers_on_agent(self.host_to_evacuate)
        if load_balancers:
            LOG.info("Evacuating loadbalancers: %s", load_balancers)
            target_agents = self.available_destination_agents()

            if not target_agents:
                LOG.error("No agents available as a destination for "
                          "the loadbalancer evacuation.")
                return(1)

            LOG.info("Available destination agents %s", target_agents)
            agents_to_restart = list(
                self.reassign_loadbalancers(load_balancers, target_agents)
            )

            if (cfg.CONF.source_agent_restart or cfg.CONF.delete_namespaces):
                # Make sure the source agent is handled first
                agents_to_restart.insert(0, self.host_to_evacuate)

            LOG.info("agents to restart: %s" % agents_to_restart)

            for host in agents_to_restart:
                LOG.info("restarting agent on %s" % host)
                if (host != self.host_to_evacuate or cfg.CONF.source_agent_restart):  # noqa
                    if cfg.CONF.use_crm:
                        restart_lbaasv2_agent_crm(host)
                    else:
                        restart_lbaasv2_agent_systemd(host)

                if (host == self.host_to_evacuate and cfg.CONF.delete_namespaces):  # noqa
                    delete_lbaasv2_namespaces(host, load_balancers)
        else:
            LOG.info("The agent on %s is not hosting any loadbalancers.",
                     self.host_to_evacuate)
        return(0)


def restart_lbaasv2_agent_crm(hostname):
    with hatool.connect_to_host(hostname, 30) as host:
        host.run_timeout = 30
        try:
            host.run("crm --wait node maintenance")
            host.run(
                "systemctl restart openstack-neutron-lbaasv2-agent")
            host.run("crm --wait node ready")
        except socket.timeout:
            LOG.warn("SSH timeout exceeded. Failed to restart "
                     "openstack-neutron-lbaasv2-agent on %s",
                     host)


def restart_lbaasv2_agent_systemd(hostname):
    with hatool.connect_to_host(hostname, 30) as host:
        host.run_timeout = 30
        try:
            host.run(
                "systemctl restart openstack-neutron-lbaasv2-agent")
        except socket.timeout:
            LOG.warn("SSH timeout exceeded. Failed to restart "
                     "openstack-neutron-lbaasv2-agent on %s",
                     host)


def delete_lbaasv2_namespaces(hostname, loadbalancer_ids):
    with hatool.connect_to_host(hostname, 30) as host:
        host.run_timeout = 30
        for lb in loadbalancer_ids:
            namespace = "qlbaas-" + lb
            hatool.Namespace(host, namespace).destroy()


if __name__ == '__main__':
    evacuate = EvacuateLbaasV2Agent()
    evacuate.setup()
    ret = evacuate.run()

    sys.exit(ret)
