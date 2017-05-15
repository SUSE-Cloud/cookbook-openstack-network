import argparse
import datetime
import unittest
import collections
import functools
import importlib
import tempfile
import mock
import socket
import multiprocessing

ha_tool = importlib.import_module("neutron-ha-tool")


class FakeNeutron(object):
    def __init__(self):
        self.routers = {}
        self.agents = {}
        self.routers_by_agent = collections.defaultdict(set)

    def add_agent(self, agent_id, props):
        self.agents[agent_id] = dict(props, id=agent_id)

    def add_router(self, agent_id, router_id, props):
        self.routers[router_id] = dict(props, id=router_id)
        self.routers_by_agent[agent_id].add(router_id)

    def agent_by_router(self, router_id):
        for agent_id, router_ids in self.routers_by_agent.items():
            if router_id in router_ids:
                return self.agents[agent_id]

        raise NotImplementedError()


class FakeNeutronClient(object):
    def __init__(self, fake_neutron):
        self.fake_neutron = fake_neutron

    def list_agents(self):
        return {
            'agents': self.fake_neutron.agents.values()
        }

    def list_routers_on_l3_agent(self, agent_id):
        return {
            'routers': [
                self.fake_neutron.routers[router_id]
                for router_id in self.fake_neutron.routers_by_agent[agent_id]
            ]
        }

    def remove_router_from_l3_agent(self, agent_id, router_id):
        self.fake_neutron.routers_by_agent[agent_id].remove(router_id)

    def add_router_to_l3_agent(self, agent_id, router_body):
        self.fake_neutron.routers_by_agent[agent_id].add(
            router_body['router_id'])

    def list_ports(self, device_id, fields):
        return {
            'ports': [
                {
                    'id': 'someid',
                    'binding:host_id':
                        self.fake_neutron.agent_by_router(device_id)['host'],
                    'binding:vif_type': 'non distributed',
                    'status': 'ACTIVE'
                }
            ]
        }

    def list_floatingips(self, router_id):
        return {
            'floatingips': [
                {
                    'id': 'irrelevant',
                    'status': 'ACTIVE'
                }
            ]
        }


def setup_fake_neutron(live_agents=0, dead_agents=0):
    fake_neutron = FakeNeutron()

    for i in range(live_agents):
        fake_neutron.add_agent(
            'live-agent-{}'.format(i), {
                'agent_type': 'L3 agent',
                'alive': True,
                'admin_state_up': True,
                'host': 'live-agent-{}-host'.format(i),
                'configurations': {
                    'agent_mode': 'Mode X'
                }
            }
        )
    for i in range(dead_agents):
        fake_neutron.add_agent(
            'dead-agent-{}'.format(i), {
                'agent_type': 'L3 agent',
                'alive': False,
                'admin_state_up': False,
                'host': 'dead-agent-{}-host'.format(i)
            }
        )
    return fake_neutron


class TestL3AgentMigrate(unittest.TestCase):

    def test_no_dead_agents_migrate_returns_without_errors(self):
        neutron_client = FakeNeutronClient(setup_fake_neutron(live_agents=2))

        # None as Agent Picker - given no dead agents, no migration, and
        # therefore no agent picking will take place
        error_count = ha_tool.l3_agent_migrate(
            neutron_client, None, ha_tool.NullRouterFilter()
        )

        self.assertEqual(0, error_count)

    def test_no_live_agents_migrate_returns_with_error(self):
        neutron_client = FakeNeutronClient(setup_fake_neutron(dead_agents=2))

        # None as Agent Picker - given no live agents, no migration, and
        # therefore no agent picking will take place
        error_count = ha_tool.l3_agent_migrate(
            neutron_client, None, ha_tool.NullRouterFilter()
        )

        self.assertEqual(1, error_count)

    def test_migrate_from_dead_agent_moves_routers_and_returns_no_errors(self):
        fake_neutron = setup_fake_neutron(live_agents=1, dead_agents=1)
        neutron_client = FakeNeutronClient(fake_neutron)
        fake_neutron.add_router('dead-agent-0', 'router-1', {})

        error_count = ha_tool.l3_agent_migrate(
            neutron_client, ha_tool.RandomAgentPicker(),
            ha_tool.NullRouterFilter(), now=True
        )

        self.assertEqual(0, error_count)
        self.assertEqual(
            set(['router-1']), fake_neutron.routers_by_agent['live-agent-0'])


class TestL3AgentEvacuate(unittest.TestCase):

    def test_evacuate_without_agents_returns_no_errors(self):
        neutron_client = FakeNeutronClient(FakeNeutron())

        # None as Agent Picker - given no agents, no migration, and therefore
        # no agent picking will take place
        error_count = ha_tool.l3_agent_evacuate(
            neutron_client, 'host1', None, ha_tool.NullRouterFilter()
        )

        self.assertEqual(0, error_count)

    def test_evacuate_live_agent_moves_routers_and_returns_no_errors(self):
        fake_neutron = setup_fake_neutron(live_agents=2)
        neutron_client = FakeNeutronClient(fake_neutron)
        fake_neutron.add_router('live-agent-0', 'router', {})

        error_count = ha_tool.l3_agent_evacuate(
            neutron_client, 'live-agent-0-host', ha_tool.RandomAgentPicker(),
            ha_tool.NullRouterFilter()
        )

        self.assertEqual(0, error_count)
        self.assertEqual(
            set(['router']),
            fake_neutron.routers_by_agent['live-agent-1']
        )

    @mock.patch('neutron-ha-tool.wait_router_migrated')
    def test_evacuate_fast_fail_return_exactly_one_error(self,
                                                         mock_wait_router):
        mock_wait_router.side_effect = RuntimeError("Failure")
        fake_neutron = setup_fake_neutron(live_agents=2)
        neutron_client = FakeNeutronClient(fake_neutron)
        fake_neutron.add_router('live-agent-0', 'router1', {})
        fake_neutron.add_router('live-agent-0', 'router2', {})

        error_count = ha_tool.l3_agent_evacuate(
            neutron_client, 'live-agent-0-host', ha_tool.RandomAgentPicker(),
            ha_tool.NullRouterFilter(), fail_fast=True
        )

        self.assertEqual(1, error_count)


class TestLeastBusyAgentPicker(unittest.TestCase):

    def setUp(self):
        self.fake_neutron = setup_fake_neutron(live_agents=2)
        self.neutron_client = FakeNeutronClient(self.fake_neutron)

    def make_picker_and_set_agents(self):
        picker = ha_tool.LeastBusyAgentPicker(self.neutron_client)
        picker.set_agents(
            [
                {'id': 'live-agent-0'},
                {'id': 'live-agent-1'}
            ]
        )
        return picker

    def test_agent_picker_queries_neutron_for_number_of_routers(self):
        self.fake_neutron.add_router('live-agent-0', 'router', {})
        picker = self.make_picker_and_set_agents()

        self.assertEqual(
            {
                'live-agent-0': 1,
                'live-agent-1': 0
            },
            picker.router_count_per_agent_id
        )

    def test_agent_with_smallest_number_of_routers_picked(self):
        self.fake_neutron.add_router('live-agent-0', 'router', {})
        picker = self.make_picker_and_set_agents()

        self.assertEqual('live-agent-1', picker.pick()['id'])

    def test_picking_an_agent_increases_internal_router_counter(self):
        self.fake_neutron.add_router('live-agent-0', 'router', {})
        picker = self.make_picker_and_set_agents()

        picked_agent = picker.pick()
        self.assertEqual('live-agent-1', picked_agent['id'])

        self.assertEqual(
            {
                'live-agent-0': 1,
                'live-agent-1': 1
            },
            picker.router_count_per_agent_id
        )

    def test_router_per_agent_cache_updated_when_cache_expired(self):
        # initializing the picker will also query neutron
        picker = self.make_picker_and_set_agents()

        # Add some routers to live-agent-0 to make sure it's the busyest
        self.fake_neutron.add_router('live-agent-0', 'router-2', {})
        self.fake_neutron.add_router('live-agent-0', 'router-3', {})

        # Emulate that cache has expired
        picker.cache_created_at = (
            picker.cache_created_at - datetime.timedelta(
                seconds=ha_tool.ROUTER_CACHE_MAX_AGE_SECONDS + 1)
        )

        # pick returns live-agent-1 - that means it consulted neutron
        self.assertEqual('live-agent-1', picker.pick()['id'])

    def test_cache_reloaded_if_difference_is_a_day(self):
        # initializing the picker will also query neutron
        picker = self.make_picker_and_set_agents()

        # Add some routers to live-agent-0 to make sure it's the busyest
        self.fake_neutron.add_router('live-agent-0', 'router-2', {})
        self.fake_neutron.add_router('live-agent-0', 'router-3', {})

        # Emulate that cache has expired
        picker.cache_created_at = (
            picker.cache_created_at - datetime.timedelta(days=1)
        )

        # pick returns live-agent-1 - that means it consulted neutron
        self.assertEqual('live-agent-1', picker.pick()['id'])

    def test_pick_on_empty_array_throws_index_error_as_random_does(self):
        picker = ha_tool.LeastBusyAgentPicker(self.neutron_client)
        picker.set_agents([])

        with self.assertRaises(IndexError):
            picker.pick()


class TestLoadRouterIds(unittest.TestCase):

    def test_loading_empty_file_returns_empty_array(self):
        with tempfile.NamedTemporaryFile() as list_file:
            router_list = ha_tool.load_router_ids(list_file.name)

        self.assertEqual([], router_list)

    def test_empty_lines_skipped(self):
        with tempfile.NamedTemporaryFile() as list_file:
            list_file.write('\n')
            list_file.write('      ')
            list_file.seek(0)
            router_list = ha_tool.load_router_ids(list_file.name)

        self.assertEqual([], router_list)

    def test_lines_stripped(self):
        with tempfile.NamedTemporaryFile() as list_file:
            list_file.write('  some-router-id   ')
            list_file.seek(0)
            router_list = ha_tool.load_router_ids(list_file.name)

        self.assertEqual(['some-router-id'], router_list)


class TestWhitelistRouterFilter(unittest.TestCase):

    def test_empty_white_list_filters_out_all_router_ids(self):
        router_filter = ha_tool.WhitelistRouterFilter([])

        filtered_router_ids = router_filter.filter_routers(
            ['router-id-1', 'router-id-2']
        )

        self.assertEqual([], filtered_router_ids)

    def test_only_whitelisted_routers_returned(self):
        router_filter = ha_tool.WhitelistRouterFilter(['router-id-1'])

        filtered_router_ids = router_filter.filter_routers(
            ['router-id-1', 'router-id-2']
        )

        self.assertEqual(['router-id-1'], filtered_router_ids)


def mocked_ssh_client(fun):
    @functools.wraps(fun)
    def decorated(*args, **kwargs):
        with mock.patch('neutron-ha-tool.paramiko.SSHClient') as ssh_client:
            mock_client = mock.Mock()

            _mock_context = mock.Mock()
            _mock_context.__enter__ = mock.Mock()
            _mock_context.__exit__ = mock.Mock()
            _mock_context.__exit__.return_value = False
            _mock_context.__enter__.return_value = mock_client

            ssh_client.return_value = _mock_context
            return fun(*tuple(args + (mock_client, )), **kwargs)

    return decorated


class TestSshDeleteRouterNamespace(unittest.TestCase):

    @mocked_ssh_client
    def test_namespace_deleted(self, mock_client):
        self.ssh_exec_side_effect_counter = 0

        def ssh_exec_side_effect(*args, **kwargs):
            mock_stdout = mock.MagicMock()
            mock_stdout.readlines.return_value = []
            mock_stdout.channel.recv_exit_status.return_value = 0
            if args[0].startswith('ip netns pids'):
                if not self.ssh_exec_side_effect_counter:
                    mock_stdout.readlines.return_value = [
                        "1234\r\n", "1235\r\n"
                    ]
                    self.ssh_exec_side_effect_counter += 1

            if args[0].startswith('ip netns list'):
                if not self.ssh_exec_side_effect_counter:
                    mock_stdout.readlines.return_value = [
                        "qrouter-routerid1\r\n",
                        "qrouter-otherid\r\n"
                    ]
                else:
                    mock_stdout.readlines.return_value = [
                        "qrouter-otherid\r\n"
                    ]

            return [mock.MagicMock, mock_stdout, mock.MagicMock]

        mock_client.exec_command.side_effect = ssh_exec_side_effect

        ha_tool.destroy_router_namespace('host1', 'routerid1')

        expected_calls = [
            mock.call("ip netns list", get_pty=mock.ANY, timeout=mock.ANY),
            mock.call("ip netns pids qrouter-routerid1", get_pty=mock.ANY,
                      timeout=mock.ANY),
            mock.call("kill 1234", get_pty=mock.ANY, timeout=mock.ANY),
            mock.call("kill 1235", get_pty=mock.ANY, timeout=mock.ANY),
            mock.call("ip netns pids qrouter-routerid1", get_pty=mock.ANY,
                      timeout=mock.ANY),
            mock.call("ip netns delete qrouter-routerid1", get_pty=mock.ANY,
                      timeout=mock.ANY)
        ]
        mock_client.connect.assert_called_once_with(
            "host1", timeout=mock.ANY)
        mock_client.exec_command.assert_has_calls(expected_calls)

    @mocked_ssh_client
    def test_namespace_does_not_exist(self, mock_client):
        def side_effect_namespace_absent(*args, **kwargs):
            mock_stdout = mock.MagicMock()
            mock_stdout.readlines.return_value = []
            mock_stdout.channel.recv_exit_status.return_value = 0
            if args[0].startswith('ip netns list'):
                mock_stdout.readlines.return_value = ["qrouter-otherid\r\n"]
            return [mock.MagicMock(), mock_stdout, mock.MagicMock()]

        mock_client.exec_command.side_effect = \
            side_effect_namespace_absent
        ha_tool.destroy_router_namespace('host1', 'routerid1')
        mock_client.exec_command.assert_called_once_with(
            "ip netns list", timeout=mock.ANY, get_pty=mock.ANY)

    @mocked_ssh_client
    def test_ssh_command_timeout(self, mock_client):
        def side_effect_ssh_connect(self, *args, **kwargs):
            raise socket.timeout

        mock_client.exec_command.side_effect = \
            side_effect_ssh_connect
        ha_tool.destroy_router_namespace('host1', 'routerid1')


class TestAgentIdBasedAgentPicker(unittest.TestCase):

    def make_picker(self, agent_id):
        picker = ha_tool.AgentIdBasedAgentPicker(agent_id)
        picker.set_agents(
            [
                {'id': 'live-agent-0', 'host': 'host-0'},
                {'id': 'live-agent-1', 'host': 'host-1'}
            ]
        )
        return picker

    def test_picking_an_agent_by_agent_id(self):
        picker = self.make_picker('live-agent-0')

        picked_agent = picker.pick()

        self.assertEqual('live-agent-0', picked_agent['id'])

    def test_agent_not_found_raises_index_error(self):
        picker = self.make_picker('invalid')

        with self.assertRaises(IndexError) as ctx:
            picker.pick()

        self.assertEqual(
            'Cannot find agent with agent id: invalid', str(ctx.exception))


class TestHostBasedAgentPicker(unittest.TestCase):

    def make_picker(self, host):
        picker = ha_tool.HostBasedAgentPicker(host)
        picker.set_agents(
            [
                {'id': 'live-agent-0', 'host': 'host-0'},
                {'id': 'live-agent-1', 'host': 'host-1'}
            ]
        )
        return picker

    def test_picking_an_agent_by_host(self):
        picker = self.make_picker('host-0')

        picked_agent = picker.pick()

        self.assertEqual('host-0', picked_agent['host'])

    def test_agent_not_found_by_host_id_raises_index_error(self):
        picker = self.make_picker('invalid')

        with self.assertRaises(IndexError) as ctx:
            picker.pick()

        self.assertEqual(
            'Cannot find agent with host: invalid', str(ctx.exception))


class TestArgumentParsing(unittest.TestCase):

    def test_argparser_default_values(self):
        argparser = ha_tool.make_argparser()

        params = argparser.parse_args([])

        self.assertEqual(
            argparse.Namespace(
                debug=False,
                quiet=False,
                noop=False,
                l3_agent_check=False,
                l3_agent_migrate=False,
                l3_agent_evacuate=None,
                l3_agent_rebalance=False,
                replicate_dhcp=False,
                now=False,
                retry=False,
                retry_max_interval=10000,
                insecure=False,
                ssh_delete_namespace=False,
                agent_selection_mode='least-busy',
                router_list_file=None,
                target_agent_id=None,
                target_host=None,
                wait_for_router=True,
                fail_fast=False
            ),
            params
        )

    def test_target_agent_id_and_target_host_are_mutually_exclusive(self):
        argparser = ha_tool.make_argparser()

        with self.assertRaises(SystemExit):
            argparser.parse_args(
                ['--target-host', 'host', '--target-agent-id', 'agent-id'])

    def test_setting_target_agent_id_option(self):
        argparser = ha_tool.make_argparser()

        params = argparser.parse_args(['--target-agent-id', 'agent-id'])

        self.assertEqual('agent-id', params.target_agent_id)

    def test_setting_target_host_option(self):
        argparser = ha_tool.make_argparser()

        params = argparser.parse_args(['--target-host', 'host'])

        self.assertEqual('host', params.target_host)


def signal_harness(queue):
    import importlib
    import time
    import sys

    ha_tool = importlib.import_module("neutron-ha-tool")
    ha_tool.register_term_signal_handler()

    with ha_tool.term_disabled():
        queue.put('started critical block')
        time.sleep(100)
        queue.put('about to exit critical block')

    time.sleep(100)
    sys.exit(1)


class TestSignalHandling(unittest.TestCase):

    def wait_for_subprocess_to_write(self, subproc, text):
        data = subproc.stdout.read(len(text))

        self.assertEqual(text, data)

    def test_term_signal_handling_functionality(self):
        queue = multiprocessing.Queue()

        proc = multiprocessing.Process(target=signal_harness, args=(queue,))
        proc.start()
        self.assertEquals('started critical block', queue.get())
        proc.terminate()
        self.assertEquals('about to exit critical block', queue.get())
        proc.join()

        self.assertEqual(0, proc.exitcode)


class SignalHandlerTest(unittest.TestCase):
    def setUp(self):
        self._globals = (
            ha_tool.SHOULD_NOT_TERMINATE, ha_tool.TERM_SIGNAL_RECEIVED
        )

    def tearDown(self):
        ha_tool.SHOULD_NOT_TERMINATE, ha_tool.TERM_SIGNAL_RECEIVED = (
            self._globals
        )


class TestSignalHandler(SignalHandlerTest):
    def test_exits_with_zero(self):
        with self.assertRaises(SystemExit) as ctx:
            ha_tool.term_signal_handler(None, None)

        self.assertEqual(0, ctx.exception.code)

    def test_sets_global_variable(self):
        with self.assertRaises(SystemExit):
            ha_tool.term_signal_handler(None, None)

        self.assertTrue(ha_tool.TERM_SIGNAL_RECEIVED)

    def test_does_not_quit_when_global_set(self):
        ha_tool.SHOULD_NOT_TERMINATE = True

        ha_tool.term_signal_handler(None, None)

        self.assertTrue(ha_tool.TERM_SIGNAL_RECEIVED)


class TestTermDisabled(SignalHandlerTest):
    def test_disables_global_flag(self):
        with ha_tool.term_disabled():
            self.assertTrue(ha_tool.SHOULD_NOT_TERMINATE)

    def test_exits_if_global_flag_set_while_in_context(self):
        with self.assertRaises(SystemExit) as ctx:
            with ha_tool.term_disabled():
                ha_tool.TERM_SIGNAL_RECEIVED = True

        self.assertEqual(0, ctx.exception.code)

    def test_term_can_be_handled_after_context(self):
        with ha_tool.term_disabled():
            pass

        self.assertFalse(ha_tool.SHOULD_NOT_TERMINATE)


def get_router_distribution(neutron_client):
    agents = neutron_client.list_agents()['agents']
    agent_ids = sorted([agent['id'] for agent in agents])
    return [
        len(neutron_client.list_routers_on_l3_agent(agent_id)['routers'])
        for agent_id in agent_ids
    ]


def fake_neutron_with_distribution(router_distribution):
    fake_neutron = setup_fake_neutron(live_agents=len(router_distribution))
    router_id = 0
    for agent_serial, num_routers in enumerate(router_distribution):
        agent_id = 'live-agent-{}'.format(agent_serial)
        for i in range(num_routers):
            fake_neutron.add_router(
                agent_id, 'router-{}'.format(router_id), {}
            )
            router_id += 1
    return fake_neutron


class TestAgentRebalancing(unittest.TestCase):
    def test_balancing_scenario(self):
        fake_neutron = fake_neutron_with_distribution([66, 67, 67, 64, 0])
        neutron_client = FakeNeutronClient(fake_neutron)

        ha_tool.l3_agent_rebalance(neutron_client)

        self.assertEqual(
            [53, 53, 53, 53, 52], get_router_distribution(neutron_client)
        )

    def test_no_agents_in_the_system(self):
        fake_neutron = fake_neutron_with_distribution([])
        neutron_client = FakeNeutronClient(fake_neutron)

        ha_tool.l3_agent_rebalance(neutron_client)

        self.assertEqual(
            [], get_router_distribution(neutron_client)
        )

    def test_already_balanced_system_is_not_touched(self):
        fake_neutron = fake_neutron_with_distribution([5, 4, 5, 4, 5])
        neutron_client = FakeNeutronClient(fake_neutron)

        ha_tool.l3_agent_rebalance(neutron_client)

        self.assertEqual(
            [5, 4, 5, 4, 5], get_router_distribution(neutron_client)
        )

    def test_only_one_agent_exists(self):
        fake_neutron = fake_neutron_with_distribution([5])
        neutron_client = FakeNeutronClient(fake_neutron)

        ha_tool.l3_agent_rebalance(neutron_client)

        self.assertEqual(
            [5], get_router_distribution(neutron_client)
        )


def mock_exec_command_call(ssh_client, lines=None, return_code=0):
    lines = lines or []
    stdout = mock.Mock()
    stdout.readlines.return_value = lines
    stdout.channel.recv_exit_status.return_value = return_code
    ssh_client.exec_command.return_value = (
        mock.Mock(), stdout, mock.Mock()
    )


class TestSSHHost(unittest.TestCase):
    @mocked_ssh_client
    def test_str_prints_hostname(self, ssh_client):
        with ha_tool.connect_to_host('somehost', 10) as ssh_host:
            self.assertEqual('somehost', str(ssh_host))

    @mocked_ssh_client
    def test_connect_timeout(self, ssh_client):
        with ha_tool.connect_to_host('somehost', 10):
            pass

        ssh_client.connect.assert_called_once_with('somehost', timeout=10)

    @mocked_ssh_client
    def test_setting_run_timeout_on_host(self, ssh_client):
        mock_exec_command_call(ssh_client)

        with ha_tool.connect_to_host('somehost', 10) as host:
            host.run_timeout = 30
            host.run('ls -la')

        ssh_client.exec_command.assert_called_once_with(
            'ls -la', timeout=30, get_pty=True)

    @mocked_ssh_client
    def test_run_return_values(self, ssh_client):
        mock_exec_command_call(
            ssh_client,
            lines=['  line 1  \r\n', '  line2  \r\n'],
            return_code=4
        )

        with ha_tool.connect_to_host('somehost', 10) as host:
            result = host.run('ls -la')

        self.assertEqual([4, ['line 1', 'line2']], result)
