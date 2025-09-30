from psl_experiments import *
from ssh_utils import *

class NimbleExperiment(PSLExperiment):
    def copy_back_build_files(self):
        super().copy_back_build_files()
        remote_repo = f"/home/{self.dev_ssh_user}/repo/Nimble"
        TARGET_BINARIES = ["endorser", "coordinator", "endpoint_rest", "light_client_rest", "psl_lb", "kvs"]
        # Copy the target/release to build directory
        for bin in TARGET_BINARIES:
            copy_file_from_remote_public_ip(f"{remote_repo}/target/release/{bin}", os.path.join(self.local_workdir, "build", bin), self.dev_ssh_user, self.dev_ssh_key, self.dev_vm)

    def bins_already_exist(self):
        super_res = super().bins_already_exist()
        remote_repo = f"/home/{self.dev_ssh_user}/repo/Nimble"
        TARGET_BINARIES = ["endorser", "coordinator", "endpoint_rest", "light_client_rest", "psl_lb", "kvs"]

        res = run_remote_public_ip([
            f"ls {remote_repo}/target/release"
        ], self.dev_ssh_user, self.dev_ssh_key, self.dev_vm, hide=True)

        return super_res and any([bin in res[0] for bin in TARGET_BINARIES])


    def generate_configs(self, deployment: Deployment, config_dir, log_dir):
        # If config_dir is not empty, assume the configs have already been generated
        if len(os.listdir(config_dir)) > 0:
            print("Skipping config generation for experiment", self.name)
            return
        # Number of nodes in deployment may be < number of nodes in deployment
        # So we reuse nodes.
        # As a default, each deployed node gets its unique port number
        # So there will be no port clash.

        rr_cnt = 0
        nodelist = []
        nodes = {}
        node_configs = {}
        node_list_for_crypto = {}
        self.binary_mapping = defaultdict(list)

        # TODO: Do this separately for node, storage, and sequencer.

        (node_vms, storage_vms, sequencer_vms) = self.get_vms(deployment)
        print("Node vms", node_vms)
        print("Storage vms", storage_vms)
        print("Sequencer vms", sequencer_vms)
        print("Client vms", deployment.get_all_client_vms())
        worker_names = []
        storage_names = []
        sequencer_names = []
        port = deployment.node_port_base

        for node_num in range(1, self.num_nodes+1):
            port += 1
            listen_addr = f"0.0.0.0:{port}"
            name = f"node{node_num}"
            domain = f"{name}.pft.org"

            _vm = node_vms[rr_cnt % len(node_vms)]
            self.binary_mapping[_vm].append(name)
            
            private_ip = _vm.private_ip
            rr_cnt += 1
            connect_addr = f"{private_ip}:{port}"

            nodelist.append(name[:])
            nodes[name] = {
                "addr": connect_addr,
                "domain": domain
            }

            worker_names.append(name)

            node_list_for_crypto[name] = (connect_addr, domain)

            config = deepcopy(self.base_node_config)
            config["net_config"]["name"] = name
            config["net_config"]["addr"] = listen_addr
            data_dir = os.path.join(self.data_dir, f"{name}-db")
            config["consensus_config"]["log_storage_config"]["RocksDB"]["db_path"] = str(data_dir)

            node_configs[name] = config

        for node_num in range(1, self.num_storage_nodes+1):
            port += 1
            listen_addr = f"0.0.0.0:{port}"
            name = f"storage{node_num}"
            domain = f"{name}.pft.org"

            _vm = storage_vms[rr_cnt % len(storage_vms)]
            self.binary_mapping[_vm].append(name)

            storage_names.append(name)
            
            private_ip = _vm.private_ip
            rr_cnt += 1
            connect_addr = f"{private_ip}:{port}"
            
            nodelist.append(name[:])
            nodes[name] = {
                "addr": connect_addr,
                "domain": domain
            }

            node_list_for_crypto[name] = (connect_addr, domain)

            config = deepcopy(self.base_node_config)
            config["net_config"]["name"] = name
            config["net_config"]["addr"] = listen_addr
            data_dir = os.path.join(self.data_dir, f"{name}-db")
            config["consensus_config"]["log_storage_config"]["RocksDB"]["db_path"] = str(data_dir)

            node_configs[name] = config


        # 0th sequencer is used for coordinator, endpoint_rest, psl_lb and kvs
        # Rest of the sequencers are used as endorsers.
        # self.num_sequencer_nodes must be >= 2
        assert self.num_sequencer_nodes >= 2
        self.endorser_addrs = []
        for node_num in range(1, self.num_sequencer_nodes+1):
            if node_num == 1:
                port += 1
                listen_addr = f"0.0.0.0:{port}"
                seq_port = port

                name = f"sequencer{node_num}"
                domain = f"{name}.pft.org"
                port += 1 # for coordinator
                self.coordinator_port = port
                port += 1 # for coordinator_ctrl
                self.coordinator_ctrl_port = port
                port += 1 # for endpoint_rest
                self.endpoint_rest_port = port
                port += 1 # for psl_lb
                self.psl_lb_port = port
                
                _vm = sequencer_vms[rr_cnt % len(sequencer_vms)]
                self.binary_mapping[_vm].append(name)                
                private_ip = _vm.private_ip
                self.nimble_endpoint_ip = private_ip
                rr_cnt += 1
                connect_addr = f"{private_ip}:{seq_port}"
                
                nodelist.append(name[:])
                nodes[name] = {
                    "addr": connect_addr,
                    "domain": domain
                }
                sequencer_names.append(name)

                node_list_for_crypto[name] = (connect_addr, domain)

                config = deepcopy(self.base_node_config)
                config["net_config"]["name"] = name
                config["net_config"]["addr"] = listen_addr
                data_dir = os.path.join(self.data_dir, f"{name}-db")
                config["consensus_config"]["log_storage_config"]["RocksDB"]["db_path"] = str(data_dir)

                node_configs[name] = config


                # For PSL LB, need to geenrate a worker config. We will use the name node0.
                name = "node0"
                connect_addr = f"{private_ip}:{self.psl_lb_port}"
                nodelist.append(name[:])
                domain = f"{name}.pft.org"
                nodes[name] = {
                    "addr": connect_addr,
                    "domain": domain
                }
                node_list_for_crypto[name] = (connect_addr, domain)
                listen_addr = f"0.0.0.0:{self.psl_lb_port}"

                config = deepcopy(self.base_node_config)
                config["net_config"]["name"] = name
                config["net_config"]["addr"] = listen_addr
                data_dir = os.path.join(self.data_dir, f"{name}-db")
                config["consensus_config"]["log_storage_config"]["RocksDB"]["db_path"] = str(data_dir)
                node_configs[name] = config
                worker_names.append(name)
            else:
                port += 1
                _vm = sequencer_vms[rr_cnt % len(sequencer_vms)]
                name = f"endorser{node_num - 1}"
                self.binary_mapping[_vm].append(name)
                
                private_ip = _vm.private_ip
                rr_cnt += 1
                self.endorser_addrs.append((private_ip, port))


        if self.client_region == -1:
            client_vms = deployment.get_all_client_vms()
        else:
            client_vms = deployment.get_all_client_vms_in_region(self.client_region)

        crypto_info = self.gen_crypto(config_dir, node_list_for_crypto, len(client_vms))
        

        print("Worker names", worker_names)
        print("Storage names", storage_names)
        print("Sequencer names", sequencer_names)
        print("Endorser addrs", self.endorser_addrs)

        gossip_downstream_worker_list = self.generate_multicast_tree(worker_names, 2)

        print("Gossip downstream worker list", gossip_downstream_worker_list)

        for k, v in node_configs.items():
            tls_cert_path, tls_key_path, tls_root_ca_cert_path,\
            allowed_keylist_path, signing_priv_key_path = crypto_info[k]

            v["net_config"]["nodes"] = deepcopy(nodes)

            if k in sequencer_names:
                v["consensus_config"]["node_list"] = storage_names[:]
            else:
                v["consensus_config"]["node_list"] = nodelist[:]

            if k in worker_names:
                v["worker_config"]["gossip_downstream_worker_list"] = gossip_downstream_worker_list[k]
            else:
                v["worker_config"]["gossip_downstream_worker_list"] = []

            v["consensus_config"]["learner_list"] =  [] # sequencer_names[:]; When running with Nimble, storage nodes don't send to sequencer.
            v["net_config"]["tls_cert_path"] = tls_cert_path
            v["net_config"]["tls_key_path"] = tls_key_path
            v["net_config"]["tls_root_ca_cert_path"] = tls_root_ca_cert_path
            v["rpc_config"]["allowed_keylist_path"] = allowed_keylist_path
            v["rpc_config"]["signing_priv_key_path"] = signing_priv_key_path
            v["worker_config"]["all_worker_list"] = worker_names[:]
            v["worker_config"]["nimble_endpoint_url"] = f"http://{self.nimble_endpoint_ip}:{self.endpoint_rest_port}"

            if k != "node0":
                v["worker_config"]["storage_list"] = storage_names[:]
            else:
                v["worker_config"]["storage_list"] = storage_names[:] # PSL LB needs to know about storage nodes.

            v["worker_config"]["sequencer_list"] = sequencer_names[:]

            # Only simulate Byzantine behavior in node1.
            if "evil_config" in v and v["evil_config"]["simulate_byzantine_behavior"] and k != "node1":
                v["evil_config"]["simulate_byzantine_behavior"] = False
                v["evil_config"]["byzantine_start_block"] = 0

            with open(os.path.join(config_dir, f"{k}_config.json"), "w") as f:
                json.dump(v, f, indent=4)

        num_clients_per_vm = [self.num_clients // len(client_vms) for _ in range(len(client_vms))]
        num_clients_per_vm[-1] += (self.num_clients - sum(num_clients_per_vm))

        for client_num in range(len(client_vms)):
            config = deepcopy(self.base_client_config)
            client = "client" + str(client_num + 1)
            config["net_config"]["name"] = client

            client_nodes = deepcopy(nodes)
            client_nodes = {k: v for k, v in client_nodes.items() if k in worker_names}
            config["net_config"]["nodes"] = client_nodes

            tls_cert_path, tls_key_path, tls_root_ca_cert_path,\
            allowed_keylist_path, signing_priv_key_path = crypto_info[client]

            config["net_config"]["tls_root_ca_cert_path"] = tls_root_ca_cert_path
            config["rpc_config"] = {"signing_priv_key_path": signing_priv_key_path}

            config["workload_config"]["num_clients"] = num_clients_per_vm[client_num]
            config["workload_config"]["duration"] = self.duration

            self.binary_mapping[client_vms[client_num]].append(client)

            with open(os.path.join(config_dir, f"{client}_config.json"), "w") as f:
                json.dump(config, f, indent=4)

        # Controller config
        config = deepcopy(self.base_client_config)
        name = "controller"
        config["net_config"]["name"] = name
        config["net_config"]["nodes"] = deepcopy(nodes)

        tls_cert_path, tls_key_path, tls_root_ca_cert_path,\
        allowed_keylist_path, signing_priv_key_path = crypto_info[name]

        config["net_config"]["tls_root_ca_cert_path"] = tls_root_ca_cert_path
        config["rpc_config"] = {"signing_priv_key_path": signing_priv_key_path}

        config["workload_config"]["num_clients"] = 1
        config["workload_config"]["duration"] = self.duration

        with open(os.path.join(config_dir, f"{name}_config.json"), "w") as f:
            json.dump(config, f, indent=4)

        if self.controller_must_run:
            self.binary_mapping[client_vms[0]].append(name)


    def generate_arbiter_script(self):
        """
        This is a copy of the generate_arbiter_script method in experiments.py.
        The only difference is that the servers are run as:
        ./server storage config.json
        """

        script_base = f"""#!/bin/bash
set -e
set -o xtrace

# This script is generated by the experiment pipeline. DO NOT EDIT.
SSH_CMD="ssh -o StrictHostKeyChecking=no -i {self.dev_ssh_key}"
SCP_CMD="scp -o StrictHostKeyChecking=no -i {self.dev_ssh_key}"

# SSH into each VM and run the binaries
"""
        # Plan the binaries to run
        for repeat_num in range(self.repeats):
            print("Running repeat", repeat_num)
            _script = script_base[:]

            # Must run the endorsers first.
            __endorser_num = 0
            for vm, bin_list in self.binary_mapping.items():
                for bin in bin_list:
                    if not("endorser" in bin):
                        continue

                    _, port = self.endorser_addrs[__endorser_num]
                    __endorser_num += 1
                    
                    _script += f"""
$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} '{self.remote_workdir}/build/endorser -t 0.0.0.0 -p {port} > {self.remote_workdir}/logs/{repeat_num}/{bin}.log 2> {self.remote_workdir}/logs/{repeat_num}/{bin}.err' &
PID="$PID $!"
"""


            for vm, bin_list in self.binary_mapping.items():
                for bin in bin_list:
                    if "node" in bin:
                        binary_name = "server worker"
                    elif "storage" in bin:
                        binary_name = "server storage"
                    elif "sequencer" in bin:
                        binary_name = "kvs"
                    elif "client" in bin:
                        binary_name = "client"
                    elif "controller" in bin:
                        binary_name = "controller"
                    elif "endorser" in bin:
                        continue # Already running


                    if binary_name != "kvs":
                        _script += f"""
$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} '{self.remote_workdir}/build/{binary_name} {self.remote_workdir}/configs/{bin}_config.json > {self.remote_workdir}/logs/{repeat_num}/{bin}.log 2> {self.remote_workdir}/logs/{repeat_num}/{bin}.err' &
PID="$PID $!"
"""
                    else:
                        endorser_param = ",".join([f"http://{ip}:{port}" for ip, port in self.endorser_addrs])
                        if self.num_storage_nodes > 0:
                            psl_lb_command = f"""
sleep 1
$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} '{self.remote_workdir}/build/psl_lb -a 0.0.0.0:50051 -w {self.remote_workdir}/configs/node0_config.json > {self.remote_workdir}/logs/{repeat_num}/psl_lb.log 2> {self.remote_workdir}/logs/{repeat_num}/psl_lb.err' &
PID="$PID $!"
"""
                            # coordinator_extra_param = "-s psl_lb -n psl_lb"
                            coordinator_extra_param = ""
                        else:
                            psl_lb_command = ""
                            coordinator_extra_param = ""

                        _script += f"""
{psl_lb_command}

sleep 1
$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} '{self.remote_workdir}/build/coordinator -t 0.0.0.0 -p {self.coordinator_port} -r {self.coordinator_ctrl_port} -e "{endorser_param}" -l 60 {coordinator_extra_param} > {self.remote_workdir}/logs/{repeat_num}/coordinator.log 2> {self.remote_workdir}/logs/{repeat_num}/coordinator.err' &
PID="$PID $!"

sleep 1
$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} '{self.remote_workdir}/build/endpoint_rest -t 0.0.0.0 -p {self.endpoint_rest_port} -c "http://127.0.0.1:{self.coordinator_port}" -l 60 > {self.remote_workdir}/logs/{repeat_num}/endpoint_rest.log 2> {self.remote_workdir}/logs/{repeat_num}/endpoint_rest.err' &
PID="$PID $!"
"""
                    
            _script += f"""
# Sleep for the duration of the experiment
sleep {self.duration}

# Kill the binaries. First with a SIGINT, then with a SIGTERM, then with a SIGKILL
echo -n $PID | xargs -d' ' -I{{}} kill -2 {{}} || true
echo -n $PID | xargs -d' ' -I{{}} kill -15 {{}} || true
echo -n $PID | xargs -d' ' -I{{}} kill -9 {{}} || true
sleep 10

# Kill the binaries in SSHed VMs as well. Calling SIGKILL on the local SSH process might have left them orphaned.
# Make sure not to kill the tmux server.
# Then copy the logs back and delete any db files. Cleanup for the next run.
"""
            for vm, bin_list in self.binary_mapping.items():
                for bin in bin_list:
                    if "node" in bin:
                        binary_name = "server"
                    elif "storage" in bin:
                        binary_name = "server"
                    elif "sequencer" in bin:
                        binary_name = "kvs"
                    elif "client" in bin:
                        binary_name = "client"
                    elif "controller" in bin:
                        binary_name = "controller"
                    elif "endorser" in bin:
                        binary_name = "endorser"
                
                    if binary_name == "kvs":
                        if self.num_storage_nodes > 0:
                            log_scp_cmd = f"""
$SCP_CMD {self.dev_ssh_user}@{vm.public_ip}:{self.remote_workdir}/logs/{repeat_num}/psl_lb.log {self.remote_workdir}/logs/{repeat_num}/psl_lb.log || true
$SCP_CMD {self.dev_ssh_user}@{vm.public_ip}:{self.remote_workdir}/logs/{repeat_num}/psl_lb.err {self.remote_workdir}/logs/{repeat_num}/psl_lb.err || true
"""
                        else:
                            log_scp_cmd = ""

                        _script += f"""
$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} 'pkill -2 -c endpoint_rest' || true
$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} 'pkill -15 -c endpoint_rest' || true
$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} 'pkill -9 -c endpoint_rest' || true

$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} 'pkill -2 -c coordinator' || true
$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} 'pkill -15 -c coordinator' || true
$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} 'pkill -9 -c coordinator' || true

$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} 'pkill -2 -c psl_lb' || true
$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} 'pkill -15 -c psl_lb' || true
$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} 'pkill -9 -c psl_lb' || true

{log_scp_cmd}

$SCP_CMD {self.dev_ssh_user}@{vm.public_ip}:{self.remote_workdir}/logs/{repeat_num}/coordinator.log {self.remote_workdir}/logs/{repeat_num}/coordinator.log || true
$SCP_CMD {self.dev_ssh_user}@{vm.public_ip}:{self.remote_workdir}/logs/{repeat_num}/coordinator.err {self.remote_workdir}/logs/{repeat_num}/coordinator.err || true

$SCP_CMD {self.dev_ssh_user}@{vm.public_ip}:{self.remote_workdir}/logs/{repeat_num}/endpoint_rest.log {self.remote_workdir}/logs/{repeat_num}/endpoint_rest.log || true
$SCP_CMD {self.dev_ssh_user}@{vm.public_ip}:{self.remote_workdir}/logs/{repeat_num}/endpoint_rest.err {self.remote_workdir}/logs/{repeat_num}/endpoint_rest.err || true
"""

                    _script += f"""
$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} 'pkill -2 -c {binary_name}' || true
"""
                # Copy the logs back
                    _script += f"""
$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} 'pkill -2 -c {binary_name}' || true
$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} 'pkill -15 -c {binary_name}' || true
$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} 'pkill -9 -c {binary_name}' || true
$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} 'rm -rf /data/*' || true
$SCP_CMD {self.dev_ssh_user}@{vm.public_ip}:{self.remote_workdir}/logs/{repeat_num}/{bin}.log {self.remote_workdir}/logs/{repeat_num}/{bin}.log || true
$SCP_CMD {self.dev_ssh_user}@{vm.public_ip}:{self.remote_workdir}/logs/{repeat_num}/{bin}.err {self.remote_workdir}/logs/{repeat_num}/{bin}.err || true
"""
                    
            _script += f"""
sleep 60
"""
                    
            # pkill -9 -c server also kills tmux-server. So we can't run a server on the dev VM.
            # It kills the tmux session and the experiment. And we end up with a lot of orphaned processes.

            with open(os.path.join(self.local_workdir, f"arbiter_{repeat_num}.sh"), "w") as f:
                f.write(_script + "\n\n")



