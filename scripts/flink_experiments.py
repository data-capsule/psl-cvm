from collections import defaultdict
from copy import deepcopy
import json
from experiments import Experiment, copy_file_from_remote_public_ip
from deployment import Deployment
import os
import subprocess
from typing import List

from ssh_utils import run_remote_public_ip, copy_remote_public_ip


class FlinkExperiment(Experiment):


    def remote_build(self):
        pass

    def copy_back_build_files(self):
        pass
        # remote_repo = f"/home/{self.dev_ssh_user}/repo"
        # TARGET_BINARIES = [self.workload]

        # # Copy the target/release to build directory
        # for bin in TARGET_BINARIES:
        #     copy_file_from_remote_public_ip(f"{remote_repo}/target/release/{bin}", os.path.join(self.local_workdir, "build", bin), self.dev_ssh_user, self.dev_ssh_key, self.dev_vm)

        # remote_script_dir = f"{remote_repo}/scripts_v2/loadtest"
        # TARGET_SCRIPTS = ["load.py", "locustfile.py", "docker-compose.yml", "toggle.py", "shamir.py", "zipfian.py"]

        # # Copy the scripts to build directory
        # for script in TARGET_SCRIPTS:
        #     copy_file_from_remote_public_ip(f"{remote_script_dir}/{script}", os.path.join(self.local_workdir, "build", script), self.dev_ssh_user, self.dev_ssh_key, self.dev_vm)


    def bins_already_exist(self):
        pass
        # TARGET_BINARIES = [self.workload]
        # remote_repo = f"/home/{self.dev_ssh_user}/repo"

        # remote_script_dir = f"{remote_repo}/scripts_v2/loadtest"
        # TARGET_SCRIPTS = ["load.py", "locustfile.py", "docker-compose.yml", "toggle.py", "shamir.py", "zipfian.py"]


        # res1 = run_remote_public_ip([
        #     f"find {remote_script_dir}"
        # ], self.dev_ssh_user, self.dev_ssh_key, self.dev_vm, hide=True)

        # res2 = run_remote_public_ip([
        #     f"ls {remote_repo}/target/release"
        # ], self.dev_ssh_user, self.dev_ssh_key, self.dev_vm, hide=True)

        # return all([bin in res2[0] for bin in TARGET_BINARIES]) and all([script in res1[0] for script in TARGET_SCRIPTS])
    

    def generate_configs(self, deployment: Deployment, config_dir, log_dir):
        # If config_dir is not empty, assume the configs have already been generated
        # if len(os.listdir(config_dir)) > 0:
        #     print("Skipping config generation for experiment", self.name)
        #     return
        # Number of nodes in deployment may be < number of nodes in deployment
        # So we reuse nodes.
        # As a default, each deployed node gets its unique port number
        # So there will be no port clash.

        rr_cnt = 0
        nodelist = []
        nodes = {}
        node_configs = {}
        vms = []
        node_list_for_crypto = {}
        # if self.node_distribution == "uniform":
        #     vms = deployment.get_all_node_vms()
        # elif self.node_distribution == "sev_only":
        #     vms = deployment.get_nodes_with_tee("sev")
        # elif self.node_distribution == "tdx_only":
        #     vms = deployment.get_nodes_with_tee("tdx")
        # elif self.node_distribution == "nontee_only":
        #     vms = deployment.get_nodes_with_tee("nontee")
        # else:
        #     vms = deployment.get_wan_setup(self.node_distribution)
        vms = deployment.get_all_client_vms()
        assert len(vms) > 1, "Flink requires at least 2 client VMs"

        flink_vm = vms[0]
        vms = vms[1:]
        
        self.binary_mapping = defaultdict(list)
        self.binary_mapping[flink_vm].append("flink")
        self.getRequestHosts = []


        node_names = ["yarn-leader", "hdfs-leader", "metastore", "hiveserver2"]

        # for node_num in range(1, self.num_nodes+1):
        for node_num, name in enumerate(node_names):
            print(f"name: {name}, node_num: {node_num}")
            port = deployment.node_port_base + node_num
            listen_addr = f"0.0.0.0:{port}"
            # name = f"node{node_num}"
            domain = f"{name}.pft.org"

            _vm = vms[rr_cnt % len(vms)]
            self.binary_mapping[_vm].append(name)
            
            private_ip = _vm.private_ip
            rr_cnt += 1
            connect_addr = f"{private_ip}:{port}"

            nodelist.append(name[:])
            nodes[name] = {
                "addr": connect_addr,
                "domain": domain
            }
            # if node_num == 1:
            #     self.probableLeader = f"http://{private_ip}:{port + 1000}"
            self.getRequestHosts.append(f"http://{private_ip}:{port + 1000}") # This +1000 is hardcoded in contrib/kms/main.rs

            node_list_for_crypto[name] = (connect_addr, domain)

            config = deepcopy(self.base_node_config)
            config["net_config"]["name"] = name
            config["net_config"]["addr"] = listen_addr
            config["consensus_config"]["log_storage_config"]["RocksDB"]["db_path"] = f"{log_dir}/{name}-db"


            node_configs[name] = config
        
        print(f" node_configs: {node_configs}")
        

        if self.client_region == -1:
            client_vms = deployment.get_all_client_vms()
        else:
            client_vms = deployment.get_all_client_vms_in_region(self.client_region)

        self.client_vms = client_vms[:]

        crypto_info = self.gen_crypto(config_dir, node_list_for_crypto, 0)
        

        for k, v in node_configs.items():
            tls_cert_path, tls_key_path, tls_root_ca_cert_path,\
            allowed_keylist_path, signing_priv_key_path = crypto_info[k]

            v["net_config"]["nodes"] = deepcopy(nodes)
            v["consensus_config"]["node_list"] = nodelist[:]
            v["consensus_config"]["learner_list"] = []
            v["net_config"]["tls_cert_path"] = tls_cert_path
            v["net_config"]["tls_key_path"] = tls_key_path
            v["net_config"]["tls_root_ca_cert_path"] = tls_root_ca_cert_path
            v["rpc_config"]["allowed_keylist_path"] = allowed_keylist_path
            v["rpc_config"]["signing_priv_key_path"] = signing_priv_key_path

            # Only simulate Byzantine behavior in node1.
            if "evil_config" in v and v["evil_config"]["simulate_byzantine_behavior"] and k != "node1":
                v["evil_config"]["simulate_byzantine_behavior"] = False
                v["evil_config"]["byzantine_start_block"] = 0

            with open(os.path.join(config_dir, f"{k}_config.json"), "w") as f:
                json.dump(v, f, indent=4)

        self.getDistribution = self.base_client_config.get("getDistribution", 50)
        self.workers_per_client = self.base_client_config.get("workers_per_client", 1)
        self.total_client_vms = len(client_vms)
        self.total_worker_processes = self.total_client_vms * self.workers_per_client
        self.locust_master = self.client_vms[0]
        self.workload = self.base_client_config.get("workload", "kms")
        # if self.workload == "smallbank":
        #     self.payment_threshold = str(self.base_client_config.get("payment_threshold", 1000))
        # else:
        #     self.payment_threshold = ""

        self.total_machines = self.workers_per_client * self.total_client_vms

        users_per_clients = [self.num_clients // self.total_machines] * self.total_machines
        users_per_clients[-1] += self.num_clients - sum(users_per_clients)
        self.users_per_clients = users_per_clients


        # for client_vm_num in range(len(client_vms)):
        #     for worker_num in range(self.workers_per_client):
        #         machineId = client_vm_num * self.workers_per_client + worker_num
        #         self.binary_mapping[client_vms[client_vm_num]].append(f"client{machineId}")

        # self.binary_mapping[self.locust_master].append("loader")
        # self.binary_mapping[self.locust_master].append("master")

        print(f"binary_mapping: {self.binary_mapping}")
        

        # # Install pip and the dependencies in client vms.
        # for vm in self.client_vms:
        #     run_remote_public_ip([
        #         f"sudo apt-get update",
        #         f"sudo apt-get install -y python3-pip",
        #         f"pip3 install locust",
        #         f"pip3 install locust-plugins[dashboards]",
        #         f"pip3 install aiohttp",
        #         f"pip3 install requests",
        #         f"pip3 install numpy",
        #     ], self.dev_ssh_user, self.dev_ssh_key, vm, hide=False)

    def generate_arbiter_script(self):
        script_lines = [
            "#!/bin/bash",
            "set -e",
            "set -o xtrace",
            "",
            "# This script is generated by the experiment pipeline. DO NOT EDIT.",
            f'SSH_CMD="ssh -o StrictHostKeyChecking=no -i {self.dev_ssh_key}"',
            f'SCP_CMD="scp -o StrictHostKeyChecking=no -i {self.dev_ssh_key}"',
            ""
        ]

        for vm, bin_list in self.binary_mapping.items():
            print(f"vm: {vm}, bin_list: {bin_list}")
            
            # Boot up the nodes first
            for bin in bin_list:
                if "flink" in bin:
                    continue
                elif "yarn" in bin:
                    cmd_block = [
                        f"$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} '",
                        f"  sudo chown -R psladmin:ubuntu /usr/local/hadoop-3.3.3 /usr/local/hadoop && start-yarn.sh \\",
                        f"  {self.remote_workdir}/configs/{bin}_config.json \\",
                        f"  > {self.remote_workdir}/logs/{bin}.log \\",
                        f"  2> {self.remote_workdir}/logs/{bin}.err' &",
                        'PID="$PID $!"',
                        "",
                        "sleep 1",
                    ]
                elif "hdfs" in bin:
                    cmd_block = [
                        f"$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} '",
                        f"  hdfs namenode -format -force -nonInteractive && start-dfs.sh \\",
                        f"  {self.remote_workdir}/configs/{bin}_config.json \\",
                        f"  > {self.remote_workdir}/logs/{bin}.log \\",
                        f"  2> {self.remote_workdir}/logs/{bin}.err' &",
                        'PID="$PID $!"',
                        "",
                        "sleep 1",
                    ]
                elif "metastore" in bin:
                    cmd_block = [
                        f"$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} '",
                        f"  hive --service metastore \\",
                        f"  {self.remote_workdir}/configs/{bin}_config.json \\",
                        f"  > {self.remote_workdir}/logs/{bin}.log \\",
                        f"  2> {self.remote_workdir}/logs/{bin}.err' &",
                        'PID="$PID $!"',
                        "",
                        "sleep 1",
                    ]
                elif "hiveserver2" in bin:
                    cmd_block = [
                        f"$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} '",
                        f"  hive --service hiveservice2 \\",
                        f"  {self.remote_workdir}/configs/{bin}_config.json \\",
                        f"  > {self.remote_workdir}/logs/{bin}.log \\",
                        f"  2> {self.remote_workdir}/logs/{bin}.err' &",
                        'PID="$PID $!"',
                        "",
                        "sleep 1",
                    ]
                else:
                    assert False, f"bin: {bin}"
                script_lines.extend(cmd_block)
        
        with open(os.path.join(self.local_workdir, f"arbiter_0.sh"), "w") as f:
            f.write("\n".join(script_lines) + "\n\n")

           
