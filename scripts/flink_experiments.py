from collections import defaultdict
from copy import deepcopy
import json
from experiments import Experiment, copy_file_from_remote_public_ip
from deployment import Deployment
import os
import subprocess
from typing import List, Tuple, Dict
from pathlib import Path
from time import sleep
import re

from ssh_utils import run_remote_public_ip, copy_remote_public_ip


class FlinkExperiment(Experiment):

    def extract_service_hosts_public_ip(self):
        nn_host = None
        node1_host = None
        for k, v in self.binary_mapping.items():
            for service_name in v:
                if "hdfs" in service_name:
                    nn_host = k.public_ip
                elif "node1" in service_name:
                    node1_host = k.public_ip
      
        assert nn_host is not None and node1_host is not None, f"nn_host: {nn_host}, node1_host: {node1_host}"
        return nn_host, node1_host


    def extract_flink_leader_private_ip(self):
        for k, v in self.binary_mapping.items():
            for service_name in v:
                if "flink_leader" in service_name:
                    return k.private_ip
        return None

    def extract_flink_hosts_private_ip(self):
        flink_hosts = []
        for k, v in self.binary_mapping.items():
            for service_name in v:
                if "flink_worker" in service_name:
                    flink_hosts.append(k.private_ip)
        return flink_hosts

    def extract_service_hosts_private_ip(self):
        nn_host = None
        node1_host = None
        for k, v in self.binary_mapping.items():
            for service_name in v:
                if "hdfs" in service_name:
                    nn_host = k.private_ip
                elif "node1" in service_name:
                    node1_host = k.private_ip
        assert nn_host is not None and node1_host is not None, f"nn_host: {nn_host}, node1_host: {node1_host}"
        return nn_host, node1_host

    def remote_build_psl(self):
        # If the local build dir is not empty, we assume the build has already been done
        # if len(os.listdir(os.path.join(self.local_workdir, "build"))) > 0:
        #     print("Skipping build for experiment", self.name)
        #     return
        
        with open(os.path.join(self.local_workdir, "git_hash.txt"), "r") as f:
            git_hash = f.read().strip()

        remote_repo = f"/home/{self.dev_ssh_user}/repo/psl-cvm"
        cmds = [
            f"git clone https://github.com/data-capsule/psl-cvm.git {remote_repo}"
        ]
        try:
            run_remote_public_ip(cmds, self.dev_ssh_user, self.dev_ssh_key, self.dev_vm)
        except Exception as e:
            print("Failed to clone repo. It may already exist. Continuing...")
        print("Ran cmds:")
        print(cmds)

        cmds = [
            f"cd {remote_repo} && git checkout psl",       # Move out of DETACHED HEAD state
            f"cd {remote_repo} && git fetch --all --recurse-submodules && git pull --all --recurse-submodules",
        ]
        try:
            run_remote_public_ip(cmds, self.dev_ssh_user, self.dev_ssh_key, self.dev_vm)
        except Exception as e:
            print("Failed to pull repo. Continuing...")
        print("after pull repo")


        # Copy the diff patch to the remote
        copy_remote_public_ip(os.path.join(self.local_workdir, "diff.patch"), f"{remote_repo}/diff.patch", self.dev_ssh_user, self.dev_ssh_key, self.dev_vm)

        # Setup git env
        cmd = []

        # Checkout the git hash and apply the diff 
        cmds = [
            f"cd {remote_repo} && git reset --hard",
            f"cd {remote_repo} && git checkout {git_hash}",
            f"cd {remote_repo} && git submodule update --init --recursive",
            f"cd {remote_repo} && git apply --allow-empty --reject --whitespace=fix diff.patch",
        ]
        
        # Then build       
        cmds.append(
            f"cd {remote_repo} && {self.build_command}"
        )

        print("Executing build commands ****")
        for cmd in cmds:
            print(cmd)
        try:
            run_remote_public_ip(cmds, self.dev_ssh_user, self.dev_ssh_key, self.dev_vm, hide=False)
        except Exception as e:
            print("Failed to build:", e)
            print("\033[91mTry committing the changes and pushing to the remote repo\033[0m")
            exit(1)
        sleep(0.5)

        self.copy_back_build_files()

    def generate_flink_conf(self, config_dir, psl_node, psl_enabled):
        flink_leader_host = self.extract_flink_leader_private_ip()
        flink_conf = f"""\
        jobmanager.rpc.address: {flink_leader_host}
        jobmanager.rpc.port: 6123
        jobmanager.bind-host: 0.0.0.0
        jobmanager.memory.process.size: 1600m

        taskmanager.bind-host: 0.0.0.0
        taskmanager.memory.process.size: 1728m

        # The number of task slots that each TaskManager offers. Each slot runs one parallel pipeline.
        taskmanager.numberOfTaskSlots: 1
        jobmanager.execution.failover-strategy: region

        # io.tmp.dirs: /tmp
        taskmanager.network.dirs: /home/psladmin/flink-tmp/shuffle
        taskmanager.tmp.dirs: /home/psladmin/flink-tmp/tmp

        # make sure BOTH TaskManagers and JobManager use that tmp dir (not /tmp)
        env.java.opts.taskmanager: "-Djava.io.tmpdir=/home/psladmin/flink-tmp/tmp"
        env.java.opts.jobmanager: "-Djava.io.tmpdir=/home/psladmin/flink-tmp/tmp"

        state.backend: rocksdb
        psl.ssl.cert: /home/psladmin/{config_dir}/Pft_root_cert.pem
        psl.ed25519.private-key: /home/psladmin/{config_dir}/client1_signing_privkey.pem
        psl.node.host: {psl_node if psl_enabled else ""}
        psl.node.port: {3001 if psl_enabled else 0}
        psl.lookup.rate: 0.10
        psl.enabled: {"true" if psl_enabled else "false"}
            """
        return flink_conf
    
    def hdfs_generate_conf(self, nn_host):
         # Default HDFS ports (unchanged)
        NN_RPC_PORT   = 9000
        NN_HTTP_PORT  = 9870

        core_site = f"""\
            <configuration>
            <property>
                <name>fs.defaultFS</name>
                <value>hdfs://{nn_host}:{NN_RPC_PORT}</value>
            </property>
            </configuration>
            """

        # ---------- hdfs-site.xml ----------
        hdfs_site = f"""\
            <configuration>
            <property><name>dfs.replication</name><value>1</value></property>
            <property><name>dfs.namenode.safemode.threshold-pct</name><value>0.0</value></property>
            <property><name>dfs.namenode.safemode.min.datanodes</name><value>1</value></property>
            <property><name>dfs.namenode.name.dir</name><value>file:///var/lib/hadoop/hdfs/namenode</value></property>
            <property><name>dfs.datanode.data.dir</name><value>file:///var/lib/hadoop/hdfs/datanode</value></property>

            <!-- Bind and advertise NN RPC/UI -->
            <property><name>dfs.namenode.rpc-bind-host</name><value>0.0.0.0</value></property>
            <property><name>dfs.namenode.rpc-address</name><value>{nn_host}:{NN_RPC_PORT}</value></property>
            <property><name>dfs.namenode.http-bind-host</name><value>0.0.0.0</value></property>
            <property><name>dfs.namenode.http-address</name><value>0.0.0.0:{NN_HTTP_PORT}</value></property>
            </configuration>
            """
        return core_site, hdfs_site


    def remote_build(self):
        remote_repo = f"/home/{self.dev_ssh_user}/repo"
        print("remote_build ****")
        self.remote_build_psl()

        cmds = [
            f"git clone https://github.com/alexthomasv/flink-psl.git {remote_repo}/flink-psl",
            f"mkdir -p {remote_repo}/flink-psl/traces",
            f"cd {remote_repo}/flink-psl/traces && wget https://fiu-trace.s3.us-east-2.amazonaws.com/write-heavy.blkparse",
        ]
       
        print("before flink-psl remote build")
        try:
            res = run_remote_public_ip(cmds, self.dev_ssh_user, self.dev_ssh_key, self.dev_vm)
            print("*************")
            for line in res:
                print(line)
            print("*************")
        except Exception as e:
            assert False, f"Failed to clone flink-sql-benchmark: {e}"

    def copy_back_build_files(self):
        remote_repo = f"/home/{self.dev_ssh_user}/repo/psl-cvm"
        TARGET_BINARIES = ["client", "server", "net-perf"]

        # Copy the target/release to build directory
        for bin in TARGET_BINARIES:
            print(f"Copying {remote_repo}/target/release/{bin} to {os.path.join(self.local_workdir, 'build', bin)}")
            copy_file_from_remote_public_ip(f"{remote_repo}/target/release/{bin}", os.path.join(self.local_workdir, "build", bin), self.dev_ssh_user, self.dev_ssh_key, self.dev_vm)


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

        flink_worker_addrs = []
        for node_num in range(1, self.num_nodes+1):
            port += 1
            listen_addr = f"0.0.0.0:{port}"
            name = f"node{node_num}"
            flink_name = f"flink_worker{node_num}"
            domain = f"{name}.pft.org"

            _vm = node_vms[rr_cnt % len(node_vms)]
            self.binary_mapping[_vm].append(name)
            self.binary_mapping[_vm].append(flink_name)
            if node_num == 1:
                self.binary_mapping[_vm].append("flink_leader")
            
            private_ip = _vm.private_ip
            rr_cnt += 1
            connect_addr = f"{private_ip}:{port}"
            flink_worker_addrs.append(private_ip)

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
            config["worker_config"]["state_storage_config"]["RocksDB"]["db_path"] = str(data_dir)

            node_configs[name] = config

            with open(os.path.join(config_dir, f"flink-conf_{node_num}.yaml"), "w") as f:
                connect_addr = nodes[name]["addr"]
                name_host = connect_addr.split(":")[0]
                flink_conf = self.generate_flink_conf(config_dir, name_host, True)
                f.write(flink_conf)
            
            with open(os.path.join(config_dir, f"flink-conf_{node_num}_psl_disabled.yaml"), "w") as f:
                connect_addr = nodes[name]["addr"]
                name_host = connect_addr.split(":")[0]
                flink_conf = self.generate_flink_conf(config_dir, name_host, False)
                f.write(flink_conf)

        
        with open(os.path.join(config_dir, "workers"), "w") as f:
            f.write("\n".join(flink_worker_addrs))

        for node_num in range(1, self.num_storage_nodes+1):
            port += 1
            listen_addr = f"0.0.0.0:{port}"
            name = f"storage{node_num}"
            domain = f"{name}.pft.org"

            _vm = storage_vms[rr_cnt % len(storage_vms)]
            self.binary_mapping[_vm].append(name)
            if node_num == 1:
                self.binary_mapping[_vm].append("hdfs")
                nn_host = _vm.private_ip
                core_site, hdfs_site = self.hdfs_generate_conf(nn_host)
                with open(os.path.join(config_dir, "core-site.xml"), "w") as f:
                    f.write(core_site)
                with open(os.path.join(config_dir, "hdfs-site.xml"), "w") as f:
                    f.write(hdfs_site)

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

        for node_num in range(1, self.num_sequencer_nodes+1):
            port += 1
            listen_addr = f"0.0.0.0:{port}"
            name = f"sequencer{node_num}"
            domain = f"{name}.pft.org"
            
            _vm = sequencer_vms[rr_cnt % len(sequencer_vms)]
            self.binary_mapping[_vm].append(name)
            
            private_ip = _vm.private_ip
            rr_cnt += 1
            connect_addr = f"{private_ip}:{port}"
            
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

            

        if self.client_region == -1:
            client_vms = deployment.get_all_client_vms()
        else:
            client_vms = deployment.get_all_client_vms_in_region(self.client_region)

        crypto_info = self.gen_crypto(config_dir, node_list_for_crypto, len(client_vms))
        
        print("Worker names", worker_names)
        print("Storage names", storage_names)
        print("Sequencer names", sequencer_names)

        gossip_downstream_worker_list = {}
        sequencer_watchlist_map = self.generate_watchlists(sequencer_names, worker_names)

        print("Gossip downstream worker list", gossip_downstream_worker_list)

        for k, v in node_configs.items():
            tls_cert_path, tls_key_path, tls_root_ca_cert_path,\
            allowed_keylist_path, signing_priv_key_path = crypto_info[k]

            v["net_config"]["nodes"] = deepcopy(nodes)

            if k in sequencer_names:
                v["consensus_config"]["node_list"] = storage_names[:]
            else:
                v["consensus_config"]["node_list"] = nodelist[:]

            # if k in worker_names:
            #     v["worker_config"]["gossip_downstream_worker_list"] = gossip_downstream_worker_list[k]
            # else:
            v["worker_config"]["gossip_downstream_worker_list"] = []

            # if True or k == storage_names[0]:
            v["consensus_config"]["learner_list"] = sequencer_names[:]

            if k in sequencer_names:
                v["consensus_config"]["watchlist"] = sequencer_watchlist_map.get(k, [])

            v["net_config"]["tls_cert_path"] = tls_cert_path
            v["net_config"]["tls_key_path"] = tls_key_path
            v["net_config"]["tls_root_ca_cert_path"] = tls_root_ca_cert_path
            v["rpc_config"]["allowed_keylist_path"] = allowed_keylist_path
            v["rpc_config"]["signing_priv_key_path"] = signing_priv_key_path
            v["worker_config"]["all_worker_list"] = worker_names[:]
            v["worker_config"]["storage_list"] = storage_names[:] 
            v["worker_config"]["sequencer_list"] = sequencer_names[:]

            # Only simulate Byzantine behavior in node1.
            if "evil_config" in v and v["evil_config"]["simulate_byzantine_behavior"] and k != "node1":
                v["evil_config"]["simulate_byzantine_behavior"] = False
                v["evil_config"]["byzantine_start_block"] = 0

            with open(os.path.join(config_dir, f"{k}_config.json"), "w") as f:
                json.dump(v, f, indent=4)

        num_clients_per_vm = [self.num_clients // len(client_vms) for _ in range(len(client_vms))]
        num_clients_per_vm[-1] += (self.num_clients - sum(num_clients_per_vm))

        client_start_index = 0
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
            config["workload_config"]["start_index"] = client_start_index
            client_start_index += num_clients_per_vm[client_num]

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


    def generate_kill_block(self, bin, vm, binary_name):
        return [
            f"$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} 'sudo pkill -2 -c {binary_name}' || true",
            f"$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} 'sudo pkill -15 -c {binary_name}' || true",
            f"$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} 'sudo pkill -9 -c {binary_name}' || true",
            f"$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} 'sudo rm -rf /data/*' || true",
            f"$SCP_CMD {self.dev_ssh_user}@{vm.public_ip}:{self.remote_workdir}/logs/{bin}.log {self.remote_workdir}/logs/{bin}.log || true",
            f"$SCP_CMD {self.dev_ssh_user}@{vm.public_ip}:{self.remote_workdir}/logs/{bin}.err {self.remote_workdir}/logs/{bin}.err || true",
        ]

    def generate_arbiter_script(self):
        psl_enabled = bool(self.base_client_config["psl_enabled"])
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

        copy_script_block = [
            f"sudo cp -f /home/psladmin/{self.local_workdir}/configs/hdfs-site.xml /usr/local/hadoop/etc/hadoop/hdfs-site.xml;",
            f"sudo cp -f /home/psladmin/{self.local_workdir}/configs/core-site.xml /usr/local/hadoop/etc/hadoop/core-site.xml;",
            f"cp -f /home/psladmin/{self.local_workdir}/configs/core-site.xml /home/psladmin/flink-psl/build-target/conf/core-site.xml;",
            f"cp -f /home/psladmin/{self.local_workdir}/configs/hdfs-site.xml /home/psladmin/flink-psl/build-target/conf/hdfs-site.xml;",
        ]

        for repeat_num in range(self.repeats):
            for vm, bin_list in self.binary_mapping.items():
                ssh_command_prefix = f"$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} '"
                # Boot up the nodes first
                for bin in bin_list:
                    log_dump_lines = [
                        f"> {self.remote_workdir}/logs/{repeat_num}/{bin}.log \\",
                        f"2> {self.remote_workdir}/logs/{repeat_num}/{bin}.err' &",
                        'PID="$PID $!"',
                        "",
                    ]
                    if "flink_leader" in bin:
                        cmd_block = [
                            ssh_command_prefix,
                            f"sudo chmod +x /etc/profile.d/bigdata_env.sh;",
                            f". /etc/profile.d/bigdata_env.sh;",
                            f"sudo cp -f /home/psladmin/{self.local_workdir}/configs/flink-conf_1{'_psl_disabled' if not psl_enabled else ''}.yaml /home/psladmin/flink-psl/build-target/conf/flink-conf.yaml;",
                            *copy_script_block,
                            f"/home/psladmin/flink-psl/build-target/bin/jobmanager.sh start \\",
                            *log_dump_lines,
                            "sleep 1",
                        ]
                    elif "flink_worker" in bin:
                        # flink-conf_{node_num}.yaml => node_num
                        extract_flink_node_num = int(re.search(r'^flink_worker(\d+)$', bin).group(1))
                        print(f"extract_flink_node_num: {extract_flink_node_num}")
                        cmd_block = [
                            ssh_command_prefix,
                            f"sudo chmod +x /etc/profile.d/bigdata_env.sh;",
                            f". /etc/profile.d/bigdata_env.sh;",
                            f"sudo cp -f /home/psladmin/{self.local_workdir}/configs/flink-conf_{extract_flink_node_num}{'_psl_disabled' if not psl_enabled else ''}.yaml /home/psladmin/flink-psl/build-target/conf/flink-conf.yaml;",
                            *copy_script_block,
                            f"/home/psladmin/flink-psl/build-target/bin/taskmanager.sh start \\",
                            *log_dump_lines,
                            "sleep 1",
                        ]
                    elif "hdfs" in bin:
                        cmd_block = [
                            ssh_command_prefix,
                            *copy_script_block,
                            # --- idempotent HDFS bootstrap ---
                            # Pick NN dir from config (first path), fall back if unset; format only if VERSION missing
                            "  if [ ! -f /var/lib/hadoop/hdfs/namenode/current/VERSION ]; then",
                            "    /usr/local/hadoop/bin/hdfs namenode -format -force -nonInteractive",
                            "  fi",
                            "  /usr/local/hadoop/sbin/start-dfs.sh \\",
                            *log_dump_lines,
                            "sleep 5",
                        ]
                    elif "node" in bin:
                        binary_name = "server worker"
                        cmd_block = [
                            ssh_command_prefix,
                            f" sudo {self.remote_workdir}/build/{binary_name} {self.remote_workdir}/configs/{bin}_config.json \\",
                            *log_dump_lines,
                            "sleep 1",
                        ]
                    elif "storage" in bin:
                        binary_name = "server storage"
                        cmd_block = [
                            ssh_command_prefix,
                            f"  sudo {self.remote_workdir}/build/{binary_name} {self.remote_workdir}/configs/{bin}_config.json \\",
                            *log_dump_lines,
                            "sleep 1",
                        ]
                    elif "sequencer" in bin:
                        binary_name = "server sequencer"
                        cmd_block = [
                            ssh_command_prefix,
                            f"sudo {self.remote_workdir}/build/{binary_name} {self.remote_workdir}/configs/{bin}_config.json \\",
                            *log_dump_lines,
                            "sleep 1",
                        ]
                    elif "client" in bin:
                        binary_name = "client"
                        cmd_block = [
                            ssh_command_prefix,
                            f"sudo {self.remote_workdir}/build/{binary_name} {self.remote_workdir}/configs/{bin}_config.json \\",
                            *log_dump_lines,
                            "sleep 1",
                        ]
                    else:
                        assert False, f"bin: {bin}"
                    script_lines.extend(cmd_block)

            remote_repo = f"/home/{self.dev_ssh_user}/repo"

            script_lines.extend([
                *copy_script_block,
                f"cp -f /home/psladmin/{self.local_workdir}/configs/flink-conf_1{'_psl_disabled' if not psl_enabled else ''}.yaml /home/psladmin/flink-psl/build-target/conf/flink-conf.yaml",
                f"cp -f /home/psladmin/{self.local_workdir}/configs/workers /home/psladmin/flink-psl/build-target/conf/workers",
            ])

            script_lines.extend([
                f"sudo chmod +x /etc/profile.d/bigdata_env.sh;",
                f". /etc/profile.d/bigdata_env.sh;",
                f"/usr/local/hadoop/bin/hdfs dfsadmin -safemode leave || true",
                f"/usr/local/hadoop/bin/hdfs dfs -mkdir -p /datasets/fiu",
                f"/usr/local/hadoop/bin/hdfs dfs -put -f {remote_repo}/flink-psl/traces/write-heavy.blkparse  /datasets/fiu/",
            ])

            flink_leader_host = self.extract_flink_leader_private_ip()
            script_lines.extend([
                f"/home/psladmin/flink-psl/build-target/bin/flink run \\",
                f"  -m {flink_leader_host}:8081 \\",
                f"  -p {self.num_nodes} \\",
                f"  -c com.example.dedup.DedupRefCountBenchmark \\",
                f"  /home/psladmin/flink-psl/build-target/lib/flink-dedup-bench-1.16.3.jar",
            ])
            
            script_lines.extend([
                f"hdfs dfs -get -f /results {self.remote_workdir}/results_{repeat_num}.txt",
            ])

            
            for vm, bin_list in self.binary_mapping.items():            
                # Boot up the nodes first
                for bin in bin_list:
                    if "flink_leader" in bin:
                        cmd_block = [
                            ssh_command_prefix,
                            f"/home/psladmin/flink-psl/build-target/bin/jobmanager.sh stop \\",
                            "sleep 1",
                        ]
                    elif "flink_worker" in bin:
                        # flink-conf_{node_num}.yaml => node_num
                        cmd_block = [
                            ssh_command_prefix,
                            f"/home/psladmin/flink-psl/build-target/bin/taskmanager.sh stop \\",
                            "sleep 1",
                        ]
                    elif "hdfs" in bin:
                        cmd_block = [
                            ssh_command_prefix,
                            f"/usr/local/hadoop/sbin/stop-dfs.sh \\",
                            "sleep 5",
                        ]
                    elif "node" in bin:
                        binary_name = "server"
                        cmd_block = self.generate_kill_block(bin, vm, binary_name)
                    elif "storage" in bin:
                        binary_name = "server"
                        cmd_block = self.generate_kill_block(bin, vm, binary_name)
                    elif "sequencer" in bin:
                        binary_name = "server"
                        cmd_block = self.generate_kill_block(bin, vm, binary_name)
                    elif "client" in bin:
                        binary_name = "client"
                        cmd_block = self.generate_kill_block(bin, vm, binary_name)
                    else:
                        assert False, f"bin: {bin}"
                    script_lines.extend(cmd_block)

            with open(os.path.join(self.local_workdir, f"arbiter_{repeat_num}.sh"), "w") as f:
                f.write("\n".join(script_lines) + "\n\n")

    def get_vms(self, deployment: Deployment) -> Tuple[List, List, List]:
        if self.node_distribution == "uniform":
            node_vms = deployment.get_all_node_vms()
        elif self.node_distribution == "sev_only":
            node_vms = deployment.get_nodes_with_tee("sev")
        elif self.node_distribution == "tdx_only":
            node_vms = deployment.get_nodes_with_tee("tdx")
        elif self.node_distribution == "nontee_only":
            node_vms = deployment.get_nodes_with_tee("nontee")
        elif self.node_distribution.startswith("tag:"):
            node_vms = deployment.get_nodes_with_tag(self.node_distribution.split(":")[1])
        else:
            node_vms = deployment.get_wan_setup(self.node_distribution)
        
        if self.storage_distribution == "uniform":
            storage_vms = deployment.get_all_storage_vms()
        elif self.storage_distribution == "sev_only":
            storage_vms = deployment.get_nodes_with_tee("sev")
        elif self.storage_distribution == "tdx_only":
            storage_vms = deployment.get_nodes_with_tee("tdx")
        elif self.storage_distribution == "nontee_only":
            storage_vms = deployment.get_nodes_with_tee("nontee")
        elif self.storage_distribution.startswith("tag:"):
            storage_vms = deployment.get_nodes_with_tag(self.storage_distribution.split(":")[1])
        else:
            storage_vms = deployment.get_wan_setup(self.storage_distribution)
        
        if self.sequencer_distribution == "uniform":
            sequencer_vms = deployment.get_all_node_vms()
        elif self.sequencer_distribution == "sev_only":
            sequencer_vms = deployment.get_nodes_with_tee("sev")
        elif self.sequencer_distribution == "tdx_only":
            sequencer_vms = deployment.get_nodes_with_tee("tdx")
        elif self.sequencer_distribution == "nontee_only":
            sequencer_vms = deployment.get_nodes_with_tee("nontee")
        elif self.sequencer_distribution.startswith("tag:"):
            sequencer_vms = deployment.get_nodes_with_tag(self.sequencer_distribution.split(":")[1])
        else:
            sequencer_vms = deployment.get_wan_setup(self.sequencer_distribution)
        
        return node_vms, storage_vms, sequencer_vms
    
    def generate_multicast_tree(self, worker_names: List[str], fanout: int) -> Dict[str, List[str]]:
        """
        Generate a multicast tree for the given worker names with a given fanout.
        Returns the gossip_downstream_worker_list for each worker.
        """

        ret = {}
        i = 0

        # Assume that the list is the flattened complete tree.
        # The children of index i are i * fanout + 1, i * fanout + 2, ..., i * fanout + fanout.
        # The parent of index i is (i - 1) // fanout.
        # Each worker sends to its children and its parent.

        while i < len(worker_names):
            curr = worker_names[i]
            children = [worker_names[i * fanout + j] for j in range(1, fanout + 1) if i * fanout + j < len(worker_names)]
            parent = (i - 1) // fanout
            ret[curr] = children[:]
            if parent >= 0:
                ret[curr].append(worker_names[parent])
            i += 1

        return ret
        # return {k: [] for k in worker_names}
    
    def generate_watchlists(self, sequencer_names: List[str], worker_names: List[str]) -> Dict[str, List[str]]:
        """
        Generate a watchlist for each sequencer.
        If there is only one sequencer, it gets the first 4 workers.
        Otherwise, worker_names split evenly between the sequencers.
        """
        ret = defaultdict(list)
        if len(sequencer_names) == 1:
            ret[sequencer_names[0]] = worker_names[:4]
            return dict(ret)

        curr_sequencer_idx = 0
        for worker_name in worker_names:
            ret[sequencer_names[curr_sequencer_idx]].append(worker_name)
            curr_sequencer_idx = (curr_sequencer_idx + 1) % len(sequencer_names)
        return dict(ret)

    def done(self):
        return False
