from collections import defaultdict
from copy import deepcopy
import json
from experiments import Experiment, copy_file_from_remote_public_ip
from deployment import Deployment
import os
import subprocess
from typing import List
from pathlib import Path

from ssh_utils import run_remote_public_ip, copy_remote_public_ip


class FlinkExperiment(Experiment):

    def extract_service_hosts_public_ip(self):
        nn_host = None
        rm_host = None
        metastore_host = None

        for k, v in self.binary_mapping.items():
            for service_name in v:
                if "yarn" in service_name:
                    rm_host = k.public_ip
                elif "hdfs" in service_name:
                    nn_host = k.public_ip
                elif "metastore" in service_name:
                    metastore_host = k.public_ip
      
        assert nn_host is not None and rm_host is not None and metastore_host is not None, f"nn_host: {nn_host}, rm_host: {rm_host}, metastore_host: {metastore_host}"
        return nn_host, rm_host, metastore_host

    def extract_service_hosts_private_ip(self):
        nn_host = None
        rm_host = None
        metastore_host = None
        hiveserver2_host = None

        for k, v in self.binary_mapping.items():
            for service_name in v:
                if "yarn" in service_name:
                    rm_host = k.private_ip
                elif "hdfs" in service_name:
                    nn_host = k.private_ip
                elif "metastore" in service_name:
                    metastore_host = k.private_ip
                elif "hiveserver2" in service_name:
                    hiveserver2_host = k.private_ip
      
        assert nn_host is not None and rm_host is not None and metastore_host is not None, f"nn_host: {nn_host}, rm_host: {rm_host}, metastore_host: {metastore_host}"
        return nn_host, rm_host, metastore_host, hiveserver2_host


    def remote_build(self):
        remote_repo = f"/home/{self.dev_ssh_user}/repo"

        copied_files = [
            "/usr/local/hadoop/share/hadoop/common/lib/hadoop-auth-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/common/hadoop-common-3.3.3-tests.jar",
            "/usr/local/hadoop/share/hadoop/common/hadoop-common-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/hdfs/lib/hadoop-auth-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/hdfs/hadoop-hdfs-3.3.3-tests.jar",
            "/usr/local/hadoop/share/hadoop/hdfs/hadoop-hdfs-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/hdfs/hadoop-hdfs-client-3.3.3-tests.jar",
            "/usr/local/hadoop/share/hadoop/hdfs/hadoop-hdfs-client-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/hdfs/hadoop-hdfs-httpfs-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/hdfs/hadoop-hdfs-native-client-3.3.3-tests.jar",
            "/usr/local/hadoop/share/hadoop/hdfs/hadoop-hdfs-native-client-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/hdfs/hadoop-hdfs-nfs-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/hdfs/hadoop-hdfs-rbf-3.3.3-tests.jar",
            "/usr/local/hadoop/share/hadoop/hdfs/hadoop-hdfs-rbf-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-app-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-common-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-hs-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-hs-plugins-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.3.3-tests.jar",
            "/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-nativetask-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-shuffle-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-uploader-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-api-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-applications-distributedshell-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-applications-mawo-core-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-applications-unmanaged-am-launcher-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-client-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-common-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-registry-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-server-applicationhistoryservice-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-server-common-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-server-nodemanager-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-server-resourcemanager-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-server-router-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-server-sharedcachemanager-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-server-tests-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-server-timeline-pluginstorage-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-server-web-proxy-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-services-api-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-services-core-3.3.3.jar",      
            "/usr/local/hadoop/share/hadoop/common/lib/hadoop-auth-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/common/hadoop-common-3.3.3-tests.jar",
            "/usr/local/hadoop/share/hadoop/common/hadoop-common-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/hdfs/lib/hadoop-auth-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/hdfs/hadoop-hdfs-3.3.3-tests.jar",
            "/usr/local/hadoop/share/hadoop/hdfs/hadoop-hdfs-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/hdfs/hadoop-hdfs-client-3.3.3-tests.jar",
            "/usr/local/hadoop/share/hadoop/hdfs/hadoop-hdfs-client-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/hdfs/hadoop-hdfs-httpfs-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/hdfs/hadoop-hdfs-native-client-3.3.3-tests.jar",
            "/usr/local/hadoop/share/hadoop/hdfs/hadoop-hdfs-native-client-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/hdfs/hadoop-hdfs-nfs-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/hdfs/hadoop-hdfs-rbf-3.3.3-tests.jar",
            "/usr/local/hadoop/share/hadoop/hdfs/hadoop-hdfs-rbf-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-app-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-common-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-hs-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-hs-plugins-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.3.3-tests.jar",
            "/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-nativetask-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-shuffle-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-uploader-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-api-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-applications-distributedshell-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-applications-mawo-core-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-applications-unmanaged-am-launcher-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-client-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-common-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-registry-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-server-applicationhistoryservice-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-server-common-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-server-nodemanager-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-server-resourcemanager-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-server-router-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-server-sharedcachemanager-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-server-tests-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-server-timeline-pluginstorage-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-server-web-proxy-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-services-api-3.3.3.jar",
            "/usr/local/hadoop/share/hadoop/yarn/hadoop-yarn-services-core-3.3.3.jar",
        ]

        cmds = [
            f"git clone https://github.com/alexthomasv/flink-sql-benchmark.git {remote_repo}/flink-sql-benchmark",
            f"mkdir -p {remote_repo}/flink-sql-benchmark/packages",
            f"cd {remote_repo}/flink-sql-benchmark/packages && wget https://github.com/alexthomasv/hdfs-3.3.3/releases/download/flink/flink-1.16.3-bin-scala_2.12.tgz",
            f"cd {remote_repo}/flink-sql-benchmark/packages && tar -xvf flink-1.16.3-bin-scala_2.12.tgz",
            f"cd {remote_repo}/flink-sql-benchmark/packages && mv ../tools/common/flink-conf.yaml flink-1.16.3/conf/ ",
            f"cd {remote_repo}/flink-sql-benchmark/packages/flink-1.16.3/lib && wget https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-hive-3.1.2_2.12/1.16.3/flink-sql-connector-hive-3.1.2_2.12-1.16.3.jar",
            f"cd {remote_repo}/flink-sql-benchmark/ && mvn clean package -DskipTests -Dflink.version=1.16.3",
        ]
        for jar in copied_files:
            cmds.append(f"cp -n {jar} {remote_repo}/flink-sql-benchmark/packages/flink-1.16.3/lib/")

        try:
            res = run_remote_public_ip(cmds, self.dev_ssh_user, self.dev_ssh_key, self.dev_vm)
            print("*************")
            for line in res:
                print(line)
            print("*************")
        except Exception as e:
            assert False, f"Failed to clone flink-sql-benchmark: {e}"

        # Default YARN/HDFS ports (unchanged)
        NN_RPC_PORT   = 9000
        NN_HTTP_PORT  = 9870
        RM_RPC        = 8032
        RM_SCHED      = 8030
        RM_TRACK      = 8031
        RM_ADMIN      = 8033
        RM_WEB        = 8088
        metastore_port = 9083

        hadoop_conf = Path("/usr/local/hadoop/etc/hadoop")
        hive_conf   = Path("/usr/local/hive/conf")

        nn_host, rm_host, metastore_host, hiveserver2_host = self.extract_service_hosts_private_ip()

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
            <property><name>dfs.namenode.name.dir</name><value>file:///var/lib/hadoop/hdfs/namenode</value></property>
            <property><name>dfs.datanode.data.dir</name><value>file:///var/lib/hadoop/hdfs/datanode</value></property>

            <!-- Bind and advertise NN RPC/UI -->
            <property><name>dfs.namenode.rpc-bind-host</name><value>0.0.0.0</value></property>
            <property><name>dfs.namenode.rpc-address</name><value>{nn_host}:{NN_RPC_PORT}</value></property>
            <property><name>dfs.namenode.http-bind-host</name><value>0.0.0.0</value></property>
            <property><name>dfs.namenode.http-address</name><value>0.0.0.0:{NN_HTTP_PORT}</value></property>
            </configuration>
            """
        # ---------- yarn-site.xml ----------
        yarn_site = f"""\
            <configuration>
            <!-- RM binds to all interfaces but advertises a reachable host -->
            <property><name>yarn.resourcemanager.bind-host</name><value>0.0.0.0</value></property>
            <property><name>yarn.resourcemanager.hostname</name><value>{rm_host}</value></property>
            <property><name>yarn.resourcemanager.address</name><value>{rm_host}:{RM_RPC}</value></property>
            <property><name>yarn.resourcemanager.scheduler.address</name><value>{rm_host}:{RM_SCHED}</value></property>
            <property><name>yarn.resourcemanager.resource-tracker.address</name><value>{rm_host}:{RM_TRACK}</value></property>
            <property><name>yarn.resourcemanager.admin.address</name><value>{rm_host}:{RM_ADMIN}</value></property>
            <property><name>yarn.resourcemanager.webapp.address</name><value>{rm_host}:{RM_WEB}</value></property>

            <!-- Make the NM provide MR shuffle -->
            <property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>
            </configuration>
            """

        # ---------- hive-site.xml ----------
        hive_site = f"""\
            <configuration>
            <!-- Embedded Derby metastore (simple dev setup) -->
            <property>
                <name>javax.jdo.option.ConnectionURL</name>
                <value>jdbc:derby:/home/psladmin/hive-metastore/metastore_db;create=true</value>
            </property>
            <property>
                <name>javax.jdo.option.ConnectionDriverName</name>
                <value>org.apache.derby.jdbc.EmbeddedDriver</value>
            </property>

            <!-- Metastore Thrift URI that HS2/clients will use -->
            <property>
                <name>hive.metastore.uris</name>
                <value>thrift://{metastore_host}:{metastore_port}</value>
            </property>

            <!-- Warehouse (in HDFS) -->
            <property>
                <name>hive.metastore.warehouse.dir</name>
                <value>hdfs://{nn_host}:{NN_RPC_PORT}/user/hive/warehouse</value>
            </property>

            <!-- Safer defaults -->
            <property><name>hive.metastore.schema.verification</name><value>true</value></property>
            <property><name>datanucleus.schema.autoCreateAll</name><value>false</value></property>
            </configuration>
            """

        conf_files_data = [core_site, hdfs_site, yarn_site, hive_site]
        conf_file_names = ["core-site.xml", "hdfs-site.xml", "yarn-site.xml", "hive-site.xml"]
        conf_file_paths = [os.path.join(self.local_workdir, "build", conf_file_name) for conf_file_name in conf_file_names]
        conf_dst_file_paths = [f"/tmp/core-site.xml", f"/tmp/hdfs-site.xml", f"/tmp/yarn-site.xml", f"/tmp/hive-site.xml"]
        assert len(conf_file_paths) == len(conf_dst_file_paths)
        for i, conf_file_data in enumerate(conf_files_data):
            # Create the file in a temporary directory
            with open(conf_file_paths[i], "w") as f:
                print(f"Writing {conf_file_paths[i]}")
                print(conf_file_data)
                f.write(conf_file_data)

        for src_file_path, dst_file_path in zip(conf_file_paths, conf_dst_file_paths):
            print(f"Copying {src_file_path} to {dst_file_path}")
            copy_remote_public_ip(src_file_path, dst_file_path, self.dev_ssh_user, self.dev_ssh_key, self.dev_vm)

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

        for node_num, name in enumerate(node_names):
            print(f"name: {name}, node_num: {node_num}")
            port = deployment.node_port_base + node_num
            listen_addr = f"0.0.0.0:{port}"
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
            self.getRequestHosts.append(f"http://{private_ip}:{port + 1000}") # This +1000 is hardcoded in contrib/kms/main.rs

            node_list_for_crypto[name] = (connect_addr, domain)

            config = deepcopy(self.base_node_config)
            config["net_config"]["name"] = name
            config["net_config"]["addr"] = listen_addr
            config["consensus_config"]["log_storage_config"]["RocksDB"]["db_path"] = f"{log_dir}/{name}-db"

            node_configs[name] = config        

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

        self.total_machines = self.workers_per_client * self.total_client_vms

        users_per_clients = [self.num_clients // self.total_machines] * self.total_machines
        users_per_clients[-1] += self.num_clients - sum(users_per_clients)
        self.users_per_clients = users_per_clients

        print(f"binary_mapping: {self.binary_mapping}")

    def generate_arbiter_script(self):
        nn_host_public, rm_host_public, metastore_host_public = self.extract_service_hosts_public_ip()
        nn_host_private, rm_host_private, metastore_host_private, hiveserver2_host_private = self.extract_service_hosts_private_ip()

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
                        f"  sudo sed -i.bak -e \"s/\\blocalhost\\b/{nn_host_private}/g\" /usr/local/hadoop/etc/hadoop/hdfs-site.xml;",
                        f"  sudo sed -i.bak -e \"s/\\blocalhost\\b/{nn_host_private}/g\" /usr/local/hadoop/etc/hadoop/core-site.xml;",
                        f"  sudo sed -i.bak -e \"s/\\blocalhost\\b/{rm_host_private}/g\" /usr/local/hadoop/etc/hadoop/yarn-site.xml;",
                        f"  /usr/local/hadoop/sbin/start-yarn.sh \\",
                        f"  > {self.remote_workdir}/logs/{bin}.log \\",
                        f"  2> {self.remote_workdir}/logs/{bin}.err' &",
                        'PID="$PID $!"',
                        "",
                        "sleep 1",
                    ]
                elif "hdfs" in bin:
                    cmd_block = [
                        f"$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} '",
                        f"  sudo sed -i.bak -e \"s/\\blocalhost\\b/{nn_host_private}/g\" /usr/local/hadoop/etc/hadoop/hdfs-site.xml;",
                        f"  sudo sed -i.bak -e \"s/\\blocalhost\\b/{nn_host_private}/g\" /usr/local/hadoop/etc/hadoop/core-site.xml;",
                        f"  /usr/local/hadoop/bin/hdfs namenode -format -force -nonInteractive; /usr/local/hadoop/sbin/start-dfs.sh \\",
                        f"  > {self.remote_workdir}/logs/{bin}.log \\",
                        f"  2> {self.remote_workdir}/logs/{bin}.err' &",
                        'PID="$PID $!"',
                        "",
                        "sleep 1",
                    ]
                elif "metastore" in bin:
                    cmd_block = [
                        f"$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} '",
                        f"  sudo sed -i.bak -e \"s/\\blocalhost\\b/{metastore_host_private}/g\" /usr/local/hive/conf/hive-site.xml;",
                        f"  source /etc/profile.d/bigdata_env.sh; schematool -dbType derby -initSchema --verbose; /usr/local/hive/bin/hive --service metastore \\",
                        f"  > {self.remote_workdir}/logs/{bin}.log \\",
                        f"  2> {self.remote_workdir}/logs/{bin}.err' &",
                        'PID="$PID $!"',
                        "",
                        "sleep 1",
                    ]
                elif "hiveserver2" in bin:
                    cmd_block = [
                        f"$SSH_CMD {self.dev_ssh_user}@{vm.public_ip} '",
                        f"  sudo sed -i.bak -e \"s/\\blocalhost\\b/{metastore_host_private}/g\" /usr/local/hive/conf/hive-site.xml;",
                        f"  source /etc/profile.d/bigdata_env.sh; schematool -dbType derby -initSchema --verbose; /usr/local/hive/bin/hive --service hiveserver2 \\",
                        f"  > {self.remote_workdir}/logs/{bin}.log \\",
                        f"  2> {self.remote_workdir}/logs/{bin}.err' &",
                        'PID="$PID $!"',
                        "",
                        "sleep 1",
                    ]
                else:
                    assert False, f"bin: {bin}"
                script_lines.extend(cmd_block)

        script_lines.extend([
            f" sudo sed -i.bak -e \"s/\\blocalhost\\b/{hiveserver2_host_private}/g\" /home/psladmin/repo/flink-sql-benchmark/tools/datagen/init_db_for_partition_tables.sh;"
        ])

        script_lines.extend([
            f"mv -f /tmp/core-site.xml /usr/local/hadoop/etc/hadoop/core-site.xml",
            f"mv -f /tmp/hdfs-site.xml /usr/local/hadoop/etc/hadoop/hdfs-site.xml",
            f"mv -f /tmp/yarn-site.xml /usr/local/hadoop/etc/hadoop/yarn-site.xml",
            f"mv -f /tmp/hive-site.xml /usr/local/hive/conf/hive-site.xml",
        ])

        script_lines.extend([
            f"sudo chmod +x /etc/profile.d/bigdata_env.sh;",
            f". /etc/profile.d/bigdata_env.sh;",
            f"HIVE_BIN=/usr/local/hive/bin/hive /home/psladmin/repo/flink-sql-benchmark/tools/datagen/init_db_for_none_partition_tables.sh 2>&1 > /home/psladmin/repo/data_gen.log",
            f"/home/psladmin/repo/flink-sql-benchmark/packages/flink-1.16.3/bin/yarn-session.sh -d -qu default 2>&1 > /home/psladmin/repo/flink_yarn.log",
        ])
        
        with open(os.path.join(self.local_workdir, f"arbiter_0.sh"), "w") as f:
            f.write("\n".join(script_lines) + "\n\n")

           
