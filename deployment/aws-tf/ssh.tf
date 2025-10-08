output "cluster_private_key" {
    value = tls_private_key.ssh_key.private_key_pem
    sensitive = true
}

output "cluster_public_key" {
    value = tls_private_key.ssh_key.public_key_pem
}

output "clientpool_machine_list" {
    value = [for i in aws_instance.clientpool[*] : [i.tags["Name"], i.private_ip, i.public_ip, i.tags["Job"]]]
}

output "storagepool_machine_list" {
    value = [for i in aws_instance.storagepool[*] : [i.tags["Name"], i.private_ip, i.public_ip, i.tags["Job"]]]
}

output "sevpool_machine_list" {
    value = [for i in aws_instance.sevpool[*] : [i.tags["Name"], i.private_ip, i.public_ip, i.tags["Job"]]]
}

output "machine_list" {
    value = concat(
        [for i in aws_instance.clientpool[*] : [i.tags["Name"], i.private_ip, i.public_ip, i.tags["Job"]]],
        [for i in aws_instance.storagepool[*] : [i.tags["Name"], i.private_ip, i.public_ip, i.tags["Job"]]],
        [for i in aws_instance.sevpool[*] : [i.tags["Name"], i.private_ip, i.public_ip, i.tags["Job"]]]
    )
}