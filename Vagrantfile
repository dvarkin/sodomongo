# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/trusty64"
  config.vm.box_check_update = false

  config.vm.synced_folder "..", "/vagrant"

  config.ssh.private_key_path=["~/.ssh/id_rsa", "~/.vagrant.d/insecure_private_key"]
  config.ssh.insert_key=false
  config.vm.provision "file", source: "~/.ssh/id_rsa.pub", destination: "~/.ssh/authorized_keys"
  
  ENV['LC_ALL']="en_US.UTF-8"


  config.vm.define "master" do |node|
    node.vm.hostname = "master"
    node.vm.network "private_network", ip: "192.168.144.110"
    
    node.vm.provider "virtualbox" do |vb|
      vb.memory = "512"
      vb.cpus = 1
    end

    node.vm.provision "shell", inline: <<-SHELL
    SHELL
  end

  
  config.vm.define "w1" do |node|
    node.vm.hostname = "slave-1"
    node.vm.network "private_network", ip: "192.168.144.111"
    
    node.vm.provider "virtualbox" do |vb|
      vb.memory = "512"
      vb.cpus = 1
    end

    node.vm.provision "shell", inline: <<-SHELL
    SHELL
  end
  
  config.vm.define "w2" do |node|
    node.vm.hostname = "slave-2"    
    node.vm.network "private_network", ip: "192.168.144.112"
    
    node.vm.provider "virtualbox" do |vb|
      vb.memory = "256"
      vb.cpus = 1
    end
    
    node.vm.provision "shell", inline: <<-SHELL
    SHELL
  end
  
  config.vm.define "w3" do |node|
    node.vm.hostname = "slave-3"    
    node.vm.network "private_network", ip: "192.168.144.113"
    
    node.vm.provider "virtualbox" do |vb|
      vb.memory = "256"
      vb.cpus = 1
    end
  end
  
end
