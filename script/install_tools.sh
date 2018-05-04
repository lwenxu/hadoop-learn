#!/bin/bash

# vim
apt install vim -y
# wget
apt install wget -y
# curl
apt install curl -y
# git
apt install git -y
# zsh
apt install zsh -y
chsh -s /bin/zsh
# oh my zsh
sh -c "$(wget https://raw.github.com/robbyrussell/oh-my-zsh/master/tools/install.sh -O -)"
# ftp
apt install vsftpd -y

