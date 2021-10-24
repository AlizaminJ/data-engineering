```
# will be using a Debian Droplet by DigitalOcean. After having created the server, open the terminal
```

```
# ban users from logging after a certain time of faulty login attempts
apt-get install fail2ban
```

```
# create a new user with sudo privileges
adduser deploy
```

```
# check the new directory
ls -la /home/deploy
```

```
# create ssh directory and copy ssh content from root (we need ssh, not bash) to make sure that we login with ssh
mkdir /home/deploy/.ssh
cp /root/.ssh/authorized_keys /home/deploy/.ssh
```

```
# to make sure that only owner can read ssh file and execute on it
chmod 700 /home/deploy/.ssh
```

```
# deploy-user owns the entire directory since as a root user we move things and make sure that deploy has access to its own directory
chown deploy:deploy -R /home/deploy/
```

```
# add sudo privileges (may use vim instead of vim: https://www.vimgolf.com/, I am a nano-fan)
export EDITOR=nano
visudo
#and add the following
deploy  ALL=(ALL:ALL) ALL
```

```
# turn off ssh root login and test our deploy-user
nano /etc/ssh/sshd_config
# pay attention that "Port 22" is open (not sure if "PubkeyAuthentication yes" should be uncommented)
# change rootLogin
"PermitRootLogin no"
# then explicitly say that password auth is not allowed by changing
"PasswordAuthentication no"
# to allow which users can login (may skip it if you are using a server with lots of users)
AllowUsers deploy
```

```
# restart ssh
service ssh restart
#OR
systemctl restart sshd
```
