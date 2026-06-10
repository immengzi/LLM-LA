sudo mkdir -p /tmp/modeltest
sudo mount -t nfs -o ro,nfsvers=4.1 7.242.102.243:/ /tmp/modeltest
ls -la /tmp/modeltest | head
sudo umount /tmp/modeltest