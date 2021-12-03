FROM jrei/systemd-centos:7

COPY CloudHashingStaker.viper.yaml /etc/CloudHashingStaker/CloudHashingStaker.viper.yaml
COPY cloud-hashing-staker /usr/local/bin/cloud-hashing-staker

CMD ["cloud-hashing-staker", "run"]
