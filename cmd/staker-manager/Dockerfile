FROM jrei/systemd-centos:7

COPY StakerManager.viper.yaml /etc/StakerManager/StakerManager.viper.yaml
COPY staker-manager /usr/local/bin/staker-manager

CMD ["staker-manager", "run"]
