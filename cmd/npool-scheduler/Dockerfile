FROM uhub.service.ucloud.cn/entropypool_public/systemd-centos:7

COPY NpoolScheduler.viper.yaml /etc/NpoolScheduler/NpoolScheduler.viper.yaml
COPY npool-scheduler /usr/local/bin/npool-scheduler

CMD ["npool-scheduler", "run"]
