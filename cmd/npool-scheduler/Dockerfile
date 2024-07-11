FROM uhub.service.ucloud.cn/entropypool/alpine:3.16

COPY NpoolScheduler.viper.yaml /etc/NpoolScheduler/NpoolScheduler.viper.yaml
COPY npool-scheduler /usr/local/bin/npool-scheduler

CMD ["npool-scheduler", "run"]
