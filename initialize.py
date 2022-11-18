Output = open(f'output/traderinfo.txt', mode='w')
Output.close()
Output = open(f'output/traderlog.txt', mode='w')
Output.close()
peers = ['1:127.0.0.1:8001\n', '2:127.0.0.1:8002\n', '3:127.0.0.1:8003\n', '4:127.0.0.1:8004\n', '5:127.0.0.1:8005\n', '6:127.0.0.1:8006\n']
with open('./config', 'w') as f:
    f.writelines(peers)
    f.close()