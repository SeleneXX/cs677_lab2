import collections
import random
import socket
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import heapq
import datetime

lock = threading.RLock()

class Peer(object):

    def __init__(self, address, peer_id):
        # address = (IP, port)
        self.address = address
        self.peer_id = peer_id
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind(address)
        self.server.listen(10000)
        self.buyID = 0
        self.sellID = 0
        self.buyNum = 0
        self.sellNum = 0
        self.traderaddress = None
        self.istrader = False
        self.traderList = collections.defaultdict(list)
        self.clock = 0
        self.is_electing = False
        self.isBuyer = 0
        self.isSeller = 0
        self.requestQ = []

    def random(self, a, b):
        # Random = random.randint(0, 2)
        # if Random == 0:
        #     self.isBuyer = 1
        # elif Random == 1:
        #     self.isSeller = 1
        # else:
        #     self.isBuyer = self.isSeller = 1
        self.isBuyer = a
        self.isSeller = b

    def trader_write(self):
        Output = open(f'output/traderinfo.txt', mode='w')
        for prodID, prodInfo in self.traderList.items():
            for prod in prodInfo:
                addr, prodNum, SellerID = prod
                addr = f'{addr[0]}-{addr[1]}'
                data = f'{prodID}|{addr}|{prodNum}|{SellerID}\n'
                Output.write(data)
        Output.close()

    def trader_read(self):
        with open(f'output/traderinfo.txt') as f:
            for line in f:
                prodID, addr, prodNum, SellerID = line.split('|')
                addr = addr.split('-')
                prodNum = int(prodNum)
                self.traderList[prodID].append((addr, prodNum, SellerID))
            f.close()

    def trader_process(self, conn):
        request = conn.recv(1024)
        data = request.decode('utf-8')
        fields = data.split('|')
        if fields[0] == '4' or fields[0] == '5':
            lock.acquire()
            heapq.heappush(self.requestQ, (int(fields[4]), fields))
            fields = heapq.heappop(self.requestQ)[1]
            lock.release()
        # print(fields)

        if fields[0] == '4':
            # receive a buy request
            # request_category|productID|quantity|addr|clock|peerID
            lock.acquire()
            self.clock = max(self.clock, int(fields[4])) + 1
            lock.release()
            prodID, prodNum = fields[1], int(fields[2])
            print(f'Try to sell {prodNum} productID{prodID}...')
            # print(f'Ori {fields[2]}')
            while prodNum > 0:
                if self.traderList[prodID]:
                    lock.acquire()
                    next_seller = self.traderList[prodID].pop()
                    self.clock += 1
                    lock.release()
                    # print(prodNum, next_seller[1])
                    if next_seller[1] <= prodNum:
                        prodNum -= next_seller[1]

                        data = f'3|{prodID}|{next_seller[1]}|{self.clock}'
                        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        client.connect((next_seller[0][0], int(next_seller[0][1])))
                        client.send(data.encode('utf-8'))
                        # client.recv(1024)
                        client.close()
                        lock.acquire()
                        Output = open(f'output/traderlog.txt', mode='a')
                        now = datetime.datetime.now()
                        Output.write(f'{now.strftime("%Y-%m-%d %H:%M:%S")}, Peer{fields[5]} purchase {next_seller[1]} product{prodID} from Peer{next_seller[2]}!\n')
                        Output.close()
                        lock.release()
                    else:
                        data = f'3|{prodID}|{prodNum}|{self.clock}'
                        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        client.connect((next_seller[0][0], int(next_seller[0][1])))
                        client.send(data.encode('utf-8'))
                        # client.recv(1024)
                        client.close()
                        lock.acquire()
                        Output = open(f'output/traderlog.txt', mode='a')
                        now = datetime.datetime.now()
                        Output.write(f'{now.strftime("%Y-%m-%d %H:%M:%S")}, Peer{fields[5]} purchase {prodNum} product{prodID} from Peer{next_seller[2]}!\n')
                        Output.close()
                        lock.release()
                        self.traderList[prodID].append((next_seller[0], next_seller[1] - prodNum, next_seller[2]))
                        prodNum = 0
                else:
                    break
                lock.acquire()
                self.trader_write()
                lock.release()
            lock.acquire()
            self.trader_write()
            lock.release()
            # reply buyer
            replyaddr = fields[3].split('-')
            lock.acquire()
            self.clock += 1
            lock.release()
            # print(f'ProdNum {prodNum}')
            data = f'2|{prodID}|{prodNum}|{self.clock}'
            # print(data)
            conn.send(data.encode('utf-8'))
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect((replyaddr[0], int(replyaddr[1])))
            client.send(data.encode('utf-8'))
            # client.recv(1024)
        if fields[0] == '5':
            # store as: productID:[(address, quantity, peerID)]
            self.is_electing = False
            prodID, prodNum, addr, SellerID = fields[1], int(fields[2]), fields[3].split('-'), fields[5]
            self.traderList[prodID].append((addr, prodNum, SellerID))
            print(f'Update stock information of product{prodID}: {prodNum}.')
            lock.acquire()
            self.trader_write()
            lock.release()
        conn.close()


    def buyer_process(self):
        myaddr = f'{self.address[0]}-{self.address[1]}'
        # if product number is 0, then random a product to buy
        if self.buyNum <= 0:
            self.buyID = random.randint(0, 2)
            # while self.buyID == self.sellID:
            #     self.buyID = random.randint(0, 2)
            # self.buyID = 0
            self.buyNum = random.randint(1, 10)
            print(f'Purchase complete. Now buying {self.buyNum} product{self.buyID}.')
        # send buy request
        # request_catagory|product_ID|quantity|address|clock|peerID
        self.clock += 1
        # print(self.buyNum)
        data = f'4|{self.buyID}|{self.buyNum}|{myaddr}|{self.clock}|{self.peer_id}'
        # print(data)
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            client.connect((self.traderaddress[0], int(self.traderaddress[1])))
            client.send(data.encode('utf-8'))
            reply = client.recv(1024)
            data = reply.decode('utf-8')
            fields = data.split('|')

            self.clock = max(self.clock, int(fields[3])) + 1
            if int(fields[2]) == self.buyNum:
                print(f'Product{self.buyID} not in stock')
            else:
                print(self.buyNum, fields[2])
                print(fields)
                buy = self.buyNum - int(fields[2])
                self.buyNum = int(fields[2])
                print(f'Sucessfully purchase {buy} product{self.buyID}')
        except:
            self.traderaddress = None
            self.election()
        client.close()


    def seller_process(self):
        myaddr = f'{self.address[0]}-{self.address[1]}'
        # if product number is 0, then random a product to sell
        if self.sellNum <= 0:
            self.sellID = random.randint(0, 2)
            # while self.sellID == self.buyID:
            #     self.sellID = random.randint(0, 2)
            # self.sellID = 1
            self.sellNum = random.randint(1, 10)
            print(f'Sell complete. Now selling {self.sellNum} porduct{self.sellID}.')
        # send stock information
        # request_catagory|product_ID|quantity|address|clock|peerID
            data = f'5|{self.sellID}|{self.sellNum}|{myaddr}|{self.clock}|{self.peer_id}'
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                client.connect((self.traderaddress[0], int(self.traderaddress[1])))
                client.send(data.encode('utf-8'))
            except:
                self.traderaddress = None
                self.election()
            client.close()


    def election(self):
        if not self.is_electing:
            print('Peer {} starting election.'.format(self.peer_id))
            self.is_electing = True
            alive_peer = []
            larger_peer = []
            with open('./config') as f:
                for line in f:
                    fields = line.split(':')
                    if int(fields[0]) > self.peer_id:
                        larger_peer.append((fields[1], int(fields[2])))
                    if int(fields[0]) != self.peer_id:
                        alive_peer.append(line)
                f.close()

            if len(larger_peer) == 0:
                self.istrader = True
                Output = open(f'output/traderlog.txt', mode='a')
                now = datetime.datetime.now()
                Output.write(
                    f'{now.strftime("%Y-%m-%d %H:%M:%S")}, I\'m Peer{self.peer_id}, I\'m the new trader!\n')
                Output.close()
                for peer in alive_peer:
                    fields = peer.split(':')
                    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    client.connect((fields[1], int(fields[2])))
                    client.send(f'0|{self.address[0]}-{self.address[1]}'.encode('utf-8'))
                    client.recv(1024)
                    client.close()
                with open('./config', 'w') as f:
                    f.writelines(alive_peer)
                    f.close()
                self.trader_read()
            else:
                for address in larger_peer:
                    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    client.connect(address)
                    client.send('1'.encode('utf-8'))
                    client.close()


    def process(self):
        # send all requests
        time.sleep(5)
        # request_category|product_id|seller_address(for reply message)
        self.election()
        while True:
            time.sleep(0.1)
            if self.istrader:
                with ThreadPoolExecutor(5) as executor:
                    while True:
                        time.sleep(0.5)
                        conn, _ = self.server.accept()
                        executor.submit(self.trader_process, conn)
                # conn, _ = self.server.accept()
                # self.trader_process(conn)

            else:
                time.sleep(0.5)
                if self.traderaddress and self.isSeller:
                    self.seller_process()

                if self.traderaddress and self.isBuyer:
                    self.buyer_process()


                conn, _ = self.server.accept()
                request = conn.recv(1024)
                data = request.decode('utf-8')
                fields = data.split('|')

                if fields[0] == '0':
                    # after election trader send his address to all peers
                    # request_category|trader_address
                    trader_address, trader_port = fields[1].split('-')
                    self.traderaddress = (trader_address, trader_port)
                    self.is_electing = False
                    conn.send('1'.encode('utf-8'))
                    print("Set new trader.")
                    time.sleep(2)
                    if self.traderaddress and self.isSeller:
                        self.seller_process()

                    if self.traderaddress and self.isBuyer:
                        self.buyer_process()
                    while fields[0] == '0' or fields[0] == '1':
                        conn.close()
                        conn, _ = self.server.accept()
                        request = conn.recv(1024)
                        data = request.decode('utf-8')
                        fields = data.split('|')
                elif fields[0] == '1':
                    # for election
                    self.election()
                    pass
                elif fields[0] == '2':
                    # for buyer
                    # request_category|product_id|quantity|clock
                    pass
                    # self.clock = max(self.clock, int(fields[3])) + 1
                    #
                    # if int(fields[2]) == self.buyNum:
                    #     print(f'Product{self.buyID} not in stock')
                    # else:
                    #     # print(self.buyNum, fields[2])
                    #     # print(fields)
                    #     buy = self.buyNum - int(fields[2])
                    #     self.buyNum = int(fields[2])
                    #     print(f'Sucessfully buy {buy} product{self.buyID}')
                    #     conn.send('1'.encode('utf-8'))

                elif fields[0] == '3':
                    # for seller
                    # request_category|product_id|quantity|clock
                    self.clock = max(self.clock, int(fields[3])) + 1
                    self.sellNum -= int(fields[2])
                    print(f'Sucessfully sell {fields[2]} product{fields[1]}')
                    # conn.send('1'.encode('utf-8'))

                conn.close()
