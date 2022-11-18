import collections
import random
import socket
import threading
import time

sem = threading.Semaphore(20)
lock = threading.Lock()


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

    def trader_process(self):
        conn, _ = self.server.accept()
        request = conn.recv(1024)
        data = request.decode('utf-8')
        fields = data.split('|')
        if fields[0] == '4':
            # receive a buy request
            # request_category|productID|quantity|addr
            prodID, prodNum = fields[1], int(fields[2])
            print(f'Try to sell {prodNum} productID{prodID}...')
            while prodNum > 0:
                if self.traderList[prodID]:
                    next_seller = self.traderList[prodID].pop()
                    if int(next_seller[1]) <= prodNum:
                        prodNum -= int(next_seller[1])
                        data = f'3|{prodID}|{next_seller[1]}'
                        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        client.connect((next_seller[0][0], int(next_seller[0][1])))
                        client.send(data.encode('utf-8'))
                        client.close()
                    else:
                        data = f'3|{prodID}|{prodNum}'
                        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        client.connect((next_seller[0][0], int(next_seller[0][1])))
                        client.send(data.encode('utf-8'))
                        client.close()
                        prodNum = 0
                else:
                    break
            # reply buyer
            replyaddr = fields[3].split('-')
            data = f'2|{prodID}|{prodNum}'
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect((replyaddr[0], int(replyaddr[1])))
            client.send(data.encode('utf-8'))
            client.close()
        if fields[0] == '5':
            # store as: productID:[(address, quantity)]
            self.is_electing = False
            prodID, prodNum, addr = fields[1], int(fields[2]), fields[3].split('-')
            self.traderList[prodID].append((addr, prodNum))
            print('Update stock information.')
        conn.close()


    def buyer_process(self):
        myaddr = f'{self.address[0]}-{self.address[1]}'
        # if product number is 0, then random a product to buy
        if self.buyNum == 0:
            self.buyID = random.randint(0, 2)
            while self.buyID == self.sellID:
                self.buyID = random.randint(0, 2)
            # self.buyID = 0
            self.buyNum = random.randint(1, 10)
            print(f'Buy {self.buyNum} porduct{self.buyID}.')
        # send buy request
        # request_catagory|product_ID|quantity|address
        data = f'4|{self.buyID}|{self.buyNum}|{myaddr}'
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            client.connect((self.traderaddress[0], int(self.traderaddress[1])))
            client.send(data.encode('utf-8'))
        except:
            self.traderaddress = None
            self.election()
        client.close()


    def seller_process(self):
        myaddr = f'{self.address[0]}-{self.address[1]}'
        # if product number is 0, then random a product to sell
        if self.sellNum == 0:
            self.sellID = random.randint(0, 2)
            while self.sellID != self.buyID:
                self.sellID = random.randint(0, 2)
            # self.sellID = 1
            self.sellNum = random.randint(1, 10)
            print(f'Sell {self.sellNum} porduct{self.sellID}.')
        # send stock information
        # request_catagory|product_ID|quantity|address
        data = f'5|{self.sellID}|{self.sellNum}|{myaddr}'
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
            with sem:
                time.sleep(0.1)
                if self.istrader:
                    self.trader_process()

                else:
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
                    elif fields[0] == '1':
                        # for election
                        self.election()
                        pass
                    elif fields[0] == '2':
                        # for buyer
                        # request_category|product_id|quantity
                        if int(fields[2]) == self.buyNum:
                            print(f'Product{self.buyID} not in stock')
                        else:
                            buy = self.buyNum - int(fields[2])
                            self.buyNum = int(fields[2])
                            print('Sucessfully purchase {} product{}'.format(buy, fields[1]))
                    elif fields[0] == '3':
                        # for seller
                        # request_category|product_id|quantity
                        self.sellNum -= int(fields[2])
                        print('Sucessfully sell {} productID{}'.format(fields[1], fields[2]))
                    conn.close()
