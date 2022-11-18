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
        self.random()
        self.clock = 0
        self.is_electing = False

    def random(self):
        self.isBuyer = random.randint(0, 1)
        self.isSeller = random.randint(0, 1)

    def election(self):
        if not self.is_electing:
            self.is_electing = True
            alive_peer = []
            larger_peer = []
            with open('./config') as f:
                for line in f:
                    alive_peer.append(line)
                    fields = line.split(':')
                    if int(fields[0]) > self.peer_id:
                        larger_peer.append((fields[1], int(fields[2])))
                    if int(fields[0]) != self.peer_id:
                        alive_peer.append(line)
                f.close()

            if len(larger_peer) == 0:
                self.istrader = True
                with open('./config', 'w') as f:
                    f.writelines(alive_peer)
                for peer in alive_peer:
                    fields = peer.split(':')
                    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    client.connect((fields[1], int(fields[2])))
                    client.send(f'0|{self.address[0]}-{self.address[1]}'.encode('utf-8'))
                    client.close()
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
                conn, _ = self.server.accept()
                request = conn.recv(1024)
                data = request.decode('utf-8')
                fields = data.split('|')
                if self.istrader:
                    if fields[0] == '4':
                        # receive a buy request
                        # request_category|productID|quantity|addr
                        prodID, prodNum = fields[1], int(fields[2])
                        while prodNum > 0:
                            if self.traderList[prodID]:
                                next_seller = self.traderList[prodID].pop()
                                if int(next_seller[1]) <= prodNum:
                                    prodNum -= int(next_seller[1])
                                    data = '|'.join(['3', prodID, next_seller[1]])
                                    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                    client.connect((next_seller[0][0], int(next_seller[0][1])))
                                    client.send(data.encode('utf-8'))
                                    client.close()
                                else:
                                    data = '|'.join(['3', prodID, prodNum])
                                    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                    client.connect((next_seller[0][0], int(next_seller[0][1])))
                                    client.send(data.encode('utf-8'))
                                    client.close()
                                    prodNum = 0
                            else:
                                break
                        replyaddr = fields[3].split('-')
                        data = '|'.join(['3', prodID, prodNum])
                        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        client.connect((replyaddr[0], int(replyaddr[1])))
                        client.send(data.encode('utf-8'))
                        client.close()
                    if fields[0] == '5':
                        # store as: productID:[(address, quantity)]
                        self.is_electing = False
                        prodID, prodNum, addr = fields[1], int(fields[2]), fields[3].split('-')
                        self.traderList[prodID].append((addr, prodNum))


                else:
                    myaddr = '-'.join([str(self.address[0]), str(self.address[1])])
                    if self.traderaddress and self.isBuyer:
                        # if product number is 0, then random a product to buy
                        if self.buyNum == 0:
                            self.buyID = random.randint(0, 2)
                            self.buyNum = random.randint(1, 10)
                        # send buy request
                        # request_catagory|product_ID|quantity|address
                        data = '|'.join(['4', str(self.buyID), str(self.buyNum), myaddr])
                        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        try:
                            client.connect((self.traderaddress[0], int(self.traderaddress[1])))
                            client.send(data.encode('utf-8'))
                        except:
                            self.traderaddress = None
                            self.election()
                        client.close()

                    if self.traderaddress and self.isSeller:
                        # if product number is 0, then random a product to sell
                        if self.sellNum == 0:
                            self.sellID = random.randint(0, 2)
                            self.sellNum = random.randint(1, 10)
                        # send stock information
                        # request_catagory|product_ID|quantity|address
                        data = '|'.join(['5', str(self.sellID), str(self.sellNum), myaddr])
                        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        try:
                            client.connect((self.traderaddress[0], int(self.traderaddress[1])))
                            client.send(data.encode('utf-8'))
                        except:
                            self.traderaddress = None
                            self.election()
                        client.close()

                    if fields[0] == '0':
                        # after election trader send his address to all peers
                        # request_category|trader_address
                        trader_address, trader_port = fields[1].split('-')
                        self.traderaddress = (trader_address, trader_port)
                        self.is_electing = False
                    elif fields[0] == '1':
                        # for election
                        self.election()
                        pass
                    elif fields[0] == '2':
                        # for buyer
                        # request_category|product_id|quantity
                        if int(fields[2]) == self.buyNum:
                            print('Not in stock')
                        else:
                            buy = self.buyNum - int(fields[2])
                            self.buyNum = int(fields[2])
                            print('Sucessfully purchase {} productID{}'.format(buy, fields[1]))
                    elif fields[0] == '3':
                        # for seller
                        # request_category|product_id|quantity
                        self.sellNum -= int(fields[2])
                        print('Sucessfully sell {} productID{}'.format(fields[1], fields[2]))
                conn.close()
