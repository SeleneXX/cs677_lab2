import peer_with_election
peer1 = peer_with_election.Peer(('127.0.0.1', 8001), 1)
peer1.random(1, 1)
peer1.process()