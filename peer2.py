import peer_with_election
peer2 = peer_with_election.Peer(('127.0.0.1', 8002), 2)
peer2.random(1, 1)
peer2.process()