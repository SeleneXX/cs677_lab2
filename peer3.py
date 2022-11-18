import peer_with_election
peer3 = peer_with_election.Peer(('127.0.0.1', 8003), 3)
peer3.random(0, 1)
peer3.process()