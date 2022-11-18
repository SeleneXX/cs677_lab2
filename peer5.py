import peer_with_election
peer5 = peer_with_election.Peer(('127.0.0.1', 8005), 5)
peer5.random(1, 0)
peer5.process()