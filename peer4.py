import peer_with_election
peer4 = peer_with_election.Peer(('127.0.0.1', 8004), 4)
peer4.random(1, 1)
peer4.process()