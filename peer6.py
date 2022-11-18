import peer_with_election
peer6 = peer_with_election.Peer(('127.0.0.1', 8006), 6)
peer6.random(1, 1)
peer6.process()