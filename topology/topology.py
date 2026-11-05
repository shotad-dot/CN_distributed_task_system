"""Legacy static topology helper; dynamic demo uses scripts/run_network.py."""
from mininet.topo import Topo
class WorkerTopology(Topo):
    def build(self):
        s=self.addSwitch("s1")
        for i in range(1,3):
            h=self.addHost(f"h{i}",ip=f"10.0.0.{i}/24"); self.addLink(h,s)
topos={"worker":WorkerTopology}
