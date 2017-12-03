from ryu.base import app_manager
from ryu.controller import ofp_event, dpset
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto.ofproto_v1_3_parser import OFPInstructionActions, OFPFlowMod, OFPActionOutput, OFPMatch, OFPPacketOut
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet
import os


class Topology(object):
    def __init__(self):
        self.leafs = {}
        self.spines = {}
        self.switches = {}

    def add_leaf(self, dp, ports):
        switch = LeafSwitch(self, dp, ports)
        self.leafs[dp.id] = switch
        self.switches[dp.id] = switch

    def add_spine(self, dp, ports):
        switch = SpineSwitch(self, dp, ports)
        self.spines[dp.id] = switch
        self.switches[dp.id] = switch

    def remove_switch(self, dp):
        self.leafs.pop(dp.id, None)
        self.spines.pop(dp.id, None)
        self.switches.pop(dp.id, None)

    def get_switch(self, dp):
        return self.switches[dp.id]

    def get_spine_count(self):
        return len(self.spines.keys())

    def printout(self):
        os.system('clear')
        print '----------------------------'
        print ' Topology '
        print '----------------------------'
        for spine in self.spines:
            switch = self.spines[spine]
            print 'SpineSwitch (ID: %s)' % (switch.dp.id,)
            for port in switch.ports:
                print '\t%s\t%s\t%s' % (port.port_no, port.name, port.hw_addr)
        for leaf in self.leafs:
            switch = self.leafs[leaf]
            print 'LeafSwitch (ID: %s)' % (switch.dp.id,)
            for port in switch.ports:
                print '\t%s\t%s\t%s' % (port.port_no, port.name, port.hw_addr)
        if len(self.switches.keys()) == 0:
            print 'No switches connected'
        print '----------------------------'


class Switch(object):
    def __init__(self, topology, dp, ports):
        self.topology = topology
        self.dp = dp
        self.ports = [p for p in ports if p.curr_speed != 0]
        self.host_table = {}

    def port_count(self):
        return len(self.ports)

    def add_flow(self, priority, match, actions):
        inst = [OFPInstructionActions(ofproto_v1_3.OFPIT_APPLY_ACTIONS,
                                      actions)]
        mod = OFPFlowMod(datapath=self.dp, priority=priority,
                         match=match, instructions=inst)
        self.dp.send_msg(mod)

    def forward_packet(self, in_port, out_port, data):
        actions = [OFPActionOutput(out_port)]
        out = OFPPacketOut(datapath=self.dp,
                           buffer_id=ofproto_v1_3.OFP_NO_BUFFER,
                           in_port=in_port, actions=actions,
                           data=data)
        self.dp.send_msg(out)


class LeafSwitch(Switch):

    def get_forwarding_ports(self, in_port, eth_pkt):

        if self.is_spine_port(in_port):
            if eth_pkt.dst in self.host_table:
                return [self.host_table[eth_pkt.dst]]
            else:
                host_count = self.port_count() - self.topology.get_spine_count()
                return range(1, host_count + 1)
        else:
            # packet from host
            self.host_table[eth_pkt.src] = in_port
            port_count = self.port_count()
            host_count = port_count - self.topology.get_spine_count()
            return range(host_count + 1, port_count + 1)

    def handle(self, in_port, data):
        pkt = packet.Packet(data)
        eth_pkt = pkt.get_protocol(ethernet.ethernet)

        ports = self.get_forwarding_ports(in_port, eth_pkt)
        self.forward_packet(in_port, ports[0], data)

        for port in ports:
            match = OFPMatch(in_port=in_port, eth_dst=eth_pkt.dst)
            actions = [OFPActionOutput(port)]
            self.add_flow(10, match, actions)
            print 'add flow for %s from port %s to %s' % (self.dp.id,in_port, port)

    def is_spine_port(self, port):
        spine_count = self.topology.get_spine_count()
        port_count = self.port_count()
        return port > port_count - spine_count


class SpineSwitch(Switch):
    def handle(self, in_port, data):
        pkt = packet.Packet(data)
        eth_pkt = pkt.get_protocol(ethernet.ethernet)

        self.host_table[eth_pkt.src] = in_port

        if eth_pkt.dst in self.host_table:
            port = self.host_table[eth_pkt.dst]
            match = OFPMatch(in_port=in_port, eth_dst=eth_pkt.dst)
            actions = [OFPActionOutput(port)]
            self.add_flow(10, match, actions)
            self.forward_packet(in_port, port, data)
        else:
            self.forward_packet(in_port, ofproto_v1_3.OFPP_FLOOD, data)


class CloudInfController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(CloudInfController, self).__init__(*args, **kwargs)
        self.topology = Topology()

    def add_flow(self, datapath, priority, match, actions):
        # construct flow_mod message and send it.
        inst = [OFPInstructionActions(ofproto_v1_3.OFPIT_APPLY_ACTIONS,
                                      actions)]
        mod = OFPFlowMod(datapath=datapath, priority=priority,
                         match=match, instructions=inst)
        datapath.send_msg(mod)

    @set_ev_cls(dpset.EventDP, MAIN_DISPATCHER)
    def link_add(self, ev):
        if ev.enter:
            if ev.ports[0].name[0] == 'l':
                self.topology.add_leaf(ev.dp, ev.ports)
            elif ev.ports[0].name[0] == 's':
                self.topology.add_spine(ev.dp, ev.ports)
            else:
                print 'invalid named link'
        else:
            self.topology.remove_switch(ev.dp)
            print 'removed link %i' % (ev.dp.id,)

        self.topology.printout()

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath

        # install the table-miss flow entry.
        match = OFPMatch()
        actions = [OFPActionOutput(ofproto_v1_3.OFPP_CONTROLLER,
                                   ofproto_v1_3.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        in_port = ev.msg.match['in_port']
        switch = self.topology.get_switch(ev.msg.datapath)
        print 'Switch %s handles packet' % (switch.dp.id,)
        switch.handle(in_port, ev.msg.data)
