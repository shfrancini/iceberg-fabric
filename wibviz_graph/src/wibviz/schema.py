#Information we need to pack and send to the receiver about what we want to graph
#This class exists to make sending results of parsing via byte streams easier

from dataclasses import dataclass, field

@dataclass
class GraphStruct:
    seq_id: int                             #seq_id from the DAQ header
    timestamp: int                          #timestamp from the DAQ header
    tick: int                               #tick number from parse, 0-63
    adc_value: int                          #adc_value
    wire_id: int                            #wire_id from channel-to-wire
    tpc: bool                               #tpc value we're filtering for
    slot: int                               #slot number we used for lookup
    plane: int                              #plane we are filtering for
    packet_file: str = field(default="")    #the name of the pcap file we used, if applicable
