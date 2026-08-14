from scapy.all import Packet, BitField

class DAQEthHeaderRaw(Packet):
  fields_desc=[
    BitField("longword1", 0, -64),
    BitField("longword2", 0, -64)  # This encodes a timestamp. Divide by 62500000 to get seconds since epoch.
  ]
class DAQEthHeaderIntermediate(Packet):
  fields_desc=[
    BitField("block_length", 0, 12),
    BitField("seq_id", 0, 12),
    BitField("reserved", 0, 6),
    BitField("stream_id", 0, 8),
    BitField("slot_id", 0, 4),
    BitField("crate_id", 0, 10),
    BitField("det_id", 0, 6),
    BitField("version", 0, 6),
  ]
class DAQEthHeader(Packet):
  fields_desc=[
    BitField("version", 0, 6),
    BitField("det_id", 0, 6),
    BitField("crate_id", 0, 10),
    BitField("slot_id", 0, 4),
    BitField("stream_id", 0, 8),
    BitField("reserved", 0, 6),
    BitField("seq_id", 0, 12),
    BitField("block_length", 0, 12),
    BitField("timestamp", 0, 64)
  ]

class WIBEthHeaderRaw(Packet):
  fields_desc=[
    BitField("longword1", 0, -64),
    BitField("longword2", 0, -64)
  ]
class WIBEthHeaderIntermediate(Packet):
  fields_desc=[
    BitField("channel", 0, 8),
    BitField("version", 0, 4),
    BitField("context", 0, 8),
    BitField("ready", 0, 1),
    BitField("calibration", 0, 1),
    BitField("pulser", 0, 1),
    BitField("femb_sync", 0, 2),
    BitField("wib_sync", 0, 1),
    BitField("lol", 0, 1),
    BitField("link_valid", 0, 2),
    BitField("crc_err", 0, 2),
    BitField("cd", 0, 1),
    BitField("pad_1", 0, 1),
    BitField("colddata_timestamp_1", 0, 15),
    BitField("pad_0", 0, 1),
    BitField("colddata_timestamp_0", 0, 15),
  ]
class WIBEthHeader(Packet):
  fields_desc=[
    BitField("colddata_timestamp_0", 0, 15),
    BitField("pad_0", 0, 1),
    BitField("colddata_timestamp_1", 0, 15),
    BitField("pad_1", 0, 1),
    BitField("cd", 0, 1),
    BitField("crc_err", 0, 2),
    BitField("link_valid", 0, 2),
    BitField("lol", 0, 1),
    BitField("wib_sync", 0, 1),
    BitField("femb_sync", 0, 2),
    BitField("pulser", 0, 1),
    BitField("calibration", 0, 1),
    BitField("ready", 0, 1),
    BitField("context", 0, 8),
    BitField("version", 0, 4),
    BitField("channel", 0, 8),
    BitField("extra_data", 0, 64),
  ]

class RawWaveform(Packet):
  fields_desc=[
    BitField("word0", 0, -64),
    BitField("word1", 0, -64),
    BitField("word2", 0, -64),
    BitField("word3", 0, -64),
    BitField("word4", 0, -64),
    BitField("word5", 0, -64),
    BitField("word6", 0, -64),

    BitField("word7", 0, -64),
    BitField("word8", 0, -64),
    BitField("word9", 0, -64),
    BitField("word10", 0, -64),
    BitField("word11", 0, -64),
    BitField("word12", 0, -64),
    BitField("word13", 0, -64),
  ]

class Waveform(Packet):
  fields_desc=[
    BitField("adc0ch0", 0, 14),
    BitField("adc0ch1", 0, 14),
    BitField("adc0ch2", 0, 14),
    BitField("adc0ch3", 0, 14),
    BitField("adc0ch4", 0, 14),
    BitField("adc0ch5", 0, 14),
    BitField("adc0ch6", 0, 14),
    BitField("adc0ch7", 0, 14),
    BitField("adc0ch8", 0, 14),
    BitField("adc0ch9", 0, 14),
    BitField("adc0ch10", 0, 14),
    BitField("adc0ch11", 0, 14),
    BitField("adc0ch12", 0, 14),
    BitField("adc0ch13", 0, 14),
    BitField("adc0ch14", 0, 14),
    BitField("adc0ch15", 0, 14),

    BitField("adc1ch0", 0, 14),
    BitField("adc1ch1", 0, 14),
    BitField("adc1ch2", 0, 14),
    BitField("adc1ch3", 0, 14),
    BitField("adc1ch4", 0, 14),
    BitField("adc1ch5", 0, 14),
    BitField("adc1ch6", 0, 14),
    BitField("adc1ch7", 0, 14),
    BitField("adc1ch8", 0, 14),
    BitField("adc1ch9", 0, 14),
    BitField("adc1ch10", 0, 14),
    BitField("adc1ch11", 0, 14),
    BitField("adc1ch12", 0, 14),
    BitField("adc1ch13", 0, 14),
    BitField("adc1ch14", 0, 14),
    BitField("adc1ch15", 0, 14),

    BitField("adc2ch0", 0, 14),
    BitField("adc2ch1", 0, 14),
    BitField("adc2ch2", 0, 14),
    BitField("adc2ch3", 0, 14),
    BitField("adc2ch4", 0, 14),
    BitField("adc2ch5", 0, 14),
    BitField("adc2ch6", 0, 14),
    BitField("adc2ch7", 0, 14),
    BitField("adc2ch8", 0, 14),
    BitField("adc2ch9", 0, 14),
    BitField("adc2ch10", 0, 14),
    BitField("adc2ch11", 0, 14),
    BitField("adc2ch12", 0, 14),
    BitField("adc2ch13", 0, 14),
    BitField("adc2ch14", 0, 14),
    BitField("adc2ch15", 0, 14),

    BitField("adc3ch0", 0, 14),
    BitField("adc3ch1", 0, 14),
    BitField("adc3ch2", 0, 14),
    BitField("adc3ch3", 0, 14),
    BitField("adc3ch4", 0, 14),
    BitField("adc3ch5", 0, 14),
    BitField("adc3ch6", 0, 14),
    BitField("adc3ch7", 0, 14),
    BitField("adc3ch8", 0, 14),
    BitField("adc3ch9", 0, 14),
    BitField("adc3ch10", 0, 14),
    BitField("adc3ch11", 0, 14),
    BitField("adc3ch12", 0, 14),
    BitField("adc3ch13", 0, 14),
    BitField("adc3ch14", 0, 14),
    BitField("adc3ch15", 0, 14),
  ]
