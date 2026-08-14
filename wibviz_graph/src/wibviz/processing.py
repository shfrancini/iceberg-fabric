import sys
from scapy.all import Raw, UDP, IP, Ether
from .formats import (
    DAQEthHeaderRaw, DAQEthHeaderIntermediate, DAQEthHeader,
    WIBEthHeaderRaw, WIBEthHeaderIntermediate, WIBEthHeader,
    RawWaveform, Waveform
)

def samplebits_word_offset(i, offset):
  start = 64 - (i + 1) * 14 - offset
  end = 64 - i * 14 - offset
  return (start, end)

def samplebits_deficit(i, offset):
  (start, end) = samplebits_word_offset(i, offset)
  assert(start < 0)
  return 14 - end

def sample_in_word(word, i, offset, verbose=False):
  bitstring = "{0:b}".format(word)
  if len(bitstring) < 64:
    bitstring = ('0' * (64 - len(bitstring))) + bitstring
  if i < 0 and offset < 0:
    answer = bitstring
    if verbose:
      print(answer + ":" + str(len(bitstring)))
    return answer
  else:
    (start, end) = samplebits_word_offset(i, offset)
    if start < 0: start = 0
    answer = bitstring[start:end]
    if verbose:
      print((' ' * (64 - (i + 1) * 14 - offset)) + answer + ":(" + str(start) + ":" + str(end) + ")")
    return answer

def sample_in_words(word1, word2, i, offset, verbose=False):
  bitstring1 = "{0:b}".format(word1)
  bitstring1 = ('0' * (64 - len(bitstring1))) + bitstring1
  bitstring2 = "{0:b}".format(word2)
  bitstring2 = ('0' * (64 - len(bitstring2))) + bitstring2
  (start, end) = samplebits_word_offset(i, offset)
  assert(start < 0)
  start = 0
  part2 = bitstring1[start:end]
  part1 = bitstring2[len(bitstring2)-(14-len(part2)):len(bitstring2)]
  if verbose:
    print(part1 + ":(" + str(len(part1)) + ")" + "|" + part2 + ":(" + str(len(part2)) + ")")
  return part1 + part2

class Processor:
    def __init__(self, config):
        self.config = config
        self.frame_no = 0
        self.channels_seen = set({})
        self.checked = {}
        self.prefix = config.prefix if hasattr(config, 'prefix') else -1
        self.latest_processed_data = None

    def process_packet(self, packet):
        self.frame_no += 1

        if self.prefix == 0: return None
        if self.prefix > 0: self.prefix -= 1

        if self.config.print_frame_no:
            print(f'Frame {self.frame_no}')

        if self.config.hexdump_frames and self.frame_no in self.config.hexdump_frames:
            packet.show()
        
        if not self.config.parse:
            return None

        daqHR = DAQEthHeaderRaw(packet[Raw].load)
        try:
            wibHR = WIBEthHeaderRaw(daqHR[Raw].load)
        except IndexError as error:
            if "IndexError('Layer [Raw] not found')" == repr(error):
                print (f"Problem encountered with frame {self.frame_no}: It appears truncated", file=sys.stderr)
                return None
            else: raise error

        daqHR.remove_payload()

        daqHI = DAQEthHeaderIntermediate(daqHR.longword1.to_bytes(8,'big'))
        daqH = DAQEthHeader(block_length=daqHI.block_length, seq_id=daqHI.seq_id, reserved=daqHI.reserved, stream_id=daqHI.stream_id, slot_id=daqHI.slot_id, crate_id=daqHI.crate_id, det_id=daqHI.det_id, version=daqHI.version, timestamp=daqHR.longword2)

        wibHI = WIBEthHeaderIntermediate(wibHR.longword1.to_bytes(8,'big'))
        wibH = WIBEthHeader(colddata_timestamp_0=wibHI.colddata_timestamp_0, pad_0=wibHI.pad_0, colddata_timestamp_1=wibHI.colddata_timestamp_1, pad_1=wibHI.pad_1, cd=wibHI.cd, crc_err=wibHI.crc_err, link_valid=wibHI.link_valid, lol=wibHI.lol, wib_sync=wibHI.wib_sync, femb_sync=wibHI.femb_sync, pulser=wibHI.pulser, calibration=wibHI.calibration, ready=wibHI.ready, context=wibHI.context, version=wibHI.version, channel=wibHI.channel, extra_data=wibHR.longword2)

        payload = wibHR[Raw]
        raw_waveform = RawWaveform(wibHR[Raw].load)
        wibH.remove_payload()
        raw_waveform.remove_payload()

        waveform = self.extract_waveform(raw_waveform);

        if self.config.show_dissection:
            eth = packet[Ether]
            eth.remove_payload()
            ip = packet[IP]
            ip.remove_payload()
            udp = packet[UDP]
            udp.remove_payload()
            frame = eth/ip/udp/daqH/wibH/waveform
            frame.show()

        self.channels_seen.add(wibH.channel)

        chan_value = {f.name: getattr(waveform, f.name) for f in waveform.fields_desc}
        
        if self.config.check:
            key = (daqH.version, daqH.det_id, daqH.crate_id, daqH.slot_id, daqH.stream_id)
            if key in self.checked:
                (seq_id, colddata_timestamp_0, channel) = self.checked[key]
                if daqH.seq_id != seq_id + 1:
                    print (f"Problem encountered with frame {self.frame_no}: seq_id went from {seq_id} to {daqH.seq_id}", file=sys.stderr)
                if wibH.colddata_timestamp_0 != colddata_timestamp_0 + 2048:
                    print (f"Problem encountered with frame {self.frame_no}: colddata_timestamp_0 went from {colddata_timestamp_0} to {wibH.colddata_timestamp_0}", file=sys.stderr)
                if wibH.colddata_timestamp_0 != wibH.colddata_timestamp_1:
                    print (f"Problem encountered with frame {self.frame_no}: colddata_timestamp_0 ({str(wibH.colddata_timestamp_0)}) and colddata_timestamp_1 ({str(wibH.colddata_timestamp_1)}) differ", file=sys.stderr)
                if wibH.channel != channel:
                    print (f"Problem encountered with frame {self.frame_no}: channel went from {channel} to {wibH.channel}", file=sys.stderr)
            self.checked[key] = (daqH.seq_id, wibH.colddata_timestamp_0, wibH.channel)

            if (len(payload.load) * 8) != 57344: # 64 x 64 x 14 bits = 57344 bits
                print("(len(payload.load) * 8) != 57344", file=sys.stderr)

        result = {
            'frame_no': self.frame_no,
            'chan_value': chan_value,
            'wibH': wibH,
            'daqH': daqH,
        }
        self.latest_processed_data = result
        return result
    
    def process_packet_all_ticks(self, packet):
        self.frame_no += 1

        # filtering based on config
        if self.prefix == 0: return None
        if self.prefix > 0: self.prefix -= 1
        if self.config.print_frame_no:
            print(f'Frame {self.frame_no}')
        if self.config.hexdump_frames and self.frame_no in self.config.hexdump_frames:
            packet.show()
        if not self.config.parse:
            return None

        # extract headers
        daqHR = DAQEthHeaderRaw(packet[Raw].load)
        try:
            wibHR = WIBEthHeaderRaw(daqHR[Raw].load)
        except IndexError as error:
            if "IndexError('Layer [Raw] not found')" == repr(error):
                print(f"Problem encountered with frame {self.frame_no}: It appears truncated", file=sys.stderr)
                return None
            else: raise error

        daqHR.remove_payload()
        daqHI = DAQEthHeaderIntermediate(daqHR.longword1.to_bytes(8,'big'))
        daqH = DAQEthHeader(
            block_length=daqHI.block_length, seq_id=daqHI.seq_id, reserved=daqHI.reserved, 
            stream_id=daqHI.stream_id, slot_id=daqHI.slot_id, crate_id=daqHI.crate_id, 
            det_id=daqHI.det_id, version=daqHI.version, timestamp=daqHR.longword2
        )

        wibHI = WIBEthHeaderIntermediate(wibHR.longword1.to_bytes(8,'big'))
        wibH = WIBEthHeader(
            colddata_timestamp_0=wibHI.colddata_timestamp_0, pad_0=wibHI.pad_0, 
            colddata_timestamp_1=wibHI.colddata_timestamp_1, pad_1=wibHI.pad_1, 
            cd=wibHI.cd, crc_err=wibHI.crc_err, link_valid=wibHI.link_valid, 
            lol=wibHI.lol, wib_sync=wibHI.wib_sync, femb_sync=wibHI.femb_sync, 
            pulser=wibHI.pulser, calibration=wibHI.calibration, ready=wibHI.ready, 
            context=wibHI.context, version=wibHI.version, channel=wibHI.channel, 
            extra_data=wibHR.longword2
        )

        # process waveforms (64 Ticks x 64 Channels)
        # the payload starts after the WIB header
        full_payload = wibHR[Raw].load
        tick_size_bytes = 112 # 14 words * 8 bytes per word = 896 bits
        num_ticks = 64
        all_waveforms = []

        for t in range(num_ticks):
            start_byte = t * tick_size_bytes
            end_byte = start_byte + tick_size_bytes
            
            # pull data for this specific tick
            tick_data = full_payload[start_byte:end_byte]
            raw_waveform = RawWaveform(tick_data)
            waveform = self.extract_waveform(raw_waveform);

            # store this tick's data
            tick_vals = {f.name: getattr(waveform, f.name) for f in waveform.fields_desc}
            all_waveforms.append(tick_vals)

        # formatting and return
        result = {
            'frame_no': self.frame_no,
            'daqH': daqH,
            'wibH': wibH,
            'waveforms': all_waveforms # waveforms[1][channel]
        }
        
        self.channels_seen.add(wibH.channel)
        self.latest_processed_data = result
        return result
    
    def extract_waveform(self, raw_waveform):
        waveform = Waveform()
        
        # ADC0
        waveform.adc0ch0 = int(sample_in_word(raw_waveform.word0, 0, 0), 2)
        waveform.adc0ch1 = int(sample_in_word(raw_waveform.word0, 1, 0), 2)
        waveform.adc0ch2 = int(sample_in_word(raw_waveform.word0, 2, 0), 2)
        waveform.adc0ch3 = int(sample_in_word(raw_waveform.word0, 3, 0), 2)
        waveform.adc0ch4 = int(sample_in_words(raw_waveform.word0, raw_waveform.word1, 4, 0), 2)
        offset = samplebits_deficit(4, 0)
        waveform.adc0ch5 = int(sample_in_word(raw_waveform.word1, 0, offset), 2)
        waveform.adc0ch6 = int(sample_in_word(raw_waveform.word1, 1, offset), 2)
        waveform.adc0ch7 = int(sample_in_word(raw_waveform.word1, 2, offset), 2)
        waveform.adc0ch8 = int(sample_in_word(raw_waveform.word1, 3, offset), 2)
        waveform.adc0ch9 = int(sample_in_words(raw_waveform.word1, raw_waveform.word2, 4, offset), 2)
        offset = samplebits_deficit(4, offset)
        waveform.adc0ch10 = int(sample_in_word(raw_waveform.word2, 0, offset), 2)
        waveform.adc0ch11 = int(sample_in_word(raw_waveform.word2, 1, offset), 2)
        waveform.adc0ch12 = int(sample_in_word(raw_waveform.word2, 2, offset), 2)
        waveform.adc0ch13 = int(sample_in_words(raw_waveform.word2, raw_waveform.word3, 3, offset), 2)
        offset = samplebits_deficit(3, offset)
        waveform.adc0ch14 = int(sample_in_word(raw_waveform.word3, 0, offset), 2)
        waveform.adc0ch15 = int(sample_in_word(raw_waveform.word3, 1, offset), 2)

        # ADC1
        waveform.adc1ch0 = int(sample_in_word(raw_waveform.word3, 2, offset), 2)
        waveform.adc1ch1 = int(sample_in_word(raw_waveform.word3, 3, offset), 2)
        waveform.adc1ch2 = int(sample_in_words(raw_waveform.word3, raw_waveform.word4, 4, offset), 2)
        offset = samplebits_deficit(4, offset)
        waveform.adc1ch3 = int(sample_in_word(raw_waveform.word4, 0, offset), 2)
        waveform.adc1ch4 = int(sample_in_word(raw_waveform.word4, 1, offset), 2)
        waveform.adc1ch5 = int(sample_in_word(raw_waveform.word4, 2, offset), 2)
        waveform.adc1ch6 = int(sample_in_words(raw_waveform.word4, raw_waveform.word5, 3, offset), 2)
        offset = samplebits_deficit(3, offset)
        waveform.adc1ch7 = int(sample_in_word(raw_waveform.word5, 0, offset), 2)
        waveform.adc1ch8 = int(sample_in_word(raw_waveform.word5, 1, offset), 2)
        waveform.adc1ch9 = int(sample_in_word(raw_waveform.word5, 2, offset), 2)
        waveform.adc1ch10 = int(sample_in_word(raw_waveform.word5, 3, offset), 2)
        waveform.adc1ch11 = int(sample_in_words(raw_waveform.word5, raw_waveform.word6, 4, offset), 2)
        offset = samplebits_deficit(4, offset)
        waveform.adc1ch12 = int(sample_in_word(raw_waveform.word6, 0, offset), 2)
        waveform.adc1ch13 = int(sample_in_word(raw_waveform.word6, 1, offset), 2)
        waveform.adc1ch14 = int(sample_in_word(raw_waveform.word6, 2, offset), 2)
        waveform.adc1ch15 = int(sample_in_word(raw_waveform.word6, 3, offset), 2)

        # ADC2
        offset = 0
        waveform.adc2ch0 = int(sample_in_word(raw_waveform.word7, 0, offset), 2)
        waveform.adc2ch1 = int(sample_in_word(raw_waveform.word7, 1, offset), 2)
        waveform.adc2ch2 = int(sample_in_word(raw_waveform.word7, 2, offset), 2)
        waveform.adc2ch3 = int(sample_in_word(raw_waveform.word7, 3, offset), 2)
        waveform.adc2ch4 = int(sample_in_words(raw_waveform.word7, raw_waveform.word8, 4, offset), 2)
        offset = samplebits_deficit(4, offset)
        waveform.adc2ch5 = int(sample_in_word(raw_waveform.word8, 0, offset), 2)
        waveform.adc2ch6 = int(sample_in_word(raw_waveform.word8, 1, offset), 2)
        waveform.adc2ch7 = int(sample_in_word(raw_waveform.word8, 2, offset), 2)
        waveform.adc2ch8 = int(sample_in_word(raw_waveform.word8, 3, offset), 2)
        waveform.adc2ch9 = int(sample_in_words(raw_waveform.word8, raw_waveform.word9, 4, offset), 2)
        offset = samplebits_deficit(4, offset)
        waveform.adc2ch10 = int(sample_in_word(raw_waveform.word9, 0, offset), 2)
        waveform.adc2ch11 = int(sample_in_word(raw_waveform.word9, 1, offset), 2)
        waveform.adc2ch12 = int(sample_in_word(raw_waveform.word9, 2, offset), 2)
        waveform.adc2ch13 = int(sample_in_words(raw_waveform.word9, raw_waveform.word10, 3, offset), 2)
        offset = samplebits_deficit(3, offset)
        waveform.adc2ch14 = int(sample_in_word(raw_waveform.word10, 0, offset), 2)
        waveform.adc2ch15 = int(sample_in_word(raw_waveform.word10, 1, offset), 2)

        # ADC3
        waveform.adc3ch0 = int(sample_in_word(raw_waveform.word10, 2, offset), 2)
        waveform.adc3ch1 = int(sample_in_word(raw_waveform.word10, 3, offset), 2)
        waveform.adc3ch2 = int(sample_in_words(raw_waveform.word10, raw_waveform.word11, 4, offset), 2)
        offset = samplebits_deficit(4, offset)
        waveform.adc3ch3 = int(sample_in_word(raw_waveform.word11, 0, offset), 2)
        waveform.adc3ch4 = int(sample_in_word(raw_waveform.word11, 1, offset), 2)
        waveform.adc3ch5 = int(sample_in_word(raw_waveform.word11, 2, offset), 2)
        waveform.adc3ch6 = int(sample_in_words(raw_waveform.word11, raw_waveform.word12, 3, offset), 2)
        offset = samplebits_deficit(3, offset)
        waveform.adc3ch7 = int(sample_in_word(raw_waveform.word12, 0, offset), 2)
        waveform.adc3ch8 = int(sample_in_word(raw_waveform.word12, 1, offset), 2)
        waveform.adc3ch9 = int(sample_in_word(raw_waveform.word12, 2, offset), 2)
        waveform.adc3ch10 = int(sample_in_word(raw_waveform.word12, 3, offset), 2)
        waveform.adc3ch11 = int(sample_in_words(raw_waveform.word12, raw_waveform.word13, 4, offset), 2)
        offset = samplebits_deficit(4, offset)
        waveform.adc3ch12 = int(sample_in_word(raw_waveform.word13, 0, offset), 2)
        waveform.adc3ch13 = int(sample_in_word(raw_waveform.word13, 1, offset), 2)
        waveform.adc3ch14 = int(sample_in_word(raw_waveform.word13, 2, offset), 2)
        waveform.adc3ch15 = int(sample_in_word(raw_waveform.word13, 3, offset), 2)
        
        return waveform