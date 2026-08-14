import curses
import os
import re
import signal
import time
from .channel_map import CHANNEL_SETS

class Display:
    def __init__(self, stdscr, interface_name, refresh_rate=1.0, processor=None):
        self.stdscr = stdscr
        self.interface_name = interface_name
        self.processor = processor
        self.counter = ""
        self.rx_bytes = 0
        self.rx_packets = 0
        self.rx_bytes_prev = 0
        self.rx_packets_prev = 0
        self.last_chan_values = {}
        self.refresh_rate = refresh_rate
        self._setup_curses()
        if self.interface_name and self.processor:
            self._setup_signal_handler()

    def _calculate_wibframechan(self, channel_name, stream_id):
        match = re.match(r'adc(\d+)ch(\d+)', channel_name)
        
        if not match:
            return None
        
        adc_index = int(match.group(1))
        chan_index = int(match.group(2))
        
        wibframechan = adc_index * 16 + chan_index
        
        return wibframechan
    
    def _setup_curses(self):
        curses.start_color()
        curses.init_pair(1, curses.COLOR_CYAN, curses.COLOR_BLACK)
        curses.init_pair(2, curses.COLOR_YELLOW, curses.COLOR_RED)
        curses.init_pair(3, curses.COLOR_WHITE, curses.COLOR_BLACK)
        curses.init_pair(4, curses.COLOR_YELLOW, curses.COLOR_BLACK)
        curses.init_pair(5, curses.COLOR_GREEN, curses.COLOR_BLACK)
        self.stdscr.clear()
        self.stdscr.addstr(0, 0, "WibViz", curses.color_pair(1))
        self.stdscr.nodelay(True) # non-blocking getch

    def _get_rx_stats(self):
        if not self.interface_name:
            return 0, 0
        try:
            ifconfig_output = os.popen(f"ifconfig {self.interface_name}").read()
            rx_line = [line for line in ifconfig_output.split('\n') if 'RX packets' in line]
            if rx_line:
                line = rx_line[0]
                packets_match = re.search(r'packets\s*:?(\d+)', line)
                bytes_match = re.search(r'bytes\s*:?(\d+)', line)
                
                rx_packets = int(packets_match.group(1)) if packets_match else 0
                rx_bytes = int(bytes_match.group(1)) if bytes_match else 0

                if rx_bytes == 0:
                    rx_bytes_line = [line for line in ifconfig_output.split('\n') if 'RX bytes' in line]
                    if rx_bytes_line:
                        bytes_match = re.search(r'bytes\s*:?(\d+)', rx_bytes_line[0])
                        rx_bytes = int(bytes_match.group(1)) if bytes_match else 0

                return rx_packets, rx_bytes
        except (ValueError, IndexError):
            return 0, 0
        return 0, 0

    def _signal_handler(self, signum, stack):
        self.rx_bytes_prev = self.rx_bytes
        self.rx_packets_prev = self.rx_packets

        self.rx_packets, self.rx_bytes = self._get_rx_stats()

        if self.rx_packets == 0 and self.rx_bytes == 0:
            self.rx_packets = self.rx_packets_prev
            self.rx_bytes = self.rx_bytes_prev

        rx_bytes_diff = self.rx_bytes - self.rx_bytes_prev
        rx_packets_diff = self.rx_packets - self.rx_packets_prev

        if self.refresh_rate > 0:
            bytes_per_second = rx_bytes_diff / self.refresh_rate
            packets_per_second = rx_packets_diff / self.refresh_rate
        else:
            bytes_per_second = 0
            packets_per_second = 0
            
        gigabytes_per_second = bytes_per_second / (1000**3)

        self.counter = f"rx_bytes = {self.rx_bytes} rx_packets = {self.rx_packets} rx_GBs = {gigabytes_per_second:.2f} rx_pps = {packets_per_second:.1f}"

        if self.processor and self.processor.latest_processed_data:
            self.update(self.processor.latest_processed_data)
            self.processor.latest_processed_data = None # Consume the data
        else:
            self.redraw_stats_only()

    def _setup_signal_handler(self):
        self.rx_packets, self.rx_bytes = self._get_rx_stats()
        
        signal.signal(signal.SIGALRM, self._signal_handler)
        signal.setitimer(signal.ITIMER_REAL, self.refresh_rate, self.refresh_rate)

    def _clear_previous_waveforms(self):
        if not self.last_chan_values:
            return

        channel_set_num = 0
        for channel_set in CHANNEL_SETS:
            x = 60 if channel_set_num % 2 != 0 else 0
            y_offset = 20 if channel_set_num >= 2 else 0
            y = 1 + y_offset
            for channel in channel_set:
                try:
                    self.stdscr.addstr(y, x, ' ' * 19, curses.color_pair(1))
                    length = int(self.last_chan_values.get(channel, 0) / 1000)
                    self.stdscr.addstr(y, x + 20, " " * length, curses.color_pair(1))
                except curses.error:
                    pass
                y += 1
            channel_set_num += 1

    def _draw_stats(self):
        self.stdscr.addstr(40, 0, " " * 100, curses.color_pair(4))
        self.stdscr.addstr(40, 0, self.counter, curses.color_pair(4))

        self.stdscr.addstr(58, 0, f"Refresh Rate: {self.refresh_rate:.1f}s (+/- to change)      ", curses.color_pair(1))

    def record_data(self, data):
        waveforms = data['waveforms']
        wibH = data['wibH']
        daqH = data['daqH']
        
        tick_start = 0
        
        map_log_lines = []
        
        for t_idx, tick_data in enumerate(waveforms):
            for channel, value in tick_data.items():
                wibframechan = self._calculate_wibframechan(channel, daqH.stream_id)
                off_ch = self.chanmap.get_offline_channel(
                    daqH.crate_id,
                    (daqH.slot_id + 1),
                    1,
                    wibframechan
                )
                plane = self.chanmap.get_plane(off_ch);
                if(plane == 2):
                    map_log_lines.append(
                        #f"Frame {data.get('frame_no', '?')}: tick: {tick_start} HW: (crate: {daqH.crate_id}, wib: {daqH.slot_id+1}, link: {daqH.stream_id%2}, wibframechan: {wibframechan}) -> offline: {off_ch} plane: {plane} ({channel}) value: {value}\n"
                        f"{t_idx+tick_start},{off_ch},{value}\n"
                    )

        if map_log_lines:
            try:
                with open("map_debug.log", "a") as log_file:
                    log_file.writelines(map_log_lines)
            except Exception:
                pass
            
    def update(self, data):
        self._clear_previous_waveforms()

        chan_value = data['chan_value']
        wibH = data['wibH']
        daqH = data['daqH']
        
        channel_set_num = 0
        
        for channel_set in CHANNEL_SETS:
            x = 60 if channel_set_num % 2 != 0 else 0
            y_offset = 20 if channel_set_num >= 2 else 0
            y = 1 + y_offset
            for channel in channel_set:
                try:
                    self.stdscr.addstr(y, x, f'{channel} = {chan_value[channel]}', curses.color_pair(3))
                    length = int(chan_value[channel] / 1000)
                    self.stdscr.addstr(y, x + 20, "\u2592" * length, curses.color_pair(2))
                except curses.error:
                    pass
                y += 1
            channel_set_num += 1

        self.stdscr.addstr(50, 0, f'wibH.channel = {wibH.channel}      ', curses.color_pair(5))
        self.stdscr.addstr(52, 0, f'APA = {daqH.crate_id}      ', curses.color_pair(5))
        self.stdscr.addstr(53, 0, f'WIB = {daqH.slot_id}      ', curses.color_pair(5))
        link = (daqH.stream_id >> 6) & 1;
        self.stdscr.addstr(55, 0, f'link = {link}      ', curses.color_pair(5))
        
        self._draw_stats()

        self.stdscr.refresh()
        self.last_chan_values = chan_value.copy()

    def redraw_stats_only(self):
        self._draw_stats()
        self.stdscr.refresh()

    def handle_input(self):
        try:
            key = self.stdscr.getch()
            if key == ord('q'):
                return 'quit'
            elif key == ord('+') or key == curses.KEY_UP:
                self.refresh_rate = max(0.1, self.refresh_rate - 0.1)
                signal.setitimer(signal.ITIMER_REAL, self.refresh_rate, self.refresh_rate)
            elif key == ord('-') or key == curses.KEY_DOWN:
                self.refresh_rate += 0.1
                signal.setitimer(signal.ITIMER_REAL, self.refresh_rate, self.refresh_rate)
        except curses.error:
            pass
        return None
