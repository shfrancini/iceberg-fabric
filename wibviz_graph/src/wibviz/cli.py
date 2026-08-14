import argparse
import curses
import logging
import os
import sys
import threading
import time

from scapy.all import AsyncSniffer, PcapReader

from .processing import Processor
from .display import Display


def main_loop(stdscr, args):
    processor = Processor(args)
    display_processor = processor if not args.pcap_file else None
    display = Display(stdscr, 
                      args.interface if not args.pcap_file else None, 
                      args.refresh_rate,
                      display_processor)
    running = True

    if args.pcap_file:
        try:
            packets = PcapReader(args.pcap_file)
            for packet in packets:
                if not running:
                    break
                processed_data = processor.process_packet(packet)
                if processed_data:
                    display.update(processed_data)

                if display.handle_input() == 'quit':
                    running = False
        except Exception as e:
            sys.exit(f"Error reading PCAP file: {e}")
    else:
        # Live sniffing mode
        sniffer_thread = None

        def sniffing_loop():
            # This function runs in a separate thread and handles the scapy sniffing
            def process_packet_callback(packet):
                processor.process_packet(packet)

            def should_stop(p):
                return not running

            sniffer = AsyncSniffer(iface=args.interface, prn=process_packet_callback, filter=args.filter, stop_filter=should_stop)
            sniffer.start()
            sniffer.join()

        # Start the first sniffing thread
        sniffer_thread = threading.Thread(target=sniffing_loop)
        sniffer_thread.start()

        while running:
            if not sniffer_thread.is_alive() and running:
                # The sniffer thread has stopped, likely due to a low-level socket error.
                # We will wait a moment and restart it to continue sniffing.
                time.sleep(1)
                sniffer_thread = threading.Thread(target=sniffing_loop)
                sniffer_thread.start()

            if display.handle_input() == 'quit':
                running = False
            time.sleep(0.1)
        
        # Wait for the sniffer thread to finish
        if sniffer_thread.is_alive():
            sniffer_thread.join()

def main():
    logging.getLogger("scapy.runtime").setLevel(logging.ERROR)
    logging.getLogger("scapy.loading").setLevel(logging.ERROR)

    parser = argparse.ArgumentParser(description="A dissector and visualizer for DUNE WIB UDP data streams.")
    
    source_group = parser.add_mutually_exclusive_group(required=False)
    source_group.add_argument('-i', '--interface', type=str, help='Network interface to sniff on.')
    source_group.add_argument('-f', '--pcap-file', type=str, help='PCAP file to read from instead of live sniffing.')

    parser.add_argument('--filter', type=str, default='udp and src port 0 and dst port 17476', help='BPF filter for sniffing packets.')
    parser.add_argument('--prefix', type=int, default=-1, help='Number of packets to process. -1 for infinite.')
    parser.add_argument('--hexdump-frames', nargs='+', type=int, help='List of frame numbers to hexdump.')
    parser.add_argument('--print-frame-no', action='store_true', help='Print frame numbers to stdout.')
    
    parser.add_argument('--parse', action='store_true', default=True, help=argparse.SUPPRESS) # Always parse
    parser.add_argument('--no-parse', action='store_false', dest='parse', help='Disable parsing packets.')
    
    parser.add_argument('--check', action='store_true', default=False, help='Enable consistency checks.')
    parser.add_argument('--no-check', action='store_false', dest='check', help=argparse.SUPPRESS)

    parser.add_argument('--show-dissection', action='store_true', help='Show scapy dissection of each packet.')
    parser.add_argument('--refresh-rate', type=float, default=1.0, help='Refresh rate for stats display in seconds.')

    args = parser.parse_args()

    if not args.interface and not args.pcap_file:
        args.interface = 'bond0.205'

    if args.interface and os.geteuid() != 0:
        sys.exit("You need to be root to run live sniffing on a network interface.")

    if args.check and not args.parse:
        parser.error('To check the content, please enable parsing.')
    if args.show_dissection and not args.parse:
        parser.error('To show the dissection, please enable parsing.')

    try:
        curses.wrapper(main_loop, args)
    except curses.error as e:
        sys.exit(f"Curses error: {e}. Your terminal might not support all features.")
    except KeyboardInterrupt:
        print("\nExiting.")
    finally:
        # Ensure terminal state is restored
        curses.endwin()

if __name__ == "__main__":
    main()
