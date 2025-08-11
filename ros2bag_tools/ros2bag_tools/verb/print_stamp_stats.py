# Copyright 2025 AIT Austrian Institute of Technology GmbH
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections import defaultdict
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from rclpy.time import Time
from ros2bag.api import add_standard_reader_args
from ros2bag.verb import VerbExtension
from ros2bag_tools.verb import get_reader_options

from rosbag2_py import SequentialReader
from rosbag2_py import StorageFilter
from rosbag2_tools.bag_view import BagView


class PrintStampStatsVerb(VerbExtension):
    """Print timestamp statistics for messages in a bag."""

    def add_arguments(self, parser, cli_name):
        add_standard_reader_args(parser)
        parser.add_argument(
            '--topics',
            nargs='*',
            help='Specific topics to analyze (default: all topics with headers)'
        )
        parser.add_argument(
            '-v', '--verbose',
            action='store_true',
            help='Show detailed statistics including all messages and averages'
        )

    def main(self, *, args):
        storage_options, converter_options = get_reader_options(args)
        reader = SequentialReader()
        reader.open(storage_options, converter_options)

        # Data structures to store statistics
        topic_stats = defaultdict(lambda: {
            'messages': [],
            'deltas': [],
            'first_msg': None,
            'last_msg': None,
            'count': 0
        })

        # Keep track of which topics we've seen first messages for (for non-verbose mode)
        topics_with_first_msg = set()
        
        # In non-verbose mode, we need to know how many topics to expect
        # Get all topic names from the bag metadata and exclude topics with zero messages
        bag_metadata = reader.get_metadata()
        topics_with_messages = {topic_info.topic_metadata.name 
                               for topic_info in bag_metadata.topics_with_message_count 
                               if topic_info.message_count > 0}
        
        # Filter topics if specified
        if args.topics:
            storage_filter = StorageFilter(topics=args.topics)
            expected_topics = set(args.topics) & topics_with_messages
        else:
            storage_filter = StorageFilter()  # No filter, process all topics
            expected_topics = topics_with_messages

        # No header/separator in compact mode
        if not args.verbose:
            # Slim header row in compact mode (no separator line)
            print(f"{'Topic'.ljust(30)}{'Received'.ljust(30)} {'Header'.ljust(30)}")

        # Process all messages
        for topic, msg, t in BagView(reader, storage_filter):
            # In non-verbose mode, skip if we already have the first message for this topic
            if not args.verbose and topic in topics_with_first_msg:
                continue

            # Get timestamps
            received_time_ns = t
            
            # Check if message has header
            if hasattr(msg, 'header'):
                header_stamp_ns = Time.from_msg(msg.header.stamp).nanoseconds
                delta_ns = received_time_ns - header_stamp_ns
                
                # Store message info with header
                msg_info = {
                    'topic': topic,
                    'has_header': True,
                    'received_time_ns': received_time_ns,
                    'header_stamp_ns': header_stamp_ns,
                    'delta_ns': delta_ns,
                    'received_time': self._nanoseconds_to_datetime(received_time_ns),
                    'header_stamp': self._nanoseconds_to_datetime(header_stamp_ns),
                    'delta_ms': delta_ns / 1e6  # Convert to milliseconds
                }
            else:
                # Store message info without header
                msg_info = {
                    'topic': topic,
                    'has_header': False,
                    'received_time_ns': received_time_ns,
                    'header_stamp_ns': None,
                    'delta_ns': None,
                    'received_time': self._nanoseconds_to_datetime(received_time_ns),
                    'header_stamp': None,
                    'delta_ms': None
                }

            # Update topic statistics
            stats = topic_stats[topic]
            if args.verbose:
                stats['messages'].append(msg_info)
                if msg_info['has_header']:
                    stats['deltas'].append(delta_ns)
            stats['count'] += 1

            # Track first and last messages
            if stats['first_msg'] is None:
                stats['first_msg'] = msg_info
                topics_with_first_msg.add(topic)
                
                # In non-verbose mode, break early if we've seen all expected topics
                if not args.verbose and len(topics_with_first_msg) >= len(expected_topics):
                    #print(f"Found first message for all {len(expected_topics)} topics, stopping early...")
                    break
                    
            if args.verbose:
                stats['last_msg'] = msg_info

        if not topic_stats:
            print("No messages found in the bag.")
            return 0

        # Print statistics for each topic
        if args.verbose:
            # Verbose mode - detailed multi-line output
            for topic, stats in topic_stats.items():
                print(f"\nTopic: {topic}")
                print("-" * 40)
                print(f"Total messages: {stats['count']}")

                if stats['count'] > 0:
                    # Always show first message info
                    first_msg = stats['first_msg']
                    print(f"\nFirst message:")
                    if first_msg['has_header']:
                        print(f"  Header stamp:   {first_msg['header_stamp']}")
                        print(f"  Received time:  {first_msg['received_time']}")
                        print(f"  Delta:          {first_msg['delta_ms']:.5f} ms")
                    else:
                        print(f"  Received time:  {first_msg['received_time']}")
                        print(f"  (No header in message)")

                    # Last message info (only if different from first)
                    if stats['count'] > 1:
                        last_msg = stats['last_msg']
                        print(f"\nLast message:")
                        if last_msg['has_header']:
                            print(f"  Header stamp:   {last_msg['header_stamp']}")
                            print(f"  Received time:  {last_msg['received_time']}")
                            print(f"  Delta:          {last_msg['delta_ms']:.5f} ms")
                        else:
                            print(f"  Received time:  {last_msg['received_time']}")
                            print(f"  (No header in message)")

                    # Average delta (only for topics with headers)
                    if len(stats['deltas']) > 0:
                        avg_delta_ns = sum(stats['deltas']) / len(stats['deltas'])
                        avg_delta_ms = avg_delta_ns / 1e6
                        print(f"\nAverage delta (received - header): {avg_delta_ms:.5f} ms")

                        # Additional statistics
                        if len(stats['deltas']) > 1:
                            min_delta_ms = min(stats['deltas']) / 1e6
                            max_delta_ms = max(stats['deltas']) / 1e6
                            print(f"Delta range: {min_delta_ms:.5f} ms to {max_delta_ms:.5f} ms")
        else:
            # Compact mode - one line per topic, fixed-width columns, no header row
            for topic, stats in topic_stats.items():
                if stats['count'] > 0:
                    first_msg = stats['first_msg']

                    received_str = first_msg['received_time']
                    header_str = first_msg['header_stamp'] if first_msg['has_header'] else "N/A"

                    # Columns: Topic (30) | Received (30) | Header (30)
                    topic_short = topic[:29] if len(topic) > 29 else topic
                    print(f"{topic_short.ljust(30)}{received_str.ljust(30)} {header_str.ljust(30)}")

        # Summary across all topics (only in verbose mode)
        if args.verbose and len(topic_stats) > 1:
            print(f"\n{'='*60}")
            print("SUMMARY ACROSS ALL TOPICS")
            print(f"{'='*60}")
            
            total_messages = sum(stats['count'] for stats in topic_stats.values())
            all_deltas = []
            for stats in topic_stats.values():
                all_deltas.extend(stats['deltas'])
            
            if all_deltas:
                avg_delta_ms = sum(all_deltas) / len(all_deltas) / 1e6
                min_delta_ms = min(all_deltas) / 1e6
                max_delta_ms = max(all_deltas) / 1e6
                
                print(f"Total messages analyzed: {total_messages}")
                print(f"Topics analyzed: {len(topic_stats)}")
                print(f"Overall average delta: {avg_delta_ms:.5f} ms")
                print(f"Overall delta range: {min_delta_ms:.5f} ms to {max_delta_ms:.5f} ms")

        return 0

    def _nanoseconds_to_datetime(self, nanoseconds):
        """Convert nanoseconds since epoch to datetime string in Europe/Berlin."""
        seconds = nanoseconds / 1e9
        dt = datetime.fromtimestamp(seconds, tz=timezone.utc).astimezone(ZoneInfo('Europe/Berlin'))
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")

