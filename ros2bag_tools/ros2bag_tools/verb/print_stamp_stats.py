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

        # Filter topics if specified
        if args.topics:
            storage_filter = StorageFilter(topics=args.topics)
        else:
            storage_filter = StorageFilter()  # No filter, process all topics

        print("Analyzing timestamp statistics...")
        print("=" * 60)

        # Process all messages
        for topic, msg, t in BagView(reader, storage_filter):
            # Only process messages with headers
            if not hasattr(msg, 'header'):
                continue

            # Get timestamps
            received_time_ns = t
            header_stamp_ns = Time.from_msg(msg.header.stamp).nanoseconds
            delta_ns = received_time_ns - header_stamp_ns

            # Store message info
            msg_info = {
                'topic': topic,
                'received_time_ns': received_time_ns,
                'header_stamp_ns': header_stamp_ns,
                'delta_ns': delta_ns,
                'received_time': self._nanoseconds_to_datetime(received_time_ns),
                'header_stamp': self._nanoseconds_to_datetime(header_stamp_ns),
                'delta_ms': delta_ns / 1e6  # Convert to milliseconds
            }

            # Update topic statistics
            stats = topic_stats[topic]
            stats['messages'].append(msg_info)
            stats['deltas'].append(delta_ns)
            stats['count'] += 1

            # Track first and last messages
            if stats['first_msg'] is None:
                stats['first_msg'] = msg_info
            stats['last_msg'] = msg_info

        if not topic_stats:
            print("No messages with headers found in the bag.")
            return 0

        # Print statistics for each topic
        for topic, stats in topic_stats.items():
            print(f"\nTopic: {topic}")
            print("-" * 40)
            print(f"Total messages: {stats['count']}")

            if stats['count'] > 0:
                # First message info
                first_msg = stats['first_msg']
                print(f"\nFirst message:")
                print(f"  Header stamp:   {first_msg['header_stamp']}")
                print(f"  Received time:  {first_msg['received_time']}")
                print(f"  Delta:          {first_msg['delta_ms']:.3f} ms")

                # Last message info (only if different from first)
                if stats['count'] > 1:
                    last_msg = stats['last_msg']
                    print(f"\nLast message:")
                    print(f"  Header stamp:   {last_msg['header_stamp']}")
                    print(f"  Received time:  {last_msg['received_time']}")
                    print(f"  Delta:          {last_msg['delta_ms']:.3f} ms")

                # Average delta
                avg_delta_ns = sum(stats['deltas']) / len(stats['deltas'])
                avg_delta_ms = avg_delta_ns / 1e6
                print(f"\nAverage delta (received - header): {avg_delta_ms:.3f} ms")

                # Additional statistics
                if len(stats['deltas']) > 1:
                    min_delta_ms = min(stats['deltas']) / 1e6
                    max_delta_ms = max(stats['deltas']) / 1e6
                    print(f"Delta range: {min_delta_ms:.3f} ms to {max_delta_ms:.3f} ms")

        # Summary across all topics
        if len(topic_stats) > 1:
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
                print(f"Overall average delta: {avg_delta_ms:.3f} ms")
                print(f"Overall delta range: {min_delta_ms:.3f} ms to {max_delta_ms:.3f} ms")

        return 0

    def _nanoseconds_to_datetime(self, nanoseconds):
        """Convert nanoseconds since epoch to datetime string."""
        seconds = nanoseconds / 1e9
        dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f UTC")

